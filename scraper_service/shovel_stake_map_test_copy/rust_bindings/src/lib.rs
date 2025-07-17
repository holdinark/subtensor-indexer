use deadpool::unmanaged;
use futures::future::join_all;
use parity_scale_codec::Decode;
use pyo3::prelude::*;
use std::str::FromStr;
use std::sync::Arc;
use subxt::{
    backend::{legacy::LegacyBackend, rpc::RpcClient},
    utils::{AccountId32, H256},
    OnlineClient, PolkadotConfig,
};

#[subxt::subxt(runtime_metadata_path = "./metadata.scale")]
pub mod subtensor {}

async fn get_api() -> OnlineClient<PolkadotConfig> {
    let url = std::env::var("SUBSTRATE_ARCHIVE_NODE_URL").unwrap();
    let client = RpcClient::from_insecure_url(url.clone())
        .await
        .expect("Failed to connect to node");
    let backend = LegacyBackend::<PolkadotConfig>::builder()
        .storage_page_size(1000)
        .build(client);
    OnlineClient::from_backend(Arc::new(backend)).await.unwrap()
}

async fn query_map_pending_emission_inner(block_hash: String) -> PyResult<Vec<(u16, u64)>> {
    let api = get_api().await;
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);
    let query = subtensor::storage()
        .subtensor_module()
        .pending_emission_iter();
    let mut iter = api.storage().at(block_hash).iter(query).await.unwrap();
    let mut kvs = Vec::new();
    while let Some(Ok(kv)) = iter.next().await {
        let mut last_two_bytes = &kv.key_bytes[kv.key_bytes.len() - 2..];
        let subnet_id = u16::decode(&mut last_two_bytes).unwrap();
        kvs.push((subnet_id, kv.value));
    }
    Ok(kvs)
}

async fn query_subnet_hotkeys_inner(
    block_hash: String,
    subnet_id: u16,
) -> PyResult<Vec<(u16, String)>> {
    let api = get_api().await;
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);
    let query = subtensor::storage()
        .subtensor_module()
        .keys_iter1(subnet_id);
    let mut iter = api.storage().at(block_hash).iter(query).await.unwrap();
    let mut kvs = Vec::new();
    while let Some(Ok(kv)) = iter.next().await {
        let mut last_two_bytes = &kv.key_bytes[kv.key_bytes.len() - 2..];
        let neuron_id = u16::decode(&mut last_two_bytes).unwrap();
        kvs.push((neuron_id, kv.value.to_string()));
    }
    Ok(kvs)
}

async fn query_hotkey_stakes_inner(
    api: &OnlineClient<PolkadotConfig>,
    block_hash: &H256,
    hotkey: &String,
) -> Vec<(String, u64)> {
    let hotkey = AccountId32::from_str(hotkey.as_str()).expect("Invalid hotkey");
    let query = subtensor::storage().subtensor_module().stake_iter1(hotkey);
    let mut iter = api
        .storage()
        .at(block_hash.clone())
        .iter(query)
        .await
        .unwrap();
    let mut kvs = Vec::new();
    while let Some(Ok(kv)) = iter.next().await {
        let last_32_bytes: [u8; 32] = kv.key_bytes[kv.key_bytes.len() - 32..].try_into().unwrap();
        let coldkey = AccountId32::from(last_32_bytes).to_string();
        kvs.push((coldkey, kv.value));
    }
    kvs
}

async fn query_hotkeys_stakes_inner(
    block_hash: String,
    hotkeys: Vec<String>,
) -> PyResult<Vec<(String, Vec<(String, u64)>)>> {
    let block_hash = hex::decode(block_hash.trim_start_matches("0x")).expect("Decoding failed");
    let block_hash = H256::from_slice(&block_hash);

    let concurrency = 32;
    let api_futures = (0..concurrency).map(|_| get_api()).collect::<Vec<_>>();
    let api_pool = join_all(api_futures).await.into_iter().collect::<Vec<_>>();
    let api_pool = unmanaged::Pool::from(api_pool);

    let futures = hotkeys.iter().map(|hotkey| {
        let hotkey = hotkey.clone();
        let block_hash = block_hash.clone();
        let api_pool = api_pool.clone();
        async move {
            let api = api_pool.get().await.unwrap();
            let stakes = query_hotkey_stakes_inner(&api, &block_hash, &hotkey).await;
            (hotkey, stakes)
        }
    });
    let r = join_all(futures).await;
    Ok(r)
}

#[pyfunction]
fn query_map_pending_emission(block_hash: String) -> PyResult<Vec<(u16, u64)>> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_map_pending_emission_inner(block_hash))
}

#[pyfunction]
fn query_subnet_hotkeys(block_hash: String, subnet_id: u16) -> PyResult<Vec<(u16, String)>> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_subnet_hotkeys_inner(block_hash, subnet_id))
}

#[pyfunction]
fn query_hotkeys_stakes(
    block_hash: String,
    hotkeys: Vec<String>,
) -> PyResult<Vec<(String, Vec<(String, u64)>)>> {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(query_hotkeys_stakes_inner(block_hash, hotkeys))
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(query_map_pending_emission, m)?)?;
    m.add_function(wrap_pyfunction!(query_subnet_hotkeys, m)?)?;
    m.add_function(wrap_pyfunction!(query_hotkeys_stakes, m)?)?;
    Ok(())
}
