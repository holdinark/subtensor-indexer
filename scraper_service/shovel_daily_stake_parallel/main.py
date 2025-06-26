import logging
import time
import struct
from typing import Dict, List, Tuple, Optional
import hashlib

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

BLOCKS_PER_DAY = 7200
FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM = 5004000
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds
SS58_FORMAT = 42  # Substrate generic format
U64F64_SCALE = 2**64  # For U64F64 fixed point type

def _retry_on_disconnect(func, *args, **kwargs):
    """Call substrate function with automatic reconnect on Broken pipe / connection reset."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logging.warning(f"Substrate connection error ({e}); reconnecting (attempt {attempt+1}/{MAX_RETRIES})…")
            reconnect_substrate()
            time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
        except Exception:
            raise
    raise ShovelProcessingError(f"Failed after {MAX_RETRIES} reconnect attempts")

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map_2"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if n % BLOCKS_PER_DAY != 0:
        return
    try:
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    coldkey String CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    netuid UInt8 CODEC(Delta, ZSTD),
                    stake UInt64 CODEC(Delta, ZSTD),
                    alpha UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, netuid, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = _retry_on_disconnect(get_block_metadata, n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            rows_inserted = fetch_all_stakes_at_block_optimized_v2(block_hash, n, block_timestamp, table_name)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch stakes from substrate: {str(e)}")

        if rows_inserted == 0:
            raise ShovelProcessingError(f"No stake data returned for block {n}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def decode_u64(data: bytes) -> int:
    """Decode a u64 from SCALE encoded bytes."""
    if len(data) < 8:
        return 0
    return struct.unpack('<Q', data[:8])[0]


def decode_u64f64(data: bytes) -> float:
    """Decode a U64F64 fixed point number from SCALE encoded bytes."""
    if len(data) < 16:
        return 0.0
    # U64F64 is stored as u128 where upper 64 bits are integer part
    # Substrate stores it as little-endian
    fractional_part = struct.unpack('<Q', data[:8])[0]
    integer_part = struct.unpack('<Q', data[8:16])[0]
    return float(integer_part) + float(fractional_part) / U64F64_SCALE


def decode_account_id(data: bytes) -> str:
    """Decode an AccountId to SS58 address."""
    if len(data) >= 32:
        return convert_address_to_ss58(data[:32], 'account')
    return data.hex()


def extract_storage_key_parts(storage_key: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """Extract hotkey, coldkey, and netuid from an Alpha storage key."""
    try:
        # Storage keys for Alpha are:
        # module_hash + method_hash + blake2_concat(hotkey) + blake2_concat(coldkey) + identity(netuid)
        key_data = bytes.fromhex(storage_key[2:])  # Remove '0x'

        # Skip module hash (16 bytes) + storage hash (16 bytes)
        offset = 32

        # For Blake2_128Concat, we have 16 bytes hash + 32 bytes data
        # Extract hotkey (skip 16 byte hash, then 32 bytes data)
        offset += 16  # Skip blake2 hash
        hotkey_raw = key_data[offset:offset+32]
        hotkey = decode_account_id(hotkey_raw)
        offset += 32

        # Extract coldkey (skip 16 byte hash, then 32 bytes data)
        offset += 16  # Skip blake2 hash
        coldkey_raw = key_data[offset:offset+32]
        coldkey = decode_account_id(coldkey_raw)
        offset += 32

        # Extract netuid (u16, identity encoding - no hash)
        if offset + 2 <= len(key_data):
            netuid = struct.unpack('<H', key_data[offset:offset+2])[0]
        else:
            netuid = 0

        return hotkey, coldkey, netuid
    except Exception as e:
        logging.error(f"Error extracting key parts from {storage_key[:20]}...: {e}")
        return None, None, None


def fetch_all_stakes_at_block_optimized(block_hash, block_number, block_timestamp, table_name):
    """
    Fetch all stakes using the optimized batch query method.
    This replaces the inefficient individual query approach.
    """
    start_time = time.time()
    substrate = get_substrate_client()
    rows_inserted = 0

    try:
        # Step 1: Get all Alpha entries using pagination to avoid response size limits
        logging.info(f"Block {block_number}: Fetching all Alpha storage keys...")
        alpha_prefix = substrate.create_storage_key(
            pallet="SubtensorModule",
            storage_function="Alpha"
        ).to_hex()[:66]  # Get just the prefix part

        # Fetch keys in pages to avoid hitting the 15MB limit
        all_alpha_keys = []
        page_size = 1000  # Keys per page
        start_key = None

        while True:
            # Use state_getKeysPaged for pagination
            if start_key:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[alpha_prefix, page_size, start_key, block_hash]
                )
            else:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[alpha_prefix, page_size, alpha_prefix, block_hash]
                )

            keys = result.get('result', [])
            if not keys:
                break

            all_alpha_keys.extend(keys)
            logging.info(f"Block {block_number}: Fetched {len(all_alpha_keys)} Alpha keys so far...")

            # If we got fewer keys than requested, we've reached the end
            if len(keys) < page_size:
                break

            # Set the start key for the next page
            start_key = keys[-1]

        logging.info(f"Block {block_number}: Found {len(all_alpha_keys)} Alpha entries total")

        if not all_alpha_keys:
            logging.warning(f"Block {block_number}: No Alpha entries found")
            return 0

        # Step 2: Batch query all Alpha values in smaller chunks
        logging.info(f"Block {block_number}: Batch querying Alpha values...")

        # Use smaller chunks to avoid response size limits
        chunk_size = 1000  # Reduced from 10000
        all_alpha_values = []

        for i in range(0, len(all_alpha_keys), chunk_size):
            chunk_keys = all_alpha_keys[i:i + chunk_size]
            try:
                alpha_response = substrate.rpc_request(
                    method="state_queryStorageAt",
                    params=[chunk_keys, block_hash]
                ).get('result', [])

                if alpha_response and len(alpha_response) > 0:
                    chunk_values = alpha_response[0].get('changes', [])
                    all_alpha_values.extend(chunk_values)

                logging.info(f"Block {block_number}: Processed {min(i + chunk_size, len(all_alpha_keys))}/{len(all_alpha_keys)} Alpha keys")
            except Exception as e:
                logging.error(f"Block {block_number}: Error fetching chunk {i//chunk_size}: {e}")
                # Try with even smaller chunk size
                if chunk_size > 100:
                    logging.info(f"Block {block_number}: Retrying with smaller chunk size...")
                    for j in range(i, min(i + chunk_size, len(all_alpha_keys)), 100):
                        small_chunk = all_alpha_keys[j:j + 100]
                        try:
                            small_response = substrate.rpc_request(
                                method="state_queryStorageAt",
                                params=[small_chunk, block_hash]
                            ).get('result', [])

                            if small_response and len(small_response) > 0:
                                small_values = small_response[0].get('changes', [])
                                all_alpha_values.extend(small_values)
                        except Exception as e2:
                            logging.error(f"Block {block_number}: Failed even with small chunk: {e2}")

        # Step 3: Parse Alpha entries
        logging.info(f"Block {block_number}: Parsing Alpha entries...")
        stake_entries = []
        hotkey_netuid_pairs = set()

        for change in all_alpha_values:
            key = change[0]
            value = change[1]

            if value:
                hotkey, coldkey, netuid = extract_storage_key_parts(key)
                if hotkey and coldkey and netuid is not None:
                    # Decode the Alpha value
                    value_bytes = bytes.fromhex(value[2:])
                    alpha_share = decode_u64f64(value_bytes)

                    if alpha_share > 0:
                        stake_entries.append((hotkey, coldkey, netuid, alpha_share))
                        hotkey_netuid_pairs.add((hotkey, netuid))

        logging.info(f"Block {block_number}: Found {len(stake_entries)} non-zero Alpha entries")

        if not stake_entries:
            logging.warning(f"Block {block_number}: No non-zero stakes found")
            return 0

        # Step 4-7: Process in batches to avoid memory issues and improve efficiency
        logging.info(f"Block {block_number}: Processing stakes in batches...")

        # Convert to list and sort for consistent ordering
        stake_entries_sorted = sorted(stake_entries, key=lambda x: (x[0], x[1], x[2]))  # Sort by hotkey, coldkey, netuid

        # Process stakes in batches
        batch_size = 5000

        for batch_start in range(0, len(stake_entries_sorted), batch_size):
            batch_end = min(batch_start + batch_size, len(stake_entries_sorted))
            batch = stake_entries_sorted[batch_start:batch_end]

            logging.info(f"Block {block_number}: Processing batch {batch_start//batch_size + 1}/{(len(stake_entries_sorted) + batch_size - 1)//batch_size} "
                        f"(entries {batch_start}-{batch_end} of {len(stake_entries_sorted)})")

            # Collect unique (hotkey, netuid) pairs for this batch
            logging.info(f"Block {block_number}: Collecting unique (hotkey, netuid) pairs for batch...")
            batch_hotkey_netuid_pairs = set()
            for hotkey, coldkey, netuid, alpha_share in batch:
                batch_hotkey_netuid_pairs.add((hotkey, netuid))

            logging.info(f"Block {block_number}: Found {len(batch_hotkey_netuid_pairs)} unique pairs in batch")

            # Build storage keys for this batch only
            logging.info(f"Block {block_number}: Building storage keys...")
            batch_alpha_keys = []
            batch_shares_keys = []
            batch_pairs_list = list(batch_hotkey_netuid_pairs)

            key_build_start = time.time()
            for idx, (hotkey, netuid) in enumerate(batch_pairs_list):
                if idx % 100 == 0:
                    elapsed = time.time() - key_build_start
                    rate = idx / elapsed if elapsed > 0 else 0
                    logging.info(f"Block {block_number}: Built keys for {idx}/{len(batch_pairs_list)} pairs ({rate:.1f} pairs/sec)...")

                try:
                    batch_alpha_keys.append(fast_storage_key("SubtensorModule", "TotalHotkeyAlpha", [hotkey, netuid]))
                    batch_shares_keys.append(fast_storage_key("SubtensorModule", "TotalHotkeyShares", [hotkey, netuid]))
                except Exception as e:
                    logging.error(f"Block {block_number}: Error creating storage key for {hotkey}, {netuid}: {e}")

            key_build_time = time.time() - key_build_start
            logging.info(f"Block {block_number}: Built {len(batch_alpha_keys)} storage keys in {key_build_time:.2f}s")

            # Query TotalHotkeyAlpha for this batch
            logging.info(f"Block {block_number}: Querying TotalHotkeyAlpha...")
            batch_alpha_map = {}
            query_chunk_size = 500

            alpha_query_start = time.time()
            for i in range(0, len(batch_alpha_keys), query_chunk_size):
                chunk_keys = batch_alpha_keys[i:i + query_chunk_size]
                logging.info(f"Block {block_number}: Querying TotalHotkeyAlpha chunk {i//query_chunk_size + 1}/{(len(batch_alpha_keys) + query_chunk_size - 1)//query_chunk_size}...")
                try:
                    response = substrate.rpc_request(
                        method="state_queryStorageAt",
                        params=[chunk_keys, block_hash]
                    ).get('result', [])

                    if response and len(response) > 0:
                        values = response[0].get('changes', [])
                        for j, value_item in enumerate(values):
                            if value_item[1]:
                                pair_idx = i + j
                                if pair_idx < len(batch_pairs_list):
                                    hotkey, netuid = batch_pairs_list[pair_idx]
                                    value_bytes = bytes.fromhex(value_item[1][2:])
                                    batch_alpha_map[(hotkey, netuid)] = decode_u64(value_bytes)
                except Exception as e:
                    logging.error(f"Block {block_number}: Error fetching TotalHotkeyAlpha: {e}")

            alpha_query_time = time.time() - alpha_query_start
            logging.info(f"Block {block_number}: Queried TotalHotkeyAlpha in {alpha_query_time:.2f}s")

            # Query TotalHotkeyShares for this batch
            logging.info(f"Block {block_number}: Querying TotalHotkeyShares...")
            batch_shares_map = {}

            shares_query_start = time.time()
            for i in range(0, len(batch_shares_keys), query_chunk_size):
                chunk_keys = batch_shares_keys[i:i + query_chunk_size]
                logging.info(f"Block {block_number}: Querying TotalHotkeyShares chunk {i//query_chunk_size + 1}/{(len(batch_shares_keys) + query_chunk_size - 1)//query_chunk_size}...")
                try:
                    response = substrate.rpc_request(
                        method="state_queryStorageAt",
                        params=[chunk_keys, block_hash]
                    ).get('result', [])

                    if response and len(response) > 0:
                        values = response[0].get('changes', [])
                        for j, value_item in enumerate(values):
                            if value_item[1]:
                                pair_idx = i + j
                                if pair_idx < len(batch_pairs_list):
                                    hotkey, netuid = batch_pairs_list[pair_idx]
                                    value_bytes = bytes.fromhex(value_item[1][2:])
                                    batch_shares_map[(hotkey, netuid)] = decode_u64f64(value_bytes)
                except Exception as e:
                    logging.error(f"Block {block_number}: Error fetching TotalHotkeyShares: {e}")

            shares_query_time = time.time() - shares_query_start
            logging.info(f"Block {block_number}: Queried TotalHotkeyShares in {shares_query_time:.2f}s")

            # Calculate and insert stakes for this batch
            logging.info(f"Block {block_number}: Calculating and inserting stakes...")
            calc_start = time.time()
            batch_rows = 0
            for hotkey, coldkey, netuid, alpha_share in batch:
                total_alpha = batch_alpha_map.get((hotkey, netuid), 0)
                total_shares = batch_shares_map.get((hotkey, netuid), 0)

                if total_shares > 0:
                    stake_tao = int(alpha_share * total_alpha / total_shares)

                    buffer_insert(
                        table_name,
                        [
                            block_number,
                            block_timestamp,
                            f"'{coldkey}'",
                            f"'{hotkey}'",
                            netuid,
                            stake_tao,
                            int(alpha_share),
                        ],
                    )
                    rows_inserted += 1
                    batch_rows += 1

            logging.info(f"Block {block_number}: Batch complete. Inserted {batch_rows} rows. Total so far: {rows_inserted}")

        elapsed_time = time.time() - start_time
        logging.info(
            f"Block {block_number}: Completed in {elapsed_time:.2f} seconds. "
            f"Inserted {rows_inserted} rows. "
            f"Rate: {rows_inserted / elapsed_time:.1f} rows/sec"
        )

        return rows_inserted

    except Exception as e:
        logging.error(f"Error in optimized fetch for block {block_number}: {e}")
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')


def fetch_all_stakes_at_block_optimized_v2(block_hash, block_number, block_timestamp, table_name):
    """
    Alternative implementation that fetches all data upfront with prefix queries.
    """
    start_time = time.time()
    substrate = get_substrate_client()
    rows_inserted = 0

    try:
        # Step 1: Get all Alpha entries using pagination
        logging.info(f"Block {block_number}: Fetching all Alpha storage keys...")
        alpha_prefix = substrate.create_storage_key(
            pallet="SubtensorModule",
            storage_function="Alpha"
        ).to_hex()[:66]  # Get just the prefix part

        # Fetch keys in pages
        all_alpha_keys = []
        page_size = 1000
        start_key = None

        while True:
            if start_key:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[alpha_prefix, page_size, start_key, block_hash]
                )
            else:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[alpha_prefix, page_size, alpha_prefix, block_hash]
                )

            keys = result.get('result', [])
            if not keys:
                break

            all_alpha_keys.extend(keys)
            logging.info(f"Block {block_number}: Fetched {len(all_alpha_keys)} Alpha keys so far...")

            if len(keys) < page_size:
                break

            start_key = keys[-1]

        logging.info(f"Block {block_number}: Found {len(all_alpha_keys)} Alpha entries total")

        if not all_alpha_keys:
            logging.warning(f"Block {block_number}: No Alpha entries found")
            return 0

        # Step 2: Fetch ALL TotalHotkeyAlpha entries upfront
        logging.info(f"Block {block_number}: Fetching all TotalHotkeyAlpha entries...")
        total_alpha_prefix = substrate.create_storage_key(
            pallet="SubtensorModule",
            storage_function="TotalHotkeyAlpha"
        ).to_hex()[:66]

        total_alpha_map = {}
        start_key = None

        while True:
            if start_key:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[total_alpha_prefix, page_size, start_key, block_hash]
                )
            else:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[total_alpha_prefix, page_size, total_alpha_prefix, block_hash]
                )

            keys = result.get('result', [])
            if not keys:
                break

            # Batch fetch values
            for i in range(0, len(keys), 1000):
                chunk_keys = keys[i:i + 1000]
                response = substrate.rpc_request(
                    method="state_queryStorageAt",
                    params=[chunk_keys, block_hash]
                ).get('result', [])

                if response and len(response) > 0:
                    changes = response[0].get('changes', [])
                    for key, value in changes:
                        if value:
                            # Extract hotkey and netuid from the key
                            key_bytes = bytes.fromhex(key[2:])
                            # Storage key structure:
                            # - Module hash: 16 bytes (0-16)
                            # - Storage hash: 16 bytes (16-32)
                            # - Hotkey (Blake2_128Concat): 16 bytes hash (32-48) + 32 bytes data (48-80)
                            # - Netuid (Identity): 2 bytes (80-82)

                            if len(key_bytes) >= 82:
                                hotkey_raw = key_bytes[48:80]
                                hotkey = decode_account_id(hotkey_raw)
                                netuid = struct.unpack('<H', key_bytes[80:82])[0]

                                value_bytes = bytes.fromhex(value[2:])
                                total_alpha_map[(hotkey, netuid)] = decode_u64(value_bytes)
                            else:
                                logging.warning(f"Block {block_number}: Key too short: {len(key_bytes)} bytes")

            logging.info(f"Block {block_number}: Fetched {len(total_alpha_map)} TotalHotkeyAlpha entries...")

            if len(keys) < page_size:
                break
            start_key = keys[-1]

        # Step 3: Fetch ALL TotalHotkeyShares entries upfront
        logging.info(f"Block {block_number}: Fetching all TotalHotkeyShares entries...")
        total_shares_prefix = substrate.create_storage_key(
            pallet="SubtensorModule",
            storage_function="TotalHotkeyShares"
        ).to_hex()[:66]

        total_shares_map = {}
        start_key = None

        while True:
            if start_key:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[total_shares_prefix, page_size, start_key, block_hash]
                )
            else:
                result = substrate.rpc_request(
                    method="state_getKeysPaged",
                    params=[total_shares_prefix, page_size, total_shares_prefix, block_hash]
                )

            keys = result.get('result', [])
            if not keys:
                break

            # Batch fetch values
            for i in range(0, len(keys), 1000):
                chunk_keys = keys[i:i + 1000]
                response = substrate.rpc_request(
                    method="state_queryStorageAt",
                    params=[chunk_keys, block_hash]
                ).get('result', [])

                if response and len(response) > 0:
                    changes = response[0].get('changes', [])
                    for key, value in changes:
                        if value:
                            # Extract hotkey and netuid from the key
                            key_bytes = bytes.fromhex(key[2:])
                            # Storage key structure:
                            # - Module hash: 16 bytes (0-16)
                            # - Storage hash: 16 bytes (16-32)
                            # - Hotkey (Blake2_128Concat): 16 bytes hash (32-48) + 32 bytes data (48-80)
                            # - Netuid (Identity): 2 bytes (80-82)

                            if len(key_bytes) >= 82:
                                hotkey_raw = key_bytes[48:80]
                                hotkey = decode_account_id(hotkey_raw)
                                netuid = struct.unpack('<H', key_bytes[80:82])[0]

                                value_bytes = bytes.fromhex(value[2:])
                                total_shares_map[(hotkey, netuid)] = decode_u64f64(value_bytes)
                            else:
                                logging.warning(f"Block {block_number}: Key too short: {len(key_bytes)} bytes")

            logging.info(f"Block {block_number}: Fetched {len(total_shares_map)} TotalHotkeyShares entries...")

            if len(keys) < page_size:
                break
            start_key = keys[-1]

        # Step 4: Process Alpha entries and calculate stakes
        logging.info(f"Block {block_number}: Processing Alpha entries and calculating stakes...")

        # Batch query Alpha values
        for i in range(0, len(all_alpha_keys), 1000):
            chunk_keys = all_alpha_keys[i:i + 1000]
            response = substrate.rpc_request(
                method="state_queryStorageAt",
                params=[chunk_keys, block_hash]
            ).get('result', [])

            if response and len(response) > 0:
                changes = response[0].get('changes', [])
                for key, value in changes:
                    if value:
                        hotkey, coldkey, netuid = extract_storage_key_parts(key)
                        if hotkey and coldkey and netuid is not None:
                            value_bytes = bytes.fromhex(value[2:])
                            alpha_share = decode_u64f64(value_bytes)

                            if alpha_share > 0:
                                total_alpha = total_alpha_map.get((hotkey, netuid), 0)
                                total_shares = total_shares_map.get((hotkey, netuid), 0)

                                if total_shares > 0:
                                    stake_tao = int(alpha_share * total_alpha / total_shares)

                                    buffer_insert(
                                        table_name,
                                        [
                                            block_number,
                                            block_timestamp,
                                            f"'{coldkey}'",
                                            f"'{hotkey}'",
                                            netuid,
                                            stake_tao,
                                            int(alpha_share),
                                        ],
                                    )
                                    rows_inserted += 1

                                    if rows_inserted % 10000 == 0:
                                        logging.info(f"Block {block_number}: Inserted {rows_inserted} rows...")

            logging.info(f"Block {block_number}: Processed {min(i + 1000, len(all_alpha_keys))}/{len(all_alpha_keys)} Alpha entries...")

        elapsed_time = time.time() - start_time
        logging.info(
            f"Block {block_number}: Completed in {elapsed_time:.2f} seconds. "
            f"Inserted {rows_inserted} rows. "
            f"Rate: {rows_inserted / elapsed_time:.1f} rows/sec"
        )

        return rows_inserted

    except Exception as e:
        logging.error(f"Error in optimized fetch v2 for block {block_number}: {e}")
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')


def fast_storage_key(pallet: str, storage_function: str, params: list) -> str:
    """
    Manually construct storage key without using substrate.create_storage_key()
    which can be very slow.
    """
    # Hash the pallet name
    pallet_hash = hashlib.blake2b(pallet.encode(), digest_size=16).digest()

    # Hash the storage function name
    storage_hash = hashlib.blake2b(storage_function.encode(), digest_size=16).digest()

    # Start with pallet and storage hashes
    key = pallet_hash + storage_hash

    # For TotalHotkeyAlpha and TotalHotkeyShares, we have:
    # - First param (hotkey): Blake2_128Concat
    # - Second param (netuid): Identity

    if len(params) >= 1:
        # First parameter (hotkey) - Blake2_128Concat
        hotkey_bytes = bytes.fromhex(params[0][2:]) if params[0].startswith('0x') else ss58_decode(params[0])
        hotkey_hash = hashlib.blake2b(hotkey_bytes, digest_size=16).digest()
        key += hotkey_hash + hotkey_bytes

    if len(params) >= 2:
        # Second parameter (netuid) - Identity encoding (just the value)
        netuid = params[1]
        key += netuid.to_bytes(2, 'little')

    return '0x' + key.hex()


def ss58_decode(address: str) -> bytes:
    """Decode SS58 address to bytes."""
    try:
        from substrateinterface.utils.ss58 import ss58_decode as substrate_ss58_decode
        return bytes.fromhex(substrate_ss58_decode(address))
    except:
        # Fallback simple implementation
        import base58
        decoded = base58.b58decode(address)
        return decoded[1:-2]  # Remove prefix and checksum


def main():
    StakeDailyMapShovel(name="stake_daily_map_2").start()


if __name__ == "__main__":
    main()
