from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
import os
from scalecodec.utils.ss58 import ss58_encode

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

last_epoch_blocks = {}

DEFAULT_VALIDATOR_HOTKEY = os.environ.get('TAOCOM_VALIDATOR_HOTKEY')
if not DEFAULT_VALIDATOR_HOTKEY:
    raise ValueError("TAOCOM_VALIDATOR_HOTKEY environment variable must be set")

# Format: "netuid1:hotkey1,netuid2:hotkey2" e.g., "1:5Dyi8aj...,3:5Xyz9..."
SUBNET_VALIDATOR_HOTKEYS = {}
subnet_hotkeys_env = os.environ.get('TAOCOM_SUBNET_VALIDATOR_HOTKEYS', '')
if subnet_hotkeys_env:
    try:
        for pair in subnet_hotkeys_env.split(','):
            if pair.strip():
                netuid_str, hotkey = pair.strip().split(':')
                SUBNET_VALIDATOR_HOTKEYS[int(netuid_str)] = hotkey
        if SUBNET_VALIDATOR_HOTKEYS:
            logging.info(f"Loaded subnet-specific validators for subnets: {list(SUBNET_VALIDATOR_HOTKEYS.keys())}")
    except Exception as e:
        logging.error(f"Failed to parse TAOCOM_SUBNET_VALIDATOR_HOTKEYS: {e}")
        logging.error(f"Expected format: 'netuid1:hotkey1,netuid2:hotkey2'")
        raise ValueError(f"Invalid TAOCOM_SUBNET_VALIDATOR_HOTKEYS format: {subnet_hotkeys_env}")

SS58_FORMAT = 42

def decode_account_id(account_id_bytes):
    if hasattr(account_id_bytes, 'value'):
        account_id_bytes = account_id_bytes.value

    if isinstance(account_id_bytes, str):
        return account_id_bytes

    try:
        return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)
    except Exception as e:
        logging.error(f"Failed to decode account ID: {e}")
        raise


def get_validator_hotkey_for_subnet(subnet_id: int) -> str:
    return SUBNET_VALIDATOR_HOTKEYS.get(subnet_id, DEFAULT_VALIDATOR_HOTKEY)


class SubnetAPYShovel(ShovelBaseClass):
    table_name = "shovel_subnet_apy"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    global last_epoch_blocks

    try:
        # Create table if it doesn't exist
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    netuid UInt16 CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    subnet_stake UInt64 CODEC(Delta, ZSTD),
                    subnet_dividend UInt64 CODEC(Delta, ZSTD),
                    tao_weight Float32 CODEC(ZSTD),
                    root_stake UInt64 CODEC(Delta, ZSTD),
                    epoch_yield Float32 CODEC(ZSTD),
                    passed_filter Bool CODEC(ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (netuid, timestamp)
                """
                get_clickhouse_client().execute(query)

                mv_query = f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}_30d
                ENGINE = AggregatingMergeTree()
                PARTITION BY toYYYYMM(now())
                ORDER BY netuid
                POPULATE AS
                SELECT
                    netuid,
                    anyState(hotkey) as validator_hotkey_state,
                    avgState(epoch_yield) as avg_yield_state,
                    sumState(subnet_dividend) as total_divs_state,
                    avgState(subnet_stake) as avg_stake_state,
                    countState() as epoch_count_state,
                    countIfState(subnet_dividend > 0) as active_epoch_count_state,
                    maxState(timestamp) as last_update_state,
                    minState(timestamp) as first_update_state
                FROM {table_name}
                WHERE timestamp >= now() - INTERVAL30 DAY
                    AND passed_filter = true
                GROUP BY netuid
                """
            get_clickhouse_client().execute(mv_query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            pending_emissions_result = substrate.query_map(
                module="SubtensorModule",
                storage_function="PendingEmission",
                block_hash=block_hash
            )

            if pending_emissions_result is None:
                return

            for storage_key, pending_emission in pending_emissions_result:
                key_bytes = storage_key.encode() if hasattr(storage_key, 'encode') else storage_key
                subnet_id = int.from_bytes(key_bytes[-2:], 'big') if isinstance(key_bytes, bytes) else storage_key[0]

                if subnet_id not in last_epoch_blocks:
                    last_epoch_blocks[subnet_id] = {"block": 0, "emission": pending_emission.value if hasattr(pending_emission, 'value') else pending_emission}
                    continue

                prev_emission = last_epoch_blocks[subnet_id]["emission"]
                curr_emission = pending_emission.value if hasattr(pending_emission, 'value') else pending_emission

                # Epoch occurred when pending_emission resets to 0
                if curr_emission == 0 and prev_emission > 0:
                    process_subnet_epoch(n, block_timestamp, block_hash, subnet_id, table_name, substrate)

                last_epoch_blocks[subnet_id] = {"block": n, "emission": curr_emission}

        except ShovelProcessingError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process epoch data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def process_subnet_epoch(block_number, block_timestamp, block_hash, subnet_id, table_name, substrate):
    """Process APY data for TAO.com's validator on this subnet at epoch boundary"""
    try:
        validator_hotkey = get_validator_hotkey_for_subnet(subnet_id)

        try:
            keys_result = substrate.query_map(
                module="SubtensorModule",
                storage_function="Keys",
                params=[subnet_id],
                block_hash=block_hash
            )

            hotkey_exists = False
            for _, account_id in keys_result:
                decoded_hotkey = decode_account_id(account_id.value if hasattr(account_id, 'value') else account_id)
                if decoded_hotkey == validator_hotkey:
                    hotkey_exists = True
                    break

            if not hotkey_exists:
                logging.debug(f"TAO.com validator {validator_hotkey} not found on subnet {subnet_id}")
                return

        except Exception as e:
            logging.warning(f"Failed to check validator existence on subnet {subnet_id}: {str(e)}")
            return

        tao_weight_result = substrate.query(
            module="SubtensorModule",
            storage_function="TaoWeight",
            block_hash=block_hash
        )
        tao_weight = (tao_weight_result.value if hasattr(tao_weight_result, 'value') else tao_weight_result) / 65535.0 if tao_weight_result else 0.5

        if tao_weight_result is None:
            logging.error(f"Failed to get TaoWeight for block {block_hash}")
            raise ShovelProcessingError("Could not fetch TaoWeight")

        try:
            subnet_stake_result = substrate.query(
                module="SubtensorModule",
                storage_function="TotalHotkeyAlpha",
                params=[validator_hotkey, subnet_id],
                block_hash=block_hash
            )
            subnet_stake = subnet_stake_result.value if hasattr(subnet_stake_result, 'value') else subnet_stake_result or 0

            root_stake_result = substrate.query(
                module="SubtensorModule",
                storage_function="TotalHotkeyAlpha",
                params=[validator_hotkey, 0],
                block_hash=block_hash
            )
            root_stake = root_stake_result.value if hasattr(root_stake_result, 'value') else root_stake_result or 0

            dividend_result = substrate.query(
                module="SubtensorModule",
                storage_function="AlphaDividendsPerSubnet",
                params=[subnet_id, validator_hotkey],
                block_hash=block_hash
            )
            subnet_dividend = dividend_result.value if hasattr(dividend_result, 'value') else dividend_result or 0

            epoch_yield = float(subnet_dividend) / float(subnet_stake) if subnet_stake > 0 else 0.0

            #
            combined_stake = root_stake * tao_weight + subnet_stake
            passed_filter = subnet_stake >= 10 and combined_stake >= 4000

            buffer_insert(table_name, [
                block_number,
                block_timestamp,
                subnet_id,
                f"'{validator_hotkey}'",
                subnet_stake,
                subnet_dividend,
                tao_weight,
                root_stake,
                epoch_yield,
                passed_filter
            ])

            if subnet_dividend > 0:
                logging.info(f"Subnet {subnet_id}: TAO.com validator earned {subnet_dividend} with yield {epoch_yield:.6f}")

        except Exception as e:
            logging.warning(f"Failed to process TAO.com validator {validator_hotkey} on subnet {subnet_id}: {str(e)}")

    except Exception as e:
        raise ShovelProcessingError(f"Failed to process subnet epoch for subnet {subnet_id}: {str(e)}")


def main():
    SubnetAPYShovel(name="taocom_validator_apy").start()


if __name__ == "__main__":
    main()
