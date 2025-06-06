import logging

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from substrate import get_substrate_client
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

BLOCKS_PER_DAY = 7200
FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM = 5004000

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map"

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
                    stake UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, netuid, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            stake_data = fetch_all_stakes_at_block(block_hash)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch stakes from substrate: {str(e)}")

        if not stake_data:
            raise ShovelProcessingError(f"No stake data returned for block {n}")

        try:
            for stake_entry in stake_data:
                buffer_insert(
                    table_name,
                    [n, block_timestamp, f"'{stake_entry['coldkey']}'", f"'{stake_entry['hotkey']}'", stake_entry['netuid'], stake_entry['stake']]
                )
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def fetch_all_stakes_at_block(block_hash):
    """Fetch all stakes at a given block hash using the new StakeInfoRuntimeApi approach."""
    try:
        substrate = get_substrate_client()

        raw_accounts = substrate.query_map(
            module='System',
            storage_function='Account',
            block_hash=block_hash,
            page_size=1000
        )

        if not raw_accounts:
            raise ShovelProcessingError("No account data returned from substrate")

        all_coldkeys = []
        for account in raw_accounts:
            try:
                address_key = account[0]
                coldkey_address = convert_address_to_ss58(address_key, "coldkey")
                all_coldkeys.append(coldkey_address)

            except Exception as e:
                logging.warning(f"Error processing account for coldkey extraction: {e}")
                continue

        all_stakes = []

        for i, coldkey in enumerate(all_coldkeys):
            if i % 1000 == 0:
                logging.debug(f"Processing coldkey {i+1}/{len(all_coldkeys)}")

            try:
                stake_info = substrate.runtime_call(
                    api="StakeInfoRuntimeApi",
                    method="get_stake_info_for_coldkey",
                    params=[coldkey],
                    block_hash=block_hash
                )

                stake_data = stake_info.value if hasattr(stake_info, 'value') else stake_info

                if stake_data and isinstance(stake_data, list):
                    for stake_entry in stake_data:
                        try:
                            hotkey_raw = stake_entry.get('hotkey', '')
                            netuid = stake_entry.get('netuid', 0)
                            stake_amount = stake_entry.get('stake', 0)

                            if hotkey_raw and stake_amount > 0:
                                hotkey_address = convert_address_to_ss58(hotkey_raw, "hotkey")

                                all_stakes.append({
                                    'coldkey': coldkey,
                                    'hotkey': hotkey_address,
                                    'netuid': netuid,
                                    'stake': stake_amount
                                })
                        except Exception as e:
                            logging.warning(f"Error processing stake entry for coldkey {coldkey}: {e}")
                            continue

            except Exception as e:
                logging.warning(f"Error getting stake info for coldkey {coldkey}: {e}")
                continue
        return all_stakes

    except Exception as e:
        raise ShovelProcessingError(f"Error fetching stakes: {str(e)}")


def main():
    StakeDailyMapShovel(name="stake_daily_map").start()


if __name__ == "__main__":
    main()
