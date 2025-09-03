import logging

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

BLOCKS_PER_10_MIN = (60/12) * 10

class BalanceDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_balance_daily_map"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 6357600

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if n % BLOCKS_PER_10_MIN != 0:
        return
    try:
        # Create table if it doesn't exist
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    address String CODEC(ZSTD),
                    free_balance UInt64 CODEC(Delta, ZSTD),
                    reserved_balance UInt64 CODEC(Delta, ZSTD),
                    frozen_balance UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (address, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            results = fetch_all_free_balances_at_block(block_hash)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch balances from substrate: {str(e)}")

        if not results:
            raise ShovelProcessingError(f"No balance data returned for block {n}")

        logging.info(f"Processing block {n}. Found {len(results)} balance entries")

        try:
            for address, balance in results.items():
                buffer_insert(
                    table_name,
                    [n, block_timestamp, f"'{address}'", balance["free"], balance["reserved"], balance["frozen"]]
                )
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def fetch_all_free_balances_at_block(block_hash):
    """Fetch all balances at a given block hash."""
    try:
        substrate = get_substrate_client()
        raw_balances = substrate.query_map(
            module='System',
            storage_function='Account',
            block_hash=block_hash,
            page_size=1000
        )

        if not raw_balances:
            raise ShovelProcessingError("No balance data returned from substrate")

        balances = {}
        for address in raw_balances:
            try:
                address_id = address[0].value
                address_info = address[1]
                balances[address_id] = {
                    'free': address_info['data']['free'],
                    'reserved': address_info['data']['reserved'],
                    'frozen': address_info['data']['frozen'] if 'frozen' in address_info['data'] else int(address_info['data']['misc_frozen'].value) + int(address_info['data']['fee_frozen'].value),
                }
            except (KeyError, ValueError) as e:
                logging.warning(f"Skipping malformed account data for {address}: {str(e)}")
                continue

        return balances

    except Exception as e:
        raise ShovelProcessingError(f"Error fetching balances: {str(e)}")


def main():
    BalanceDailyMapShovel(name="balance_daily_map_temp").start()


if __name__ == "__main__":
    main()
