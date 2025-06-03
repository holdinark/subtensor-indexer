from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.block_metadata import get_block_metadata
import logging

BLOCKS_PER_10MIN = 10 * 60 / 12

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

class SubnetStatsShovel(ShovelBaseClass):
    table_name = "shovel_subnet_stats"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 4920351

    def process_block(self, n):
        if n % BLOCKS_PER_10MIN != 0:
            return
        do_process_block(self, n)


def do_process_block(self, n):
    try:
        substrate = get_substrate_client()

        try:
            if not table_exists(self.table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    netuid UInt8 CODEC(Delta, ZSTD),
                    tempo UInt32 CODEC(Delta, ZSTD),
                    emission UInt64 CODEC(Delta, ZSTD),
                    alpha_in UInt64 CODEC(Delta, ZSTD),
                    alpha_out UInt64 CODEC(Delta, ZSTD),
                    tao_in UInt64 CODEC(Delta, ZSTD),
                    alpha_out_emission UInt64 CODEC(Delta, ZSTD),
                    alpha_in_emission UInt64 CODEC(Delta, ZSTD),
                    tao_in_emission UInt64 CODEC(Delta, ZSTD),
                    pending_alpha_emission UInt64 CODEC(Delta, ZSTD),
                    pending_root_emission UInt64 CODEC(Delta, ZSTD),
                    subnet_volume UInt64 CODEC(Delta, ZSTD),
                    price UInt64 CODEC(Delta, ZSTD),
                    market_cap UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (block_number, netuid)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            block_timestamp, block_hash = get_block_metadata(n)
            if block_timestamp == 0 and n != 0:
                raise ShovelProcessingError(f"Invalid block timestamp (0) for block {n}")
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            result = substrate.runtime_call(
                api="SubnetInfoRuntimeApi",
                method="get_all_dynamic_info",
                params=[],
                block_hash=block_hash
            )
            subnet_info = result.value

            for subnet_data in subnet_info:
                netuid = subnet_data['netuid']

                alpha_in = subnet_data['alpha_in']
                alpha_out = subnet_data['alpha_out']
                tao_in = subnet_data['tao_in']

                price = 1 if netuid == 0 else (tao_in / alpha_in if alpha_in > 0 else 0)

                market_cap = (alpha_out + alpha_in) * price

                buffer_insert(
                    self.table_name,
                    [
                        n,
                        block_timestamp,
                        netuid,
                        subnet_data['tempo'],
                        subnet_data['emission'],
                        alpha_in,
                        alpha_out,
                        tao_in,
                        subnet_data['alpha_out_emission'],
                        subnet_data['alpha_in_emission'],
                        subnet_data['tao_in_emission'],
                        subnet_data['pending_alpha_emission'],
                        subnet_data['pending_root_emission'],
                        subnet_data['subnet_volume'],
                        int(price * 1e9),
                        int(market_cap)
                    ]
                )

        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def main():
    SubnetStatsShovel(name="subnet_stats").start()


if __name__ == "__main__":
    main()
