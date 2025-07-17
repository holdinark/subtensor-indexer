from collections import defaultdict
import logging
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.substrate import get_substrate_client
from shared.shovel_base_class import ShovelBaseClass
from shared.clickhouse.batch_insert import buffer_insert
from shared.block_metadata import get_block_metadata
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from datetime import datetime
import time
import rust_bindings
from tqdm import tqdm
from functools import lru_cache


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

last_stakes_proof = None
prev_pending_emissions = {}
stake_map = dict()

STAKES_PREFIX = "0x658faa385070e074c85bf6b568cf055522fbe0bd0cb77b6b6f365f641b0de381"


def check_root_read_proof(block_hash):
    """Check if the stake map has changed using storage proof."""
    global last_stakes_proof

    try:
        substrate = get_substrate_client()
        r = substrate.rpc_request(
            "state_getReadProof",
            params=[[STAKES_PREFIX], block_hash]
        )
        this_stakes_proof = set(r["result"]["proof"])
        stake_map_changed = last_stakes_proof is None or last_stakes_proof.isdisjoint(
            this_stakes_proof
        )
        last_stakes_proof = this_stakes_proof

        return stake_map_changed
    except Exception as e:
        raise ShovelProcessingError(f"Failed to check root read proof: {str(e)}")


class StakeDoubleMapShovelTestCopy(ShovelBaseClass):
    table_name = "shovel_stake_double_map_test_copy"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    try:
        # Create table if it doesn't exist
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    hotkey String CODEC(ZSTD),
                    coldkey String CODEC(ZSTD),
                    stake UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, timestamp)
                """
                get_clickhouse_client().execute(query)

            if not table_exists("agg_stake_events_test_copy"):
                query = """
                CREATE VIEW agg_stake_events_test_copy
                (
                    `block_number` UInt64,
                    `timestamp` DateTime,
                    `hotkey` String,
                    `coldkey` String,
                    `amount` Int64,
                    `operation` String
                )
                AS SELECT
                    l.block_number AS block_number,
                    l.timestamp AS timestamp,
                    l.tuple_0 AS hotkey,
                    r.coldkey AS coldkey,
                    l.tuple_1 AS amount,
                    'remove' AS operation
                FROM shovel_hotkey_owner_map AS r
                INNER JOIN shovel_events_SubtensorModule_StakeRemoved_v0 AS l ON (l.tuple_0 = r.hotkey) AND (l.timestamp = r.timestamp)
                UNION ALL
                SELECT
                    sa.block_number,
                    sa.timestamp,
                    sa.tuple_0 AS hotkey,
                    r.coldkey,
                    sa.tuple_1 AS amount,
                    'add' AS operation
                FROM shovel_hotkey_owner_map AS r
                INNER JOIN shovel_events_SubtensorModule_StakeAdded_v0 AS sa ON (sa.tuple_0 = r.hotkey) AND (sa.timestamp = r.timestamp);
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check tables: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        hotkeys_needing_update = set()

        try:
            # Get pending emission amount for every subnet
            result = rust_bindings.query_map_pending_emission(block_hash)
            if result is None:
                raise ShovelProcessingError(f"Failed to get pending emissions for block {n}")

            for subnet_id, pending_emission in result:
                if (subnet_id not in prev_pending_emissions) or pending_emission == 0 and prev_pending_emissions[subnet_id] != 0:
                    logging.info(f"Refreshing all hotkeys from subnet {subnet_id} due to update...")

                    subnet_hotkeys = rust_bindings.query_subnet_hotkeys(
                        block_hash, subnet_id
                    )
                    if subnet_hotkeys is None:
                        raise ShovelProcessingError(f"Failed to get subnet hotkeys for subnet {subnet_id}")

                    count = 0
                    for (neuron_id, hotkey) in subnet_hotkeys:
                        hotkeys_needing_update.add(hotkey)
                        count += 1
                    logging.info(f"Found {count} hotkeys for {subnet_id}")

                prev_pending_emissions[subnet_id] = pending_emission

        except ShovelProcessingError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process subnet data: {str(e)}")

        try:
            # Check if we're up to date with dependencies
            events_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'events';"
            events_synced_block = get_clickhouse_client().execute(
                events_synced_block_query)[0][0]
            hotkey_owner_map_synced_block_query = "SELECT block_number FROM shovel_checkpoints FINAL WHERE shovel_name = 'hotkey_owner_map';"
            hotkey_owner_map_synced_block = get_clickhouse_client().execute(
                hotkey_owner_map_synced_block_query)[0][0]

            while (events_synced_block < n or hotkey_owner_map_synced_block < n):
                logging.info("Waiting for events and hotkey_owner_map tables to sync...")
                time.sleep(60)
                events_synced_block = get_clickhouse_client().execute(
                    events_synced_block_query)[0][0]
                hotkey_owner_map_synced_block = get_clickhouse_client().execute(
                    hotkey_owner_map_synced_block_query)[0][0]

            # Get hotkeys with stake events this block
            dt_object = datetime.fromtimestamp(block_timestamp)
            formatted_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")
            distinct_hotkeys_query = f"""
                SELECT DISTINCT(hotkey) from agg_stake_events WHERE timestamp = '{formatted_date}'
            """
            distinct_hotkeys = get_clickhouse_client().execute(distinct_hotkeys_query)

            for r in distinct_hotkeys:
                hotkeys_needing_update.add(r[0])

        except Exception as e:
            raise DatabaseConnectionError(f"Failed to check dependency sync status: {str(e)}")

        try:
            # Get agg stake events for this block
            r = rust_bindings.query_hotkeys_stakes(
                block_hash, list(hotkeys_needing_update)
            )
            if r is None:
                raise ShovelProcessingError(f"Failed to get stakes for hotkeys in block {n}")

            for (hotkey, coldkey_stakes) in r:
                for (coldkey, stake) in coldkey_stakes:
                    stake_map[(hotkey, coldkey)] = stake

            try:
                for ((hotkey, coldkey), stake) in stake_map.items():
                    buffer_insert(
                        table_name,
                        [n, block_timestamp, f"'{hotkey}'", f"'{coldkey}'", stake]
                    )
            except Exception as e:
                raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

        except DatabaseConnectionError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process stake data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


@lru_cache
def create_storage_key_cached(pallet, storage, args):
    return get_substrate_client().create_storage_key(pallet, storage, list(args))


def main():
    StakeDoubleMapShovelTestCopy(name="stake_double_map_test_copy").start()


if __name__ == "__main__":
    main() 