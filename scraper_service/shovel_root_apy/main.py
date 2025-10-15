from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert, flush_buffer_immediately
from shared.shovel_base_class import ShovelBaseClass
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
import os
import asyncio
from typing import List, Dict
from bittensor import AsyncSubtensor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

VALIDATOR_HOTKEY = os.environ.get('TAOCOM_VALIDATOR_HOTKEY')
if not VALIDATOR_HOTKEY:
    raise ValueError("TAOCOM_VALIDATOR_HOTKEY environment variable must be set")

BLOCK_SECONDS = 12
INTERVAL_SECONDS = {
    "30d": 60 * 60 * 24 * 30,
    "year": 60 * 60 * 24 * 365,
}
REQUIRED_BLOCKS_RATIO = 0.9
MIN_STAKE_THRESHOLD = 4000

BLOCKS_PER_24H = 24 * 60 * 60 // BLOCK_SECONDS


class RootAPYShovel(ShovelBaseClass):
    table_name = "shovel_root_apy"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 6650338

    def process_block(self, n):
        if n % BLOCKS_PER_24H != 0:
            return
        do_process_block(n, self.table_name)


def calculate_apy(yield_value, compound_periods):
    """Calculate APY from yield and compounding periods"""
    return ((1 + yield_value) ** compound_periods - 1) * 100


async def calculate_root_apy_async(node_url: str, hotkey: str, block: int, batch_size: int = 10000) -> tuple:
    """
    Calculate root APY using the same logic as apy-calculator/root_apy.py
    Uses bittensor AsyncSubtensor to properly query the chain

    Returns:
        tuple: (apy, total_root_divs, period_yield, events_processed, events_skipped)
    """
    interval = "30d"
    interval_seconds = INTERVAL_SECONDS[interval]
    actual_interval_blocks = int(interval_seconds / BLOCK_SECONDS)
    actual_interval_seconds = actual_interval_blocks * BLOCK_SECONDS
    start_block = block - actual_interval_blocks

    logging.info(f"Calculating root APY for {hotkey} from block {start_block} to {block}")

    async with AsyncSubtensor(node_url) as subtensor:
        subnets = await subtensor.get_all_subnets_info(block=block)
        events: List[Dict] = []
        for subnet in subnets:
            netuid = subnet.netuid
            tempo = subnet.tempo
            period = tempo + 1
            last_epoch_block = block - subnet.blocks_since_epoch
            epoch = last_epoch_block

            while epoch >= start_block:
                events.append({"block": epoch, "netuid": netuid, "tempo": tempo})
                epoch -= period

        events.sort(key=lambda x: (x["block"], x["netuid"]))

        logging.info(f"Processing {len(events)} epoch events across all subnets")

        async def query_divs(block: int, params: List) -> int:
            result = await subtensor.query_subtensor("TaoDividendsPerSubnet", block=block, params=params)
            return result.value if result else 0

        root_div_tasks = [
            lambda event=event: query_divs(event["block"], [event["netuid"], hotkey])
            for event in events
        ]

        logging.info(f"Fetching dividends in batches of {batch_size}...")
        root_divs: List[int] = []
        for i in range(0, len(root_div_tasks), batch_size):
            batch = root_div_tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*[task() for task in batch], return_exceptions=True)
            batch_results = [result if not isinstance(result, Exception) else -1 for result in batch_results]
            root_divs.extend(batch_results)
            if i % 1000 == 0:
                logging.info(f"Fetched dividends: {i}/{len(events)} ({100*i/len(events):.1f}%)")

        async def query_stake(block: int, params: List) -> int:
            result = await subtensor.query_subtensor("TotalHotkeyAlpha", block=block, params=params)
            return result.value if result else 0

        stake_tasks = [
            lambda event=event: query_stake(event["block"], [hotkey, 0])
            for event in events
        ]

        logging.info(f"Fetching stakes in batches of {batch_size}...")
        stakes: List[int] = []
        for i in range(0, len(stake_tasks), batch_size):
            batch = stake_tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*[task() for task in batch], return_exceptions=True)
            batch_results = [result if not isinstance(result, Exception) else -1 for result in batch_results]
            stakes.extend(batch_results)
            if i % 1000 == 0:
                logging.info(f"Fetched stakes: {i}/{len(events)} ({100*i/len(events):.1f}%)")

        logging.info(f"Finished fetching all stakes. Total events: {len(events)}, divs: {len(root_divs)}, stakes: {len(stakes)}")

    logging.info("Starting yield calculation...")
    yield_product = 1.0
    total_root_divs = 0
    skipped = 0
    processed = 0

    for event_index in range(len(events)):
        root_div = root_divs[event_index]
        stake = stakes[event_index]

        if root_div == 0:
            continue

        if root_div == -1 or stake == -1 or stake == 0:
            skipped += 1
            continue

        if stake < MIN_STAKE_THRESHOLD:
            skipped += 1
            continue

        epoch_yield = root_div / stake
        total_root_divs += root_div
        yield_product *= (1 + epoch_yield)
        processed += 1

    logging.info(f"Yield calculation complete. Processed: {processed}, Skipped: {skipped}")

    if skipped > 0:
        logging.info(f"Skipped {skipped} events due to query failures or applied filters.")
        if len(events) - skipped < REQUIRED_BLOCKS_RATIO * len(events):
            logging.warning(f"Coverage is less than: {REQUIRED_BLOCKS_RATIO * 100:.6f}% - may lead to inaccurate results.")

    period_yield = yield_product - 1
    logging.info(f"Total {interval} yield: {period_yield * 100:.6f}%")
    logging.info(f"Total {interval} dividends: {total_root_divs / 1e9:.6f} TAO")

    compounding_periods = INTERVAL_SECONDS["year"] / actual_interval_seconds
    apy = calculate_apy(period_yield, compounding_periods)
    logging.info(f"APY: {apy:.6f}%")

    return apy, total_root_divs, period_yield, processed, skipped


def do_process_block(n, table_name):
    """Process block and calculate root APY for the validator hotkey"""
    try:
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    hotkey String CODEC(ZSTD),
                    apy Float32 CODEC(ZSTD),
                    total_dividends UInt64 CODEC(Delta, ZSTD),
                    period_yield Float32 CODEC(ZSTD),
                    events_processed UInt32 CODEC(ZSTD),
                    events_skipped UInt32 CODEC(ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (hotkey, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            logging.info(f"\n{'='*60}")
            logging.info(f"Block {n}: Calculating Root APY for {VALIDATOR_HOTKEY}")
            logging.info(f"{'='*60}")

            node_url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
            if not node_url:
                raise ShovelProcessingError("SUBSTRATE_ARCHIVE_NODE_URL environment variable not set")

            logging.info("Starting APY calculation with AsyncSubtensor...")
            apy, total_divs, period_yield, processed, skipped = asyncio.run(
                calculate_root_apy_async(node_url, VALIDATOR_HOTKEY, n)
            )

            logging.info(f"APY calculation finished successfully!")
            logging.info(f"Block {n}: Root APY = {apy:.6f}%")
            logging.info(f"Total dividends: {total_divs / 1e9:.6f} TAO, Period yield: {period_yield * 100:.6f}%")
            logging.info(f"Events processed: {processed}, Events skipped: {skipped}")
            logging.info(f"{'='*60}\n")

            logging.info(f"Inserting APY data into ClickHouse table '{table_name}'...")
            buffer_insert(table_name, [
                n,
                block_timestamp,
                f"'{VALIDATOR_HOTKEY}'",
                apy,
                total_divs,
                period_yield,
                processed,
                skipped
            ])

            logging.info(f"Flushing buffer immediately to persist APY data...")
            flush_buffer_immediately()
            logging.info(f"Successfully flushed APY data to ClickHouse for block {n}")

        except Exception as e:
            raise ShovelProcessingError(f"Failed to calculate/insert APY data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def main():
    RootAPYShovel(name="root_apy").start()


if __name__ == "__main__":
    main()
