from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
import os
import asyncio
from typing import List, Dict

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

VALIDATOR_HOTKEY = os.environ.get('TAOCOM_VALIDATOR_HOTKEY')
if not VALIDATOR_HOTKEY:
    raise ValueError("TAOCOM_VALIDATOR_HOTKEY environment variable must be set")

# Constants from apy-calculator
BLOCK_SECONDS = 12
INTERVAL_SECONDS = {
    "30d": 60 * 60 * 24 * 30,
    "year": 60 * 60 * 24 * 365,
}
REQUIRED_BLOCKS_RATIO = 0.9
MIN_STAKE_THRESHOLD = 4000

# Calculate APY every 24 hours (7200 blocks)
BLOCKS_PER_24H = 24 * 60 * 60 // BLOCK_SECONDS


class RootAPYShovel(ShovelBaseClass):
    table_name = "shovel_root_apy"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 6650338

    def process_block(self, n):
        # Calculate APY once every 24 hours
        if n % BLOCKS_PER_24H != 0:
            return
        do_process_block(n, self.table_name)


def calculate_apy(yield_value, compound_periods):
    """Calculate APY from yield and compounding periods"""
    return ((1 + yield_value) ** compound_periods - 1) * 100


async def calculate_root_apy_async(substrate, hotkey: str, block: int, batch_size: int = 100) -> tuple:
    """
    Calculate root APY using the same logic as apy-calculator/root_apy.py

    Returns:
        tuple: (apy, total_root_divs, period_yield, events_processed, events_skipped)
    """
    # Calculate the interval in blocks (30 days)
    interval = "30d"
    interval_seconds = INTERVAL_SECONDS[interval]
    actual_interval_blocks = int(interval_seconds / BLOCK_SECONDS)
    actual_interval_seconds = actual_interval_blocks * BLOCK_SECONDS
    start_block = block - actual_interval_blocks

    logging.info(f"Calculating root APY for {hotkey} from block {start_block} to {block}")

    # Fetch subnet info synchronously
    block_hash = substrate.get_block_hash(block)
    result = substrate.runtime_call(
        api="SubnetInfoRuntimeApi",
        method="get_all_dynamic_info",
        params=[],
        block_hash=block_hash
    )
    subnets = result.value

    # Build list of epoch events across all subnets
    events: List[Dict] = []
    for subnet_data in subnets:
        netuid = subnet_data['netuid']
        tempo = subnet_data['tempo']
        blocks_since_epoch = subnet_data['blocks_since_epoch']

        period = tempo + 1  # Epoch period in blocks
        last_epoch_block = block - blocks_since_epoch
        epoch = last_epoch_block

        while epoch >= start_block:
            events.append({"block": epoch, "netuid": netuid, "tempo": tempo})
            epoch -= period

    # Sort events for consistent processing
    events.sort(key=lambda x: (x["block"], x["netuid"]))

    logging.info(f"Processing {len(events)} epoch events across all subnets")

    # Fetch dividends
    root_divs: List[int] = []
    for event in events:
        try:
            event_block_hash = substrate.get_block_hash(event["block"])
            result = substrate.query(
                module="SubtensorModule",
                storage_function="TaoDividendsPerSubnet",
                params=[event["netuid"], hotkey],
                block_hash=event_block_hash
            )
            div_value = result.value if result and hasattr(result, 'value') else 0
            root_divs.append(div_value or 0)
        except Exception as e:
            logging.warning(f"Failed to fetch dividend for block {event['block']}, netuid {event['netuid']}: {e}")
            root_divs.append(-1)

    # Fetch stakes
    stakes: List[int] = []
    for event in events:
        try:
            event_block_hash = substrate.get_block_hash(event["block"])
            result = substrate.query(
                module="SubtensorModule",
                storage_function="TotalHotkeyAlpha",
                params=[hotkey, 0],  # 0 = root network
                block_hash=event_block_hash
            )
            stake_value = result.value if result and hasattr(result, 'value') else 0
            stakes.append(stake_value or 0)
        except Exception as e:
            logging.warning(f"Failed to fetch stake for block {event['block']}: {e}")
            stakes.append(-1)

    # Process results and compute compounded yield
    yield_product = 1.0
    total_root_divs = 0
    skipped = 0
    processed = 0

    for event_index in range(len(events)):
        root_div = root_divs[event_index]
        stake = stakes[event_index]

        # No dividends has no effect on the yield product.
        if root_div == 0:
            continue

        # Such cases mean that the query failed or stake is zero (zero division).
        if root_div == -1 or stake == -1 or stake == 0:
            skipped += 1
            continue

        # Filter validators with stake less than MIN_STAKE_THRESHOLD
        if stake < MIN_STAKE_THRESHOLD:
            skipped += 1
            continue

        epoch_yield = root_div / stake
        total_root_divs += root_div
        yield_product *= (1 + epoch_yield)
        processed += 1

    if skipped > 0:
        logging.info(f"Skipped {skipped} events due to query failures or applied filters.")
        if len(events) - skipped < REQUIRED_BLOCKS_RATIO * len(events):
            logging.warning(f"Coverage is less than: {REQUIRED_BLOCKS_RATIO * 100:.6f}% - may lead to inaccurate results.")

    # Calculate period yield and APY
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
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            logging.info(f"\n{'='*60}")
            logging.info(f"Block {n}: Calculating Root APY for {VALIDATOR_HOTKEY}")
            logging.info(f"{'='*60}")

            # Calculate APY using async function (but run it synchronously in this context)
            apy, total_divs, period_yield, processed, skipped = asyncio.run(
                calculate_root_apy_async(substrate, VALIDATOR_HOTKEY, n)
            )

            logging.info(f"Block {n}: Root APY = {apy:.6f}%")
            logging.info(f"{'='*60}\n")

            # Insert into ClickHouse
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
