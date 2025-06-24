from shared.clickhouse.batch_insert import buffer_insert, flush_buffer, batch_insert_into_clickhouse_table
from shared.substrate import get_substrate_client, reconnect_substrate
from time import sleep
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from tqdm import tqdm
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import sys


class ShovelBaseClass:
    checkpoint_block_number = 0
    last_buffer_flush_call_block_number = 0
    name = None
    skip_interval = 1
    MAX_RETRIES = 3
    RETRY_DELAY = 5

    def __init__(self, name, skip_interval=1):
        """
        Choose a unique name for the shovel.
        """
        self.name = name
        self.skip_interval = skip_interval
        self.starting_block = 0  # Default value, can be overridden by subclasses

    def start(self):
        retry_count = 0
        while True:
            try:
                print("Initialising Substrate client")
                substrate = get_substrate_client()

                print("Fetching the finalized block")
                finalized_block_hash = substrate.get_chain_finalised_head()
                finalized_block_number = substrate.get_block_number(finalized_block_hash)

                # Start the clickhouse buffer
                print("Starting Clickhouse buffer")
                executor = ThreadPoolExecutor(max_workers=1)
                buffer_thread = threading.Thread(
                    target=flush_buffer,
                    args=(executor, self._buffer_flush_started, self._buffer_flush_done),
                    daemon=True  # Make it a daemon thread so it exits with the main thread
                )
                buffer_thread.start()

                last_scraped_block_number = self.get_checkpoint()
                logging.info(f"Last scraped block is {last_scraped_block_number}")

                # Create a list of block numbers to scrape
                while True:
                    try:
                        block_numbers = list(range(
                            last_scraped_block_number + 1,
                            finalized_block_number + 1,
                            self.skip_interval
                        ))

                        if len(block_numbers) > 0:
                            logging.info(f"Catching up {len(block_numbers)} blocks")
                            for block_number in tqdm(block_numbers):
                                try:
                                    self.process_block(block_number)
                                    self.checkpoint_block_number = block_number
                                except DatabaseConnectionError as e:
                                    logging.error(f"Database connection error while processing block {block_number}: {str(e)}")
                                    raise  # Re-raise to be caught by outer try-except
                                except Exception as e:
                                    logging.error(f"Fatal error while processing block {block_number}: {str(e)}")
                                    raise ShovelProcessingError(f"Failed to process block {block_number}: {str(e)}")
                        else:
                            logging.info("Already up to latest finalized block, checking again in 12s...")

                        # Reset retry count on successful iteration
                        retry_count = 0

                        # Make sure to sleep so buffer with checkpoint update is flushed to Clickhouse
                        sleep(12)
                        last_scraped_block_number = self.get_checkpoint()
                        finalized_block_hash = substrate.get_chain_finalised_head()
                        finalized_block_number = substrate.get_block_number(finalized_block_hash)

                    except DatabaseConnectionError as e:
                        retry_count += 1
                        if retry_count > self.MAX_RETRIES:
                            logging.error(f"Max retries ({self.MAX_RETRIES}) exceeded for database connection. Exiting.")
                            raise ShovelProcessingError("Max database connection retries exceeded")

                        logging.warning(f"Database connection error (attempt {retry_count}/{self.MAX_RETRIES}): {str(e)}")
                        logging.info(f"Retrying in {self.RETRY_DELAY} seconds...")
                        sleep(self.RETRY_DELAY)
                        reconnect_substrate()  # Try to reconnect to substrate
                        continue

            except ShovelProcessingError as e:
                logging.error(f"Fatal shovel error: {str(e)}")
                sys.exit(1)
            except Exception as e:
                logging.error(f"Unexpected error: {str(e)}")
                sys.exit(1)

    def process_block(self, n):
        raise NotImplementedError(
            "Please implement the process_block method in your shovel class!"
        )

    def _buffer_flush_started(self):
        self.last_buffer_flush_call_block_number = self.checkpoint_block_number

    def _buffer_flush_done(self, tables, rows):
        if self.last_buffer_flush_call_block_number == 0:
            return

        print(
            f"Block {self.last_buffer_flush_call_block_number}: Flushed {
                rows} rows across {tables} tables to Clickhouse"
        )

        # Create checkpoint table if it doesn't exist
        if not table_exists("shovel_checkpoints"):
            query = """
            CREATE TABLE IF NOT EXISTS shovel_checkpoints (
                shovel_name String,
                block_number UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (shovel_name)
            """
            get_clickhouse_client().execute(query)

        # Update checkpoint
        buffer_insert(
            "shovel_checkpoints",
            [f"'{self.name}'", self.last_buffer_flush_call_block_number],
        )

    def get_checkpoint(self):
        if not table_exists("shovel_checkpoints"):
            return max(0, self.starting_block - 1)

        # First check if our shovel has any entries
        query = f"""
            SELECT count(*)
            FROM shovel_checkpoints
            WHERE shovel_name = '{self.name}'
        """
        count = get_clickhouse_client().execute(query)[0][0]
        if count == 0:
            return max(0, self.starting_block - 1)

        query = f"""
            SELECT block_number
            FROM shovel_checkpoints
            WHERE shovel_name = '{self.name}'
            ORDER BY block_number DESC
            LIMIT 1
        """
        res = get_clickhouse_client().execute(query)
        if res:
            return res[0][0]
        else:
            return max(0, self.starting_block - 1)  # This case shouldn't happen due to count check above
