from shared.clickhouse.batch_insert import buffer_insert, flush_buffer, batch_insert_into_clickhouse_table
from shared.substrate import get_substrate_client, reconnect_substrate
from time import sleep, time
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.telemetry import init_telemetry, get_tracer, trace_method, trace, get_metrics
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

        # Initialize telemetry with shovel name as fallback service name
        tracer, meter = init_telemetry(fallback_name=f"shovel-{self.name}")
        if tracer:
            logging.info(f"Telemetry initialized for shovel: {self.name}")

        # Get metrics instance
        self.metrics = get_metrics()

    @trace_method("shovel.start")
    def start(self):
        """Start the shovel processing loop."""
        self._start_internal()

    def _start_internal(self):
        retry_count = 0
        while True:
            try:
                # Initialize components
                substrate, finalized_block_hash, finalized_block_number, last_scraped_block_number = self._initialize_components()

                # Main processing loop
                while True:
                    try:
                        last_scraped_block_number = self._process_blocks_cycle(
                            last_scraped_block_number, finalized_block_number, substrate
                        )

                        # Reset retry count on successful iteration
                        retry_count = 0

                    except DatabaseConnectionError as e:
                        retry_count = self._handle_database_error(e, retry_count)
                        continue

            except ShovelProcessingError as e:
                self._handle_fatal_error("ShovelProcessingError", str(e))
            except Exception as e:
                self._handle_fatal_error(type(e).__name__, str(e))

    @trace_method("shovel.initialization")
    def _initialize_components(self):
        """Initialize all shovel components."""
        substrate = self._initialize_substrate()
        finalized_block_hash, finalized_block_number = self._get_finalized_block(substrate)
        executor, buffer_thread = self._start_buffer()
        last_scraped_block_number = self._get_initial_checkpoint()
        return substrate, finalized_block_hash, finalized_block_number, last_scraped_block_number

    @trace("shovel.substrate.initialize")
    def _initialize_substrate(self):
        """Initialize substrate client."""
        print("Initialising Substrate client")
        return get_substrate_client()

    @trace("shovel.substrate.get_finalized_block")
    def _get_finalized_block(self, substrate):
        """Get finalized block info."""
        print("Fetching the finalized block")
        finalized_block_hash = substrate.get_chain_finalised_head()
        finalized_block_number = substrate.get_block_number(finalized_block_hash)
        return finalized_block_hash, finalized_block_number

    @trace("shovel.buffer.start")
    def _start_buffer(self):
        """Start clickhouse buffer."""
        print("Starting Clickhouse buffer")
        executor = ThreadPoolExecutor(max_workers=1)
        buffer_thread = threading.Thread(
            target=flush_buffer,
            args=(executor, self._buffer_flush_started, self._buffer_flush_done),
            daemon=True
        )
        buffer_thread.start()
        return executor, buffer_thread

    @trace_method("shovel.checkpoint.get_initial")
    def _get_initial_checkpoint(self):
        """Get initial checkpoint."""
        last_scraped_block_number = self.get_checkpoint()
        logging.info(f"Last scraped block is {last_scraped_block_number}")
        return last_scraped_block_number

    @trace_method("shovel.processing_cycle", attributes={"stage": "main_loop"})
    def _process_blocks_cycle(self, last_scraped_block_number, finalized_block_number, substrate):
        """Process a cycle of blocks."""
        block_numbers = list(range(
            last_scraped_block_number + 1,
            finalized_block_number + 1,
            self.skip_interval
        ))

        # Update blocks behind metric
        blocks_behind = finalized_block_number - last_scraped_block_number
        self.metrics.set_blocks_behind(blocks_behind, shovel_name=self.name)

        if len(block_numbers) > 0:
            logging.info(f"Catching up {len(block_numbers)} blocks")
            self._process_block_batch(block_numbers)
        else:
            logging.info("Already up to latest finalized block, checking again in 12s...")

        # Make sure to sleep so buffer with checkpoint update is flushed to Clickhouse
        sleep(12)
        last_scraped_block_number = self.get_checkpoint()
        finalized_block_hash = substrate.get_chain_finalised_head()
        finalized_block_number = substrate.get_block_number(finalized_block_hash)

        return last_scraped_block_number

    @trace_method("shovel.process_block_batch")
    def _process_block_batch(self, block_numbers):
        """Process a batch of blocks."""
        batch_start_time = time()

        # Add batch metadata to current span
        tracer = get_tracer()
        if tracer:
            span = tracer.get_current_span()
            if span:
                span.set_attribute("block_count", len(block_numbers))
                span.set_attribute("start_block", block_numbers[0])
                span.set_attribute("end_block", block_numbers[-1])

        for block_number in tqdm(block_numbers):
            self._process_single_block(block_number)

        # Record batch processing time
        batch_duration = time() - batch_start_time
        self.metrics.record_batch_processing_time(
            batch_duration,
            shovel_name=self.name,
            block_count=len(block_numbers)
        )

    @trace_method("shovel.process_block")
    def _process_single_block(self, block_number):
        """Process a single block with error handling and metrics."""
        block_start_time = time()

        # Add block number to current span
        tracer = get_tracer()
        if tracer:
            span = tracer.get_current_span()
            if span:
                span.set_attribute("block_number", block_number)

        # Update current block metric
        self.metrics.set_current_block(block_number, shovel_name=self.name)

        try:
            self.process_block(block_number)
            self.checkpoint_block_number = block_number

            # Record successful processing
            block_duration = time() - block_start_time
            self.metrics.record_block_processing_time(
                block_duration,
                shovel_name=self.name,
                block_number=block_number,
                status="success"
            )
            self.metrics.increment_blocks_processed(
                1,
                shovel_name=self.name,
                status="success"
            )

            # Mark successful processing in span
            if tracer:
                span = tracer.get_current_span()
                if span:
                    span.set_attribute("processing_successful", True)
                    span.set_attribute("processing_duration_seconds", block_duration)

        except DatabaseConnectionError as e:
            self._record_error("database_connection", block_number, str(e))
            self.metrics.increment_database_errors(1, shovel_name=self.name, error_type="connection")
            self.metrics.increment_blocks_failed(1, shovel_name=self.name, error_type="database")
            raise  # Re-raise to be caught by outer try-except
        except Exception as e:
            self._record_error("processing", block_number, str(e), type(e).__name__)
            self.metrics.increment_blocks_failed(1, shovel_name=self.name, error_type="processing")
            raise ShovelProcessingError(f"Failed to process block {block_number}: {str(e)}")

    def _record_error(self, error_type, block_number, message, exception_type=None):
        """Record error information in telemetry."""
        logging.error(f"{error_type.replace('_', ' ').title()} error while processing block {block_number}: {message}")

        tracer = get_tracer()
        if tracer:
            span = tracer.get_current_span()
            if span:
                span.set_attribute("processing_successful", False)
                span.set_attribute("error.type", exception_type or error_type)
                span.set_attribute("error.message", message)
                span.set_attribute("error.block_number", block_number)

    def _handle_database_error(self, error, retry_count):
        """Handle database connection errors with retry logic."""
        retry_count += 1
        if retry_count > self.MAX_RETRIES:
            logging.error(f"Max retries ({self.MAX_RETRIES}) exceeded for database connection. Exiting.")
            raise ShovelProcessingError("Max database connection retries exceeded")

        logging.warning(f"Database connection error (attempt {retry_count}/{self.MAX_RETRIES}): {str(error)}")
        logging.info(f"Retrying in {self.RETRY_DELAY} seconds...")

        self._retry_delay(retry_count)
        return retry_count

    @trace("shovel.retry_delay")
    def _retry_delay(self, retry_count):
        """Execute retry delay with telemetry."""
        tracer = get_tracer()
        if tracer:
            span = tracer.get_current_span()
            if span:
                span.set_attribute("retry_count", retry_count)
                span.set_attribute("delay_seconds", self.RETRY_DELAY)

        sleep(self.RETRY_DELAY)
        reconnect_substrate()

    def _handle_fatal_error(self, error_type, message):
        """Handle fatal errors and exit."""
        logging.error(f"Fatal shovel error: {message}")

        tracer = get_tracer()
        if tracer:
            with tracer.start_as_current_span("shovel.fatal_error") as span:
                span.set_attribute("error.type", error_type)
                span.set_attribute("error.message", message)

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

        # Record database insertion metrics
        self.metrics.increment_records_inserted(
            rows,
            shovel_name=self.name,
            tables_count=tables,
            block_number=self.last_buffer_flush_call_block_number
        )

        self._update_checkpoint_with_metrics(tables, rows)

    @trace_method("shovel.checkpoint_update")
    def _update_checkpoint_with_metrics(self, tables, rows):
        """Update checkpoint with flush metrics."""
        # Add metrics to current span
        tracer = get_tracer()
        if tracer:
            span = tracer.get_current_span()
            if span:
                span.set_attribute("block_number", self.last_buffer_flush_call_block_number)
                span.set_attribute("rows_flushed", rows)
                span.set_attribute("tables_updated", tables)

        self._update_checkpoint()

    @trace("shovel.database.update_checkpoint")
    def _update_checkpoint(self):
        """Update checkpoint in database."""
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

    @trace_method("shovel.checkpoint.get")
    def get_checkpoint(self):
        """Get current checkpoint for this shovel."""
        return self._get_checkpoint_internal()

    @trace("shovel.database.query_checkpoint")
    def _get_checkpoint_internal(self):
        """Internal checkpoint retrieval logic."""
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
