from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
import traceback
import sys
import os

from shovel_extrinsics.utils import (
    create_clickhouse_table,
    format_value,
    generate_column_definitions,
    get_table_name,
)

# Allow debug logging via environment variable
log_level = logging.DEBUG
logging.basicConfig(level=log_level,
                    format="%(asctime)s %(process)d [%(levelname)s] %(message)s")


class ExtrinsicsShovel(ShovelBaseClass):
    def __init__(self, name):
        logging.info(f"Initializing ExtrinsicsShovel with name: {name}")
        super().__init__(name)
        logging.info("ExtrinsicsShovel initialization complete")

    def process_block(self, n):
        logging.debug(f"Processing block {n}")
        do_process_block(n)


def main():
    logging.info("Extrinsics shovel starting up...")
    logging.info(f"Python version: {sys.version}")
    try:
        shovel = ExtrinsicsShovel(name="extrinsics")
        logging.info("Starting shovel processing loop...")
        shovel.start()
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        raise


def do_process_block(n):
    logging.info(f"Starting to process block {n}")
    try:
        try:
            logging.debug(f"Getting substrate client for block {n}")
            substrate = get_substrate_client()
            logging.debug(f"Fetching block metadata for block {n}")
            (block_timestamp, block_hash) = get_block_metadata(n)
            logging.debug(f"Block {n} metadata: timestamp={block_timestamp}, hash={block_hash}")
        except Exception as e:
            logging.error(f"Failed to initialize block {n} processing: {str(e)}")
            logging.error(f"Stack trace:\n{traceback.format_exc()}")
            raise ShovelProcessingError(f"Failed to initialize block processing: {str(e)}")

        try:
            logging.debug(f"Fetching extrinsics for block {n}")
            extrinsics = substrate.get_extrinsics(block_number=n)
            logging.info(f"Block {n}: Found {len(extrinsics) if extrinsics else 0} extrinsics")
            if not extrinsics and n != 0:
                logging.warning(f"No extrinsics returned for block {n}")
                raise ShovelProcessingError(f"No extrinsics returned for block {n}")

            logging.debug(f"Fetching events for block {n}")
            events = substrate.query(
                "System",
                "Events",
                block_hash=block_hash,
            )
            logging.debug(f"Block {n}: Found {len(events) if events else 0} events")
            if not events and n != 0:
                logging.warning(f"No events returned for block {n}")
                raise ShovelProcessingError(f"No events returned for block {n}")
        except ShovelProcessingError:
            raise
        except Exception as e:
            logging.error(f"Failed to fetch extrinsics or events from substrate for block {n}: {str(e)}")
            logging.error(f"Stack trace:\n{traceback.format_exc()}")
            raise ShovelProcessingError(f"Failed to fetch extrinsics or events from substrate: {str(e)}")

        # Map extrinsic success/failure status
        logging.debug(f"Mapping extrinsic success/failure status for block {n}")
        extrinsics_success_map = {}
        system_events_count = 0
        for e in events:
            event = e.value
            # Skip irrelevant events
            if event["event"]["module_id"] != "System" or (event["event"]["event_id"] != "ExtrinsicSuccess" and event["event"]["event_id"] != "ExtrinsicFailed"):
                continue

            system_events_count += 1
            extrinsics_success_map[int(event["extrinsic_idx"])] = event["event"]["event_id"] == "ExtrinsicSuccess"

        logging.debug(f"Block {n}: Found {system_events_count} System success/failure events")

        # Needed to handle edge case of duplicate events in the same block
        extrinsic_id = 0
        processed_count = 0
        failed_count = 0
        logging.info(f"Block {n}: Processing {len(extrinsics)} extrinsics")

        for e in extrinsics:
            try:
                extrinsic = e.value
                address = extrinsic.get("address", None)
                nonce = extrinsic.get("nonce", None)
                tip = extrinsic.get("tip", None)
                call_function = extrinsic["call"]["call_function"]
                call_module = extrinsic["call"]["call_module"]

                logging.debug(f"Block {n}, Extrinsic {extrinsic_id}: {call_module}.{call_function}")

                # Get extrinsic hash for Balances module
                extrinsic_hash = None
                if call_module == "Balances" and hasattr(e, 'extrinsic_hash'):
                    extrinsic_hash = '0x' + e.extrinsic_hash.hex()

                # Build base columns - add hash for Balances module
                if call_module == "Balances":
                    base_column_names = ["block_number", "timestamp", "extrinsic_index",
                                       "call_function", "call_module", "success", "address", "nonce", "tip", "hash"]
                    base_column_types = ["UInt64", "DateTime", "UInt64", "String",
                                       "String", "Bool", "Nullable(String)", "Nullable(UInt64)", "Nullable(UInt64)", "String"]
                    base_column_values = [format_value(value) for value in [
                        n, block_timestamp, extrinsic_id, call_function, call_module, extrinsics_success_map[extrinsic_id], address, nonce, tip, extrinsic_hash]]
                else:
                    base_column_names = ["block_number", "timestamp", "extrinsic_index",
                                       "call_function", "call_module", "success", "address", "nonce", "tip"]
                    base_column_types = ["UInt64", "DateTime", "UInt64", "String",
                                       "String", "Bool", "Nullable(String)", "Nullable(UInt64)", "Nullable(UInt64)"]
                    base_column_values = [format_value(value) for value in [
                        n, block_timestamp, extrinsic_id, call_function, call_module, extrinsics_success_map[extrinsic_id], address, nonce, tip]]

                # Let column generation errors propagate up - we want to fail on new extrinsic types
                arg_column_names = []
                arg_column_types = []
                arg_values = []
                for arg in extrinsic["call"]["call_args"]:
                    (_arg_column_names, _arg_column_types, _arg_values) = generate_column_definitions(
                        arg["value"], arg["name"], arg["type"]
                    )
                    arg_column_names.extend(_arg_column_names)
                    arg_column_types.extend(_arg_column_types)
                    arg_values.extend(_arg_values)

                column_names = base_column_names + arg_column_names
                column_types = base_column_types + arg_column_types
                values = base_column_values + arg_values

                try:
                    table_name = get_table_name(
                        call_module, call_function, tuple(column_names)
                    )
                    logging.debug(f"Block {n}, Extrinsic {extrinsic_id}: Table name = {table_name}")

                    # Dynamically create table if not exists
                    if not table_exists(table_name):
                        logging.info(f"Creating new table: {table_name} with {len(column_names)} columns")
                        create_clickhouse_table(
                            table_name, column_names, column_types
                        )
                except Exception as e:
                    logging.error(f"Failed to create/check table {table_name}: {str(e)}")
                    logging.error(f"Stack trace:\n{traceback.format_exc()}")
                    raise DatabaseConnectionError(f"Failed to create/check table {table_name}: {str(e)}")

                try:
                    buffer_insert(table_name, values)
                    extrinsic_id += 1
                    processed_count += 1

                    if processed_count % 100 == 0:
                        logging.info(f"Block {n}: Processed {processed_count}/{len(extrinsics)} extrinsics")
                except Exception as e:
                    logging.error(f"Failed to insert data into buffer for extrinsic {extrinsic_id}: {str(e)}")
                    logging.error(f"Stack trace:\n{traceback.format_exc()}")
                    raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

            except DatabaseConnectionError:
                raise
            except Exception as e:
                failed_count += 1
                logging.error(f"Failed to process extrinsic {extrinsic_id} in block {n}: {str(e)}")
                logging.error(f"Extrinsic details: module={call_module}, function={call_function}")
                logging.error(f"Stack trace:\n{traceback.format_exc()}")
                # Convert any other errors to ShovelProcessingError to fail the shovel
                raise ShovelProcessingError(f"Failed to process extrinsic in block {n}: {str(e)}")

        # Verify we processed all extrinsics
        if len(extrinsics_success_map) != extrinsic_id:
            logging.error(f"Extrinsic count mismatch in block {n}: expected {len(extrinsics_success_map)}, processed {extrinsic_id}")
            raise ShovelProcessingError(
                f"Expected {len(extrinsics_success_map)} extrinsics, but only found {extrinsic_id}")

        logging.info(f"Block {n} processing complete: {processed_count} extrinsics processed successfully")

    except (DatabaseConnectionError, ShovelProcessingError) as e:
        # Re-raise these exceptions to be handled by the base class
        logging.error(f"Known error type in block {n}: {type(e).__name__} - {str(e)}")
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        logging.error(f"Unexpected error processing block {n}: {type(e).__name__} - {str(e)}")
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Extrinsics shovel shutdown requested")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unhandled exception in main: {str(e)}")
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)
