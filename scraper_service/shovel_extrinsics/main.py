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

from shovel_extrinsics.utils import (
    create_clickhouse_table,
    format_value,
    generate_column_definitions,
    get_table_name,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class ExtrinsicsShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


def main():
    ExtrinsicsShovel(name="extrinsics").start()


def do_process_block(n):
    try:
        try:
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to initialize block processing: {str(e)}")

        try:
            extrinsics = substrate.get_extrinsics(block_number=n)
            if not extrinsics and n != 0:
                raise ShovelProcessingError(f"No extrinsics returned for block {n}")

            events = substrate.query(
                "System",
                "Events",
                block_hash=block_hash,
            )
            if not events and n != 0:
                raise ShovelProcessingError(f"No events returned for block {n}")
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch extrinsics or events from substrate: {str(e)}")

        # Map extrinsic success/failure status
        extrinsics_success_map = {}
        for e in events:
            event = e.value
            # Skip irrelevant events
            if event["event"]["module_id"] != "System" or (event["event"]["event_id"] != "ExtrinsicSuccess" and event["event"]["event_id"] != "ExtrinsicFailed"):
                continue

            extrinsics_success_map[int(event["extrinsic_idx"])] = event["event"]["event_id"] == "ExtrinsicSuccess"

        # Needed to handle edge case of duplicate events in the same block
        extrinsic_id = 0
        for e in extrinsics:
            try:
                extrinsic = e.value
                address = extrinsic.get("address", None)
                nonce = extrinsic.get("nonce", None)
                tip = extrinsic.get("tip", None)
                call_function = extrinsic["call"]["call_function"]
                call_module = extrinsic["call"]["call_module"]

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

                    # Dynamically create table if not exists
                    if not table_exists(table_name):
                        create_clickhouse_table(
                            table_name, column_names, column_types
                        )
                except Exception as e:
                    raise DatabaseConnectionError(f"Failed to create/check table {table_name}: {str(e)}")

                try:
                    buffer_insert(table_name, values)
                    extrinsic_id += 1
                except Exception as e:
                    raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

            except DatabaseConnectionError:
                raise
            except Exception as e:
                # Convert any other errors to ShovelProcessingError to fail the shovel
                raise ShovelProcessingError(f"Failed to process extrinsic in block {n}: {str(e)}")

        # Verify we processed all extrinsics
        if len(extrinsics_success_map) != extrinsic_id:
            raise ShovelProcessingError(
                f"Expected {len(extrinsics_success_map)} extrinsics, but only found {extrinsic_id}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


if __name__ == "__main__":
    main()
