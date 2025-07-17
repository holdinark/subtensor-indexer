from functools import lru_cache
from substrateinterface.base import is_valid_ss58_address
from shared.clickhouse.utils import (
    escape_column_name,
    get_clickhouse_client,
    table_exists,
)


def format_value(value):
    """
    SQL requires strings to be wrapped in single quotes
    """
    if isinstance(value, str):
        return f"'{value}'"
    else:
        return value


def get_column_type(value):
    if isinstance(value, str):
        return "String"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif value is None:
        return None
    else:
        print(f"Unhandled type: {type(value)}")
        return "String"


def generate_column_definitions(item, parent_key=None):
    column_names = []
    column_types = []
    values = []

    if isinstance(item, dict):
        for key, value in item.items():
            column_name = f"{parent_key}__{key}" if parent_key else key
            (_column_names, _column_types, _values) = generate_column_definitions(
                value, column_name
            )
            column_names.extend(_column_names)
            column_types.extend(_column_types)
            values.extend(_values)

    elif isinstance(item, tuple):
        for i, item in enumerate(item):
            item_key = f"tuple_{i}"
            item_name = f"{parent_key}.{item_key}" if parent_key else item_key
            (_column_names, _column_types, _values) = generate_column_definitions(
                item, item_name
            )
            column_names.extend(_column_names)
            column_types.extend(_column_types)
            values.extend(_values)

    else:
        column_type = get_column_type(item)
        if column_type is not None:
            column_name = parent_key if parent_key else "value"
            column_names.append(column_name)
            column_types.append(column_type)
            values.append(format_value(item))

    return (column_names, column_types, values)


def create_clickhouse_table(table_name, column_names, column_types, values):
    additional_columns = [
        "block_number UInt64",
        "timestamp DateTime",
    ]

    columns = list(
        map(
            lambda x, y: f"{escape_column_name(x)} {
                y}",
            column_names,
            column_types,
        )
    )
    all_columns = additional_columns + columns
    column_definitions = ", ".join(all_columns)

    # OrderBy block_number, timestamp, then any addresses
    order_by = ["block_number", "timestamp"]
    for i, value in enumerate(values):
        if isinstance(value, str) and is_valid_ss58_address(value):
            order_by.append(column_names[i])

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_definitions}
    ) ENGINE = ReplacingMergeTree()
    ORDER BY ({", ".join(order_by)})
    """

    get_clickhouse_client().execute(sql)


@lru_cache(maxsize=None)
def get_table_name(module_id, event_id, columns):
    """
    Returns a unique table name for the event_id and its columns.

    Initializes the table if it doesn't yet exist.

    Multiple tables for the same event can exist when the schema of the event changes.

    'columns' must be passed as a tuple to be hashable.
    """
    event_id = f"{module_id}_{event_id}"
    columns = ["block_number", "timestamp"] + list(columns)
    client = get_clickhouse_client()

    version = 0

    # Allow up to 50 tables for the same event_id
    MAX_VERSIONS = 50

    while version < MAX_VERSIONS:
        table_name = f"events_shovel_{event_id}_v{version}"

        # If the stable doesn't exist, we will create it for this version of the event we are
        # processing
        if not table_exists(table_name):
            return table_name

        # If table exists, we need to check the schema at this version matches the event we're
        # currently processing
        else:
            query = f"DESCRIBE TABLE '{table_name}'"
            result = client.execute(query)
            different_version = False

            if len(result) != len(columns):
                different_version = True

            for i, column in enumerate(result):
                if different_version or column[0] != columns[i]:
                    different_version = True
                    break
            if different_version:
                version += 1
            else:
                return table_name

    logging.error(f"Max versions reached for event {event_id}")
