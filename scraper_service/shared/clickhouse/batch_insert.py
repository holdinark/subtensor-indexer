import threading
from time import sleep
from shared.clickhouse.utils import get_clickhouse_client
import logging

# Global debug flag
_DEBUG_MODE = False

def set_debug_mode(enabled: bool):
    global _DEBUG_MODE
    _DEBUG_MODE = enabled
    logging.info(f"ClickHouse debug mode {'enabled' if enabled else 'disabled'}")

def debug_log(message: str):
    if _DEBUG_MODE:
        logging.info(f"[ClickHouse DEBUG] {message}")

buffer = {}
buffer_lock = threading.Lock()


def batch_insert_into_clickhouse_table(table, rows):
    try:
        debug_log(f"Attempting to insert {len(rows)} rows into table {table}")
        formatted_rows = ", ".join(
            f"({','.join(str(value) for value in row)})" for row in rows
        )
        sql = f"INSERT INTO {table} SETTINGS async_insert=1, wait_for_async_insert=1 VALUES {formatted_rows}"
        debug_log(f"Executing SQL: {sql}")
        get_clickhouse_client().execute(sql)
        debug_log(f"Successfully inserted {len(rows)} rows into table {table}")
    except Exception as e:
        debug_log(f"Error inserting into {table}: {str(e)}")
        debug_log(f"Error type: {type(e).__name__}")
        if len(rows) > 1:
            mid = len(rows) // 2
            debug_log(f"Retrying with smaller batches... Splitting {len(rows)} rows into {mid} and {len(rows)-mid}")
            batch_insert_into_clickhouse_table(table, rows[:mid])
            batch_insert_into_clickhouse_table(table, rows[mid:])
        else:
            debug_log(f"Error inserting single row into {table}: {e}")
            formatted_rows = ", ".join(
                f"({','.join(str(value) for value in row)})" for row in rows
            )
            sql = f"INSERT INTO {table} SETTINGS async_insert=1, wait_for_async_insert=1 VALUES {formatted_rows}"
            debug_log(f"Failed SQL: {sql}")
            raise e


def buffer_insert(table_name, row):
    """
    Queues a row for insertion. This should be the only way data is inserted into Clickhouse.
    """
    global buffer
    debug_log(f"Buffer insert called for table {table_name}")
    with buffer_lock:
        if table_name not in buffer:
            buffer[table_name] = []
            debug_log(f"Created new buffer for table {table_name}")

        buffer[table_name].append(row)
        debug_log(f"Added row to buffer for table {table_name}. Buffer size: {len(buffer[table_name])}")

    # Throttle if buffer is getting too large
    while table_name in buffer and len(buffer[table_name]) > 1_000_000:
        debug_log(f"Buffer for table {table_name} too large ({len(buffer[table_name])} rows), throttling...")
        sleep(1)


def flush_buffer_immediately():
    """
    Immediately flush the buffer for all tables.
    Useful for critical data that should be persisted right away.
    """
    global buffer
    debug_log("Immediate buffer flush requested")
    with buffer_lock:
        tasks = [(table_name, rows) for table_name, rows in buffer.items()]
        buffer.clear()
        debug_log(f"Immediate flush: Cleared buffer. Tasks to process: {len(tasks)}")

    for table_name, rows in tasks:
        try:
            batch_insert_into_clickhouse_table(table_name, rows)
            debug_log(f"Immediate flush: Successfully inserted {len(rows)} rows into {table_name}")
        except Exception as e:
            debug_log(f"Immediate flush: Failed to insert into {table_name}: {str(e)}")
            raise


# Continuously flush the buffer
def flush_buffer(executor, started_cb, done_cb):
    """
    Continuously flush the buffer.
    """
    global buffer
    debug_log("Starting buffer flush thread")
    while True:
        started_cb()
        with buffer_lock:
            tasks = [(table_name, rows) for table_name, rows in buffer.items()]
            buffer.clear()
            debug_log(f"Cleared buffer. Tasks to process: {len(tasks)}")

        futures = [
            executor.submit(batch_insert_into_clickhouse_table,
                            table_name, rows)
            for table_name, rows in tasks
        ]
        debug_log(f"Submitted {len(futures)} tasks to executor")
        for future in futures:
            try:
                future.result()
                debug_log("Task completed successfully")
            except Exception as e:
                debug_log(f"Task failed: {str(e)}")
                debug_log(f"Task error type: {type(e).__name__}")
        done_cb(len(tasks), sum(len(rows) for _, rows in tasks))
        debug_log("Buffer flush cycle completed")
        sleep(1)
