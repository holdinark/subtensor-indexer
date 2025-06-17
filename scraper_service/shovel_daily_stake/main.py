import logging
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import threading
import os
from queue import Queue

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from substrate import get_substrate_client, reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58
from hotkeys import _fetch_staking_hotkeys_parallel_pooled

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(process)d %(message)s")

# Mute very verbose third-party libraries; keep our DEBUG statements visible.
for noisy in (
    "substrateinterface",
    "async_substrate_interface",
    "scalecodec",
    "websockets",
    "websocket",
    "websocket-client",
    "clickhouse_driver",
    "urllib3",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Filter out DEBUG logs emitted by third-party libraries that use the root
# logger directly (e.g. substrateinterface/scalecodec). We still want to keep
# our own DEBUG messages originating from files under the project directory.
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # two levels up


class ProjectOnlyDebugFilter(logging.Filter):
    """Allow DEBUG records only if they originate from inside this project."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        if record.levelno == logging.DEBUG:
            # Keep DEBUG if the file path starts with our project root
            if record.pathname.startswith(PROJECT_ROOT):
                return True
            # Drop DEBUG from external libs
            return False
        # Allow non-DEBUG records through unchanged
        return True


# Attach the filter to the root logger
logging.getLogger().addFilter(ProjectOnlyDebugFilter())

BLOCKS_PER_DAY = 7200
FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM = 5004000
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds

# ---------------------------------------------------------------------------
# Module-level caches to avoid refetching the gigantic StakingHotkeys map
# ---------------------------------------------------------------------------
# Tuple (block_hash) -> list(staking_entries)
_staking_hotkeys_cache = {
    "block_hash": None,       # hash of the block the cache was built from
    "entries": None           # cached list of (coldkey, Vec<hotkey>) entries
}

# How often (in blocks) we force a full refresh of the cache. 1 day = 7 200 blocks.
CACHE_REFRESH_INTERVAL = BLOCKS_PER_DAY

# Thread-pool size for parallel hotkey processing
MAX_WORKERS = 12   # at most 32 threads

# Page size for each paged RPC call
PAGE_SIZE = 1000

# ----- add near the top of the helper ---------------------------------
progress_lock   = threading.Lock()
pages_done = 0
LOG_EVERY_PAGES = 10          # adjust as you like
# ----------------------------------------------------------------------

def _retry_on_disconnect(func, *args, **kwargs):
    """Call substrate function with automatic reconnect on Broken pipe / connection reset."""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            logging.warning(f"Substrate connection error ({e}); reconnecting (attempt {attempt+1}/{MAX_RETRIES})…")
            reconnect_substrate()
            time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
        except Exception:
            raise
    raise ShovelProcessingError(f"Failed after {MAX_RETRIES} reconnect attempts")

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map"

    def __init__(self, name):
        super().__init__(name)
        print(f"starting new shovel version ")
        self.starting_block = FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if n < FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM:
        return
    if n % BLOCKS_PER_DAY != 0:
        return
    try:
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    coldkey String CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    netuid UInt8 CODEC(Delta, ZSTD),
                    stake UInt64 CODEC(Delta, ZSTD),
                    alpha UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, netuid, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = _retry_on_disconnect(get_block_metadata, n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            rows_inserted = fetch_all_stakes_at_block(block_hash, n, block_timestamp, table_name)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch stakes from substrate: {str(e)}")

        if rows_inserted == 0:
            raise ShovelProcessingError(f"No stake data returned for block {n}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def _extract_int(value_obj):
    """Extract integer from substrate result (int/str/hex/dict)."""
    if value_obj is None:
        return 0
    if isinstance(value_obj, int):
        return value_obj
    if isinstance(value_obj, str):
        try:
            return int(value_obj, 16) if value_obj.startswith('0x') else int(value_obj)
        except Exception:
            return 0
    if isinstance(value_obj, dict):
        if 'bits' in value_obj:
            return _extract_int(value_obj['bits'])
        if 'value' in value_obj:
            return _extract_int(value_obj['value'])
    try:
        return int(value_obj)
    except Exception:
        return 0


def _fixed128_to_float(bits_int: int) -> float:
    MASK64 = (1 << 64) - 1
    int_part = bits_int >> 64
    frac_part = bits_int & MASK64
    return float(int_part) + float(frac_part) / 2 ** 64


def _query_int(substrate, func, params, block_hash):
    res = _retry_on_disconnect(substrate.query,
        'SubtensorModule',
        func,
        params,
        block_hash=block_hash
    )
    return _extract_int(res.value if hasattr(res, 'value') else res)


def _query_fixed_float(substrate, func, params, block_hash):
    bits = _query_int(substrate, func, params, block_hash)
    return _fixed128_to_float(bits)


def fetch_all_stakes_at_block(block_hash, block_number, block_timestamp, table_name):
    """Stream stakes from chain and insert directly into ClickHouse buffer, return rows inserted."""
    try:
        block_start_time = time.time()
        logging.info(f"Block {block_number}: starting stake snapshot")
        substrate = get_substrate_client()

        # Cache: per (netuid, hotkey) -> (hotkey_alpha_int, total_hotkey_shares_float)
        hotkey_metrics_cache = {}

        t0 = time.time()
        active_subnets = _retry_on_disconnect(
            substrate.query_map,
            'SubtensorModule',
            'NetworksAdded',
            block_hash=block_hash,
        )
        t1 = time.time()
        netuids = [_extract_int(net[0]) for net in active_subnets]

        logging.debug(
            f"Block {block_number}: fetched {len(netuids)} active subnets in {t1 - t0:.2f}s"
        )

        # -------------------------------------------------------------------
        # Fetch or reuse the StakingHotkeys map (coldkey -> Vec<hotkey>)
        # -------------------------------------------------------------------
        if (
            _staking_hotkeys_cache["entries"] is not None
            and _staking_hotkeys_cache["block_hash"] is not None
            and (block_number - _extract_int(substrate.get_block_number(_staking_hotkeys_cache["block_hash"]))) < CACHE_REFRESH_INTERVAL
        ):
            staking_entries = _staking_hotkeys_cache["entries"]
            logging.info(
                f"Block {block_number}: cache hit → reused StakingHotkeys snapshot (≈{len(staking_entries)} entries); skipping on-chain download altogether"
            )
        else:
            logging.info(
                f"Block {block_number}: cache miss → fetching StakingHotkeys map afresh"
            )
            t2 = time.time()

            # Compute storage-key prefix for StakingHotkeys once per call
            staking_prefix = (
                substrate.create_storage_key(
                    "SubtensorModule", "StakingHotkeys", []
                ).to_hex()[2:]
            )

            # If we already have a snapshot, query only the changes
            if _staking_hotkeys_cache["entries"] is not None:
                # Measure how long the on-chain diff retrieval takes
                t_diff_start = time.time()
                changes = _retry_on_disconnect(
                    substrate.rpc_request,
                    "state_queryStorage",
                    [[f"0x{staking_prefix}"], _staking_hotkeys_cache["block_hash"], block_hash],
                )
                t_diff_end = time.time()
                logging.info(
                    f"Block {block_number}: fetched StakingHotkeys diff with {len(changes[0])} change-items in {t_diff_end - t_diff_start:.2f}s (incremental update)"
                )

                # Start from cached snapshot and patch in-place
                staking_entries = _staking_hotkeys_cache["entries"]
                key_to_index = {e[0]: idx for idx, e in enumerate(staking_entries)}

                for change in changes[0]:  # single prefix list
                    key_hex = change[0]
                    new_value = change[1]
                    coldkey_raw = bytes.fromhex(key_hex[-64:])
                    if new_value is None:
                        # deletion
                        if coldkey_raw in key_to_index:
                            del staking_entries[key_to_index[coldkey_raw]]
                    else:
                        staking_entries.append((coldkey_raw, new_value))

            else:
                staking_entries = _fetch_staking_hotkeys_parallel_pooled(
                    block_hash,
                    block_number,
                    page_size=PAGE_SIZE,
                    fetch_workers=MAX_WORKERS,
                )

            t3 = time.time()

            # Update cache
            _staking_hotkeys_cache["entries"] = staking_entries
            _staking_hotkeys_cache["block_hash"] = block_hash

            logging.debug(
                f"Block {block_number}: prepared {len(staking_entries)} StakingHotkeys entries in {t3 - t2:.2f}s"
            )

        if len(staking_entries) == 0:
            raise ShovelProcessingError('No StakingHotkeys data returned')

        rows_inserted = 0

        # ---------------------- per-thread row tracking --------------------
        thread_rows_counter = defaultdict(int)
        thread_elapsed_counter = defaultdict(float)
        thread_rows_lock = threading.Lock()
        
        # Progress tracking for entry processing
        entries_processed = 0
        entries_progress_lock = threading.Lock()
        total_entries = len(staking_entries)
        # -----------------------------------------------------------------

        def to_ss58(raw_addr, addr_type):
            """Convert address to SS58 format, handling various input types."""
            try:
                # Handle None/empty cases
                if raw_addr is None:
                    return None
                
                # If it's already a string and looks like SS58, return it
                if isinstance(raw_addr, str):
                    if raw_addr.startswith('5') and len(raw_addr) >= 47:  # SS58 format check
                        return raw_addr
                    return None
                
                # Handle ScaleType objects (most common case now)
                if hasattr(raw_addr, 'value'):
                    value = raw_addr.value
                    if isinstance(value, str) and value.startswith('5') and len(value) >= 47:
                        return value
                    # If value is bytes, try to convert
                    if isinstance(value, bytes):
                        return convert_address_to_ss58(value, addr_type)
                    return None
                
                # Handle direct access to string value (some ScaleType variations)
                if hasattr(raw_addr, '__str__'):
                    str_val = str(raw_addr)
                    if str_val.startswith('5') and len(str_val) >= 47:
                        return str_val
                
                # Handle bytes (original case)
                if isinstance(raw_addr, bytes):
                    return convert_address_to_ss58(raw_addr, addr_type)
                
                # Handle other iterables that might contain bytes
                if hasattr(raw_addr, '__iter__') and not isinstance(raw_addr, str):
                    try:
                        # Try to convert to bytes
                        byte_data = bytes(raw_addr)
                        return convert_address_to_ss58(byte_data, addr_type)
                    except:
                        pass
                
                # Log what we couldn't handle for debugging
                logging.warning(f"Error converting {addr_type} to SS58: unsupported type {type(raw_addr)}")
                logging.warning(f"{addr_type} type: {type(raw_addr)}, value: {raw_addr}")
                return None
                
            except Exception as e:
                # Log the specific error and the problematic data
                logging.warning(f"Error converting {addr_type} to SS58: {e}")
                logging.warning(f"{addr_type} type: {type(raw_addr)}, value: {raw_addr}")
                return None

        def process_entry(entry_idx, entry):
            nonlocal entries_processed
            
            local_rows = 0
            proc_start = time.time()
            coldkey_raw = entry[0]
            hotkeys_raw_list = entry[1]
            coldkey_ss58 = to_ss58(coldkey_raw, 'coldkey')
            if not coldkey_ss58 or not hotkeys_raw_list:
                # Update progress counter even for skipped entries
                with entries_progress_lock:
                    entries_processed += 1
                return 0
            if hasattr(hotkeys_raw_list, 'value'):
                hotkeys_raw_list = hotkeys_raw_list.value

            local_substrate = get_substrate_client()

            def get_hotkey_metrics_local(netuid_local, hotkey_address_local):
                key_local = (netuid_local, hotkey_address_local)
                if key_local in hotkey_metrics_cache:
                    return hotkey_metrics_cache[key_local]
                hotkey_alpha_int_local = _query_int(
                    local_substrate,
                    'TotalHotkeyAlpha',
                    [hotkey_address_local, netuid_local],
                    block_hash,
                )
                total_hotkey_shares_float_local = _query_fixed_float(
                    local_substrate,
                    'TotalHotkeyShares',
                    [hotkey_address_local, netuid_local],
                    block_hash,
                )
                hotkey_metrics_cache[key_local] = (
                    hotkey_alpha_int_local,
                    total_hotkey_shares_float_local,
                )
                return hotkey_metrics_cache[key_local]

            try:
                for hotkey_raw_container in hotkeys_raw_list:
                    hotkey_raw = hotkey_raw_container
                    if isinstance(hotkey_raw, (tuple, list)) and len(hotkey_raw) == 1 and isinstance(hotkey_raw[0], (tuple, list)):
                        hotkey_raw = hotkey_raw[0]
                    hotkey_ss58 = to_ss58(hotkey_raw, 'hotkey')
                    if not hotkey_ss58:
                        continue

                    for netuid in netuids:
                        hotkey_process_start = time.time()
                        try:
                            alpha_share_float = _query_fixed_float(
                                local_substrate,
                                'Alpha',
                                [hotkey_ss58, coldkey_ss58, netuid],
                                block_hash,
                            )
                            if alpha_share_float == 0:
                                continue
                            hotkey_alpha_int, total_hotkey_shares_float = get_hotkey_metrics_local(
                                netuid, hotkey_ss58
                            )
                            if total_hotkey_shares_float == 0:
                                continue
                            stake_tao = int(alpha_share_float * hotkey_alpha_int / total_hotkey_shares_float)
                            alpha_int = int(alpha_share_float)

                            buffer_insert(
                                table_name,
                                [
                                    block_number,
                                    block_timestamp,
                                    f"'{coldkey_ss58}'",
                                    f"'{hotkey_ss58}'",
                                    netuid,
                                    stake_tao,
                                    alpha_int,
                                ],
                            )
                            local_rows += 1
                        finally:
                            # Track per-hotkey timing
                            hotkey_elapsed = time.time() - hotkey_process_start
                            # Log slow hotkey queries
                            if hotkey_elapsed > 2.0:
                                logging.debug(
                                    f"Block {block_number}: slow hotkey query {hotkey_ss58[:8]}.../{netuid} took {hotkey_elapsed:.2f}s"
                                )

                elapsed = time.time() - proc_start
                
                # Update progress tracking
                with entries_progress_lock:
                    entries_processed += 1
                    current_progress = entries_processed
                    
                # Log progress every 1000 entries or if processing is slow
                if current_progress % 1000 == 0 or elapsed > 5.0:
                    progress_pct = (current_progress / total_entries) * 100
                    logging.info(
                        f"Block {block_number}: processed entry {current_progress}/{total_entries} "
                        f"({progress_pct:.1f}%) - coldkey {coldkey_ss58[:8]}... in {elapsed:.2f}s, "
                        f"inserted {local_rows} rows"
                    )
                
                with thread_rows_lock:
                    t_name = threading.current_thread().name
                    thread_rows_counter[t_name] += local_rows
                    thread_elapsed_counter[t_name] += elapsed
                return local_rows
            except Exception as e:
                # Update progress counter even for failed entries
                with entries_progress_lock:
                    entries_processed += 1
                # Log the failing coldkey/hotkey with stack trace for diagnostics
                logging.exception(
                    f"Error processing staking entry #{entry_idx}: coldkey={coldkey_ss58} – {e}"
                )
                return 0

        # ---------------------- parallel processing -------------------------
        processing_start = time.time()
        
        logging.info(
            f"Block {block_number}: starting parallel processing of {total_entries} staking entries "
            f"using {MAX_WORKERS} workers"
        )

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_entry, idx, e) for idx, e in enumerate(staking_entries)]
            for idx, fut in enumerate(futures):
                rows_inserted += fut.result()
                
                # Log overall progress every 5000 completed futures
                if (idx + 1) % 5000 == 0:
                    progress_pct = ((idx + 1) / total_entries) * 100
                    elapsed_so_far = time.time() - processing_start
                    logging.info(
                        f"Block {block_number}: completed {idx + 1}/{total_entries} entries "
                        f"({progress_pct:.1f}%) in {elapsed_so_far:.1f}s, {rows_inserted} rows inserted so far"
                    )

        processing_elapsed = time.time() - processing_start
        # Log per-thread contribution (rows inserted per worker)
        for t_name, r_cnt in thread_rows_counter.items():
            elapsed_t = thread_elapsed_counter.get(t_name, 0.0)
            logging.info(
                f"Block {block_number}: worker {t_name} inserted {r_cnt} rows in {elapsed_t:.2f}s"
            )

        block_elapsed = time.time() - block_start_time
        logging.info(
            f"Block {block_number}: processed {total_entries} staking entries in {processing_elapsed:.2f}s "
            f"using {MAX_WORKERS} threads, inserted {rows_inserted} total rows"
        )
        return rows_inserted

    except Exception as e:
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')


# ---------------------------------------------------------------------------
# Parallel retrieval helper for the massive StakingHotkeys map
# ---------------------------------------------------------------------------
# def _fetch_staking_hotkeys_parallel(block_hash: str, block_number: int, page_size: int = PAGE_SIZE, fetch_workers: int = MAX_WORKERS):
#     """Download the entire `SubtensorModule.StakingHotkeys` map using multiple
#     `SubstrateInterface` connections in parallel.  Returns a list with the same
#     shape as `substrate.query_map(...)` but typically several times faster on
#     high-latency links."""

#     task_queue: "Queue[str]" = Queue()
#     collected_records = []
#     collected_lock = threading.Lock()
#     visited_start_keys = set()
#     seen_keys = set()

#     # Use a substrate client from the main thread only to derive the storage prefix.
#     main_substrate = get_substrate_client()
#     storage_key_prefix = main_substrate.create_storage_key(
#         "SubtensorModule", "StakingHotkeys", []
#     ).to_hex()

#     # Seed the queue with 256 sub-prefixes (00-ff) so multiple workers start
#     # immediately.  We'll deduplicate records as we collect them, so overlap
#     # between buckets is no longer a correctness problem.

#     for b in range(256):
#         subprefix = storage_key_prefix + f"{b:02x}"
#         task_queue.put(subprefix)
#         visited_start_keys.add(subprefix)

#     def worker():
#         t_handshake_start = time.time()
#         logging.info(
#             f"Block {block_number}: {threading.current_thread().name} connecting to Substrate…"
#         )
#         local_substrate = get_substrate_client()
#         logging.info(
#             f"Block {block_number}: {threading.current_thread().name} connected ({time.time() - t_handshake_start:.2f}s handshake)"
#         )
#         started_at = time.time()
#         pages_fetched = 0
#         records_fetched = 0
#         while True:
#             start_key = task_queue.get()
#             # A value of `None` is used as a sentinel to signal graceful shutdown.
#             if start_key is None:
#                 task_queue.task_done()
#                 break

#             try:
#                 page_t0 = time.time()
#                 qmr = _retry_on_disconnect(
#                     local_substrate.query_map,
#                     module="SubtensorModule",
#                     storage_function="StakingHotkeys",
#                     block_hash=block_hash,
#                     page_size=page_size,
#                     start_key=start_key,
#                 )
#                 page_elapsed = time.time() - page_t0
#                 logging.info(
#                     f"Block {block_number}: {threading.current_thread().name} fetched page with {len(qmr.records)} records in {page_elapsed:.2f}s"
#                 )

#                 page_records = qmr.records  # Only the first page! Avoid implicit pagination.

#                 # Deduplicate by coldkey storage key (the first element of each
#                 # record tuple).  This avoids the massive double-count we saw
#                 # when multiple bucket scans return overlapping keys.
#                 with collected_lock:
#                     for rec in page_records:
#                         key = rec[0]
#                         if key not in seen_keys:
#                             seen_keys.add(key)
#                             collected_records.append(rec)

#                 # ---- progress bookkeeping ------------------------------------
#                 with progress_lock:
#                     global pages_done
#                     if len(page_records) > 0:
#                         pages_done += 1
#                     if pages_done % 5 == 0:
#                         logging.info(
#                             f"Block {block_number}: {pages_done} pages fetched (≈{len(seen_keys)} unique records)"
#                         )
#                 # --------------------------------------------------------------

#                 # If the page is full, schedule the next page.
#                 if len(page_records) == page_size and qmr.last_key and qmr.last_key not in visited_start_keys:
#                     visited_start_keys.add(qmr.last_key)
#                     task_queue.put(qmr.last_key)

#                 pages_fetched += 1
#                 records_fetched += len(page_records)

#             finally:
#                 task_queue.task_done()

#         elapsed = time.time() - started_at
#         logging.info(
#             f"Block {block_number}: worker {threading.current_thread().name} fetched {records_fetched} records ({pages_fetched} pages) in {elapsed:.2f}s"
#         )

#     overall_start = time.time()
#     logging.info(
#         f"Block {block_number}: starting parallel StakingHotkeys fetch with {fetch_workers} workers"
#     )
#     threads = [threading.Thread(target=worker, daemon=True) for _ in range(fetch_workers)]
#     for t in threads:
#         t.start()

#     task_queue.join()
#     # Send one sentinel per worker to signal that no more tasks will arrive.
#     for _ in threads:
#         task_queue.put(None)

#     # Wait for all worker threads to terminate cleanly before returning.
#     for t in threads:
#         t.join()

#     overall_elapsed = time.time() - overall_start
#     logging.info(
#         f"Block {block_number}: parallel StakingHotkeys fetch finished, {len(seen_keys)} unique records in {overall_elapsed:.2f}s using {fetch_workers} workers"
#     )
#     return collected_records


def main():
    StakeDailyMapShovel(name="stake_daily_map").start()


if __name__ == "__main__":
    main()
