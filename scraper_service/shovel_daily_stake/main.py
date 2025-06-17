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
from substrate import get_substrate_client, reconnect_substrate, get_pool
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58
from hotkeys import _fetch_staking_hotkeys_parallel_pooled
from concurrent.futures import ThreadPoolExecutor, as_completed


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


# Global hotkey processing pool


# Global hotkey processing pool
HOTKEY_POOL_SIZE = 32  # Align with connection pool size

# --- concurrency + progress tracking --------------------------------------
concurrent_tasks = 0
concurrent_lock = threading.Lock()

# Completed hotkey-subnet tasks (for user-visible progress logging)
processed_hotkey_tasks = 0
processed_hotkey_tasks_lock = threading.Lock()

# Count of hotkey tasks that have been submitted to executors so far
submitted_hotkey_tasks = 0
submitted_hotkey_tasks_lock = threading.Lock()

# Semaphore to cap the true parallel hotkey RPC queries **across the whole process**
hotkey_query_semaphore = threading.Semaphore(HOTKEY_POOL_SIZE)

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import threading
import os
from queue import Queue

# Global progress tracking with more detail
progress_data = {
    'total_hotkey_tasks': 0,
    'completed_hotkey_tasks': 0,
    'failed_hotkey_tasks': 0,
    'total_entries': 0,
    'completed_entries': 0,
    'start_time': None,
    'rows_inserted': 0,
    'last_log_time': 0,
    'tasks_per_second': []  # Rolling window of completion rates
}
progress_lock = threading.Lock()

def update_progress(field, value, increment=True):
    """Thread-safe progress update."""
    with progress_lock:
        if increment:
            progress_data[field] += value
        else:
            progress_data[field] = value
        return progress_data.copy()

def reset_progress_data(start_time):
    """Reset all progress tracking data."""
    for key in progress_data:
        if key == 'tasks_per_second':
            update_progress(key, [], increment=False)
        else:
            update_progress(key, 0, increment=False)
    update_progress('start_time', start_time, increment=False)

def get_progress_stats():
    """Get current progress statistics with fixed ETA calculation."""
    with progress_lock:
        data = progress_data.copy()
    
    if data['total_hotkey_tasks'] > 0:
        hotkey_pct = (data['completed_hotkey_tasks'] / data['total_hotkey_tasks']) * 100
    else:
        hotkey_pct = 0
    
    if data['total_entries'] > 0:
        entry_pct = (data['completed_entries'] / data['total_entries']) * 100
    else:
        entry_pct = 0
    
    elapsed = time.time() - data['start_time'] if data['start_time'] else 0
    
    # Calculate current rate
    if data['completed_hotkey_tasks'] > 0 and elapsed > 0:
        overall_rate = data['completed_hotkey_tasks'] / elapsed
        
        # Calculate recent rate from rolling window
        if data['tasks_per_second'] and len(data['tasks_per_second']) > 2:
            recent_rates = [r for r in data['tasks_per_second'][-10:] if r > 0]
            if recent_rates:
                recent_rate = sum(recent_rates) / len(recent_rates)
            else:
                recent_rate = overall_rate
        else:
            recent_rate = overall_rate
        
        # Use recent rate for ETA if available and reasonable
        rate = recent_rate if recent_rate > 0 else overall_rate
        remaining = data['total_hotkey_tasks'] - data['completed_hotkey_tasks']
        eta_seconds = remaining / rate if rate > 0 else 0
        
        # Sanity check on ETA
        if eta_seconds > 0 and eta_seconds < 86400 * 365:  # Less than a year
            if eta_seconds > 3600:
                eta_str = f"{eta_seconds/3600:.1f}h"
            elif eta_seconds > 60:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "calculating..."
    else:
        eta_str = "calculating..."
        rate = 0
        overall_rate = 0
    
    return {
        'hotkey_pct': hotkey_pct,
        'entry_pct': entry_pct,
        'elapsed': elapsed,
        'eta': eta_str,
        'rate': rate,
        'overall_rate': overall_rate,
        'failed': data['failed_hotkey_tasks'],
        **data
    }
def progress_monitor(block_number, stop_event, interval=2.0):
    """Background thread to monitor and log progress."""
    last_completed = 0
    last_time = time.time()
    
    while not stop_event.is_set():
        try:
            current_time = time.time()
            stats = get_progress_stats()
            
            # Calculate rate for this interval
            if current_time - last_time > 0:
                interval_tasks = stats['completed_hotkey_tasks'] - last_completed
                interval_rate = interval_tasks / (current_time - last_time)
                
                # Update rolling window
                with progress_lock:
                    progress_data['tasks_per_second'].append(interval_rate)
                    # Keep only last 20 samples
                    if len(progress_data['tasks_per_second']) > 20:
                        progress_data['tasks_per_second'].pop(0)
            
            # Always log progress every interval, even if no change
            if current_time - stats['last_log_time'] >= interval:
                update_progress('last_log_time', current_time, increment=False)
                
                logging.info(
                    f"Block {block_number} PROGRESS: "
                    f"Entries: {stats['completed_entries']}/{stats['total_entries']} ({stats['entry_pct']:.1f}%) | "
                    f"Hotkeys: {stats['completed_hotkey_tasks']}/{stats['total_hotkey_tasks']} ({stats['hotkey_pct']:.1f}%) | "
                    f"Failed: {stats['failed']} | "
                    f"Rows: {stats['rows_inserted']} | "
                    f"Rate: {stats['rate']:.0f} tasks/s (avg: {stats['overall_rate']:.0f}) | "
                    f"Elapsed: {stats['elapsed']/60:.1f}m | "
                    f"ETA: {stats['eta']}"
                )
                
                last_completed = stats['completed_hotkey_tasks']
                last_time = current_time
            
        except Exception as e:
            logging.error(f"Progress monitor error: {e}")
        
        time.sleep(min(interval, 0.5))  # Check more frequently but log less often

def fetch_active_subnets(substrate, block_hash):
    """Fetch list of active subnet IDs."""
    t0 = time.time()
    active_subnets = _retry_on_disconnect(
        substrate.query_map,
        'SubtensorModule',
        'NetworksAdded',
        block_hash=block_hash,
    )
    t1 = time.time()
    netuids = [_extract_int(net[0]) for net in active_subnets]
    logging.debug(f"Fetched {len(netuids)} active subnets in {t1 - t0:.2f}s")
    return netuids

def get_staking_entries(block_number, block_hash, substrate):
    """Get staking entries either from cache or by fetching."""
    if (_staking_hotkeys_cache["entries"] is not None and
        _staking_hotkeys_cache["block_hash"] is not None and
        (block_number - _extract_int(substrate.get_block_number(_staking_hotkeys_cache["block_hash"]))) < CACHE_REFRESH_INTERVAL):
        staking_entries = _staking_hotkeys_cache["entries"]
        logging.info(f"Block {block_number}: cache hit → reused StakingHotkeys snapshot (≈{len(staking_entries)} entries)")
    else:
        logging.info(f"Block {block_number}: cache miss → fetching StakingHotkeys map afresh")
        staking_entries = _fetch_staking_hotkeys_parallel_pooled(
            block_hash,
            block_number,
            page_size=PAGE_SIZE,
            fetch_workers=MAX_WORKERS,
        )
        _staking_hotkeys_cache["entries"] = staking_entries
        _staking_hotkeys_cache["block_hash"] = block_hash
    
    return staking_entries

def calculate_total_work(staking_entries, netuids):
    """Calculate total number of hotkey tasks."""
    total_hotkey_tasks = 0
    valid_entries = 0
    
    for entry in staking_entries:
        coldkey_raw = entry[0]
        hotkeys_raw_list = entry[1]
        
        # Quick validation without full conversion
        if coldkey_raw is None or hotkeys_raw_list is None:
            continue
            
        if hasattr(hotkeys_raw_list, 'value'):
            hotkeys_raw_list = hotkeys_raw_list.value
            
        if hotkeys_raw_list:
            try:
                hotkey_count = len(hotkeys_raw_list)
                total_hotkey_tasks += hotkey_count * len(netuids)
                valid_entries += 1
            except:
                pass
                
    return total_hotkey_tasks, valid_entries

def to_ss58(raw_addr, addr_type):
    """Convert address to SS58 format, handling various input types."""
    try:
        if raw_addr is None:
            return None
        
        if isinstance(raw_addr, str):
            if raw_addr.startswith('5') and len(raw_addr) >= 47:
                return raw_addr
            return None
        
        if hasattr(raw_addr, 'value'):
            value = raw_addr.value
            if isinstance(value, str) and value.startswith('5') and len(value) >= 47:
                return value
            if isinstance(value, bytes):
                return convert_address_to_ss58(value, addr_type)
            return None
        
        if hasattr(raw_addr, '__str__'):
            str_val = str(raw_addr)
            if str_val.startswith('5') and len(str_val) >= 47:
                return str_val
        
        if isinstance(raw_addr, bytes):
            return convert_address_to_ss58(raw_addr, addr_type)
        
        if hasattr(raw_addr, '__iter__') and not isinstance(raw_addr, str):
            try:
                byte_data = bytes(raw_addr)
                return convert_address_to_ss58(byte_data, addr_type)
            except:
                pass
        
        return None
        
    except Exception as e:
        logging.warning(f"Error converting {addr_type} to SS58: {e}")
        return None

def get_hotkey_metrics_cached(netuid_local, hotkey_address_local, substrate_conn, 
                            block_hash, hotkey_metrics_cache, hotkey_metrics_lock):
    """Get hotkey metrics with caching."""
    key_local = (netuid_local, hotkey_address_local)
    
    with hotkey_metrics_lock:
        if key_local in hotkey_metrics_cache:
            return hotkey_metrics_cache[key_local]
    
    # Fetch metrics with retry on metadata errors
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hotkey_alpha_int_local = _query_int(
                substrate_conn,
                'TotalHotkeyAlpha',
                [hotkey_address_local, netuid_local],
                block_hash,
            )
            total_hotkey_shares_float_local = _query_fixed_float(
                substrate_conn,
                'TotalHotkeyShares',
                [hotkey_address_local, netuid_local],
                block_hash,
            )
            
            result = (hotkey_alpha_int_local, total_hotkey_shares_float_local)
            
            with hotkey_metrics_lock:
                hotkey_metrics_cache[key_local] = result
            
            return result
        except (AttributeError, NotImplementedError) as e:
            if "metadata" in str(e).lower() and attempt < max_retries - 1:
                logging.warning(f"Metadata error in metrics cache, retrying...")
                try:
                    substrate_conn.init_runtime(block_hash=block_hash)
                except:
                    pass
                time.sleep(0.5)
            else:
                raise

def process_single_hotkey_subnet(coldkey_ss58, hotkey_ss58, netuid, substrate_conn, 
                               block_hash, block_number, block_timestamp,
                               hotkey_metrics_cache, hotkey_metrics_lock):
    """Process a single hotkey-subnet combination with better error handling."""
    global concurrent_tasks
    
    # Acquire semaphore to enforce a hard upper-bound on concurrent RPCs
    hotkey_query_semaphore.acquire()
    
    with concurrent_lock:
        concurrent_tasks += 1
        current_concurrent = concurrent_tasks
    
    thread_name = threading.current_thread().name
    task_id = f"{hotkey_ss58[:8]}/{netuid}"
    
    try:
        t0 = time.time()
        logging.debug(
            f"Block {block_number}: {thread_name} starting {task_id} "
            f"(concurrent tasks: {current_concurrent})"
        )
        
        # Handle metadata errors with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Query Alpha first - if zero, skip expensive queries
                alpha_share_float = _query_fixed_float(
                    substrate_conn,
                    'Alpha',
                    [hotkey_ss58, coldkey_ss58, netuid],
                    block_hash,
                )
                if alpha_share_float == 0:
                    return None
                
                hotkey_alpha_int, total_hotkey_shares_float = get_hotkey_metrics_cached(
                    netuid, hotkey_ss58, substrate_conn, block_hash,
                    hotkey_metrics_cache, hotkey_metrics_lock
                )
                if total_hotkey_shares_float == 0:
                    return None
                
                stake_tao = int(alpha_share_float * hotkey_alpha_int / total_hotkey_shares_float)
                alpha_int = int(alpha_share_float)
                
                elapsed = time.time() - t0
                if elapsed > 2.0:
                    logging.info(
                        f"Block {block_number}: {thread_name} slow hotkey query {task_id} took {elapsed:.2f}s"
                    )
                else:
                    logging.debug(
                        f"Block {block_number}: {thread_name} completed {task_id} in {elapsed:.2f}s"
                    )
                
                return [
                    block_number,
                    block_timestamp,
                    f"'{coldkey_ss58}'",
                    f"'{hotkey_ss58}'",
                    netuid,
                    stake_tao,
                    alpha_int,
                ]
                
            except (AttributeError, NotImplementedError) as e:
                if "metadata" in str(e).lower() and attempt < max_retries - 1:
                    logging.warning(f"Metadata error for {task_id}, attempt {attempt + 1}/{max_retries}")
                    try:
                        substrate_conn.init_runtime(block_hash=block_hash)
                    except:
                        pass
                    time.sleep(0.5 * (attempt + 1))
                else:
                    raise
                    
    except Exception as e:
        logging.warning(f"Block {block_number}: {thread_name} error processing {task_id}: {e}")
        update_progress('failed_hotkey_tasks', 1)
        return None
    finally:
        # Always update completed count and release resources
        update_progress('completed_hotkey_tasks', 1)
        
        # Decrement counters and release semaphore
        with concurrent_lock:
            concurrent_tasks -= 1
        hotkey_query_semaphore.release()

def extract_hotkeys_from_entry(entry):
    """Extract and convert hotkeys from a staking entry."""
    coldkey_raw = entry[0]
    hotkeys_raw_list = entry[1]
    coldkey_ss58 = to_ss58(coldkey_raw, 'coldkey')
    
    if not coldkey_ss58 or not hotkeys_raw_list:
        return None, []
    
    if hasattr(hotkeys_raw_list, 'value'):
        hotkeys_raw_list = hotkeys_raw_list.value
    
    # Extract all hotkeys for this coldkey
    hotkeys = []
    for hotkey_raw_container in hotkeys_raw_list:
        hotkey_raw = hotkey_raw_container
        if isinstance(hotkey_raw, (tuple, list)) and len(hotkey_raw) == 1 and isinstance(hotkey_raw[0], (tuple, list)):
            hotkey_raw = hotkey_raw[0]
        hotkey_ss58 = to_ss58(hotkey_raw, 'hotkey')
        if hotkey_ss58:
            hotkeys.append(hotkey_ss58)
    
    return coldkey_ss58, hotkeys

def process_all_entries(staking_entries, netuids, pool, block_hash, block_number,
                       block_timestamp, table_name):
    """Process all staking entries with proper task distribution."""
    processing_start = time.time()
    rows_inserted = 0
    rows_lock = threading.Lock()
    
    # Shared cache for hotkey metrics
    hotkey_metrics_cache = {}
    hotkey_metrics_lock = threading.Lock()
    
    total_entries = len(staking_entries)
    
    # Collect all tasks upfront
    logging.info(f"Block {block_number}: collecting all stake processing tasks...")
    
    all_tasks = []
    entry_task_counts = {}  # Track how many tasks each entry has
    entries_to_complete = set()  # Entries that need completion tracking
    
    for idx, entry in enumerate(staking_entries):
        coldkey_ss58, hotkeys = extract_hotkeys_from_entry(entry)
        
        if not coldkey_ss58 or not hotkeys:
            # Don't mark as complete here - we'll do it after processing
            continue
        
        task_count = 0
        for hotkey_ss58 in hotkeys:
            for netuid in netuids:
                all_tasks.append((coldkey_ss58, hotkey_ss58, netuid, idx))
                task_count += 1
        
        if task_count > 0:
            entry_task_counts[idx] = task_count
            entries_to_complete.add(idx)
    
    # Update actual task count and reset entries completed
    actual_total_tasks = len(all_tasks)
    update_progress('total_hotkey_tasks', actual_total_tasks, increment=False)
    update_progress('completed_entries', 0, increment=False)  # Reset entries
    update_progress('total_entries', len(entries_to_complete), increment=False)  # Only count valid entries
    
    logging.info(
        f"Block {block_number}: processing {actual_total_tasks} tasks from "
        f"{len(entry_task_counts)} entries using {HOTKEY_POOL_SIZE} workers"
    )
    
    # Shuffle tasks to avoid all threads hitting same hotkey
    import random
    random.shuffle(all_tasks)
    
    # Track entry completions
    entry_completed_tasks = defaultdict(int)
    entry_completed_lock = threading.Lock()
    entries_completed = set()
    
    # Process tasks in batches to avoid memory issues
    batch_size = 10000
    task_batches = [all_tasks[i:i + batch_size] for i in range(0, len(all_tasks), batch_size)]
    
    for batch_idx, batch in enumerate(task_batches):
        logging.info(f"Block {block_number}: processing batch {batch_idx + 1}/{len(task_batches)} ({len(batch)} tasks)")
        
        with ThreadPoolExecutor(max_workers=HOTKEY_POOL_SIZE) as executor:
            # Submit batch tasks
            futures = []
            for task in batch:
                coldkey_ss58, hotkey_ss58, netuid, entry_idx = task
                substrate_conn = pool.get()
                
                future = executor.submit(
                    process_single_hotkey_subnet,
                    coldkey_ss58, hotkey_ss58, netuid, substrate_conn,
                    block_hash, block_number, block_timestamp,
                    hotkey_metrics_cache, hotkey_metrics_lock
                )
                futures.append((future, substrate_conn, entry_idx))
            
            # Process results
            for future, substrate_conn, entry_idx in futures:
                try:
                    result = future.result(timeout=60)
                    if result:
                        buffer_insert(table_name, result)
                        with rows_lock:
                            rows_inserted += 1
                        update_progress('rows_inserted', 1)
                    
                    # Track entry completion
                    with entry_completed_lock:
                        entry_completed_tasks[entry_idx] += 1
                        if (entry_idx not in entries_completed and 
                            entry_idx in entry_task_counts and
                            entry_completed_tasks[entry_idx] >= entry_task_counts[entry_idx]):
                            entries_completed.add(entry_idx)
                            update_progress('completed_entries', 1)
                        
                except Exception as e:
                    logging.warning(f"Task processing error: {e}")
                    update_progress('failed_hotkey_tasks', 1)
                    
                    # Still track completion for the entry
                    with entry_completed_lock:
                        entry_completed_tasks[entry_idx] += 1
                        if (entry_idx not in entries_completed and 
                            entry_idx in entry_task_counts and
                            entry_completed_tasks[entry_idx] >= entry_task_counts[entry_idx]):
                            entries_completed.add(entry_idx)
                            update_progress('completed_entries', 1)
                finally:
                    pool.put(substrate_conn)
    
    processing_elapsed = time.time() - processing_start
    return rows_inserted, processing_elapsed

def log_final_stats(block_number, rows_inserted, processing_elapsed, block_elapsed):
    """Log final statistics for the block processing."""
    final_stats = get_progress_stats()
    logging.info(
        f"Block {block_number}: FINAL STATS - "
        f"processed {final_stats['completed_entries']}/{final_stats['total_entries']} entries, "
        f"completed {final_stats['completed_hotkey_tasks']}/{final_stats['total_hotkey_tasks']} hotkey tasks "
        f"({final_stats['failed_hotkey_tasks']} failed) in {processing_elapsed:.2f}s, "
        f"inserted {rows_inserted} rows. Total block time: {block_elapsed:.2f}s"
    )

def fetch_all_stakes_at_block(block_hash, block_number, block_timestamp, table_name):
    """Main function to fetch all stakes at a specific block."""
    block_start_time = time.time()
    logging.info(f"Block {block_number}: starting stake snapshot")
    
    # Reset progress tracking
    reset_progress_data(block_start_time)
    
    # Get connection pool and initial substrate connection
    pool = get_pool(block_hash)
    substrate = pool.get()
    
    try:
        # Fetch active subnets
        netuids = fetch_active_subnets(substrate, block_hash)
        
        # Get staking entries (from cache or fresh)
        staking_entries = get_staking_entries(block_number, block_hash, substrate)
        
        if len(staking_entries) == 0:
            raise ShovelProcessingError('No StakingHotkeys data returned')
        
        # Calculate and set total work metrics
        total_hotkey_tasks, valid_entries = calculate_total_work(staking_entries, netuids)
        
        update_progress('total_entries', valid_entries, increment=False)
        update_progress('total_hotkey_tasks', total_hotkey_tasks, increment=False)
        
        logging.info(
            f"Block {block_number}: total work calculated - "
            f"{valid_entries} valid entries (of {len(staking_entries)} total), "
            f"~{total_hotkey_tasks} hotkey tasks"
        )
        
        # Start progress monitor thread
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=progress_monitor,
            args=(block_number, stop_monitor),
            daemon=True
        )
        monitor_thread.start()
        
        try:
            # Process all entries
            rows_inserted, processing_elapsed = process_all_entries(
                staking_entries, netuids, pool, block_hash, 
                block_number, block_timestamp, table_name
            )
            
            block_elapsed = time.time() - block_start_time
            
            # Log final statistics
            log_final_stats(block_number, rows_inserted, processing_elapsed, block_elapsed)
            
        finally:
            # Stop the monitor thread
            stop_monitor.set()
            monitor_thread.join(timeout=1)
        
        return rows_inserted
        
    except Exception as e:
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')
    finally:
        pool.put(substrate)

def main():
    StakeDailyMapShovel(name="stake_daily_map").start()


if __name__ == "__main__":
    main()
