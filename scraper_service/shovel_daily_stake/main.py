import logging
import random
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
from substrate import SubstrateConnectionPool, get_substrate_client, reconnect_substrate, get_pool
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58
from hotkeys import _fetch_staking_hotkeys_parallel_pooled
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s- %(message)s")

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

parallel_logger = logging.getLogger('parallel_debug')
parallel_logger.setLevel(logging.INFO)

# Add these new global variables
active_workers = {}
active_workers_lock = threading.Lock()

# Add this new helper function
def log_worker_activity(worker_id, status, details=""):
    """Log worker activity with timestamps."""
    with active_workers_lock:
        if status == "start":
            active_workers[worker_id] = time.time()
        elif status == "end" and worker_id in active_workers:
            duration = time.time() - active_workers[worker_id]
            del active_workers[worker_id]
            details += f" (duration: {duration:.2f}s)"
        
        active_count = len(active_workers)
    
    parallel_logger.info(
        f"[WORKER {status.upper()}] {worker_id} | Active workers: {active_count}/{HOTKEY_POOL_SIZE} | {details}"
    )

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
def process_all_entries_debug(staking_entries, netuids, pool, block_hash, block_number,
                             block_timestamp, table_name):
    """Enhanced version with detailed parallel execution logging."""
    processing_start = time.time()
    rows_inserted = 0
    rows_lock = threading.Lock()
    
    # Shared cache for hotkey metrics
    hotkey_metrics_cache = {}
    hotkey_metrics_lock = threading.Lock()
    
    # Track batch processing times
    batch_times = []
    batch_times_lock = threading.Lock()
    
    logging.info(f"Block {block_number}: Starting parallel processing with {HOTKEY_POOL_SIZE} workers")
    
    # Collect all tasks
    logging.info(f"Block {block_number}: Collecting tasks...")
    
    tasks_by_entry = defaultdict(list)
    all_tasks = []
    valid_entries = 0
    
    for idx, entry in enumerate(staking_entries):
        if idx % 5000 == 0:
            logging.info(f"Collecting tasks: {idx}/{len(staking_entries)} entries")
        
        coldkey_ss58, hotkeys = extract_hotkeys_from_entry(entry)
        if not coldkey_ss58 or not hotkeys:
            continue
        
        valid_entries += 1
        
        for hotkey_ss58 in hotkeys:
            for netuid in netuids:
                task = (coldkey_ss58, hotkey_ss58, netuid, idx)
                tasks_by_entry[idx].append(task)
                all_tasks.append(task)
    
    total_tasks = len(all_tasks)
    update_progress('total_hotkey_tasks', total_tasks, increment=False)
    update_progress('total_entries', valid_entries, increment=False)
    
    # Create smaller batches for better parallelization
    batch_size = max(50, total_tasks // (HOTKEY_POOL_SIZE * 20))  # Smaller batches
    task_batches = [all_tasks[i:i + batch_size] for i in range(0, len(all_tasks), batch_size)]
    
    logging.info(
        f"Block {block_number}: Created {len(task_batches)} batches of ~{batch_size} tasks each. "
        f"Total tasks: {total_tasks} from {valid_entries} entries"
    )
    
    # Shuffle batches for better distribution
    random.shuffle(task_batches)
    
    # Track batch assignment
    batch_assignment = {}
    completed_batches = set()
    failed_batches = set()
    
    def process_batch_with_logging(batch_idx, batch):
        """Process a batch with detailed logging."""
        worker_name = threading.current_thread().name
        worker_id = f"{worker_name}-B{batch_idx}"
        
        # Log worker start
        log_worker_activity(worker_id, "start", f"Processing batch {batch_idx} ({len(batch)} tasks)")
        
        batch_start = time.time()
        batch_rows = 0
        batch_completed = 0
        batch_failed = 0
        
        # Track batch assignment
        batch_assignment[batch_idx] = worker_name
        
        # Get connection from pool
        conn_start = time.time()
        substrate_conn = pool.get()
        conn_time = time.time() - conn_start
        
        if conn_time > 1.0:
            logging.warning(f"{worker_name}: Slow connection acquisition: {conn_time:.2f}s")
        
        try:
            # Initialize connection if needed
            init_start = time.time()
            if hasattr(substrate_conn, '_ensure_initialized'):
                substrate_conn._ensure_initialized()
            elif not hasattr(substrate_conn, 'metadata') or substrate_conn.metadata is None:
                substrate_conn.init_runtime(block_hash=block_hash)
            init_time = time.time() - init_start
            
            if init_time > 1.0:
                logging.warning(f"{worker_name}: Slow connection init: {init_time:.2f}s")
            
            # Log batch processing start
            parallel_logger.info(
                f"[BATCH START] {worker_name} processing batch {batch_idx}/{len(task_batches)} "
                f"({len(batch)} tasks)"
            )
            
            # Process tasks
            task_times = []
            for task_idx, (coldkey_ss58, hotkey_ss58, netuid, entry_idx) in enumerate(batch):
                task_start = time.time()
                
                try:
                    result = process_single_hotkey_subnet_with_timing(
                        coldkey_ss58, hotkey_ss58, netuid, substrate_conn,
                        block_hash, block_number, block_timestamp,
                        hotkey_metrics_cache, hotkey_metrics_lock
                    )
                    
                    if result:
                        buffer_insert(table_name, result)
                        batch_rows += 1
                        with rows_lock:
                            nonlocal rows_inserted
                            rows_inserted += 1
                        update_progress('rows_inserted', 1)
                    
                    batch_completed += 1
                    task_time = time.time() - task_start
                    task_times.append(task_time)
                    
                    # Log slow tasks
                    if task_time > 1.0:
                        logging.warning(
                            f"{worker_name}: Slow task {hotkey_ss58[:8]}/{netuid} took {task_time:.2f}s"
                        )
                    
                    # Periodic progress within batch
                    if task_idx > 0 and task_idx % 100 == 0:
                        avg_task_time = sum(task_times[-100:]) / len(task_times[-100:])
                        parallel_logger.info(
                            f"[BATCH PROGRESS] {worker_name} B{batch_idx}: {task_idx}/{len(batch)} "
                            f"({task_idx/len(batch)*100:.1f}%), avg task time: {avg_task_time:.3f}s"
                        )
                    
                except Exception as e:
                    logging.error(f"{worker_name}: Task failed: {e}")
                    batch_failed += 1
                    update_progress('failed_hotkey_tasks', 1)
                
                update_progress('completed_hotkey_tasks', 1)
            
            # Calculate batch statistics
            batch_elapsed = time.time() - batch_start
            avg_task_time = sum(task_times) / len(task_times) if task_times else 0
            tasks_per_second = batch_completed / batch_elapsed if batch_elapsed > 0 else 0
            
            # Log batch completion
            parallel_logger.info(
                f"[BATCH COMPLETE] {worker_name} B{batch_idx}: "
                f"{batch_completed} tasks in {batch_elapsed:.1f}s "
                f"({tasks_per_second:.1f} tasks/s), "
                f"avg task: {avg_task_time:.3f}s, "
                f"{batch_rows} rows, {batch_failed} failed"
            )
            
            # Track batch time
            with batch_times_lock:
                batch_times.append((batch_idx, batch_elapsed, tasks_per_second))
            
            completed_batches.add(batch_idx)
            
            return batch_completed, batch_failed, batch_rows, batch_elapsed
            
        except Exception as e:
            logging.error(f"{worker_name}: Batch {batch_idx} failed: {e}")
            failed_batches.add(batch_idx)
            remaining = len(batch) - batch_completed
            update_progress('failed_hotkey_tasks', remaining)
            update_progress('completed_hotkey_tasks', remaining)
            return batch_completed, batch_failed + remaining, batch_rows, time.time() - batch_start
            
        finally:
            # Return connection
            pool.put(substrate_conn)
            
            # Log worker end
            log_worker_activity(worker_id, "end", f"Batch {batch_idx} done")
    
    # Monitor thread to log parallel execution status
    def monitor_parallel_execution(stop_event):
        """Monitor and log parallel execution status."""
        while not stop_event.is_set():
            with active_workers_lock:
                active = list(active_workers.keys())
            
            if active:
                parallel_logger.info(
                    f"[PARALLEL STATUS] Active workers: {len(active)}/{HOTKEY_POOL_SIZE} | "
                    f"Workers: {', '.join(active[:10])}{'...' if len(active) > 10 else ''} | "
                    f"Completed batches: {len(completed_batches)}/{len(task_batches)}"
                )
            
            time.sleep(3)
    
    # Start monitor
    stop_monitor = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_parallel_execution,
        args=(stop_monitor,),
        daemon=True
    )
    monitor_thread.start()
    
    # Process batches in parallel
    logging.info(f"Block {block_number}: Starting ThreadPoolExecutor with {HOTKEY_POOL_SIZE} workers")
    
    completed_tasks = 0
    failed_tasks = 0
    
    with ThreadPoolExecutor(max_workers=HOTKEY_POOL_SIZE, thread_name_prefix='Worker') as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(process_batch_with_logging, idx, batch): (idx, batch)
            for idx, batch in enumerate(task_batches)
        }
        
        logging.info(f"Block {block_number}: Submitted {len(future_to_batch)} batches to executor")
        
        # Track completion
        for i, future in enumerate(as_completed(future_to_batch)):
            batch_idx, batch = future_to_batch[future]
            
            try:
                batch_completed, batch_failed, batch_rows, batch_time = future.result()
                completed_tasks += batch_completed
                failed_tasks += batch_failed
                
                # Log overall progress
                progress_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                elapsed = time.time() - processing_start
                overall_rate = completed_tasks / elapsed if elapsed > 0 else 0
                
                # Calculate parallel efficiency
                with batch_times_lock:
                    if batch_times:
                        recent_times = batch_times[-10:]
                        avg_batch_time = sum(t[1] for t in recent_times) / len(recent_times)
                        avg_batch_rate = sum(t[2] for t in recent_times) / len(recent_times)
                        parallel_efficiency = (avg_batch_rate * HOTKEY_POOL_SIZE) / overall_rate if overall_rate > 0 else 0
                    else:
                        avg_batch_time = 0
                        parallel_efficiency = 0
                
                logging.info(
                    f"Block {block_number}: Batch {batch_idx} done ({i+1}/{len(task_batches)}). "
                    f"Progress: {completed_tasks}/{total_tasks} ({progress_pct:.1f}%), "
                    f"Rate: {overall_rate:.1f} tasks/s, "
                    f"Avg batch time: {avg_batch_time:.1f}s, "
                    f"Parallel efficiency: {parallel_efficiency:.1f}x, "
                    f"Rows: {rows_inserted}"
                )
                
            except Exception as e:
                logging.error(f"Batch {batch_idx} failed: {e}")
    
    # Stop monitor
    stop_monitor.set()
    monitor_thread.join(timeout=1)
    
    processing_elapsed = time.time() - processing_start
    
    # Log final statistics with parallel efficiency analysis
    with batch_times_lock:
        if batch_times:
            total_batch_time = sum(t[1] for t in batch_times)
            avg_batch_time = total_batch_time / len(batch_times)
            theoretical_serial_time = total_batch_time
            actual_parallel_time = processing_elapsed
            speedup = theoretical_serial_time / actual_parallel_time if actual_parallel_time > 0 else 0
            
            logging.info(
                f"Block {block_number}: PARALLEL ANALYSIS - "
                f"Serial time (sum of batches): {theoretical_serial_time/60:.1f}m, "
                f"Parallel time: {actual_parallel_time/60:.1f}m, "
                f"Speedup: {speedup:.1f}x, "
                f"Efficiency: {speedup/HOTKEY_POOL_SIZE*100:.1f}%"
            )
    
    logging.info(
        f"Block {block_number}: COMPLETED - "
        f"Processed {completed_tasks} tasks ({failed_tasks} failed) in {processing_elapsed/60:.1f}m "
        f"({completed_tasks/processing_elapsed:.1f} tasks/s), "
        f"inserted {rows_inserted} rows"
    )
    
    return rows_inserted, processing_elapsed


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

def process_all_entries(staking_entries, netuids, pool: SubstrateConnectionPool, block_hash, block_number,
                            block_timestamp, table_name):
    """Fixed version with better parallelization and connection management."""
    processing_start = time.time()
    rows_inserted = 0
    rows_lock = threading.Lock()
    
    # Shared cache for hotkey metrics
    hotkey_metrics_cache = {}
    hotkey_metrics_lock = threading.Lock()
    
    # NOTE: We no longer pre-initialize every connection here. Connections are lazily
    # initialized on first use and then re-used by the pool, ensuring we only pay the
    # initialization cost once during the lifetime of the process rather than once per
    # block processing cycle. This avoids redundant round-trips to the node and speeds
    # up the overall pipeline.
    
    # Collect all tasks upfront with better organization
    logging.info(f"Block {block_number}: Collecting tasks...")
    
    # Group tasks by entry for better tracking
    tasks_by_entry = defaultdict(list)
    all_tasks = []
    valid_entries = 0
    
    for idx, entry in enumerate(staking_entries):
        if idx % 5000 == 0:
            logging.info(f"Collecting tasks: {idx}/{len(staking_entries)} entries")
        
        coldkey_ss58, hotkeys = extract_hotkeys_from_entry(entry)
        if not coldkey_ss58 or not hotkeys:
            continue
        
        valid_entries += 1
        entry_tasks = []
        
        # Create tasks for this entry
        for hotkey_ss58 in hotkeys:
            for netuid in netuids:
                task = (coldkey_ss58, hotkey_ss58, netuid, idx)
                entry_tasks.append(task)
                all_tasks.append(task)
        
        if entry_tasks:
            tasks_by_entry[idx] = entry_tasks
    
    total_tasks = len(all_tasks)
    update_progress('total_hotkey_tasks', total_tasks, increment=False)
    update_progress('total_entries', valid_entries, increment=False)
    
    logging.info(
        f"Block {block_number}: Collected {total_tasks} tasks from {valid_entries} entries. "
        f"Starting processing with {HOTKEY_POOL_SIZE} workers..."
    )
    
    # Shuffle tasks for better distribution
    random.shuffle(all_tasks)
    
    # Use ThreadPoolExecutor for better management
    completed_tasks = 0
    failed_tasks = 0
    entries_completed = set()
    entry_task_counts = {idx: len(tasks) for idx, tasks in tasks_by_entry.items()}
    entry_completed_counts = defaultdict(int)
    
    # Create batches for workers
    batch_size = max(100, total_tasks // (HOTKEY_POOL_SIZE * 10))
    task_batches = [all_tasks[i:i + batch_size] for i in range(0, len(all_tasks), batch_size)]
    
    logging.info(f"Block {block_number}: Created {len(task_batches)} batches of ~{batch_size} tasks each")
    
    def process_batch(batch_idx, batch):
        """Process a batch of tasks."""
        worker_name = threading.current_thread().name
        batch_start = time.time()
        batch_rows = 0
        batch_completed = 0
        batch_failed = 0
        
        # Get connection from pool
        substrate_conn = pool.get()
        connection_initialized = False
        
        try:
            # Ensure connection is ready
            if not connection_initialized:
                try:
                    if hasattr(substrate_conn, '_ensure_initialized'):
                        substrate_conn._ensure_initialized()
                    elif not hasattr(substrate_conn, 'metadata') or substrate_conn.metadata is None:
                        substrate_conn.init_runtime(block_hash=block_hash)
                    connection_initialized = True
                except Exception as e:
                    logging.error(f"{worker_name}: Failed to initialize connection: {e}")
                    raise
            
            logging.info(
                f"{worker_name}: Starting batch {batch_idx} with {len(batch)} tasks",
                stacklevel=2,
            ) if False else logging.debug(
                f"{worker_name}: Starting batch {batch_idx} with {len(batch)} tasks"
            )
            
            for task_idx, (coldkey_ss58, hotkey_ss58, netuid, entry_idx) in enumerate(batch):
                try:
                    # Process the task
                    result = process_single_hotkey_subnet(
                        coldkey_ss58, hotkey_ss58, netuid, substrate_conn,
                        block_hash, block_number, block_timestamp,
                        hotkey_metrics_cache, hotkey_metrics_lock
                    )
                    
                    if result:
                        buffer_insert(table_name, result)
                        batch_rows += 1
                        with rows_lock:
                            nonlocal rows_inserted
                            rows_inserted += 1
                        update_progress('rows_inserted', 1)
                    
                    batch_completed += 1
                    
                    # Update entry completion
                    with progress_lock:
                        entry_completed_counts[entry_idx] += 1
                        if (entry_idx not in entries_completed and 
                            entry_idx in entry_task_counts and
                            entry_completed_counts[entry_idx] >= entry_task_counts[entry_idx]):
                            entries_completed.add(entry_idx)
                            update_progress('completed_entries', 1)
                    
                    # Suppress per-batch detailed progress logs; keep overall monitor
                    if task_idx > 0 and task_idx % 50 == 0:
                        elapsed = time.time() - batch_start
                        rate = batch_completed / elapsed
                        logging.debug(
                            f"{worker_name}: Batch {batch_idx} progress: "
                            f"{task_idx}/{len(batch)} tasks, {rate:.1f} tasks/s"
                        )
                    
                except Exception as e:
                    logging.debug(f"{worker_name}: Task failed: {e}")
                    batch_failed += 1
                    update_progress('failed_hotkey_tasks', 1)
                    
                    # Still update entry completion for failed tasks
                    with progress_lock:
                        entry_completed_counts[entry_idx] += 1
                        if (entry_idx not in entries_completed and 
                            entry_idx in entry_task_counts and
                            entry_completed_counts[entry_idx] >= entry_task_counts[entry_idx]):
                            entries_completed.add(entry_idx)
                            update_progress('completed_entries', 1)
                
                update_progress('completed_hotkey_tasks', 1)
            
            batch_elapsed = time.time() - batch_start
            logging.debug(
                f"{worker_name}: Completed batch {batch_idx} - "
                f"{batch_completed} tasks in {batch_elapsed:.1f}s "
                f"({batch_completed/batch_elapsed:.1f} tasks/s), "
                f"{batch_rows} rows inserted, {batch_failed} failed"
            )
            
            return batch_completed, batch_failed, batch_rows
            
        except Exception as e:
            logging.debug(f"{worker_name}: Batch {batch_idx} failed: {e}")
            # Mark all remaining tasks as failed
            remaining = len(batch) - batch_completed
            update_progress('failed_hotkey_tasks', remaining)
            update_progress('completed_hotkey_tasks', remaining)
            return batch_completed, batch_failed + remaining, batch_rows
            
        finally:
            # Return connection to pool
            pool.put(substrate_conn)
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=HOTKEY_POOL_SIZE) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(process_batch, idx, batch): (idx, batch)
            for idx, batch in enumerate(task_batches)
        }
        
        # Monitor progress
        completed_batches = 0
        for future in as_completed(future_to_batch):
            batch_idx, batch = future_to_batch[future]
            try:
                batch_completed, batch_failed, batch_rows = future.result()
                completed_tasks += batch_completed
                failed_tasks += batch_failed
                completed_batches += 1
                
                # Log overall progress
                progress_pct = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
                elapsed = time.time() - processing_start
                overall_rate = completed_tasks / elapsed if elapsed > 0 else 0
                
                logging.info(
                    f"Block {block_number}: Batch {batch_idx} done. "
                    f"Overall: {completed_tasks}/{total_tasks} ({progress_pct:.1f}%), "
                    f"{completed_batches}/{len(task_batches)} batches, "
                    f"{overall_rate:.1f} tasks/s, {rows_inserted} rows"
                )
                
            except Exception as e:
                logging.error(f"Batch {batch_idx} failed: {e}")
    
    processing_elapsed = time.time() - processing_start
    
    logging.info(
        f"Block {block_number}: COMPLETED - "
        f"Processed {completed_tasks} tasks ({failed_tasks} failed) in {processing_elapsed/60:.1f}m "
        f"({completed_tasks/processing_elapsed:.1f} tasks/s), "
        f"inserted {rows_inserted} rows"
    )
    
    return rows_inserted, processing_elapsed

def fetch_all_stakes_at_block(block_hash, block_number, block_timestamp, table_name):
    """Fixed version with better initialization and error handling."""
    block_start_time = time.time()
    logging.info(f"Block {block_number}: Starting stake snapshot")
    
    # Reset progress tracking
    reset_progress_data(block_start_time)
    
    # Create pool with proper size
    pool = get_pool(block_hash, use_lazy=False, size=HOTKEY_POOL_SIZE)
    
    # Get initial connection for setup queries
    substrate = pool.get()
    
    try:
        # Initialize the connection
        if not hasattr(substrate, 'metadata') or substrate.metadata is None:
            substrate.init_runtime(block_hash=block_hash)
        
        # Start progress monitor
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=progress_monitor,
            args=(block_number, stop_monitor, 5.0),  # Log every 5 seconds
            daemon=True
        )
        monitor_thread.start()
        
        try:
            # Fetch active subnets
            netuids = fetch_active_subnets(substrate, block_hash)
            logging.info(f"Block {block_number}: Found {len(netuids)} active subnets")
            
            # Return connection before getting staking entries
            pool.put(substrate)
            substrate = None
            
            # Get staking entries (this uses the pool internally)
            staking_entries = get_staking_entries(block_number, block_hash, pool.get())
            
            if len(staking_entries) == 0:
                raise ShovelProcessingError('No StakingHotkeys data returned')
            
            # Calculate total work
            total_hotkey_tasks, valid_entries = calculate_total_work(staking_entries, netuids)
            
            logging.info(
                f"Block {block_number}: Processing {valid_entries} entries "
                f"with ~{total_hotkey_tasks} hotkey tasks across {len(netuids)} subnets"
            )
            
            # Process with fixed function
            rows_inserted, processing_elapsed = process_all_entries_debug(
                staking_entries, netuids, pool, block_hash,
                block_number, block_timestamp, table_name
            )
            
            block_elapsed = time.time() - block_start_time
            
            # Log final statistics
            log_final_stats(block_number, rows_inserted, processing_elapsed, block_elapsed)
            
            return rows_inserted
            
        finally:
            stop_monitor.set()
            monitor_thread.join(timeout=1)
            
    except Exception as e:
        logging.error(f"Block {block_number}: Fatal error: {e}")
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')
        
    finally:
        # Return substrate connection if we still have it
        if substrate:
            pool.put(substrate)

def process_single_hotkey_subnet_with_timing(coldkey_ss58, hotkey_ss58, netuid, substrate_conn, 
                               block_hash, block_number, block_timestamp,
                               hotkey_metrics_cache, hotkey_metrics_lock):
    """Process a single hotkey-subnet combination with detailed timing logs."""
    global concurrent_tasks
    
    task_id = f"{hotkey_ss58[:8]}/{netuid}"
    thread_name = threading.current_thread().name
    task_start = time.time()
    
    # Dictionary to track timing of each operation
    timings = {
        'semaphore_acquire': 0,
        'alpha_query': 0,
        'metrics_cache_check': 0,
        'total_hotkey_alpha_query': 0,
        'total_hotkey_shares_query': 0,
        'calculations': 0,
        'total': 0
    }
    
    # Acquire semaphore with timing
    sem_start = time.time()
    hotkey_query_semaphore.acquire()
    timings['semaphore_acquire'] = time.time() - sem_start
    
    with concurrent_lock:
        concurrent_tasks += 1
        current_concurrent = concurrent_tasks
    
    try:
        # Handle metadata errors with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Query Alpha with detailed timing
                alpha_start = time.time()
                alpha_share_float = _query_fixed_float(
                    substrate_conn,
                    'Alpha',
                    [hotkey_ss58, coldkey_ss58, netuid],
                    block_hash,
                )
                timings['alpha_query'] = time.time() - alpha_start
                
                if alpha_share_float == 0:
                    return None
                
                # Get hotkey metrics with timing breakdown
                metrics_start = time.time()
                
                # Check cache first
                cache_check_start = time.time()
                key_local = (netuid, hotkey_ss58)
                with hotkey_metrics_lock:
                    cached = key_local in hotkey_metrics_cache
                    if cached:
                        hotkey_alpha_int, total_hotkey_shares_float = hotkey_metrics_cache[key_local]
                timings['metrics_cache_check'] = time.time() - cache_check_start
                
                if not cached:
                    # Query TotalHotkeyAlpha
                    alpha_query_start = time.time()
                    hotkey_alpha_int = _query_int(
                        substrate_conn,
                        'TotalHotkeyAlpha',
                        [hotkey_ss58, netuid],
                        block_hash,
                    )
                    timings['total_hotkey_alpha_query'] = time.time() - alpha_query_start
                    
                    # Query TotalHotkeyShares
                    shares_query_start = time.time()
                    total_hotkey_shares_float = _query_fixed_float(
                        substrate_conn,
                        'TotalHotkeyShares',
                        [hotkey_ss58, netuid],
                        block_hash,
                    )
                    timings['total_hotkey_shares_query'] = time.time() - shares_query_start
                    
                    # Cache the results
                    with hotkey_metrics_lock:
                        hotkey_metrics_cache[key_local] = (hotkey_alpha_int, total_hotkey_shares_float)
                
                if total_hotkey_shares_float == 0:
                    return None
                
                # Calculations
                calc_start = time.time()
                stake_tao = int(alpha_share_float * hotkey_alpha_int / total_hotkey_shares_float)
                alpha_int = int(alpha_share_float)
                timings['calculations'] = time.time() - calc_start
                
                # Total time
                timings['total'] = time.time() - task_start
                
                # Log detailed timing if task was slow
                if timings['total'] > 1.0:
                    logging.warning(
                        f"{thread_name}: SLOW TASK {task_id} took {timings['total']:.2f}s | "
                        f"Breakdown: semaphore={timings['semaphore_acquire']:.3f}s, "
                        f"alpha_query={timings['alpha_query']:.3f}s, "
                        f"cache_check={timings['metrics_cache_check']:.3f}s, "
                        f"hotkey_alpha_query={timings['total_hotkey_alpha_query']:.3f}s, "
                        f"hotkey_shares_query={timings['total_hotkey_shares_query']:.3f}s, "
                        f"calc={timings['calculations']:.3f}s"
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
                    logging.warning(f"[RETRY] Metadata error for {task_id}, attempt {attempt + 1}/{max_retries}")
                    try:
                        substrate_conn.init_runtime(block_hash=block_hash)
                    except:
                        pass
                    time.sleep(0.5 * (attempt + 1))
                else:
                    raise
                    
    except Exception as e:
        logging.error(f"[ERROR] {thread_name} failed {task_id}: {e}")
        update_progress('failed_hotkey_tasks', 1)
        return None
    finally:
        # Always update completed count and release resources
        update_progress('completed_hotkey_tasks', 1)
        
        # Decrement counters and release semaphore
        with concurrent_lock:
            concurrent_tasks -= 1
            remaining = concurrent_tasks
        hotkey_query_semaphore.release()

# Also enhance the _query_int and _query_fixed_float functions with timing
def _query_int_with_timing(substrate, func, params, block_hash):
    """Query with timing breakdown."""
    query_start = time.time()
    res = substrate.query(
        'SubtensorModule',
        func,
        params,
        block_hash=block_hash
    )
    query_time = time.time() - query_start
    
    # Log slow queries
    if query_time > 0.5:
        logging.warning(
            f"SLOW RPC: {func} took {query_time:.3f}s for params {params[0][:8] if params else 'none'}"
        )
    
    return _extract_int(res.value if hasattr(res, 'value') else res)

def _query_fixed_float_with_timing(substrate, func, params, block_hash):
    """Query with timing breakdown."""
    query_start = time.time()
    bits = _query_int(substrate, func, params, block_hash)
    query_time = time.time() - query_start
    
    # Log slow queries
    if query_time > 0.5:
        logging.warning(
            f"SLOW RPC: {func} took {query_time:.3f}s for params {params[0][:8] if params else 'none'}"
        )
    
    return _fixed128_to_float(bits)
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



def main():
    StakeDailyMapShovel(name="stake_daily_map").start()


if __name__ == "__main__":
    main()
