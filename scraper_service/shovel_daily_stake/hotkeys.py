from substrate import get_pool
import threading
from queue import Queue
import time
import logging

# Progress tracking globals
progress_lock = threading.Lock()
pages_done = 0


def _fetch_staking_hotkeys_parallel_pooled(block_hash: str, block_number: int, 
                                         page_size: int = 1000, 
                                         fetch_workers: int = 12):
    """Fixed version with proper connection handling and metadata recovery."""
    
    # Initialize pool with block_hash for historical queries
    pool = get_pool(block_hash=block_hash)
    
    task_queue = Queue()
    collected_records = []
    collected_lock = threading.Lock()
    visited_start_keys = set()
    seen_keys = set()
    
    # Progress tracking
    total_pages_fetched = 0
    total_pages_lock = threading.Lock()
    MAX_PAGES = 50  # Increased limit
    stop_fetching = False
    failed_connections = set()  # Track failed connections
    
    # Get storage key prefix
    temp_client = pool.get()
    try:
        # Ensure connection is initialized
        if not hasattr(temp_client, 'metadata') or temp_client.metadata is None:
            temp_client.init_runtime(block_hash=block_hash)
            
        storage_key_prefix = temp_client.create_storage_key(
            "SubtensorModule", "StakingHotkeys", []
        ).to_hex()
    finally:
        pool.put(temp_client)
    
    # Seed the task queue
    for b in range(8):
        subprefix = storage_key_prefix + f"{b:02x}"
        task_queue.put(subprefix)
        visited_start_keys.add(subprefix)
    
    logging.info(f"Block {block_number}: seeded with {task_queue.qsize()} initial prefixes")
    

    def safe_query_map(substrate_conn, start_key, block_hash, pool, max_retries=3):
        """Safely perform query_map with connection recovery on metadata errors."""
        current_conn = substrate_conn
        
        for attempt in range(max_retries):
            try:
                # Check if connection has metadata before using it
                if current_conn is None or not hasattr(current_conn, 'metadata') or current_conn.metadata is None:
                    logging.warning(
                        f"Connection has no metadata on attempt {attempt + 1}/{max_retries}, getting fresh connection"
                    )
                    # Return bad connection to pool and get a fresh one
                    if current_conn:
                        try:
                            pool.put(current_conn)
                        except:
                            pass
                    current_conn = pool.get()
                    # Ensure it's initialized with the block
                    if not hasattr(current_conn, 'metadata') or current_conn.metadata is None:
                        current_conn.init_runtime(block_hash=block_hash)
                
                # Try the query
                result = current_conn.query_map(
                    module="SubtensorModule",
                    storage_function="StakingHotkeys",
                    block_hash=block_hash,
                    page_size=page_size,
                    start_key=start_key,
                )
                return result, current_conn
                
            except (AttributeError, NotImplementedError) as e:
                error_msg = str(e).lower()
                if ("metadata" in error_msg or "decoder" in error_msg or "nonetype" in error_msg):
                    logging.warning(
                        f"Metadata error on attempt {attempt + 1}/{max_retries}: {e}"
                    )
                    
                    if attempt < max_retries - 1:
                        # Try to fix the connection
                        try:
                            # First try to reinitialize
                            current_conn.init_runtime(block_hash=block_hash)
                            logging.info(f"Successfully reinitialized metadata")
                        except Exception as init_e:
                            logging.warning(f"Failed to reinitialize: {init_e}")
                            # Get a completely fresh connection
                            try:
                                if current_conn:
                                    pool.put(current_conn)
                            except:
                                pass
                            
                            current_conn = pool.get()
                            try:
                                current_conn.init_runtime(block_hash=block_hash)
                                logging.info(f"Got fresh connection and initialized")
                            except Exception as fresh_e:
                                logging.error(f"Even fresh connection failed: {fresh_e}")
                                if attempt == max_retries - 1:
                                    raise
                        
                        time.sleep(0.5 * (attempt + 1))  # Progressive backoff
                    else:
                        raise
                else:
                    # Not a metadata error, raise immediately
                    raise
                    
            except Exception as e:
                logging.error(f"Unexpected error in query_map: {type(e).__name__}: {e}")
                raise
        
        # Should not reach here
        raise Exception(f"Failed after {max_retries} attempts")

    
    def worker():
        nonlocal stop_fetching, total_pages_fetched
        
        # Get a connection from pool
        local_substrate = pool.get()
        connection_id = id(local_substrate)
        
        try:
            # Ensure connection is initialized
            if not hasattr(local_substrate, 'metadata') or local_substrate.metadata is None:
                logging.info(f"Worker {threading.current_thread().name} initializing connection")
                local_substrate.init_runtime(block_hash=block_hash)
            
            started_at = time.time()
            pages_fetched = 0
            records_fetched = 0
            consecutive_failures = 0
            
            while not stop_fetching and consecutive_failures < 3:
                try:
                    start_key = task_queue.get(timeout=5.0)
                except:
                    break
                    
                if start_key is None or stop_fetching:
                    task_queue.task_done()
                    break
                
                try:
                    # Check if we've hit the safety limit
                    with total_pages_lock:
                        if total_pages_fetched >= MAX_PAGES:
                            stop_fetching = True
                            task_queue.task_done()
                            break
                        total_pages_fetched += 1
                        current_total = total_pages_fetched
                    
                    page_t0 = time.time()
                    
                    # Use safe query with connection recovery
                    qmr, local_substrate = safe_query_map(local_substrate, start_key, block_hash, pool)
                    
                    page_elapsed = time.time() - page_t0
                    
                    # Log progress
                    if current_total % 10 == 0 or page_elapsed > 5.0:
                        logging.info(
                            f"Block {block_number}: {threading.current_thread().name} "
                            f"page {current_total}/{MAX_PAGES} with {len(qmr.records)} records in {page_elapsed:.2f}s"
                        )
                    
                    page_records = qmr.records
                    new_records = 0
                    
                    with collected_lock:
                        for rec in page_records:
                            key = rec[0]
                            
                            # Handle different key types
                            if hasattr(key, 'to_hex'):
                                key_str = key.to_hex()
                            elif hasattr(key, '__bytes__'):
                                key_str = bytes(key).hex()
                            elif isinstance(key, bytes):
                                key_str = key.hex()
                            elif isinstance(key, str):
                                key_str = key
                            else:
                                key_str = str(key)
                            
                            if key_str not in seen_keys:
                                seen_keys.add(key_str)
                                collected_records.append(rec)
                                new_records += 1
                    
                    # Continue pagination if needed
                    if (len(page_records) == page_size and 
                        qmr.last_key and 
                        new_records > 0 and 
                        not stop_fetching):
                        
                        if hasattr(qmr.last_key, 'to_hex'):
                            last_key_str = qmr.last_key.to_hex()
                        elif isinstance(qmr.last_key, bytes):
                            last_key_str = qmr.last_key.hex()
                        else:
                            last_key_str = str(qmr.last_key)
                            
                        if last_key_str not in visited_start_keys:
                            visited_start_keys.add(last_key_str)
                            task_queue.put(last_key_str)
                    
                    pages_fetched += 1
                    records_fetched += len(page_records)
                    consecutive_failures = 0  # Reset on success
                    
                    # Log progress every 10 pages
                    if pages_fetched % 10 == 0:
                        with collected_lock:
                            unique_records = len(collected_records)
                        logging.info(
                            f"Block {block_number}: {threading.current_thread().name} "
                            f"fetched {pages_fetched} pages, {unique_records} unique records total"
                        )
                
                except Exception as e:
                    consecutive_failures += 1
                    logging.error(
                        f"Error processing start_key {start_key}: {type(e).__name__}: {e} "
                        f"(failure {consecutive_failures}/3)"
                    )
                    
                    # If this connection is bad, mark it
                    if "metadata" in str(e).lower() or "NoneType" in str(e):
                        failed_connections.add(connection_id)
                        logging.warning(f"Marking connection {connection_id} as failed")
                    
                    # Continue with next task if not too many failures
                    if consecutive_failures >= 3:
                        logging.error(f"Worker {threading.current_thread().name} stopping after 3 consecutive failures")
                        break
                        
                finally:
                    task_queue.task_done()
            
            elapsed = time.time() - started_at
            with collected_lock:
                final_unique = len(collected_records)
            logging.info(
                f"Block {block_number}: worker {threading.current_thread().name} "
                f"fetched {records_fetched} records ({pages_fetched} pages) in {elapsed:.2f}s, "
                f"contributed to {final_unique} unique records total"
            )
        except Exception as e:
            logging.error(f"Worker {threading.current_thread().name} crashed: {type(e).__name__}: {e}")
        finally:
            # Return connection to pool only if it's not failed
            if connection_id not in failed_connections:
                pool.put(local_substrate)
            else:
                logging.warning(f"Not returning failed connection {connection_id} to pool")
    
    # Start workers
    overall_start = time.time()
    logging.info(f"Block {block_number}: starting parallel StakingHotkeys fetch with {fetch_workers} workers")
    
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(fetch_workers)]
    for t in threads:
        t.start()
    
    # Wait for completion or timeout
    try:
        task_queue.join()
    except KeyboardInterrupt:
        stop_fetching = True
    
    # Signal workers to stop
    stop_fetching = True
    for _ in threads:
        try:
            task_queue.put(None)
        except:
            pass
    
    # Wait for all workers to finish
    for t in threads:
        t.join(timeout=10.0)
    
    overall_elapsed = time.time() - overall_start
    logging.info(
        f"Block {block_number}: parallel StakingHotkeys fetch finished, "
        f"{len(seen_keys)} unique records in {overall_elapsed:.2f}s using {fetch_workers} workers, "
        f"fetched {total_pages_fetched} pages total, {len(failed_connections)} failed connections"
    )
    
    return collected_records  
   
