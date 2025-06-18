import os
from substrateinterface import SubstrateInterface
import threading
import logging
import time
from queue import Queue

# Thread-local storage for substrate clients
thread_local = threading.local()

# ---------------------------------------------------------------------------
# Default connection-pool size. Can be overridden via env var SUBSTRATE_POOL_SIZE.
# We now keep it aligned with HOTKEY_POOL_SIZE (defaults to 50) to avoid creating
# excessive websocket connections at startup.
# ---------------------------------------------------------------------------

DEFAULT_POOL_SIZE = int(os.getenv("SUBSTRATE_POOL_SIZE", "32"))

def get_substrate_client():
    """Get a substrate client for the current thread."""
    if not hasattr(thread_local, "client"):
        url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        # Create connection with logging to see timing
        t0 = time.time()
        thread_name = threading.current_thread().name
        logging.info(f"{thread_name}: Creating substrate connection...")
        thread_local.client = SubstrateInterface(url)
        t1 = time.time()
        logging.info(f"{thread_name}: Connection created in {t1-t0:.2f}s")
    return thread_local.client

def create_fresh_substrate_client():
    """Create a fresh substrate client without caching."""
    url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
    t0 = time.time()
    thread_name = threading.current_thread().name
    logging.info(f"{thread_name}: Creating fresh substrate connection...")
    client = SubstrateInterface(url)
    t1 = time.time()
    logging.info(f"{thread_name}: Fresh connection created in {t1-t0:.2f}s")
    return client

def reconnect_substrate():
    """Reconnect the substrate client."""
    logging.info("Reconnecting Substrate...")
    if hasattr(thread_local, "client"):
        try:
            thread_local.client.close()
        except:
            pass
        del thread_local.client
    get_substrate_client()

class LazySubstrateConnection:
    """Wrapper that delays runtime initialization until first use."""
    def __init__(self, url, block_hash=None):
        self.url = url
        self.block_hash = block_hash
        self._client = None
        self._initialized = False
        self._lock = threading.Lock()
        
    def _ensure_initialized(self):
        """Initialize runtime on first use."""
        if not self._initialized:
            with self._lock:
                if not self._initialized:  # Double-check after acquiring lock
                    if self._client is None:
                        self._client = SubstrateInterface(self.url)
                    
                    if not hasattr(self._client, 'metadata') or self._client.metadata is None:
                        if self.block_hash:
                            self._client.init_runtime(block_hash=self.block_hash)
                        else:
                            self._client.init_runtime()
                    self._initialized = True
    
    def __getattr__(self, name):
        """Delegate all attribute access to the underlying client."""
        self._ensure_initialized()
        return getattr(self._client, name)
    
    def close(self):
        """Close the underlying connection."""
        if self._client:
            self._client.close()

class SubstrateConnectionPool:
    """Pre-create connections to avoid serial connection setup."""
    
    def __init__(self, size: int = DEFAULT_POOL_SIZE):
        self.size = size
        self.connections = Queue()
        self.url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        self._lock = threading.Lock()
        self._block_hash = None
        
    def initialize(self, block_hash=None):
        """Create all connections upfront WITHOUT initializing runtime."""
        self._block_hash = block_hash
        logging.info(f"Creating {self.size} substrate connections (lazy init)...")
        
        # Create connections in parallel - just the websocket connection, no runtime init
        def create_conn(i):
            t0 = time.time()
            try:
                # Create lazy wrapper instead of initializing runtime
                conn = LazySubstrateConnection(self.url, block_hash)
                t1 = time.time()
                logging.info(f"Connection {i+1}/{self.size} created in {t1-t0:.2f}s (lazy)")
                return conn
            except Exception as e:
                logging.error(f"Failed to create connection {i+1}/{self.size}: {e}")
                return None
        
        # This should be nearly instant since we're not initializing runtime
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.size) as executor:
            futures = [executor.submit(create_conn, i) for i in range(self.size)]
            for future in futures:
                conn = future.result()
                if conn is not None:
                    self.connections.put(conn)
        
        actual_size = self.connections.qsize()
        logging.info(f"Created {actual_size}/{self.size} connections successfully")
        
        # Fill any missing connections
        while self.connections.qsize() < self.size:
            try:
                conn = LazySubstrateConnection(self.url, block_hash)
                self.connections.put(conn)
            except Exception as e:
                logging.error(f"Failed to create additional connection: {e}")
                break
    
    def get(self)->LazySubstrateConnection:
        """Get a connection from the pool."""
        return self.connections.get()
    
    def put(self, conn):
        """Return a connection to the pool."""
        self.connections.put(conn)
    
    def get_fresh_connection(self):
        """Get a fresh connection, not from the pool."""
        return create_fresh_substrate_client()
    
    def replace_connection(self, old_conn):
        """Replace a bad connection with a fresh one."""
        with self._lock:
            try:
                if old_conn:
                    old_conn.close()
            except:
                pass
            
            # Create a fresh lazy connection
            fresh_conn = LazySubstrateConnection(self.url, self._block_hash)
            self.connections.put(fresh_conn)
            return fresh_conn

# Alternative: Just use basic connections without pre-initialization
class SimpleSubstrateConnectionPool:
    """Simpler pool that creates basic connections only."""
    
    def __init__(self, size: int = DEFAULT_POOL_SIZE):
        self.size = size
        self.connections = Queue()
        self.url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        
    def initialize(self, block_hash=None):
        """Create connections without runtime init."""
        logging.info(f"Creating {self.size} substrate connections (simple)...")
        
        # Just create the connections, no runtime init
        t0 = time.time()
        for i in range(self.size):
            try:
                conn = SubstrateInterface(self.url)
                self.connections.put(conn)
            except Exception as e:
                logging.error(f"Failed to create connection {i+1}: {e}")
        
        t1 = time.time()
        actual_size = self.connections.qsize()
        logging.info(f"Created {actual_size}/{self.size} connections in {t1-t0:.2f}s total")
    
    def get(self):
        """Get a connection from the pool."""
        conn = self.connections.get()
        # Caller is responsible for init_runtime if needed
        return conn
    
    def put(self, conn):
        """Return a connection to the pool."""
        self.connections.put(conn)

# Global pool instance
_pool = None

# ---------------------------------------------------------------------------
# Public helper to obtain the global pool. If size is not explicitly provided
# we fall back to `DEFAULT_POOL_SIZE`.
# ---------------------------------------------------------------------------

def get_pool(block_hash=None, use_lazy=True, size: int = DEFAULT_POOL_SIZE):
    global _pool
    if _pool is None:
        if use_lazy:
            _pool = SubstrateConnectionPool(size=size)
        else:
            _pool = SimpleSubstrateConnectionPool(size=size)
        _pool.initialize(block_hash)
    return _pool