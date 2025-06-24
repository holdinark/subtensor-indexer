# stake_total_snapshot.py
"""
Fetch total stake (RAO) controlled by each cold-key on the live Bittensor / Subtensor
network.  Uses the correct delegation-aware formula

    stake_alpha = Alpha_share * TotalHotkeyAlpha / TotalHotkeyShares
    stake_rao   = stake_alpha * alpha_price

All chain RPC reads are batched with `query_multi` to minimise round-trips.

Requirements
------------
    pip install substrate-interface==1.9.3 websocket-client requests
"""

from __future__ import annotations

import json
import math
import os
import ssl
import time
import logging
from typing import Dict, List, Tuple

from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from websocket import WebSocketException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------------- configuration ------------------------------
WS_URL = "wss://private.chain.opentensor.ai:443"
PAGE_SZ = 1000                # pagination when scanning StakingHotkeys
BATCH_SZ = 500                # maximum storage calls per query_multi batch
# Limit the number of Alpha storage reads per coldkey during testing
ALPHA_TEST_LIMIT = 100  # set to 0 or None to disable limiting

# Local on-disk cache for the full StakingHotkeys map.  Fetching all cold-keys
# over RPC can take multiple minutes and place significant load on the node.
# With ~65 k cold-keys the data does not change very frequently, so we cache the
# result for a day.
CACHE_FILE = os.getenv("STAKING_HOTKEYS_CACHE", "staking_hotkeys.json")
CACHE_TTL_SEC = int(os.getenv("STAKING_HOTKEYS_CACHE_TTL", "86400"))  # 24 h

# ----------------------------------------------------------------------

U64F64_FACTOR = 1 << 64  # 2^64  (used to convert fixed-point numbers)


def u64f64_to_float(value: int) -> float:
    """Convert substrate fixed-point U64F64 (unsigned 128-bit) to python float."""
    return value / U64F64_FACTOR


def extract_value(obj):
    """substrate-interface wraps SCALE primitives – unwrap if needed."""
    if obj is None:
        return 0
    return getattr(obj, "value", obj)


class ChainReader:
    def __init__(self, url: str):
        self.url = url
        self.substrate = SubstrateInterface(url)
        self.price_cache: Dict[int, float] = {}
        self.metrics_cache: Dict[Tuple[str, int], Tuple[int, float]] = {}

    # ------------------------ helpers ------------------------
    def batch_query(self, calls, *, label: str | None = None):
        """Execute multiple storage reads in batches, preserving input order.

        Accepts either of the following call formats:
          • dict-style: {"module": "SubtensorModule", "storage_function": "Alpha", "params": [...]} (legacy)
          • tuple-style: ("SubtensorModule", "Alpha", [...])

        Returns a list of SCALE objects (or `None` on failure) matching the
        order of the provided `calls` list.
        """

        results: List = []
        key_cache: dict = {}

        # -------------------------------------------------------------
        # Measure time spent **solely** on assembling StorageKey objects.
        # This gives visibility into the cost of SCALE key b ut lgeneration which
        # can dominate overall latency for large batches.
        # -------------------------------------------------------------
        total_key_build_time: float = 0.0

        total = len(calls)
        if total == 0:
            return results

        total_chunks = math.ceil(total / BATCH_SZ)

        print(f"len calls: {total} total_chunks: {total_chunks}")
        for i in range(0, total, BATCH_SZ):
            chunk = calls[i : i + BATCH_SZ]
            chunk_num = i // BATCH_SZ + 1

            # --- Build (or reuse) StorageKey objects --------------------
            t_build_start = time.time()
            storage_keys = []
            for idx,call in enumerate(chunk):
                # Support both dict and tuple style
                if isinstance(call, dict):
                    mod = call["module"]
                    item = call["storage_function"]
                    params = call.get("params", [])
                else:
                    mod, item, params = call

                cache_key = (mod, item, tuple(params))
                sk = key_cache.get(cache_key)
                if sk is None:
                    try:
                        sk = self.substrate.create_storage_key(mod, item, params)
                    except Exception as e:
                        logging.error(f"Failed creating storage key for {cache_key}: {e}")
                        sk = None
                    key_cache[cache_key] = sk
                storage_keys.append(sk)

            # Accumulate build time for this chunk
            total_key_build_time += time.time() - t_build_start

            # Remove None keys (failed creations) but keep alignment with placeholders
            valid_pairs = [(idx, sk) for idx, sk in enumerate(storage_keys) if sk is not None]

            # --- Query this batch, retrying on socket drops -------------
            try_count = 0
            while True:
                try_count += 1
                try:
                    logging.info(f"chunk {chunk_num}/{total_chunks} – {len(valid_pairs)} keys (attempt {try_count})")
                    t0 = time.time()

                    if valid_pairs:
                        # Perform query on valid storage keys
                        time_before = time.time()
                        print(f"querying multi on {len(valid_pairs)} keys")
                        kv_pairs = self.substrate.query_multi([sk for _, sk in valid_pairs])
                        time_after = time.time()
                        print(f"time taken to query multi: {time_after - time_before} seconds")
                        val_map = {k.to_hex(): v for k, v in kv_pairs}
                    else:
                        val_map = {}

                    t1 = time.time()
                    logging.info(f"chunk {chunk_num} done in {t1 - t0:.2f}s")

                    # Reconstruct results preserving order (None for failures)
                    for sk in storage_keys:
                        if sk is None:
                            results.append(None)
                        else:
                            results.append(val_map.get(sk.to_hex()))
                    break  # success

                except (ssl.SSLError, WebSocketException):
                    logging.warning("socket closed – reconnecting …")
                    self._reconnect()
                    continue
                except Exception as e:
                    logging.error(f"query_multi failed: {e}")
                    # Append None for entire chunk to maintain alignment
                    results.extend([None] * len(chunk))
                    break

        # After processing all chunks, report aggregated key-build timing
        if label:
            logging.info(
                f"[{label}] built {len(key_cache)} unique StorageKey objects for {total} calls in "
                f"{total_key_build_time:.2f}s"
            )
        else:
            logging.info(
                f"batch_query: assembled {len(key_cache)} unique StorageKey objects "
                f"for {total} calls in {total_key_build_time:.2f}s"
            )

        return results

    def _reconnect(self):
        self.substrate.close()
        self.substrate = SubstrateInterface(self.url)

    # -------------------- chain primitives -------------------
    def active_subnets(self) -> List[int]:
        """Return list of currently added subnets (includes root net 0)."""
        entries = self.substrate.query_map("SubtensorModule", "NetworksAdded")
        netuids = [int(extract_value(k)) for (k, added) in entries if bool(extract_value(added))]
        if 0 not in netuids:
            netuids.insert(0, 0)
        return netuids

    def alpha_price(self, netuid: int) -> float:
        """Get (and cache) price of α on a subnet in TAO."""
        if netuid in self.price_cache:
            return self.price_cache[netuid]
        logging.info(f"[netuid:{netuid}] fetching alpha price (cache miss)")
        price = extract_value(
            self.substrate.query("SubtensorModule", "get_alpha_price", [netuid])
        )
        # substrate-interface calls runtime functions via state_call; result is str/int
        price_f = float(price)
        self.price_cache[netuid] = price_f
        return price_f

    # -------------- staking maps (batched access) -----------
    def staking_hotkeys(self) -> List[Tuple[str, List[str]]]:
        """Return list of (cold_ss58, [hot_ss58, …]).

        Uses a JSON cache on disk to avoid refetching the entire map on every
        execution.  Cache is considered fresh if its mtime is younger than
        `CACHE_TTL_SEC` (24 h by default).
        """

        # ---------- fast path: load from cache if recent ------------------
        try:
            if os.path.exists(CACHE_FILE):
                age = time.time() - os.path.getmtime(CACHE_FILE)
                if age < CACHE_TTL_SEC:
                    with open(CACHE_FILE, "r", encoding="utf-8") as fh:
                        cached_data = json.load(fh)
                    logging.info(
                        f"Loaded {len(cached_data)} coldkeys from cache '{CACHE_FILE}' (age {age/3600:.1f} h)"
                    )
                    # Ensure correct typing (tuple, list)
                    return [(str(cold), list(hots)) for cold, hots in cached_data]
        except Exception as e:
            logging.warning(f"Failed to read staking cache: {e} – proceeding to fetch from chain …")

        # ----------------- fetch from chain (slow path) ------------------
        records, start_key = [], None
        while True:
            res = self.substrate.query_map(
                "SubtensorModule",
                "StakingHotkeys",
                page_size=PAGE_SZ,
                start_key=start_key,
            )
            # Skip duplicated first key if pagination continues
            page_records = res.records[1:] if start_key else res.records
            records.extend(page_records)
            logging.info(f"fetched {len(records)} coldkeys …")
            if getattr(res, "complete", False) or not res.last_key or len(res.records) <= 1:
                break
            start_key = res.last_key

        # convert SCALE -> python types
        out: List[Tuple[str, List[str]]] = []
        for cold_raw, hot_vec in records:
            cold_ss58 = str(extract_value(cold_raw))
            if hasattr(hot_vec, "value"):
                hot_vec = hot_vec.value
            hot_ss58s = [str(extract_value(h)) for h in hot_vec]
            out.append((cold_ss58, hot_ss58s))

        # ---------------------- persist cache -----------------------------
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as fh:
                # Ensure plain JSON-serialisable types
                json.dump([[c, h] for c, h in out], fh)
            logging.info(f"StakingHotkeys cache written to '{CACHE_FILE}'")
        except Exception as e:
            logging.warning(f"Failed to write staking cache: {e}")

        return out

    # ---------------------------------------------------------
    def get_hotkey_metrics(self, hotkey: str, netuid: int) -> Tuple[int, float]:
        """Return (TotalHotkeyAlpha, TotalHotkeyShares_float) for (hot, net)."""
        key = (hotkey, netuid)
        if key in self.metrics_cache:
            return self.metrics_cache[key]
        
        logging.info(f"[{hotkey}:{netuid}] fetching hotkey metrics (cache miss)")

        calls = [
            {
                "module": "SubtensorModule",
                "storage_function": "TotalHotkeyAlpha",
                "params": [hotkey, netuid],
            },
            {
                "module": "SubtensorModule",
                "storage_function": "TotalHotkeyShares",
                "params": [hotkey, netuid],
            },
        ]
        time_before = time.time()
        alpha_raw, shares_raw = self.batch_query(calls)
        time_after = time.time()
        logging.info(f"time taken to get hotkey metrics: {time_after - time_before} seconds")
        alpha_int = int(extract_value(alpha_raw))
        shares_float = u64f64_to_float(int(extract_value(shares_raw)))
        self.metrics_cache[key] = (alpha_int, shares_float)
        return self.metrics_cache[key]

    # ---------------------------------------------------------
    def coldkey_stake_rao(self, cold_ss58: str, hotkeys: List[str], netuids: List[int]) -> int:
        total_rao = 0
        # --- build batch of Alpha share calls we still need ----------------
        calls: List = []
        call_keys: List[Tuple[str, str, int]] = []
        for hot in hotkeys:
            for netuid in netuids:
                call_keys.append((hot, cold_ss58, netuid))
                calls.append(
                    {
                        "module": "SubtensorModule",
                        "storage_function": "Alpha",
                        "params": [hot, cold_ss58, netuid],
                    }
                )

        # ---------------- optional hard-coded limit for performance testing ---------
        if ALPHA_TEST_LIMIT and len(calls) > ALPHA_TEST_LIMIT:
            logging.info(
                f"[DEBUG] Limiting Alpha queries from {len(calls)} to first {ALPHA_TEST_LIMIT} calls for performance test"
            )
            calls = calls[:ALPHA_TEST_LIMIT]
            call_keys = call_keys[:ALPHA_TEST_LIMIT]

        logging.info(f"[{cold_ss58}] creating storage keys for {len(calls)} (hotkey, netuid) pairs …")
        t_query_start = time.time()
        alpha_results = self.batch_query(calls, label=f"coldkey:{cold_ss58}")
        query_duration = time.time() - t_query_start
        logging.info(f"[{cold_ss58}] fetched Alpha for {len(alpha_results)} pairs in {query_duration:.2f}s.")

        # iterate results alongside their descriptors
        processed_count = 0
        for (hot, cold, netuid), alpha_share_raw in zip(call_keys, alpha_results):
            raw_val = extract_value(alpha_share_raw)
            try:
                alpha_share_float = u64f64_to_float(int(raw_val))
            except (TypeError, ValueError):
                logging.debug(f"Skipping non-numeric alpha result for ({hot}, {netuid}): {raw_val}")
                continue

            if alpha_share_float == 0:
                continue

            processed_count += 1
            if processed_count % 500 == 0:
                logging.info(f"[{cold}] ... processed {processed_count}/{len(call_keys)} alpha results")

            hot_alpha_int, total_hot_shares = self.get_hotkey_metrics(hot, netuid)
            if total_hot_shares == 0:
                continue
            stake_alpha = alpha_share_float * hot_alpha_int / total_hot_shares
            stake_rao = int(stake_alpha * self.alpha_price(netuid) * 1e9)
            total_rao += stake_rao
        
        logging.info(f"[{cold_ss58}] processed {processed_count} non-zero alpha results (first batch only).")
        return total_rao


# ----------------------------- main -----------------------------------

def main():
    chain = ChainReader(WS_URL)

    logging.info("Fetching active subnets …")
    netuids = chain.active_subnets()
    logging.info(f"Active subnets: {netuids}")

    logging.info("Enumerating StakingHotkeys …")
    staking_entries = chain.staking_hotkeys()
    logging.info(f"Total cold-keys: {len(staking_entries)}")

    for idx, (cold, hotkeys) in enumerate(staking_entries, 1):
        if not hotkeys:  # Skip if no hotkeys associated
            continue
        logging.info(f"[{idx}/{len(staking_entries)}] Processing coldkey: {cold} with {len(hotkeys)} hotkey(s)")
        total_rao = chain.coldkey_stake_rao(cold, hotkeys, netuids)
        if total_rao:
            tao = total_rao / 1e9
            print(f"{cold}: {tao:,.3f} TAO  ({total_rao} RAO)")

    logging.info("done.")


if __name__ == "__main__":
    main()