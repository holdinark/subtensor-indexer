"""Fast stake snapshot using chain-side map iteration.

This script computes the total stake (in RAO) controlled by each cold-key on
Bittensor/Subtensor.  It avoids the extremely slow Python construction of
individual `StorageKey`s by streaming the storage maps directly via
`query_map()`.  Runtime on a typical RPC endpoint is a few minutes instead of
many hours.

Requirements
------------
    pip install substrate-interface==1.9.3 websocket-client requests
"""

from __future__ import annotations

import logging
import math
import os
import ssl
import time
from collections import defaultdict
from typing import Dict, List, Tuple

from substrateinterface import SubstrateInterface
from websocket import WebSocketException
from substrateinterface.utils.ss58 import ss58_decode
from xxhash import xxh64
import struct

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------------- configuration ------------------------------
WS_URL = os.getenv("SUBTENSOR_WS", "wss://private.chain.opentensor.ai:443")
# Maximum page size accepted by most substrate RPC nodes is 1000.  Use the
# smaller of env-override and 1000 to avoid "count exceeds maximum value" errors.
PAGE_SZ = min(int(os.getenv("PAGE_SZ", "1000")), 1000)
# batch size for query_multi
BATCH_SZ = int(os.getenv("BATCH_SZ", "20000"))
# ----------------------------------------------------------------------

U64F64_FACTOR = 1 << 64  # 2^64 (used to convert fixed-point numbers)


# ------------------------- helpers ------------------------------------

def u64f64_to_float(value: int) -> float:
    """Convert substrate fixed-point U64F64 (unsigned 128-bit) to python float."""
    return value / U64F64_FACTOR


def extract_value(obj):
    """Recursively unwrap `.value` attributes produced by ScaleTypes."""
    while hasattr(obj, "value"):
        obj = obj.value
    return obj


# helper to coerce decoded objects to int safely
def to_int(maybe_int):
    try:
        return int(maybe_int)
    except (TypeError, ValueError):
        if isinstance(maybe_int, dict):
            # attempt to grab first numeric-looking value in dict
            for v in maybe_int.values():
                try:
                    return int(v)
                except (TypeError, ValueError):
                    continue
        raise


class ChainReader:
    """Thin wrapper around SubstrateInterface with simple caches."""

    def __init__(self, url: str):
        self.substrate = SubstrateInterface(url)
        self.price_cache: Dict[int, float] = {}

    # -------------------- chain primitives ---------------------------
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
        price_raw = self.substrate.query("SubtensorModule", "get_alpha_price", [netuid])
        price = float(extract_value(price_raw))
        self.price_cache[netuid] = price
        return price

    # -------------------- staking map ---------------------------
    def staking_hotkeys(self) -> List[Tuple[str, List[str]]]:
        """Return list of (cold_ss58, [hot_ss58, …])."""
        records = self.substrate.query_map(
            "SubtensorModule",
            "StakingHotkeys",
            page_size=PAGE_SZ,
        )
        out: List[Tuple[str, List[str]]] = []
        for cold_raw, hot_vec in records:
            cold_ss58 = str(extract_value(cold_raw))
            if hasattr(hot_vec, "value"):
                hot_vec = hot_vec.value
            hot_ss58s = [str(extract_value(h)) for h in hot_vec]
            out.append((cold_ss58, hot_ss58s))
        return out

    # ------------------------ helpers ------------------------
    def _twox128(self, data: bytes) -> bytes:
        return xxh64(data, seed=0).digest() + xxh64(data, seed=1).digest()

    def _twox64concat(self, data) -> bytes:
        """Twox64Concat hash: first 8 bytes of xxhash64 + original bytes.
        Accepts bytes or hex/utf8 string and coerces to bytes.
        """
        if isinstance(data, bytes):
            b = data
        elif isinstance(data, str):
            # attempt hex decoding, fallback to utf-8 bytes
            try:
                b = bytes.fromhex(data)
            except ValueError:
                b = data.encode()
        else:
            b = bytes(data)
        return xxh64(b, seed=0).digest()[:8] + b

    def _alpha_key(self, hot: str, cold: str, netuid: int) -> str:
        pallet_prefix = self._twox128(b"SubtensorModule")
        item_prefix = self._twox128(b"Alpha")
        hot_part = self._twox64concat(ss58_decode(hot))
        cold_part = self._twox64concat(ss58_decode(cold))
        netuid_part = self._twox64concat(struct.pack("<H", netuid))
        return "0x" + (pallet_prefix + item_prefix + hot_part + cold_part + netuid_part).hex()

    class _SimpleScale:
        def __init__(self, val: int):
            self.value = val

    def batch_query(self, calls, *, label: str | None = None):
        """Custom batch query using raw RPC and hand-crafted storage keys."""
        print("called batch_query")

        results: List = []
        total = len(calls)
        if total == 0:
            return results

        total_chunks = math.ceil(total / BATCH_SZ)
        # Track timing over groups of 50 chunks instead of each individual RPC call.
        group_start = time.time()
        chunks_in_group = 0  # how many chunks have been processed in current timing group

        for i in range(0, total, BATCH_SZ):
            chunk = calls[i : i + BATCH_SZ]
            chunk_num = i // BATCH_SZ + 1

            hex_keys: List[str] = []
            for call in chunk:
                mod = call["module"]
                item = call["storage_function"]
                params = call.get("params", [])

                if mod != "SubtensorModule" or item != "Alpha" or len(params) != 3:
                    raise ValueError("batch_query only supports Alpha calls of form (hot, cold, netuid)")

                hot, cold, netuid = params
                hex_keys.append(self._alpha_key(hot, cold, int(netuid)))

            if label:
                logging.info(f"[{label}] chunk {chunk_num}/{total_chunks} – querying {len(hex_keys)} keys")

            try:
                rpc_result = self.substrate.rpc_request("state_queryStorageAt", [hex_keys, None])
                changes = rpc_result["result"][0]["changes"]
                val_map = {k: v for k, v in changes}
            except Exception as e:
                logging.error(f"state_queryStorageAt failed: {e}")
                val_map = {}

            # Accumulate chunk count and, once we hit 50 chunks, log the elapsed time.
            chunks_in_group += 1
            if chunks_in_group == 50:
                elapsed = time.time() - group_start
                if label:
                    logging.info(f"[{label}] 50 chunks completed in {elapsed:.2f}s ({50 / elapsed:.1f} chunks/s)")
                else:
                    logging.info(f"50 chunks completed in {elapsed:.2f}s ({50 / elapsed:.1f} chunks/s)")
                # reset counters for next group
                group_start = time.time()
                chunks_in_group = 0

            for key_hex in hex_keys:
                v_hex = val_map.get(key_hex)
                if v_hex is None:
                    results.append(None)
                else:
                    # decode U64F64
                    try:
                        hex_body = v_hex[2:] if v_hex.startswith("0x") else v_hex
                        val_int = int.from_bytes(bytes.fromhex(hex_body), "little")
                        results.append(self._SimpleScale(val_int))
                    except Exception:
                        results.append(self._SimpleScale(0))

        return results


# ----------------------------- main -----------------------------------


def main():
    chain = ChainReader(WS_URL)

    logging.info("Fetching active subnets …")
    time_before = time.time()
    netuids = chain.active_subnets()
    time_after = time.time()
    logging.info(f"Active subnets: {netuids} in {time_after - time_before} seconds")

    # -------------------------------------------------
    # Enumerate cold→hot key relations once.
    # -------------------------------------------------
    logging.info("Enumerating StakingHotkeys …")
    time_before = time.time()
    staking_entries = chain.staking_hotkeys()  # List[(cold_ss58, [hot_ss58,…])]
    time_after = time.time()
    logging.info(f"Total cold-keys: {len(staking_entries):,} in {time_after - time_before} seconds")

    # -------------------------------------------------
    # Pre-load TotalHotkeyAlpha & Shares for all (hot, net) pairs.
    # -------------------------------------------------
    logging.info("Pre-loading TotalHotkeyAlpha …")
    hot_alpha: Dict[Tuple[str, int], int] = {}
    t0 = time.time()
    entries = chain.substrate.query_map("SubtensorModule", "TotalHotkeyAlpha", page_size=PAGE_SZ)
    time_after = time.time()
    logging.info(f"TotalHotkeyAlpha: {len(list(entries)):,} in {time_after - time_before} seconds")
    time_before = time.time()
    for (hot, netuid_raw), alpha_val in entries:
        netuid = int(extract_value(netuid_raw))
        hot_alpha[(str(extract_value(hot)), netuid)] = to_int(extract_value(alpha_val))
    time_after = time.time()
    logging.info(f"Loaded {len(hot_alpha):,} TotalHotkeyAlpha entries in {time_after - time_before:.1f}s")

    logging.info("Pre-loading TotalHotkeyShares …")
    hot_shares: Dict[Tuple[str, int], float] = {}
    t0 = time.time()
    entries = chain.substrate.query_map("SubtensorModule", "TotalHotkeyShares", page_size=PAGE_SZ)
    time_after = time.time()
    logging.info(f"TotalHotkeyShares: {len(list(entries))}:, in {time_after - time_before} seconds")
    time_before = time.time()
    for (hot, netuid_raw), shares_val in entries:
        netuid = int(extract_value(netuid_raw))
        try:
            shares_int = to_int(extract_value(shares_val))
        except Exception:
            continue  # skip malformed entry
        shares_float = u64f64_to_float(shares_int)
        hot_shares[(str(extract_value(hot)), netuid)] = shares_float
    time_after = time.time()
    logging.info(f"Loaded {len(hot_shares):,} TotalHotkeyShares entries in {time_after - time_before:.1f}s")

    # -------------------------------------------------
    # Stream Alpha map and accumulate stake per cold-key
    # -------------------------------------------------
    logging.info("Streaming Alpha map …")
    t0 = time.time()
    cold_totals: Dict[str, int] = defaultdict(int)
    processed = 0
    last_log_ts = t0
    LOG_EVERY = int(os.getenv("LOG_EVERY", "100000"))  # pairs between logs
    SUB_LOG_EVERY = int(os.getenv("SUB_LOG_EVERY", "50000"))

    # 1. Build all Alpha keys we still need
    calls = []
    key_info = []          # keep (hot, cold, netuid) alongside its StorageKey
    print(f"building calls.... with {len(staking_entries)} cold keys")
    for cold, hotkeys in staking_entries:        # from chain.staking_hotkeys()
        for hot in hotkeys:
            for netuid in netuids:
                key_info.append((hot, cold, netuid))
                calls.append({
                    "module": "SubtensorModule",
                    "storage_function": "Alpha",
                    "params": [hot, cold, netuid],
                })

    # 2. Fetch in batches (batch_query already handles paging & reconnect)
    alpha_values = chain.batch_query(calls, label="Alpha full scan")

    # 3. Iterate results
    for (hot, cold, netuid), scale_obj in zip(key_info, alpha_values):
        share_fp = int(getattr(scale_obj, "value", 0) or 0)
        if share_fp == 0:
            continue
        share = u64f64_to_float(share_fp)
        hot_id = str(extract_value(hot))

        hot_alpha_int = hot_alpha.get((hot_id, netuid))
        total_hot_shares = hot_shares.get((hot_id, netuid))
        if hot_alpha_int is None or not total_hot_shares:
            continue

        stake_alpha = share * hot_alpha_int / total_hot_shares
        stake_rao = int(stake_alpha * chain.alpha_price(netuid) * 1e9)
        cold_totals[str(extract_value(cold))] += stake_rao

        processed += 1
        now = time.time()
        if processed % LOG_EVERY == 0 or (now - last_log_ts) >= 30:
            rate = processed / (now - t0)
            logging.info(
                f"… processed {processed:,} Alpha pairs | {rate:,.0f} pairs/s | "
                f"elapsed {(now - t0):.0f}s"
            )
            last_log_ts = now

        if processed % SUB_LOG_EVERY == 0:
            logging.info(f"[netuid:{netuid}] … {processed:,} pairs so far")

    logging.info(f"Finished streaming Alpha map – {processed:,} entries in {time.time() - t0:.1f}s")

    # ------------------- output -----------------------
    for cold, rao in sorted(cold_totals.items(), key=lambda kv: kv[1], reverse=True):
        tao = rao / 1e9
        print(f"{cold}: {tao:,.3f} TAO  ({rao} RAO)")

    logging.info("done.")


if __name__ == "__main__":
    main() 