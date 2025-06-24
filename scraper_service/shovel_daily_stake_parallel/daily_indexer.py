#!/usr/bin/env python3
"""
Subtensor daily-stake indexer (Substrate-only)
================================================
• Streams blocks from an archive node starting at START_BLOCK
• Watches `SubtensorModule` events that change stake:
    – StakeAdded / AggregatedStakeAdded              (+amount)
    – StakeRemoved / AggregatedStakeRemoved          (-amount)
    – StakeTransferred                               (-/+) amount
    – StakeSwapped                                   (netuid change: no effect on total)
• Maintains an in-memory  `coldkey → total_tao_rao` map
• Emits one CSV snapshot per UTC-day to OUT_DIR/YYYY-MM-DD.csv

Heavy baseline queries or EVM precompile are **not** used; the entire state is
reconstructed incrementally from genesis (or any `START_BLOCK`).  Re-running the
script from the last written snapshot is idempotent because snapshots are
written *after* all blocks of a day have been processed.

Usage
-----
$ python stake_daily_indexer.py               # starts at START_BLOCK
$ python stake_daily_indexer.py --resume      # continues from last CSV date

Requirements
------------
  pip install substrate-interface==1.9.3 websocket-client requests tqdm python-dateutil
"""
from __future__ import annotations

import argparse
import csv
import os
import ssl
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, date
from pathlib import Path
from typing import DefaultDict, Dict

import websocket
from dateutil import tz
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from substrateinterface.utils.ss58 import ss58_encode
from tqdm import tqdm

# ---------------- configuration --------------------------------------------
WS_URL       = os.getenv("SUBTENSOR_ARCHIVE_WS", "wss://archive.chain.opentensor.ai:443")
START_BLOCK  = int(os.getenv("START_BLOCK", "0"))
OUT_DIR      = Path(os.getenv("OUT_DIR", "stake_snapshots"))
PAGE_SIZE    = 2000                       # used only for occasional map queries

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def fresh_conn() -> SubstrateInterface:
    """Always give a fresh websocket – keeps long runs healthy."""
    return SubstrateInterface(WS_URL, auto_reconnect=True)


def block_ts(substrate: SubstrateInterface, block_hash: str) -> datetime:
    """Return the block timestamp as UTC datetime."""
    ts_ms = substrate.query("Timestamp", "Now", block_hash=block_hash).value
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


STAKE_EVENTS_ADD = {
    "StakeAdded", "AggregatedStakeAdded",
}
STAKE_EVENTS_REMOVE = {
    "StakeRemoved", "AggregatedStakeRemoved",
}
STAKE_EVENTS_MOVE = {
    "StakeTransferred",  # origin coldkey → destination coldkey
}
# stake_swapped keeps same cold-key total → ignored


# ---------------------------------------------------------------------------
# core logic
# ---------------------------------------------------------------------------

def iter_blocks(from_height: int):
    """Generator (height, hash, substrate_conn)."""
    sub = fresh_conn()
    latest = sub.get_block_number(sub.get_chain_head())
    h = from_height
    pbar = tqdm(total=latest - from_height + 1, desc="scan blocks", unit="blk")
    while h <= latest:
        try:
            bh = sub.get_block_hash(h)
            yield h, bh, sub
            h += 1
            pbar.update(1)
        except websocket.WebSocketException:
            sub = fresh_conn()
            continue
        except SubstrateRequestException as e:
            print(f"substrate RPC error at height {h}: {e}; retrying …", file=sys.stderr)
            time.sleep(0.5)
            continue
    pbar.close()


# ---------------------------------------------------------------------------
# snapshot helpers
# ---------------------------------------------------------------------------

def snapshot_filename(day: date) -> Path:
    OUT_DIR.mkdir(exist_ok=True, parents=True)
    return OUT_DIR / f"{day.isoformat()}.csv"


def load_last_totals() -> tuple[int, Dict[str, int]]:
    """If --resume, read the latest CSV so we can continue."""
    if not OUT_DIR.exists():
        return START_BLOCK, {}
    files = sorted(OUT_DIR.glob("*.csv"))
    if not files:
        return START_BLOCK, {}
    last_file = files[-1]
    last_height = int(last_file.stem.split("-")[-1]) if "-" in last_file.stem else START_BLOCK
    totals: Dict[str, int] = {}
    with last_file.open() as fh:
        for row in csv.reader(fh):
            ck, val = row
            totals[ck] = int(val)
    return last_height + 1, totals  # start at the very next block


def write_snapshot(day: date, height: int, totals: Dict[str, int]):
    fname = snapshot_filename(day).with_name(f"{day.isoformat()}-{height}.csv")
    with fname.open("w", newline="") as fh:
        wr = csv.writer(fh)
        for ck, val in totals.items():
            wr.writerow([ck, val])
    print(f"✔ wrote snapshot {fname}  ({len(totals)} cold-keys)")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true", help="resume from last written snapshot")
    ap.add_argument("--deltas", action="store_true", help="stream blocks and print stake deltas only (demo mode)")
    ap.add_argument("--start-block", type=int, help="override starting block height")
    args = ap.parse_args()

    if args.deltas:
        start_block = args.start_block if args.start_block is not None else START_BLOCK
        totals = defaultdict(int)
        print(f"[demo] streaming stake deltas from block {start_block} …")
    elif args.resume:
        start_block, totals = load_last_totals()
        print(f"starting from block {start_block} …  cold-keys in memory: {len(totals)}")
    else:
        start_block = args.start_block if args.start_block is not None else START_BLOCK
        totals = {}
        print(f"starting from block {start_block} …  cold-keys in memory: {len(totals)}")

    current_day: date | None = None

    for height, bh, sub in iter_blocks(start_block):
        ts = block_ts(sub, bh)
        day = ts.date()

        # demo mode skips snapshot logic -----------------------------------
        if not args.deltas:
            # roll snapshot at day boundary
            if current_day is None:
                current_day = day
            elif day != current_day:
                write_snapshot(current_day, height - 1, totals)
                current_day = day

        events = sub.get_events(block_hash=bh)
        for ev in events:
            print(height, ev.event_module.name, ev.event.name)   # raw dump
            mod = getattr(ev, "event_module", None)
            if mod is None or mod.name != "SubtensorModule":
                continue
            ev_name = ev.event.name
            print(ev_name)
            if ev_name in STAKE_EVENTS_ADD:
                coldkey_ss58 = ss58_encode(ev.params[0].value)
                amount = ev.params[2].value  # tao in RAO (u64)
                if args.deltas:
                    print(f"{height} {coldkey_ss58} +{amount}")
                totals[coldkey_ss58] += amount
            elif ev_name in STAKE_EVENTS_REMOVE:
                coldkey_ss58 = ss58_encode(ev.params[0].value)
                amount = ev.params[2].value
                if args.deltas:
                    print(f"{height} {coldkey_ss58} -{amount}")
                totals[coldkey_ss58] = max(0, totals[coldkey_ss58] - amount)
            elif ev_name in STAKE_EVENTS_MOVE:
                origin_ck = ss58_encode(ev.params[0].value)
                dest_ck   = ss58_encode(ev.params[1].value)
                amount    = ev.params[5].value
                if args.deltas:
                    print(f"{height} {origin_ck} -{amount}")
                    print(f"{height} {dest_ck} +{amount}")
                totals[origin_ck] = max(0, totals[origin_ck] - amount)
                totals[dest_ck]   += amount
            # StakeSwapped keeps same coldkey – total unchanged → ignore

    if not args.deltas and current_day:
        # write final snapshot at tip
        write_snapshot(current_day, height, totals)


if __name__ == "__main__":
    main()
