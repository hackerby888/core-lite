#!/usr/bin/env python3
"""Find wrong-solution txs for a Qubic identity.

Hybrid: public RPC (rpc.qubic.org) enumerates candidate txs by filter; Bob
indexer provides log counts per tx hash. RPC's archive query is much faster
than chunk-scanning ticks; Bob's /tx returns logIdFrom/logIdTo directly so we
don't need /log walking.

  Phase A  POST <rpc>/query/v1/getTransactionsForIdentity
           filters: destination=burn, amount=1M; ranges: timestamp window
           -> candidate {hash, tickNumber, timestamp, ...}

  Phase B  GET <bob>/tx/{hash}
           -> logIdFrom, logIdTo, executed
           log_count = logIdTo - logIdFrom + 1 (both >= 0)
           wrong-solution iff log_count == 1

Public RPC is rate-limited (per-IP token bucket): conservative pacing + 429
backoff + circuit breaker. Bob is operator-owned: light pacing only.
"""

import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    sys.stderr.write("error: this script requires the `requests` package (pip install requests)\n")
    sys.exit(2)


DEFAULT_RPC = "https://rpc.qubic.org"
BURN_ADDRESS = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB"
DEFAULT_AMOUNT = "1000000"
USER_AGENT = "qubic-wrong-solution-scan/0.2"
RPC_PAGE_SIZE = 1000
RPC_HITS_CAP = 10000


WINDOW_RE = re.compile(r"^(\d+)([dhm])$")


def parse_window_ms(s):
    m = WINDOW_RE.match(s.strip())
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid --window '{s}'; expected like 1d, 3d, 7d, 12h, 30m"
        )
    n = int(m.group(1))
    unit_ms = {"d": 86_400_000, "h": 3_600_000, "m": 60_000}[m.group(2)]
    return n * unit_ms


def validate_identity(s, label="identity"):
    if not (isinstance(s, str) and len(s) == 60 and s.isascii() and s.isalpha()):
        raise SystemExit(f"error: {label} must be 60 letters A-Z (any case), got {s!r}")
    return s


def iso_ms(ms):
    try:
        ms = int(ms)
    except (TypeError, ValueError):
        return "?"
    if ms == 0:
        return "?"
    if ms < 10_000_000_000:  # looks like seconds, not ms
        ms *= 1000
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class RpcClient:
    """Public RPC client. Aggressive rate-limit handling."""

    def __init__(self, base, max_workers, verbose=False):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Content-Type": "application/json"})
        self.verbose = verbose
        self._lock = threading.Lock()
        self._next_slot = 0.0
        self._gap = 0.1  # 100 ms global gap
        self._consec_429 = 0
        self._max_workers = max_workers
        self._tripped = False

    def _pace(self):
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_slot - now)
            self._next_slot = max(now, self._next_slot) + self._gap
        if wait:
            time.sleep(wait)

    def _on_429(self, retry_after):
        with self._lock:
            self._consec_429 += 1
            tripped_now = self._consec_429 >= 3 and not self._tripped
            if tripped_now:
                self._tripped = True
                new_workers = max(1, self._max_workers // 2)
                sys.stderr.write(
                    f"\n[rate-limit] 3 consecutive 429s; pausing 30s, "
                    f"workers {self._max_workers}->{new_workers}\n"
                )
                self._max_workers = new_workers
        if tripped_now:
            time.sleep(30.0)
        if retry_after:
            try:
                time.sleep(min(60.0, float(retry_after)))
            except (TypeError, ValueError):
                pass

    def _on_ok(self):
        with self._lock:
            self._consec_429 = 0

    def post(self, path, body):
        url = f"{self.base}{path}"
        for attempt in range(6):
            self._pace()
            try:
                r = self.session.post(url, data=json.dumps(body), timeout=30)
            except requests.RequestException as e:
                if attempt >= 3:
                    raise SystemExit(f"error: rpc POST {path} network: {e}")
                delay = min(8.0, 0.5 * (2 ** attempt))
                if self.verbose:
                    sys.stderr.write(f"[rpc net] {e}; retry in {delay:.1f}s\n")
                time.sleep(delay)
                continue
            if r.status_code == 200:
                self._on_ok()
                return r.json()
            if r.status_code == 429:
                self._on_429(r.headers.get("Retry-After"))
                if self.verbose:
                    sys.stderr.write(f"[rpc 429] attempt {attempt+1}\n")
                continue
            if 500 <= r.status_code < 600 and attempt < 3:
                time.sleep(min(8.0, 0.5 * (2 ** attempt)))
                continue
            raise SystemExit(
                f"error: rpc POST {path} -> {r.status_code} {r.reason}\n{r.text[:500]}"
            )
        raise SystemExit(f"error: rpc POST {path} exhausted retries")


class BobClient:
    """Bob indexer client. Light pacing, 5xx backoff."""

    def __init__(self, base, verbose=False):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.verbose = verbose
        self._lock = threading.Lock()
        self._next_slot = 0.0
        self._gap = 0.02  # 20 ms global gap (bob is local/operator)

    def _pace(self):
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_slot - now)
            self._next_slot = max(now, self._next_slot) + self._gap
        if wait:
            time.sleep(wait)

    def get(self, path):
        url = f"{self.base}{path}"
        for attempt in range(4):
            self._pace()
            try:
                r = self.session.get(url, timeout=30)
            except requests.RequestException as e:
                if attempt >= 2:
                    raise SystemExit(f"error: bob GET {path} network: {e}")
                time.sleep(0.5 * (2 ** attempt))
                continue
            if r.status_code == 200:
                return r.json()
            if 500 <= r.status_code < 600 and attempt < 2:
                if self.verbose:
                    sys.stderr.write(f"[bob {r.status_code}] {path} retry\n")
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise SystemExit(
                f"error: bob GET {path} -> {r.status_code} {r.reason}\n{r.text[:500]}"
            )
        raise SystemExit(f"error: bob GET {path} exhausted retries")


# ----- Phase A: enumerate candidates from RPC ------------------------------


def rpc_enumerate(rpc, identity, burn, amount, start_ms, end_ms):
    out = []
    _enum(rpc, identity, burn, amount, start_ms, end_ms, out)
    sys.stderr.write(f"[rpc] candidates: {len(out)}\n")
    return out


def _enum(rpc, identity, burn, amount, start_ms, end_ms, out):
    body = {
        "identity": identity,
        "filters": {"destination": burn, "amount": amount},
        "ranges": {"timestamp": {"gte": str(start_ms), "lte": str(end_ms)}},
        "pagination": {"offset": 0, "size": RPC_PAGE_SIZE},
    }
    resp = rpc.post("/query/v1/getTransactionsForIdentity", body)
    total = int((resp.get("hits") or {}).get("total") or 0)

    if total >= RPC_HITS_CAP and (end_ms - start_ms) > 60_000:
        mid = (start_ms + end_ms) // 2
        sys.stderr.write(
            f"[bisect] total>={RPC_HITS_CAP} in [{iso_ms(start_ms)}..{iso_ms(end_ms)}]; splitting\n"
        )
        _enum(rpc, identity, burn, amount, start_ms, mid, out)
        _enum(rpc, identity, burn, amount, mid + 1, end_ms, out)
        return

    out.extend(resp.get("transactions") or [])
    fetched = len(resp.get("transactions") or [])
    offset = RPC_PAGE_SIZE
    while fetched < total and offset < RPC_HITS_CAP:
        body["pagination"] = {"offset": offset, "size": RPC_PAGE_SIZE}
        resp = rpc.post("/query/v1/getTransactionsForIdentity", body)
        chunk = resp.get("transactions") or []
        if not chunk:
            break
        out.extend(chunk)
        fetched += len(chunk)
        offset += RPC_PAGE_SIZE


# ----- Phase B: log count from Bob -----------------------------------------


def bob_log_count(bob, tx_hash):
    info = bob.get(f"/tx/{tx_hash}")
    lf = info.get("logIdFrom")
    lt = info.get("logIdTo")
    try:
        lf = int(lf)
        lt = int(lt)
    except (TypeError, ValueError):
        return None, info
    if lf < 0 or lt < 0 or lt < lf:
        return 0, info
    return lt - lf + 1, info


# ----- main ----------------------------------------------------------------


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bob-rpc", required=True, help="Bob REST base URL, e.g. http://144.76.234.62:40420")
    p.add_argument("--rpc", default=DEFAULT_RPC, help=f"public RPC base URL (default {DEFAULT_RPC})")
    p.add_argument("--identity", required=True, help="60-letter identity")
    p.add_argument("--window", default="1d", help="time window: 1d, 3d, 7d, 12h, 30m")
    p.add_argument("--amount", default=DEFAULT_AMOUNT, help=f"exact tx amount in QUS (default {DEFAULT_AMOUNT})")
    p.add_argument("--burn-address", default=BURN_ADDRESS, help="destination identity (default zero-pubkey)")
    p.add_argument("--rpc-workers", type=int, default=2, help="RPC parallelism (default 2, cap 4)")
    p.add_argument("--bob-workers", type=int, default=8, help="Bob /tx parallelism (default 8, cap 32)")
    p.add_argument("--out", help="write CSV to this path (default: TSV to stdout)")
    p.add_argument("--verbose", action="store_true", help="log retries/backoff to stderr")
    args = p.parse_args()

    identity = validate_identity(args.identity, "--identity")
    burn = validate_identity(args.burn_address, "--burn-address")
    rpc_workers = max(1, min(4, args.rpc_workers))
    bob_workers = max(1, min(32, args.bob_workers))

    window_ms = parse_window_ms(args.window)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - window_ms

    sys.stderr.write(
        f"[scan] identity={identity} window={args.window} "
        f"({iso_ms(start_ms)} .. {iso_ms(now_ms)})\n"
        f"[scan] rpc={args.rpc} bob={args.bob_rpc}\n"
    )

    rpc = RpcClient(args.rpc, rpc_workers, verbose=args.verbose)
    bob = BobClient(args.bob_rpc, verbose=args.verbose)

    t0 = time.monotonic()
    candidates = rpc_enumerate(rpc, identity, burn, args.amount, start_ms, now_ms)
    sys.stderr.write(f"[rpc] done in {time.monotonic() - t0:.1f}s\n")

    wrong = []
    done = 0
    last_progress = time.monotonic()
    t1 = time.monotonic()

    def task(tx):
        n, info = bob_log_count(bob, tx["hash"])
        return tx, info, n

    with ThreadPoolExecutor(max_workers=bob_workers) as ex:
        futures = [ex.submit(task, tx) for tx in candidates]
        for fut in as_completed(futures):
            tx, info, n_logs = fut.result()
            done += 1
            if n_logs == 1:
                wrong.append((tx, info))
            now = time.monotonic()
            if now - last_progress >= 5.0:
                sys.stderr.write(f"[bob] {done}/{len(candidates)} checked, wrong={len(wrong)}\n")
                last_progress = now

    sys.stderr.write(f"[bob] done in {time.monotonic() - t1:.1f}s\n")

    wrong.sort(key=lambda pair: int(pair[0].get("tickNumber") or 0))

    cols = ["tick", "timestamp_iso", "hash", "logIdFrom", "logIdTo", "executed"]
    rows = []
    for tx, info in wrong:
        rows.append(
            [
                tx.get("tickNumber"),
                iso_ms(tx.get("timestamp") or 0),
                tx.get("hash"),
                info.get("logIdFrom"),
                info.get("logIdTo"),
                info.get("executed"),
            ]
        )

    if args.out:
        with open(args.out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
    else:
        for row in rows:
            sys.stdout.write("\t".join(str(c) for c in row) + "\n")

    sys.stderr.write(f"[done] candidates={len(candidates)} wrong={len(wrong)}\n")


if __name__ == "__main__":
    main()
