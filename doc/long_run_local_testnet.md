# LONG_RUN_LOCAL_TESTNET — design notes

Build mode for a single unattended node running all 676 computors for weeks/months.
Operator docs: README.md "Long-Run Local Testnet". This file holds the *why* behind
non-obvious code choices.

## Tick pacing (`processTick`)

`--tick-duration <ms>` (default 1000, max 30000) paces ticks to a wall-clock period using
an absolute TSC schedule: sleep only the remainder the previous tick didn't consume, so the
inter-tick duration is `max(period, processing time)`. If a tick overruns (e.g. epoch
transition), the schedule restarts from now instead of bursting to catch up. The sleep is
chunked into 100 ms slices so shutdown and the node-state-persist handshake are never
blocked for a whole period. Max 30000 ms because `isTickTimeOut` fires at
5 × TARGET_TICK_DURATION (7000 ms) = 35 s and would discard next-tick data.

The cxxopts option deliberately has no `default_value`: cxxopts defaults don't register in
`count()`, so the guard would never read it — the `tickDurationMs` initializer in
`extensions/overload.h` is the single source of truth.

## Epoch switching

The epoch switches only when the tick buffers are about to run out, or on manual F7 /
`SPECIAL_COMMAND_FORCE_SWITCH_EPOCH`. No time-based rollover. Buffers hold
`LONG_RUN_EPOCH_TICK_CAPACITY` ticks (default 5,184,000 = 60 days at 1 s ticks;
CMake-overridable).

## mainAuxStatus starts at 3 (MAIN&MAIN)

`endEpoch()` swaps the two mode bits at every transition. 3 is the only fixed point:
1 (MAIN&aux) silently becomes aux after one transition and the node stops voting;
0 never ticks at all. `--node-mode` with any value other than 3 logs a warning.

## PAUSE_BEFORE_CLEAR_MEMORY forced 0

Default builds with logging enabled require an operator to press F10 at every epoch
transition before memory is cleared (a drain window for external log readers). An
unattended node can never wait for F10. Consequence: log readers have no drain window
around a transition — fetch an epoch's events before forcing one.

## Transaction-digest hash map shrink (shadow constant in `TransactionsDigestAccess`)

By default the map has one slot per possible transaction
(`MAX_NUMBER_OF_TICKS_PER_EPOCH × NUMBER_OF_TRANSACTIONS_PER_TICK`). With the 60-day
capacity that is 21 billion slots, and under USE_SWAP each insert hashes to a random slot,
touching (and eventually writing) a random ~10 MB swap page — i.e. up to ~10 MB of disk
per distinct transaction. Observed: 160 GB written in ~50k ticks with one exec-fee tx per
tick. A local testnet carries few transactions, so `TransactionsDigestAccess` declares a
same-named member constant at 1/64 of the outer value: every indexing site inside the
struct (hash, probes, bounds) sees the shrunk length with zero changes to those lines.
The allocation sites outside the struct still reserve full size - harmless, reservation
is lazy (PROT_NONE) and only the first 1/64 is ever touched. Disk footprint bounded at
`slots/64 × 40 B` (~13 GB at default capacity).

## Old-epoch state files

Per-epoch files (`spectrum.NNN`, `universe.NNN`, `contractNNNN.NNN`, exec-fee files,
~2 GB per set) are never deleted by the node. With transitions only every ~60 days or on
manual F7, accumulation is negligible; clean up by hand if it ever matters.

## Misc

- `watchAndCheckin` (phones home to api.qubic.global every 31 min, even in TESTNET builds)
  is skipped — a local testnet should not call out.
- RAM: non-LITE ~32 GB RSS observed; combine with TESTNET_LITE_RAM (~7 GB) for small boxes.
- Disk under USE_SWAP at 1 s ticks: ~50 GB/day raw, dominated by vote storage
  (676 × 352 B/tick ≈ 21 GB/day, incompressible); `--swap-compression` roughly halves the
  total since the rest is mostly zeros.
