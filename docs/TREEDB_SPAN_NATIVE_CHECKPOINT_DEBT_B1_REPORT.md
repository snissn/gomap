# TreeDB span-native checkpoint-debt B1 report

Issue: #2794. Parent tracker: #2782.

## Decision

B1 is a **default-off / opt-in-only** checkpoint-debt decision. It does not
change TreeDB runtime defaults, persistent formats, benchmark boundaries, or
hot-path scheduling.

Span-native apply and backlog coalescing remain explicit workload-specific
knobs. They must not be treated as default-ready by #2795, #2787, or #2788
unless a later issue removes the checkpoint-boundary debt with same-host
before/after evidence and no unaccepted throughput/storage/read regression.

## Why no runtime candidate is kept

M2 already identified the dominant c4/c16 checkpoint-inclusive debt as
checkpoint-boundary flush coordination: active background flush wait plus
checkpoint `flush_all` / reducer publish work. Leaf value-log sync was not the
main c4 blocker.

The only narrow scheduler direction that had remote 10MM evidence before B1 was
rejected in M2: checkpoint-request-aware background yielding reduced some active
wait, but moved too much work into checkpoint fallback drains, materially
regressing c4/default throughput and c16 post-run checkpoint latency.

B1 re-audited the current code paths and re-smoked the remaining narrow knob
direction (stricter backlog/writer assist). That direction did not survive local
smoke and would be unsafe to promote into a runtime default or selector without
contradicting the M2 regression evidence.

## Inputs consumed

Primary reports:

- `docs/TREEDB_CHECKPOINT_LATENCY_M2_REPORT.md`
- `docs/TREEDB_SPAN_NATIVE_DEFAULT_ADMISSION_M3_REPORT.md`
- `docs/TREEDB_RANDOM_WRITE_CEILING_BREAKER_M14_REPORT.md`

Remote evidence roots used as predecessor inputs:

- M1 cache admission:
  `/mnt/fast4tb/gomap-profiles/2784_cache_admission_10mm_20260616_175721`
- M1 c4 recheck:
  `/mnt/fast4tb/gomap-profiles/2784_cache_admission_c4_recheck_20260616_181941`
- M2 checkpoint experiment:
  `/mnt/fast4tb/gomap-profiles/2785_checkpoint_10mm_20260616_173405`

Local negative-control smoke root:

- `/tmp/2794_b1_smoke1mm_20260616_170330`

The local smoke is not a same-host acceptance gate; it is only negative evidence
for whether an obvious narrow runtime candidate was worth promoting to the
required remote 10MM matrix.

## Code-path audit

Current checkpoint and flush scheduling already enforce the safety properties
that downstream work must preserve:

- `TreeDB/caching/db.go` `Checkpoint` takes `flushMu` before setting the
  checkpointing flag, records `flushmu_wait` / active-background-flush wait, then
  rotates the mutable memtable and calls `flushAllLocked(true, ...)` for the
  durability boundary.
- `flushAllLocked` drains queued memtables while holding `flushMu`; background
  flushes also hold this mutex, so a checkpoint that arrives during an active
  background pass must wait for that pass before owning the boundary.
- `TreeDB/caching/flush_backlog_coalescing.go` normalizes collection modes and
  intentionally skips backlog coalescing for checkpoint, close, stop-pressure,
  and foreground-assist drains. This keeps M12 fallback behavior fail-closed.
- `flushSpanNativeFallbackReasonForCollectionMode` maps checkpoint/close drains
  to the `close_or_checkpoint` span-native fallback reason. Checkpoint drains
  must not be made span-native/backlog eligible just to hide latency.
- Adaptive backpressure and writer assist can move work from checkpoint into the
  write phase, but doing so is a throughput/latency tradeoff, not free debt
  removal.

## Negative evidence

### M2 rejected-runtime evidence

M2's same-host 10MM experiment showed that yielding active background flushes to
checkpoint requests shifted work into checkpoint fallback drains and regressed
material metrics:

| Row | random_write ops/s | Random checkpoint | Post-run checkpoint | active bg wait total | flush_all total | close/checkpoint fallback ops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `main_span_native_c4` | 287,017 | 9.84s | 10.55s | 26.88s | 16.09s | 218,612 |
| `candidate_span_native_c4` | 240,504 | 9.10s | 9.50s | 12.08s | 38.35s | 6,138,327 |
| `main_span_native_c16` | 325,962 | 7.06s | 9.29s | 22.16s | 17.45s | 711,086 |
| `candidate_span_native_c16` | 346,443 | 6.20s | 13.32s | 19.70s | 29.93s | 4,656,439 |

That is not an acceptable B1 fix: c4 throughput regressed by 16.2%, c16
post-run checkpoint worsened, `flush_all` grew, and checkpoint fallback ops
ballooned.

### B1 local knob smoke

B1 also tested the remaining narrow direction from the prior handoff: tighter
backlog thresholds plus higher writer assist. Command shape:

```sh
./bin/unified-bench \
  -dbs treedb \
  -test sequential_write,batch_random,random_write \
  -keys 1000000 \
  -valsize 128 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -path-label m8-m14-10mm-gate \
  -treedb-journal-lanes=1 \
  -checkpoint-between-tests \
  -progress=false
./bin/benchprof -profiles-dir "$OUT"
```

Span-native c4 rows used the M14 opt-in apply knobs:

```sh
-treedb-flush-apply-min-entries=1 \
-treedb-flush-apply-min-spans=1 \
-treedb-flush-apply-min-bytes=1 \
-treedb-flush-apply-concurrency=4 \
-treedb-flush-apply-span-native
```

The `c4` row additionally enabled `-treedb-flush-backlog-coalescing`. The
`c4_strict` row also used:

```sh
-treedb-slowdown-backlog-seconds=0.25 \
-treedb-stop-backlog-seconds=0.5 \
-treedb-max-backlog-bytes=536870912 \
-treedb-writer-flush-max-memtables=8
```

Local 1MM result:

| Row | Seq ops/s | Batch ops/s | Random ops/s | Random checkpoint | Post-run checkpoint | Disk note |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `default` | 1,557,967 | 3,436,950 | 1,943,057 | 606.15ms | 1.01s | index 13 MiB, leaf_vlog 126 MiB |
| `c4` | 2,009,181 | 3,374,020 | 2,267,584 | 407.34ms | 606.29ms | index 13 MiB, leaf_vlog 126 MiB |
| `c4_strict` | 1,820,901 | 3,558,543 | 1,836,923 | 407.71ms | 608.29ms | index 13 MiB, leaf_vlog 126 MiB |
| `c4_no_backlog` | 1,944,486 | 2,848,215 | 1,423,293 | 405.89ms | 610.25ms | index 13 MiB, leaf_vlog 126 MiB |

The strict writer-assist/backpressure row regressed c4 random-write throughput
locally (`2.27M -> 1.84M ops/s`) without improving the random-write or post-run
checkpoint timings. The no-backlog attribution row was worse. This does not
prove remote 10MM behavior, but it is enough to reject this narrow direction as
a B1 runtime candidate before spending the required remote matrix.

## Downstream contract

Until a later issue proves a runtime fix with same-host 10MM before/after
evidence, downstream default-readiness work must use this contract:

1. **Unconfigured/default TreeDB:** span-native apply and backlog coalescing stay
   off.
2. **Explicit span-native/backlog c4/c16:** allowed only as experimental
   workload-specific opt-ins with checkpoint-inclusive profiling.
3. **Checkpoint/close drains:** remain fail-closed; keep `close_or_checkpoint`
   fallback accounting and backlog-coalescing skip accounting.
4. **#2795 admission policy:** must not convert c4/c16 throughput rows into an
   unconfigured/default behavior. A selector, if implemented, must fail closed on
   unresolved checkpoint debt as well as low-concurrency c1 evidence.
5. **#2787/#2788:** must treat the span-native/backlog stack as opt-in-only or
   default-off. A default-on claim is blocked unless a later PR removes B1 debt
   without material throughput, read/cache, storage, fallback, or accounting
   regression.

Future attempts to remove the debt should still report:

- `treedb.cache.checkpoint.flushmu_wait_*`
- `treedb.cache.checkpoint.active_background_flush_wait_ns_*`
- `treedb.cache.checkpoint.stage.flush_all.*`
- `treedb.cache.checkpoint.stage.value_log_flush.*`
- `treedb.cache.checkpoint.stage.leaf_value_log_sync.*`
- `treedb.cache.checkpoint.stage.reducer_publish.*`
- span-native used/fallback reason counters, especially `close_or_checkpoint`
- backlog-coalescing admission/skip counters
- random-write throughput, CPU/alloc/block/mutex/checkpoint profiles, and disk
  usage

## Validation

This B1 PR is documentation/policy only. Runtime scheduling tests are not
meaningful because no runtime behavior changes are kept.

Validation commands run:

```sh
go test ./TreeDB/caching ./TreeDB/db ./cmd/unified_bench
```
