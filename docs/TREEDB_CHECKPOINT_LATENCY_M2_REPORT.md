# TreeDB span-native checkpoint latency M2 report

Issue: #2785. Parent tracker: #2782.

## Scope and outcome

This M2 pass investigated the checkpoint-inclusive latency regression that kept
span-native/backlog default-readiness blocked after M14. The main result is a
profiled blocker, not a runtime change:

- the checkpoint regression is dominated by checkpoint-boundary flush debt
  (active background flush wait plus `flush_all`/reducer publish work), not by a
  local leaf value-log sync-only hotspot;
- a narrow checkpoint-request-aware background-yield experiment reduced some
  active-wait time but shifted too much work into checkpoint fallback drains,
  regressing throughput/storage and worsening the c16 post-run checkpoint;
- therefore this PR keeps span-native/backlog opt-in and treats remaining
  checkpoint latency as a default-on blocker for #2786/#2788.

No #2784 cache-policy row is included here because #2784 had not merged at the
collection time. Cache-policy work must be consumed only after merge or explicit
coordinator sync.

## Inputs

- Base/current main: `889ef430fd0db0c53fd19eae9d540bcb5913584b`.
- M14 evidence root: `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256`.
- M2 rejected-runtime experiment root:
  `/mnt/fast4tb/gomap-profiles/2785_checkpoint_10mm_20260616_173405`.
- Remote host: `mikers-B560-DS3H-AC-Y1`, 12 logical CPUs, Linux
  `6.8.0-124-generic`, Go `go1.25.0`.

Common 10MM shape:

```sh
./bin/unified-bench \
  -dbs treedb \
  -test sequential_write,batch_random,random_write \
  -keys 10000000 \
  -valsize 128 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -path-label m8-m14-10mm-gate \
  -treedb-journal-lanes=1 \
  -checkpoint-between-tests \
  -progress=false
```

Span-native rows additionally used the M14 opt-in apply/backlog knobs:

```sh
-treedb-flush-apply-min-entries=1 \
-treedb-flush-apply-min-spans=1 \
-treedb-flush-apply-min-bytes=1 \
-treedb-flush-apply-concurrency=<C> \
-treedb-flush-apply-span-native \
-treedb-flush-backlog-coalescing
```

## Current-main checkpoint read

M14 showed that c4/c16 improve random-write throughput but move more unsettled
work to checkpoint boundaries:

| Row | random_write ops/s | Random checkpoint | Post-run checkpoint | active bg wait total | flush_all total | reducer publish total | leaf sync total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `default_unconfigured` | 145,427 | 2.70s | 6.49s | 33.24s | 20.30s | 18.07s | 10.97s |
| `span_native_c4` | 282,282 | 8.20s | 9.22s | 26.08s | 15.84s | 14.03s | 0.23s |
| `span_native_c16` | 312,391 | 7.01s | 8.17s | 22.24s | 17.43s | 13.78s | 2.51s |
| `span_native_c4_no_backlog` | 276,950 | 8.11s | 11.50s | 27.57s | 16.49s | 14.75s | 0.30s |

Interpretation:

- `leaf_value_log_sync` is not the primary c4 checkpoint blocker in the M14
  span-native rows; it is much smaller than active wait and reducer/flush work.
- Backlog coalescing is not the sole cause: disabling backlog made post-run
  checkpoint worse in M14.
- The faster write path leaves boundary debt that slower/default rows partly pay
  during the measured write phase; this is real checkpoint-inclusive cost, not a
  benchmark-boundary artifact to hide.

## Rejected runtime experiment

A transient candidate (commit `2a245856f80330aec069b9dddc4ac2d3f46c661d`, not
kept) made async background flushes yield when a checkpoint was waiting, so the
checkpoint could own the durability boundary instead of waiting behind a full
background pass.

Same-host 10MM result against current main:

| Row | random_write ops/s | Random checkpoint | Post-run checkpoint | active bg wait total | flush_all total | close/checkpoint fallback ops | Disk note |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `main_default_unconfigured` | 153,997 | 11.61s | 9.67s | 42.23s | 23.08s | 0 | index 619 MiB, leaf_vlog 3.7 GiB |
| `candidate_default_unconfigured` | 143,923 | 10.81s | 3.97s | 28.10s | 31.88s | 0 | index 587 MiB, leaf_vlog 3.7 GiB |
| `main_span_native_c4` | 287,017 | 9.84s | 10.55s | 26.88s | 16.09s | 218,612 | index 440 MiB, leaf_vlog 3.3 GiB |
| `candidate_span_native_c4` | 240,504 | 9.10s | 9.50s | 12.08s | 38.35s | 6,138,327 | index 517 MiB, leaf_vlog 3.4 GiB |
| `main_span_native_c16` | 325,962 | 7.06s | 9.29s | 22.16s | 17.45s | 711,086 | index 442 MiB, leaf_vlog 3.4 GiB |
| `candidate_span_native_c16` | 346,443 | 6.20s | 13.32s | 19.70s | 29.93s | 4,656,439 | index 461 MiB, leaf_vlog 3.4 GiB |

Decision: reject this runtime direction for now. It did reduce active wait in
some rows, but it caused unacceptable material regressions:

- `span_native_c4` random-write throughput dropped from 287,017 to 240,504
  ops/s (-16.2%).
- default/unconfigured random-write throughput dropped from 153,997 to 143,923
  ops/s (-6.5%).
- `span_native_c16` post-run checkpoint worsened from 9.29s to 13.32s.
- c4 checkpoint drains shifted from span-native/background work to
  close/checkpoint fallback (`218k` -> `6.1M` fallback ops), grew `flush_all`,
  and increased index/leaf-log footprint.

This confirms that simply moving async background drain work into the checkpoint
boundary is not a safe fix.

## Reporting changes kept

This PR keeps reporting-only changes that make future gates easier to read:

- `cmd/unified_bench` selected stats now prints:
  - `checkpoint.flushmu_wait_total_ms`,
  - `checkpoint.stage.value_log_flush.total_ns`,
  - `checkpoint.stage.flush_all.total_ns`.
- `scripts/treedb_m14_matrix_summary.py` now carries `flush_all` alongside the
  existing active-wait, leaf-sync, and reducer checkpoint counters.

The underlying TreeDB stats already existed on current main; this PR only makes
those counters visible in standard benchmark artifacts.

## Handoff for #2786/#2787/#2788

Downstream default-readiness work can rely on these checkpoint decisions:

- Use `treedb.cache.checkpoint.flushmu_wait_*`,
  `active_background_flush_wait_ns_*`, and `stage.flush_all.*` as the primary
  boundary-debt counters.
- Use `stage.leaf_value_log_sync.*` and `stage.reducer_publish.*` to separate
  leaf-log sync from root/reducer publish; for M14 c4, leaf sync was not the
  dominant blocker.
- Use `treedb.flush_apply.root_reduce.ns_total`,
  `treedb.flush_apply.guarded_publish.ns_total`, and
  `treedb.cache.flush_backlog_coalescing.*` to explain whether throughput rows
  are paying work during writes or deferring it to checkpoint.
- Remaining c4/c16 checkpoint regression blocks default-on unless a later PR
  removes the debt without regressing throughput/storage/read guardrails or the
  coordinator explicitly accepts an opt-in-only tradeoff.
