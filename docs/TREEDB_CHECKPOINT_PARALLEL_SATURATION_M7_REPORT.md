# TreeDB Checkpoint Parallel Saturation M7 Report (#2899 / #2907)

Date: 2026-06-21
Base: `f8dae1f7008a6c1a0550fe3df570044b5f98928e` (after #2915)

## Summary

M0-M6 removed the checkpoint `close_or_checkpoint` span-native fallback and
proved worker CPU saturation for c4/c8 checkpoint drains. The final 10MM matrix
still does **not** satisfy all north-star gates:

- checkpoint drain worker saturation passes (`c4` effective cores 3.69,
  `c8` effective cores 7.22; busy ratio >96%);
- `close_or_checkpoint` fallback is zero in final rows;
- ops/span remains workload-limited at ~3, below the target >=8;
- same-concurrency write+checkpoint throughput is not materially better than M0
  across all rows;
- active-background checkpoint wait remains a large wall component.

Decision: keep the default auto policy conservative (c4/adaptive). Do not raise
defaults to c8/c16 from this gate. Parent #2899 remains blocked by #2916 unless
that workload-shape / active-background-wait limiter is fixed or explicitly
waived.

## Reproduction command shape

Explicit cN rows:

```sh
OUT=/tmp/treedb_2907_final_explicit_cN_$(date +%Y%m%d_%H%M%S)
./bin/unified-bench \
  -dbs treedb \
  -test sequential_write,batch_random,random_write \
  -keys 10000000 -valsize 128 -batchsize 8000 \
  -checkpoint-between-tests \
  -treedb-flush-admission-policy=explicit \
  -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries=1 \
  -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-concurrency=<4|8|16> \
  -profile-dir "$OUT" -progress=false
./bin/benchprof -profiles-dir "$OUT"
```

Forced-off and auto rows use the same workload with
`-treedb-flush-admission-policy=off` or `auto`.

## Artifact roots

- M0 c4 baseline: `<remote-profile-root>/treedb_parallel_saturation_m0_20260620_082057/c4`
- M0 c8 baseline: `<remote-profile-root>/treedb_parallel_saturation_m0_20260620_082057/c8`
- M0 c16 baseline: `<remote-profile-root>/treedb_parallel_saturation_m0_20260620_082057/c16`
- M7 explicit c4: `<remote-profile-root>/treedb_2905_single_lane_direct_noyield_c4_20260620_182515`
- M7 explicit c8: `<remote-profile-root>/treedb_2905_single_lane_direct_noyield_c8_20260620_182716`
- M7 explicit c16: `<remote-profile-root>/treedb_2907_final_explicit_c16_20260620_185648`
- M7 forced off: `<remote-profile-root>/treedb_2907_final_forced_off_20260620_185829`
- M7 auto: `<remote-profile-root>/treedb_2907_final_auto_20260620_190102`

## Final 10MM matrix

| row | seq ops/s | batch ops/s | random ops/s | checkpoint after batch | checkpoint after random | post-run checkpoint | ops/span | busy ratio | effective cores | close/checkpoint fallback | active-bg wait | flush_all | leaf_vlog |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| M0 c4 | 2,471,900 | 508,054 | 257,889 | 1.17s | 6.96s | 10.98s | 2.94 | 97.1% | 3.70 | 218,612 | 23.22s | 17.43s | 3.5 GiB |
| M7 explicit c4 | 2,448,568 | 560,773 | 260,199 | 1.24s | 6.86s | 11.03s | 2.97 | 96.9% | 3.69 | 0 | 27.13s | 12.78s | 3.5 GiB |
| M7 explicit c8 | 2,382,208 | 577,415 | 282,637 | 1.42s | 6.54s | 11.23s | 2.94 | 96.6% | 7.22 | 0 | 16.71s | 17.70s | 3.5 GiB |
| M7 explicit c16 | 2,489,263 | 605,854 | 257,295 | 1.37s | 6.51s | 5.49s | 3.05 | 96.0% | 10.66 | 0 | 20.28s | 13.51s | 3.4 GiB |
| M7 forced off | 2,642,578 | 401,589 | 131,327 | 835ms | 12.17s | 8.86s | 0.00 | 0.0% | 0.00 | 0 | 53.23s | 23.73s | 3.7 GiB |
| M7 auto | 2,340,508 | 536,813 | 243,458 | 771ms | 6.55s | 5.80s | 3.05 | 96.8% | 3.70 | 0 | 16.48s | 18.30s | 3.4 GiB |

Effective cores are computed as
`worker_busy_ns_total / worker_wait_ns_total`. Busy ratio is
`worker_busy / (worker_busy + worker_idle)`.

## Gate review

| Gate | Status | Evidence |
|---|---|---|
| Eligible point checkpoint drains have near-zero `close_or_checkpoint` fallback | Pass | final c4/c8/c16/auto rows report `0` fallback ops |
| c4 checkpoint drain effective CPU >=3 and busy ratio >=80% | Pass | c4: 3.69 effective cores, 96.9% busy |
| c8 checkpoint drain effective CPU >=5 when work exists | Pass | c8: 7.22 effective cores, 96.6% busy |
| Checkpoint ops/span >=8 or accepted workload limit | Fail/open | final rows remain ~2.94-3.05 ops/span |
| Single-op span ratio materially lower than M0 | Fail/open | ops/span is effectively unchanged from M0 |
| Write+checkpoint throughput materially improves vs same-concurrency M0 | Mixed/fail-open | c8 random improves, but c4/c16 are within noise/mixed |
| No material read/cache/reopen/GC/storage regression | Deferred/no new blocker found | M0-M6 correctness/CI remained green; footprint is comparable except forced-off leaf_vlog growth. Dedicated settled/mixed read-cache-scan rows were not rerun in M7 because the parent-close gate is already blocked by #2916; #2916 must include or explicitly waive those guardrail rows before parent closure. |

## Fan-in check from M6

M6 found no reducer/root/leaf-log fan-in stage large enough for a focused M6 code
PR:

| row | checkpoint total | flush_all | active-bg wait | reducer_publish | leaf_value_log_sync | backend_boundary | wal_rotate |
|---|---:|---:|---:|---:|---:|---:|---:|
| c4 | 44.30s | 12.78s | 27.13s | 0.51s | 0.28s | 3.22s | 1.07s |
| c8 | 36.25s | 17.70s | 16.71s | 0.52s | 1.04s | 0.73s | 1.08s |

The remaining wall is active-background wait / work-shape coordination, not a
small serial reducer/publish stage.

## Policy decision

- Keep `FlushAdmissionPolicyAuto` at the existing conservative c4/adaptive path.
- Keep c8/c16 explicit. c8 can improve random-write throughput in this matrix,
  but c16 does not reliably improve the full row set and has higher worker CPU.
- Do not close #2899 as complete from this gate. #2916 tracks the remaining
  final-gate blocker.

## Follow-up blocker

#2916 tracks the remaining required work or waiver:

- lift random-key checkpoint ops/span materially above ~3, or
- reduce active-background checkpoint wait with a safe in-flight coordination
  design, or
- explicitly waive/document the workload-shape limit and keep the conservative
  policy.
