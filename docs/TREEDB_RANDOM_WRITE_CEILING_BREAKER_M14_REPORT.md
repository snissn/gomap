# TreeDB random-write ceiling breaker M14 final gate

Issue: #2774 (M14). Parent tracker: #2743. Runtime base for the first
remote matrix: `origin/main@e0a71320db7a727b451ebdd4b8254853cfc072a2` after
M13 / PR #2780.

This report closes the post-M7 span-run/span-native ceiling-breaker stack by
recording same-host evidence and a default-readiness decision. It is deliberately
not an ops/sec-only report: the decision must account for old-leaf decode
bytes/op, leaf merges/op, append frames/op, span shape, fallback reasons,
checkpoint-inclusive wall time, stalls/backpressure, allocation/profile rows,
and disk footprint.

## Evidence collection plan

Remote host and artifact root:

- host: `mikers@100.78.120.42` (`mikers-B560-DS3H-AC-Y1`)
- artifact parent: `/mnt/fast4tb/gomap-profiles`
- current matrix root: `/mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256`

Reproducibility helpers added by this PR:

```sh
ROOT=/mnt/fast4tb/gomap-profiles \
  COMMIT=$(git rev-parse HEAD) \
  scripts/treedb_m14_final_gate.sh

python3 scripts/treedb_m14_matrix_summary.py \
  /mnt/fast4tb/gomap-profiles/2774_m14_matrix_20260616_132256 \
  --baseline-label default_unconfigured
```

Primary command shape for each 10M row:

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
./bin/benchprof -profiles-dir "$OUT"
```

Rows under collection:

| Row | Apply concurrency | Span-native | Backlog coalescing | Cache axis | Purpose |
|---|---:|---:|---:|---|---|
| `default_unconfigured` | default | false | false | default leaf read cache | Current default behavior / rollback floor. |
| `legacy_parallel_c4` | 4 | false | false | default leaf read cache | Parallel recursive apply without span-native/backlog. |
| `span_native_c1` | 1 | true | true | default leaf read cache | Span-native/backlog lower bound. |
| `span_native_c2` | 2 | true | true | default leaf read cache | Span-native/backlog sweep. |
| `span_native_c4` | 4 | true | true | default leaf read cache | M13 comparison/default candidate row. |
| `span_native_c8` | 8 | true | true | default leaf read cache | Span-native/backlog sweep. |
| `span_native_c16` | 16 | true | true | default leaf read cache | Span-native/backlog upper sweep. |
| `span_native_c4_no_backlog` | 4 | true | false | default leaf read cache | Isolate M11 adaptive backlog coalescing. |
| `span_native_c4_cache_disabled` | 4 | true | true | outer leaf read cache disabled | Cache-disabled guardrail row. |

`FlushApplyConcurrency=1,2,4,8,16` rows all use the primary min gates
`-treedb-flush-apply-min-entries=1 -treedb-flush-apply-min-spans=1
-treedb-flush-apply-min-bytes=1`. The default row intentionally omits opt-in
apply/span/backlog flags to measure the current shipped default.

## Final matrix summary

Pending remote matrix completion. The final PR update will paste the generated
`m14_matrix_summary.md` tables and preserve artifact paths for every row.

## Default-readiness decision

Pending remote matrix completion. The decision will include rollback knobs and
any accepted regressions/caveats explicitly.
