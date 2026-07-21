# Index Vacuum M0 Baseline

Issue #3943 freezes the current fail-closed production contract before M1
changes online-vacuum behavior. The fixture and capture script are test-only
characterization tools; they do not authorize `VacuumIndexOnline`.

Run:

```sh
RUN_DIR=/tmp/treedb-vacuum-m0 scripts/treedb_vacuum_m0_capture.sh
```

The capture emits `fixture.json`, `results.json`, `summary.md`, the raw
`benchstat` input, and ten interleaved raw samples for the legacy test-only path
and the public fail-closed path. The legacy rows
include `ns/op`, `max-writer-pause-ns`, foreground p95/p99, `B/op`, and
`allocs/op`. The summarizer fails unless total vacuum time, maximum writer pause,
and foreground p99 each have a coefficient of variation at most 10%. Public
rows must report `vacuum-unsupported/op = 1` and zero unexpected errors; that
status is never a successful or fast vacuum result. Legacy rows must report
zero concurrent aborts, so an aborted run cannot become a timing baseline. The
capture runs the three-build determinism, debt, shrink, and reopen gate before
writing `fixture.json`.

The legacy benchmark catalog includes 131,072 deterministic ordinary metadata
entries plus one live collection root. The entries bypass collection-root
cloning and enlarge only the production system-tree rewrite inside the cutover
lock, giving the maximum-pause stability gate enough real work. No synthetic
delay is included in the timing. Each sample also includes 4,096 fixed
foreground point/range operations, providing a stable p99 population while the
coalesced tail remains below the production cutover cap.

`fixture.json` records the SHA, Go platform, command, logical digest, index and
value-log bytes, live/reclaimable pages, collection-root span, and offline
ceiling. The focused contract requires three deterministic fixture builds, at
least 50% reclaimable index pages, at least 40% offline index shrink, and
reopen digest parity.

## Ownership

| Current callsite or assertion group | Owner | Required successor behavior |
| --- | --- | --- |
| `db/vacuum_online.go` fail-closed entry and `docs/spec/recoverable-root-set-maintenance-3681.md` | M1 (#3944) | Obtain a current recoverable-root-set capability and publish only after revalidation. |
| `db/vacuum_online_legacy_test.go`, `db/vacuum_online_swap_test.go`, and `db/vacuum_collection_snapshot_test.go` | M1 (#3944) | Replace test-only authorization with fenced swap, runtime rebind, crash-cut, tail-mutation, close, and snapshot tests. |
| `db/vacuum_collection_roots_test.go`, `maintenance_leaf_vlog_test.go`, and pointer/outer-leaf assertions | M1 (#3944) | Preserve collection descriptors, roots, persistent value pointers, and leaf references without rewriting persistent logs. |
| `internal/dictdb/stable_resource_capture_test.go` and `internal/templatedb/stable_resource_capture_test.go` skips/errors | M1 (#3944) | Prove stable-resource pin precedence and retry after release through the production path. |
| `db/close_hooks_test.go` fail-closed assertion | M1 (#3944) | Serialize close through maintenance and preserve a coherent replacement runtime. |
| `background_vacuum_test.go` disabled success tests and `bg_vacuum.go` classification | M2 (#3945) | Enable only after M1 succeeds; retain backoff and explicit unsupported/error accounting. |
| `public.go` public/close-time calls, `caching/db.go` wrappers, and `collections/api_test.go` assertions | M2 (#3945) | Route every public and cached entry through the production seam with actionable status. |
| `db/vacuum_collection_external_bench_test.go`, `db/vacuum_collection_latency_external_bench_test.go`, and `collections/bench_test.go` | M2 (#3945) | Require zero unsupported/unexpected errors, positive overlap, and exact fixed work on supported platforms. |
| `db/compact_storage.go`, `db/compact_storage_test.go`, and M0 maintenance measurement/reporting | M3 (#3946) | Execute the fenced phase and report success/retry/error plus byte convergence truthfully. |
| Offline `VacuumIndexOffline` operator semantics | M3 (#3946) | Keep offline byte minimization as the ceiling; do not conflate it with online eligibility. |

The M0 test-first exception is deliberate: production success assertions remain
red until M1. M0 instead asserts the current fail-closed result, disabled
background behavior, deterministic fixture debt, offline parity, and artifact
schema completeness.

The legacy timing baseline explicitly drains and disables the current
root-publication coordinator on its private benchmark DB before authorizing the
test-only swap. That recreates the runtime mode the legacy implementation was
designed for while still overlapping vacuum with foreground user-tree point and
range writes and retaining a live collection root. Without that isolation, the
legacy direct swap advances durable state without rebinding coordinator lineage;
the next foreground candidate fails with `durable-root transaction lineage is
not consecutive`. M1 must keep the coordinator enabled, cover collection-tail
mutation separately, and compare its production implementation against this
fixed-work legacy ceiling without weakening either workload.
