# Index Vacuum M0 Baseline

Issue #3943 freezes the current fail-closed production contract before M1
changes online-vacuum behavior. The fixture and capture script are test-only
characterization tools; they do not authorize `VacuumIndexOnline`.

Run:

```sh
RUN_DIR=/tmp/treedb-vacuum-m0 scripts/treedb_vacuum_m0_capture.sh
```

The capture emits `fixture.json`, `summary.md`, and ten interleaved raw samples
for the legacy test-only path and the public fail-closed path. The legacy rows
include `ns/op`, `max-writer-pause-ns`, foreground p95/p99, `B/op`, and
`allocs/op`. Public rows must report `vacuum-unsupported/op`; that status is
never a successful or fast vacuum result.

`fixture.json` records the SHA, Go platform, command, logical digest, index and
value-log bytes, live/reclaimable pages, collection-root span, and offline
ceiling. The focused contract requires three deterministic fixture builds, at
least 50% reclaimable index pages, at least 40% offline index shrink, and
reopen digest parity.

## Ownership

| Current callsite or assertion | Owner | Required successor behavior |
| --- | --- | --- |
| `db/vacuum_online.go:VacuumIndexOnline` fail-closed error | M1 (#3944) | Obtain a current recoverable-root-set capability and publish only after revalidation. |
| `db/vacuum_online_legacy_test.go` legacy swap tests | M1 (#3944) | Replace test-only authorization with fenced swap, runtime rebind, crash-cut, and snapshot tests. |
| `db/vacuum_collection_snapshot_test.go` skipped collection/snapshot cases | M1 (#3944) | Prove collection roots and pinned snapshots survive a fenced replacement. |
| `background_vacuum_test.go` disabled successful debt tests and `bg_vacuum.go` classification | M2 (#3945) | Enable only after M1 succeeds; retain backoff and explicit unsupported/error accounting. |
| `public.go:VacuumIndexOnline` public method and external PL-06 benchmarks | M2 (#3945) | Route the public API and require `vacuum-errors/op == 0` for successful production samples. |
| `compact_storage_m0_measurement_test.go` index-vacuum unavailable phase | M3 (#3946) | Integrate fenced index vacuum and report actual success/retry/error status plus byte convergence. |
| `compact_storage.go` and offline `VacuumIndexOffline` operator semantics | M3 (#3946) | Keep offline byte-minimization as the ceiling; do not conflate it with online eligibility. |

The M0 test-first exception is deliberate: production success assertions remain
red until M1. M0 instead asserts the current fail-closed result, disabled
background behavior, deterministic fixture debt, offline parity, and artifact
schema completeness.

At the M0 base SHA, the legacy foreground-churn benchmark can fail before it
produces a usable timing sample with `durable-root transaction lineage is not
consecutive`. This is a legacy test-only correctness blocker, not a production
result and not a reason to relax the fixture gates. M1 owns the repair before
the ten-sample stability gate can be claimed.
