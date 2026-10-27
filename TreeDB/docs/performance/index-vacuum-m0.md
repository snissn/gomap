# Index Vacuum Capture

## Current production stability contract

The capture script now measures
`BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x`.
It uses the existing shared 4,096-operation fixture (2,048 point writes and
2,048 range deletes), with the publication coordinator enabled and the
production recoverable-root maintenance entry. It retains ten fresh interleaved
samples and the unchanged CV <= 0.10 predicates for total vacuum time and
foreground p99. Every sample must complete all fixed work,
with zero vacuum errors, exposure misses or concurrent aborts. The independent
160-operation PL06 public status arm and its classifier remain unchanged; they
cannot replace the 4,096-operation stability arm.

Run `RUN_DIR=/mnt/fast4tb/<task>/vacuum-m0 scripts/treedb_vacuum_m0_capture.sh`.
New schema-v3 output names its acceptance contract, stability arm and exact benchmark; raw files are
`raw/production-*.txt` and `raw/public-*.txt`, with
`production-benchstat-input.txt`. The summarizer binds raw benchmark identity,
fixed iteration count and successful process completion. Its explicit
`--acceptance-contract v2` mode (the summarizer default) retains all three
original CV gates for either historical stability arm, without relabeling
metrics or failures. The capture script selects production and v3 explicitly.

V3 retains every maximum-writer-pause sample, median, maximum and CV, including
its original <= 0.10 diagnostic pass/fail. It requires separate outcome-owner
review of the complete distribution and source phase evidence before merge;
the capture records that review as pending. CV of one per-vacuum extreme is
no longer a v3 merge predicate: reducing its mean can increase relative
dispersion, while adding a delay could improve CV and make the database slower.
No delay, numerical tolerance, absolute service bound or sample exclusion is
introduced. Correctness, progress, debt or demonstrated material regressions
remain blocking independently of this diagnostic.

This is an explicit prospective acceptance revision, not a v2 pass. The
19b713e047d6ba39715ac1a9a544284bc8e9a5dc capture failed v2 with pause
CV0.127754 (2.567–3.871ms); total and foreground-p99 CVs passed. Its original
result and the earlier bb1 pass remain source-bound. Schema2/v2 and legacy
decoders preserve their three-predicate interpretation.

This is a prospective contract revision. Historical legacy captures and failed
CV predicates remain valid under their original source and contract. A new
production result requires a complete capture at its actual clean source head;
route separation and the public status arm alone provide no quantitative pass.
The legacy entry requires a package-private test capability and deliberately
removes the coordinator. It remains available as a characterization benchmark,
while the current product gate exercises the production path. This maintenance
fixture is distinct from Dgraph's cached COW commit-path qualification.

The historical sections below document the previous capture contract and its
scheduling revisions; their legacy artifact names describe those captures.

## Historical M0 baseline

Issue #3943 froze the fail-closed production contract before M1 changed the
internal online-vacuum backend. The fixture and capture script remain
characterization tools: their legacy path is a same-head performance comparator
and never authorizes production maintenance. Production correctness and
performance certification belong to #3944 and its captured evidence.

Run:

```sh
RUN_DIR=/tmp/treedb-vacuum-m0 scripts/treedb_vacuum_m0_capture.sh
```

The capture emits `fixture.json`, `results.json`, `summary.md`, the raw
`benchstat` input, and ten interleaved raw samples for the legacy test-only path
and the `db.DB` production-backend path. The legacy rows
include `ns/op`, `max-writer-pause-ns`, foreground p95/p99, `B/op`, and
`allocs/op`. The summarizer fails unless total vacuum time, maximum writer pause,
and foreground p99 each have a coefficient of variation at most 10%. Backend
rows must be uniform across all ten samples: either explicitly unavailable with
`vacuum-unsupported/op = 1`, `foreground-exposure-misses/op = 1`, zero
foreground overlap, and zero retry/unexpected-error metrics, or available with
zero unsupported/unexpected-error/exposure-miss metrics, only typed transient
retries, positive overlap in every sample, and at least one successful sample.
Mixed or ambiguous status fails closed. The available classification is backend
evidence only; top-level cached/public routing remains M2-owned. Legacy rows
must report zero concurrent aborts, so an aborted run cannot become a timing
baseline. The capture runs the three-build determinism, debt, shrink, and reopen
gate before writing `fixture.json`.

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

## Public status scheduling revision (#4519)

`BenchmarkPL06ExternalVacuumCollectionForegroundChurn` is an admitted-paused
vacuum status fixture. Its test-only bridge uses the existing
`testHookVacuumAfterBaseSnapshot` witness after the production entry has captured
recoverable roots, rebuilt the older root, and started its real mutation recorder
with a base snapshot. It pauses that SAME admitted vacuum while four workers
perform the unchanged 40 operations each (80 point writes and 80 range deletes,
with the original one-millisecond worker interval), then resumes the SAME call.
The production tail replay, root revalidation/recapture, cutover and durability
path remain active. There is no second vacuum, manufactured success, or production
API change. An early refusal/failure still executes and drains all foreground
work, with the original unsupported/error accounting and an exposure miss.

Positive foreground overlap now means exposure to an admitted, paused vacuum.
It does **not** demonstrate foreground performance during a simultaneously
advancing rebuild. Public `ns/op`, foreground percentiles and allocation metrics
include this changed coordination and typed-result logging; they are not
comparable to historical uncoordinated PL06 timings and do not certify production
maintenance performance. Historic captures retain their original interpretation
and failed results. The legacy benchmark, timing boundary, three 10% CV gates,
fixed work, ten interleaved sample pairs, workflow timeout, and classifier's
at-least-one-success rule remain unchanged.

The package-global witness is installed before the vacuum goroutine starts and
restored to its prior value only after that call is joined. The fixture releases
the pause after workers drain or its context is canceled. Hook-using fixtures must
run serially. Every public result logs its concrete error type/text and separate
matches for concurrent mutation, stale root set and stale WAL cleanup proof.

This is an incremental scheduling/observability repair under #4519. It does not
complete that issue's broader runner topology, controlled CV, or five hosted
invocation requirements. Correctness/race checks and new hosted evidence remain
required for the revised fixture; an old uncoordinated capture cannot validate it.

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
| top-level/cached external vacuum benchmarks and `collections/bench_test.go` | M2 (#3945) | Require zero unsupported/unexpected errors, positive overlap, and exact fixed work on supported platforms. |
| `db/compact_storage.go`, `db/compact_storage_test.go`, and M0 maintenance measurement/reporting | M3 (#3946) | Active: the bounded planner executes the fenced phase and reports `not_required`, `succeeded`, `deferred`, `unsupported`, or `failed` without overstating convergence. |
| Offline `VacuumIndexOffline` operator semantics | M3 (#3946) | Retained as the exclusive-open byte-minimization fallback and parity ceiling; online eligibility remains separately reported. |

The M0 test-first exception was deliberate: its original unsupported result
remains a valid frozen pre-M1 classification, while M1 adds the verified-success
classification for the internal DB-minted backend. Public/background routing
is activated by M2 while this M0 artifact remains a frozen pre-activation
baseline. M0 continues to assert disabled background behavior,
deterministic fixture debt, offline parity, and artifact schema completeness.

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

## Environment receipts and controlled-runner admission (#4519)

Hosted captures keep their existing first-two-allowed-logical-CPU placement,
`GOMAXPROCS=2`, `GOMEMLIMIT=8GiB`, ten interleaved sample pairs, timing boundaries
and v3 numerical predicates. Admission checks that the selected CPUs are allowed
and the scheduler count is positive and no larger than the selected set. This
adds environment diagnostics; it does not repair or reclassify an existing CV
failure, certify a controlled topology, or complete #4519.

Each benchmark has before/after environment receipts outside its process. They
retain UTC and monotonic timestamps, actual affinity, CPU package/core/SMT
identities, runner/run identity, kernel, CPU/steal counters, load, disk counters,
pressure and resolved cgroup-v2 controls. Unavailable optional files carry a
reason. Go environment/compiler receipts describe the resolved SDK. The EXIT
receipt preserves the original failing exit code, last phase and hashes of all
raw files, including partial output and stderr; there are no retries, discarded
samples, load-based skips or telemetry-derived acceptance overrides. A failed
sample has a before receipt and terminal failure, rather than a success receipt.

The existing administrator-selected same-repository PR route additionally uses
`TREEDB_M0_CPU_SET`, `TREEDB_M0_GOMAXPROCS` and `TREEDB_M0_LEASE_PATH` repository
variables. All three are required when that route is armed; hosted jobs ignore
them. Select distinct physical cores explicitly: SMT siblings or unavailable
core topology fail controlled admission. This is a prospective topology, and old
captures keep their original interpretation. Forks and other PRs retain hosted
routing. No runner is registered or reconfigured by the workflow.

The infrastructure owner must provision a dedicated one-job runner under an
exclusive whole-file **host** `flock` held by an ancestor of `Runner.Listener`,
for example an administrator wrapper around the runner's one-job listener
lifetime. The lock path must already exist and be an absolute regular file;
the kernel lock table must be readable. Admission verifies the kernel WRITE
FLOCK's device/inode, whole-file range, owner process and listener ancestry.
Every boundary and the post-upload step reobserve the same custody identity;
missing or changed custody fails closed, including after artifact upload. The
post-upload receipt is retained in job logs because the packet has already
uploaded. The helper never acquires a capture-only lock or releases the host
lease. The administrator wrapper must retain it until the one-job listener exits,
including failures, cancellation cleanup and all artifact uploads, then release
it through normal process/descriptor cleanup. Do not share this runner with a
second listener, and do not treat Actions concurrency or `taskset` as host
exclusivity. All cooperating local timed work must honor the same host lock.

These are observations of a required external custody contract, not proof that
uncooperative host activity was excluded or that a wrapper never unlocked
between observations. Receipts explicitly make no continuous-custody or
performance-acceptance claim. The owner must independently review the wrapper,
runner isolation and host workload before controlled qualification. Do not arm
this route until that provisioning is verified. The unchanged failed hosted
capture stays RED; the controlled stability and five hosted-allocation exit
criteria remain open.
