# Dgraph MVCC Readiness Closeout (#3673)

Status: pre-alpha downstream contract for the first restricted Dgraph Alpha
benchmark. This document closes gomap issue
[#3673](https://github.com/snissn/gomap/issues/3673) and the TreeDB side of
[#3668](https://github.com/snissn/gomap/issues/3668). The downstream tracker is
[snissn/dgraph#16](https://github.com/snissn/dgraph/issues/16).

TreeDB and its MVCC format remain pre-alpha. This closeout pins tested behavior
for one Dgraph experiment; it is not a general compatibility or migration
promise.

## Intended Dgraph module pin

Dgraph MUST pin the first `github.com/snissn/gomap` **merged-main commit that
contains this closeout PR**, using the Go pseudo-version resolved for that exact
commit. It MUST NOT pin a worker branch, a predecessor branch, or a floating
`main`. The dependency chain through all #3668 children is present on main at
`e94013508e7fad1d9fb89034ec8dfd6d88c7e6e2`; the final closeout merge commit is
recorded in the Dgraph adapter PR when that descendant starts.

The evidence below is collected on the closeout code commit named in
`artifacts/dgraph-mvcc-closeout/README.md`. The final evidence-only commit may
follow it without changing production MVCC code.

## Reusable conformance surface

`TreeDB/mvcc/mvcctest` is a downstream test package, not a production adapter.
Its closure-based `Adapter` deliberately avoids requiring Dgraph types to
implement TreeDB-specific interfaces. `mvcctest.FromStore` is the minimal
TreeDB example. `mvcctest.Run` executes:

- the committed `public_trace_v1.json` golden history;
- durable close/reopen before and after floor advancement and pruning;
- nil/empty-key duplicate handling, zero timestamps, durability rejection,
  monotonic floors, and commit/read rejection at the floor;
- forward/reverse iterator order, logical bounds, prefix and read-time ceiling,
  directional seek, exact accounting, and caller-owned entry bytes across
  `Next`, `Seek`, and `Close`;
- a deterministic seed-3673 randomized point-read and all-version oracle over
  empty, embedded-zero, and `0xff` logical keys;
- concurrent point readers and writers with an invocation/response barrier
  proving one post-barrier Adapter read/commit call interval overlap, while an
  older iterator retains its pinned snapshot.

Except for the durable reopen trace, the public suite runs each case against
both WAL-on relaxed and WAL-off relaxed stores. The durable trace separately
pins sync acknowledgement and reopen behavior.

`TreeDB/mvcc/conformance_external_test.go` invokes the suite from the external
`mvcc_test` package, so package-private helpers cannot accidentally become part
of the dependency. `TreeDB/mvcc/mvcctest/example_test.go` is a compiling example
that imports only public TreeDB, MVCC, and harness APIs.

The physical codec remains intentionally internal. Its exact v1 bytes and
ordering are independently pinned by
`TreeDB/internal/mvcckey/testdata/codec_v1_golden.json`; round-trip, malformed
input, ordering, bound, and fuzz properties remain in the codec package.

## Supported semantics for the restricted Alpha

| Surface | Supported contract |
| --- | --- |
| Timestamp ownership | The caller supplies a nonzero `uint64`. One successful commit uses one timestamp for its full mutation batch. |
| Atomic mutation batch | Puts and tombstones publish through one TreeDB batch. Duplicate logical keys in a batch are rejected; nil and empty keys are the same identity. |
| Point visibility | `GetAt(k, ts)` returns the newest retained version at or below `ts`, distinguishing absent, present-empty, present-nonempty, and tombstone. |
| Same key/timestamp | A later successful commit replaces the one physical version at the same logical key and timestamp. |
| Logical keys | Arbitrary bytes, including empty, zero bytes, and `0xff`, within the codec envelope. |
| All-version iteration | Snapshot-bound forward/reverse iteration, logical bounds, prefix, read-time ceiling, directional seek, explicit tombstones, owned returned bytes, and accounting. |
| Discard floor | One durable monotonic global floor. Reads/scans at or below it fail; commits must be strictly above it. |
| Pruning | Bounded, restartable value/tombstone pruning with pinned-reader safety and no value resurrection. Each delete batch is atomic, but a full prune pass is not operation-atomic. `Skipped` is a subset of retained records. |
| Durability | `CommitDurable` and durable floor/prune operations require `DurabilityDurable` and acknowledge through sync publication. Relaxed commits and floor advances retain operation-level atomicity but are not fsync acknowledgements. Relaxed pruning is restartable and batch-atomic, not operation-atomic across a full pass. |
| Reopen/crash | Durable commits and floor-first pruning survive reopen and abrupt child-process exit in the committed MVCC crash tests. |
| Concurrency | Concurrent point readers, commits, snapshot iterators, pruning, and serialized floor advancement are race-tested. |
| Ownership | Exactly one `mvcc.Store` owns one open TreeDB handle and reserved MVCC namespace. |
| Errors | Validation, malformed-record, storage, floor, and durability failures remain distinguishable with `errors.Is`. Storage acknowledgement errors may be commit-ambiguous but never expose a partial batch. |

## Unsupported or Dgraph-owned semantics

These are not silently emulated by TreeDB MVCC:

- encryption at rest or Badger encryption-key rotation;
- TTL/expiry, leases, subscriptions, backup streams, bulk loaders, or managed
  compaction scheduling;
- Badger transaction objects, optimistic conflict detection, predicates,
  compare-and-set, or conditional transaction semantics;
- Dgraph posting-list metadata, namespace policy, rollup/split envelopes,
  cache accounting, iterator prefetch/value callbacks, or Alpha lifecycle;
- multiple `mvcc.Store` owners for one handle, raw writes in the reserved MVCC
  namespace, or per-key discard floors;
- stable cross-version on-disk migration or a public physical codec API.

Dgraph owns any translation from its backend-neutral seam to this surface. A
missing envelope is an explicit restricted-runtime capability error, not a
reason to expand TreeDB's generic MVCC layer in this closeout.

## Correctness evidence matrix

| Contract | Committed evidence |
| --- | --- |
| Codec bytes/order/bounds | `TreeDB/internal/mvcckey/codec_golden_test.go`, `codec_test.go`, `codec_fuzz_test.go` |
| Atomic commit and point histories | `TreeDB/mvcc/mvcc_test.go`, golden and randomized public harness cases |
| Iteration, tombstones, seek, ownership | `TreeDB/mvcc/versions_test.go`, public golden trace |
| Floor/reopen/prune/idempotence | `versions_test.go`, `mvcctest.Run/golden_reopen_floor_prune` |
| Abrupt exit | `TestCommitAtDurableProcessCrashRecovery`, `TestPruneDurableProcessCrashAfterDeleteBatch` |
| Reader/writer/prune concurrency | `TestGetAtConcurrentReaders`, `TestPruneConcurrentSnapshotReaders`, `TestPruneAfterSnapshotCaptureDoesNotBlockForegroundOperations`, public concurrency case |
| Downstream public-only compilation | `TestPublicSurfaceConformance`, `ExampleFromStore` |

Focused reproducible gates:

```sh
GOWORK=off go test ./TreeDB/internal/mvcckey ./TreeDB/mvcc/...
GOWORK=off go test -race ./TreeDB/mvcc/... -run \
  'TestPublicSurfaceConformance|TestGetAtConcurrentReaders|TestPruneConcurrentSnapshotReaders|TestPruneAfterSnapshotCaptureDoesNotBlockForegroundOperations|TestFloorAdvanceRaceNeverServesPrunedHistoricalRead'
GOWORK=off go test ./TreeDB/mvcc -run \
  'TestCommitAtDurableProcessCrashRecovery|TestPruneDurableProcessCrashAfterDeleteBatch'
GOWORK=off go test ./TreeDB/internal/mvcckey -run '^$' \
  -fuzz '^FuzzRoundTrip$' -fuzztime=10s
GOWORK=off go test ./TreeDB/internal/mvcckey -run '^$' \
  -fuzz '^FuzzDecodeNeverPanics$' -fuzztime=10s
```

The two codec fuzz targets are run separately because `go test -fuzz` accepts
only one matching target per invocation.

## Performance evidence and boundaries

The raw-path no-regression gate remains `scripts/mvcc_raw_path_gate.sh`: it
compiles base/head benchmark binaries, pairs each individual benchmark group
immediately in alternating AB/BA order on one pinned CPU, and covers point
reads, batch writes, snapshot seek, iterator reuse, and durable synced writes.
The point-read and batch-write rows use separate invocations so one row cannot
delay the base/head pair for the other. Eight samples provide exactly four AB
and four BA pairs; odd sample counts are rejected. The gate rejects a median
paired candidate/base timing regression over 5%, any allocation increase, or a material `B/op`
increase. The checker hashes all six actual test-binary paths and attributes
each row to its owning `db`, `caching`, or `treedb` base/head binary pair.
Raw PASS/FAIL medians and paired timing deltas remain reported for every row.
A failed row with a byte-identical owning binary is reported as
non-attributable (`EQUIVALENT` at row level); a row whose owning binary changed
remains threshold-enforced. Mixed binary evidence may produce aggregate
`EQUIVALENT` only when every changed-owning-binary row passes. Missing,
malformed, duplicated, or mismatched binary evidence fails closed and cannot
produce equivalence acceptance.
The batch-write row uses a fixed 1,000-iteration shape. Each measured group
contains eight ordinary writes under the production coordinator's fixed 100 ms
timer; it fails if the asynchronous publisher runs during the group, then
checkpoints with timing and allocation accounting stopped. This isolates the
foreground candidate-build/enqueue path from process-wide publisher allocation
accounting while keeping the base/head foreground iteration count identical.
The aggregate verdict is `PASS` when all measurements pass, `EQUIVALENT` when
only equivalent-owning-binary rows fail, and `FAIL` when any changed-owning
binary row fails. Machine-readable `no_attributable_regression` is true exactly
when the verdict is accepted (`PASS` or `EQUIVALENT`).

`scripts/mvcc_adapter_overhead_gate.sh` separately compares public MVCC commit,
get, and all-version iteration rows with their direct TreeDB/physical controls.
Each benchmark group is paired immediately in the same alternating AB/BA order.
The gate also requires an even sample count and defaults to eight. It applies
the same base/head regression limits and rejects candidate MVCC overhead above
2x. Candidate MVCC CPU profiles and top reports are emitted for all three pairs
on every run, including a failing run.

`scripts/mvcc_closeout_matrix.sh` captures the final downstream matrix at an
exact checkout. It emits host/CPU/Go metadata, benchmark-binary checksum, raw
samples, `/usr/bin/time -v` CPU/RSS evidence, a CPU profile/top, and JSON plus
Markdown medians. The matrix separates these acknowledgement classes:

- `durable_sync`: durable TreeDB plus `CommitDurable`/durable floor/prune;
- `wal_on_relaxed`: WAL-on TreeDB plus `CommitRelaxed`/relaxed floor/prune;
- `wal_off_relaxed`: WAL-off TreeDB plus relaxed operations.

Each class covers `CommitAt` batches 1/32, `GetAt` depths 1/64, 64-key
all-version scans at depths 1/32, and 64-key depth-16 pruning at floors 4/12.
Go benchmark rows report latency, throughput, `B/op`, and `allocs/op`.
Commit, get, and scan rows use duration calibration. Prune rows use exactly one
fresh populated database per external sample (`-benchtime=1x`), because
duration calibration would multiply excluded fixture setup and make process RSS
describe fixture churn rather than the bounded pruning operation. Five external
samples still provide five independent prune latency/throughput observations.
The timer is stopped before the parent temporary directory and all other setup,
then started only around `PruneVersions`; the earlier `dbea38e0` matrix is
superseded because its single prune sample included pre-loop temporary-directory
setup.
`storage_bytes/op` is normalized final logical file footprint, including fixed
TreeDB files and preallocation. `durable_footprint_bytes/op` is emitted only for
the sync-acknowledged rows and remains final logical footprint; it does not
measure bytes forced to the physical device. Prune's existing
delete-write-amplification metric is reported separately.

Reproduction:

```sh
OUT_DIR=/absolute/output/path \
CANDIDATE_HASH=<exact-commit> RUNS=5 BENCHTIME=750ms CPUSET=0 \
GOWORK=off ./scripts/mvcc_closeout_matrix.sh

OUT_DIR=/absolute/raw-gate-path BASELINE_HASH=<base> CANDIDATE_HASH=<head> \
GOWORK=off ./scripts/mvcc_raw_path_gate.sh

OUT_DIR=/absolute/adapter-gate-path BASELINE_HASH=<base> CANDIDATE_HASH=<head> \
GOWORK=off ./scripts/mvcc_adapter_overhead_gate.sh
```

The committed compact evidence index is
`TreeDB/docs/spec/artifacts/dgraph-mvcc-closeout/README.md`. Raw local evidence
is retained outside git and the exact-head CI raw-path artifact is attached to
the pull request run; generated CPU profiles are not committed.

### Exact-target closeout outcome

The raw-path and adapter code target is
`dbea38e0e8ad0c7d1e0bb05ac564bd9b57dd747a`, compared with base
`f9c9b2a37838909d0e669818cfa2840c0a8d5f85`. The corrected closeout-matrix
target is `103f9c5af85d8d6a5801119fc2247be3b9c87fad`, which changes only prune
timer accounting and its structural guard relative to the previously measured
matrix target. It does not change the production MVCC implementation. The raw
and adapter gates were not rerun. After one passing 30-second quiet audit, the
closeout matrix was rerun exactly once; its summaries were regenerated and
revalidated from the preserved raw inputs.

- The hosted raw-path verdict is **EQUIVALENT**, not PASS. Its durable-sync row
  measured +27.94% and failed the timing threshold, while all three
  row-producing base/head binary pairs were byte-identical. The other four
  measured rows ranged from -1.76% to +1.12%.
- The adapter-overhead gate verdict is **FAIL**. Direct/MVCC commit co-moved at
  +9.77%/+7.03%, and physical/MVCC all-version iteration co-moved at
  +18.88%/+13.41%. Get rows remained within the 5% revision ceiling.
  Allocations were unchanged, bytes remained flat, and every candidate
  adapter/direct ratio passed the 2x ceiling; the maximum was 1.121x.
- The corrected five-sample durability-matched closeout matrix completed
  successfully with all 24 rows and required metrics present. The prior matrix
  is invalid for prune timing and is superseded, not combined with the
  corrected samples.

The four adapter revision-ceiling misses are accepted only as a scoped risk for
the first restricted pre-alpha Dgraph benchmark. They are not relabeled as a
passing adapter gate. The acceptance is based on the non-production nature of
the base-to-target diff, direct and MVCC co-movement, passing adapter/direct
ratios, unchanged allocations, flat bytes, byte-identical hosted raw-path
binaries, and the successful matrix. Exact samples and the full rationale are
preserved in the compact evidence index.

Performance results compare only like-for-like rows. In particular, relaxed
writes are never described as equivalent to synced writes, and one-iteration
smokes are not used as stable throughput evidence.
