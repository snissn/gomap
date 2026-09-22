# Collections Native Fast-Path Roadmap

Status: draft roadmap, non-normative.

This document translates the fast-path proposal into an execution plan for a
clean rewrite of the cached collections engine path.

It is intentionally implementation-oriented. The proposal defines the target
architecture. This roadmap defines how to get there without losing correctness,
without confusing benchmark signals, and without further extending the current
transitional path.

See also:

- `TreeDB/docs/spec/collections-native-fastpath-proposal.md`
- `TreeDB/docs/spec/contracts.md`
- `TreeDB/docs/spec/verification.md`

## 1. Scope

This roadmap applies only to the cached collections execution path.

It does **not** rewrite:

- the public collections API,
- the collection catalog model,
- the logical primary/index-state/secondary root design,
- JSON or index-field extraction in this sprint.

It **does** rewrite:

- named-root cached staging,
- root-domain snapshot and flush behavior,
- batch probe shape,
- warm-root publish behavior,
- batch ingest execution shape,
- grouped system-root and named-root publication.

## 2. Rewrite Strategy

### 2.1 Keep the old branch as the semantic oracle

The old cached collections branch remains the reference behavior until the
rewrite path proves:

- correctness parity,
- reopen and maintenance parity,
- clear performance superiority.

The oracle is external to the rewrite branch. It is not the execution base and
it should not be merged forward mechanically.

The oracle branch should be treated as:

- a test oracle,
- a benchmark oracle,
- a behavioral reference only.

It should not receive new architectural work beyond fixes required to keep the
oracle usable.

### 2.2 Build the rewrite on a fresh main-based execution branch

The rewrite should land on a fresh `main`-based execution branch.

The execution base should:

- start from current `main`,
- keep oracle comparisons external-branch based,
- avoid importing the old implementation unless a later decision explicitly
  changes the plan.

If the rewrite later introduces multiple native variants, an internal selector
MAY be used for those variants. It is not required for oracle comparison.

Benchmark and profile output must still remain unambiguous. Every
artifact captured during the rewrite must identify:

- oracle vs native-fastpath when relevant,
- workload profile,
- durability profile.

### 2.3 No mixed publish path inside one operation

A single collection mutation operation MUST use one path end-to-end:

- one native implementation path only.

The implementation MUST NOT route some roots through one publish path and others
through another inside one logical publish group.

## 3. Rewrite North Star

The desired steady-state path is:

`collection batch plan -> per-root sorted runs -> vectorized probes -> grouped native publish -> page-preserving warm apply`

The rewrite should be considered off-track if it reintroduces any of these on
the new path:

- root-local steady-state B-tree restaging,
- `[]batch.Entry` materialization as a transport format,
- detached-batch replay,
- per-document existence or uniqueness probes,
- full warm-root rebuild as the normal case.

## 4. Branch and PR Management

### 4.1 Branch model

Use one rewrite stack rooted from the current main-based prep branch, not from
the external oracle branch.

Suggested branch naming:

- `pr/native-fastpath-r0-oracle-baseline`
- `pr/native-fastpath-r1-root-runs`
- `pr/native-fastpath-r2-vector-probes`
- `pr/native-fastpath-r3-grouped-publish`
- `pr/native-fastpath-r4-warm-apply`
- `pr/native-fastpath-r5-batch-planner`
- `pr/native-fastpath-r6-single-doc-cutover`
- `pr/native-fastpath-r7-default-flip-cleanup`

Each PR should target the immediately previous branch in the stack.

The external oracle branch remains outside this stack. It is used only for
behavior and benchmark comparison.

### 4.2 Per-PR rules

Every PR in the rewrite stack MUST follow this order:

1. add failing tests or benchmarks first,
2. implement the minimum code to pass,
3. add regressions for discovered bugs,
4. capture focused profiles and benchmark deltas,
5. update the roadmap or proposal if implementation reality diverges.

Every PR should contain:

- one tests-first commit,
- one implementation commit,
- optional follow-up commits only for regressions or perf fixes.

## 5. Phase Plan

## R0. Oracle Freeze + Baseline Harness

### Scope

Freeze the external-oracle comparison model and the baseline harness used to
judge the rewrite.

### Tests first

Add failing tests for:

- benchmark harness labeling of engine path,
- explicit labeling of durability or workload profile in benchmark artifacts,
- native rewrite harness availability on the main-based branch when collection
  benchmarks are not yet present there,
- baseline capture metadata completeness where that is represented in testable
  tooling or scripts,
- explicit distinction between external oracle artifacts and main-based native
  artifacts where that is represented in testable tooling or scripts.

### Implementation

- freeze the oracle branch name, current main execution base, and exact
  `origin/main` baseline SHA,
- freeze the canonical benchmark command set,
- introduce or stub the native-path collection benchmark/report harness on the
  main-based branch if it is not already present,
- plumb benchmark labels so every run says `oracle` or `native-fastpath` when
  comparison output is emitted,
- document the baseline capture block that must exist before `R1`.

`R0` MUST NOT assume an in-tree legacy/native execution selector. The oracle is
an external branch reference, not a runtime path on the rewrite branch.

### Perf gates

Capture baseline focused profiles for:

- `BenchmarkCollectionInsertBatchProvidedID`
- `BenchmarkCollectionInsertBatchWithSecondaryIndexes`
- `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes`

When the main-based execution branch does not yet contain the collection bench
harness, the pre-`R0` collection baseline is oracle-only. In that case, `R0`
must first land the native-path benchmark harness and then recapture the native
side of the baseline before `R1`.

Also capture:

- raw cached TreeDB batch-write baseline on the same harness,
- one `wal_on_fast`-style focused baseline for the collection path,
- one settled-after-checkpoint baseline separate from mixed-under-debt results,
- one checkpoint or flush-drain focused baseline so throughput wins cannot hide
  deferred work.

Unified-bench anchors for this phase SHOULD include:

- write-path anchor:
  - `write_seq`
  - `write_rand`
  - `batch_write`
  - `batch_random`
  - `batch_delete`
  - `delete_rand`
- read and snapshot anchor:
  - `random_read`
  - `random_read_parallel_acquire_snapshot`
  - `full_scan`
  - `prefix_scan`
- deferred-work anchor:
  - `-suite flushdrain`

### Exit criteria

- oracle branch, execution base, exact main SHA, and benchmark command set are
  frozen,
- native-path benchmark harness exists on the execution branch,
- baseline artifacts are checked in or attached to the PR notes.

## R1. Native Root-Domain Runs

### Scope

Replace root-local staging with native mutable and immutable run structures for
system root and named roots.

### Tests first

Add failing tests for:

- root-domain snapshot reads from mutable + immutable + published state,
- newest-wins tombstone semantics across multiple runs,
- run promotion without semantic drift,
- same-handle visibility before checkpoint using the native path,
- no steady-state per-entry B-tree insertion on the native batch path,
- native-path reads proving root domains, not overlay state, are authoritative.

### Implementation

- introduce native root-domain run structures,
- add run sealing and versioned snapshot publication,
- keep oracle comparison external,
- wire the native path reads through root-domain snapshots.

The implementation should prefer flat append/sort or equivalent sorted-run
builders. Tree-backed mutable staging is acceptable only as an explicitly tested
tiny-write fallback.

### Perf gates

Add focused microbenches for:

- root-domain point lookup,
- root-domain prefix iterator,
- run seal/promotion cost.

Unified-bench gate for this phase:

- no material regression beyond documented noise on the read and snapshot anchor
  bundle,
- no material regression beyond documented noise on the write-path anchor
  bundle.

### Exit criteria

- native path reads do not depend on transitional overlay machinery,
- run promotion is the only path from mutable to immutable state,
- profiles no longer show root-local batch staging dominated by B-tree insert,
- tiny-batch fallback counters are zero on the targeted native batch
  benchmarks, unless this phase is explicitly marked scaffolding-only with a
  documented reason.

## R2. Vectorized Batch Probes

### Scope

Make primary existence and unique-prefix checks run as sorted vector probes over
one snapshot.

### Tests first

Add failing tests for:

- sorted key batch existence probe exactness,
- sorted prefix batch probe exactness,
- tombstoned base entries not counting as conflicts,
- duplicate ids inside the same batch,
- duplicate unique values inside the same batch,
- buffered-state conflicts and persisted-state conflicts,
- fail-fast duplicate detection before heavy payload construction,
- native path not using correctness-fallback per-item probe loops.

### Implementation

- add native probe APIs for sorted deduped keys and prefixes,
- make the batch planner dedupe and sort probes before execution,
- use canonical unique probe keys instead of full secondary entry keys.

The native path should use sort + linear scan for in-batch duplicate detection
and uniqueness planning. `map[string]...`-style keyed materialization should be
treated as fallback/debug behavior, not the steady-state design.

### Perf gates

Add focused microbenches for:

- `HasManySortedAtRoot`
- `HasPrefixesSortedAtRoot`
- batch planner dedupe/probe planning overhead.

Unified-bench gate for this phase:

- no material regression beyond documented noise on:
  - `random_read`
  - `random_read_parallel_acquire_snapshot`
  - `prefix_scan`
- if probe changes impact write-side scheduling or batching, no material
  regression on:
  - `batch_write`
  - `batch_random`

### Exit criteria

- no per-document point or prefix probes remain on the native batch path,
- batch conflicts are resolved from one merged probe view plus in-batch state,
- profiles stop showing repeated probe helper dominance,
- per-item key and prefix probe fallback counters are zero on the targeted
  native batch benchmarks.

## R3. Grouped Native Publish

### Scope

Publish root-domain runs and system-root descriptor runs through one grouped
native path.

### Tests first

Add failing tests for:

- grouped publish of primary + index-state + secondary + system descriptor runs,
- failure or retry preserving the entire publish group,
- no partial visibility across roots,
- checkpoint and close using the grouped native publisher,
- system-root native publication without detached-batch replay,
- multi-root reads observing one pinned snapshot view across roots,
- hook or counter assertions that the native path did not fall back to
  iterator-to-entry replay.

### Implementation

- add grouped publish primitives over ordered runs,
- make scheduler track dirty work by publish group id,
- route system-root descriptor publication through the same grouped publisher,
- leave warm-root steady-state algorithm unchanged for the moment,
- introduce pinned multi-root snapshot view support at the backend/cached
  boundary if it does not already exist in a form usable by the native path.

### Perf gates

Add focused microbenches for:

- grouped cold-root publish,
- grouped mixed system+named-root publish,
- checkpoint publish without warm delta apply.

Unified-bench gate for this phase:

- no material regression beyond documented noise on:
  - `batch_write`
  - `batch_random`
  - `batch_delete`
- no material regression beyond documented noise on:
  - `-suite flushdrain`

### Exit criteria

- the native path does not materialize `[]batch.Entry` or detached batches for
  grouped publication,
- grouped publish is the only publish path used by native cached collections,
- pinned multi-root snapshot views are the only source of coherent multi-root
  collection reads on the native path,
- detached-batch replay counters are zero on the targeted grouped-publish
  benchmarks.

## R4. Warm-Root Native Apply

### Scope

Replace steady-state warm-root rebuild with targeted warm apply.

### Tests first

Add failing tests for:

- warm-root publish preserving unchanged pages,
- threshold fallback rebuild policy,
- value-log delta derivation during warm apply,
- recovery and maintenance correctness after warm apply,
- large steady-state batch updates on existing roots,
- hook or counter assertions that steady-state warm publish did not perform a
  full base scan, full rebuild, or per-key base lookup for retention deltas.

### Implementation

- add zipper-native or page-aware delta apply,
- derive value-log retention deltas inline,
- reserve rebuild as threshold fallback only,
- document the threshold policy.

Warm apply should be treated as the primary algorithm in this phase. Full
rebuild is only the fallback path and should be named and instrumented as such.

### Perf gates

Add focused microbenches for:

- warm-root apply on sparse deltas,
- warm-root apply on dense deltas,
- threshold crossover behavior.

Unified-bench gate for this phase:

- no material regression beyond documented noise on the write-path anchor
  bundle,
- no material regression beyond documented noise on `-suite flushdrain`,
- the collection checkpointed indexed-batch benchmark SHOULD improve materially
  or the phase should not advance without explicit rationale.

### Exit criteria

- steady-state warm publish does not do full base scan + rebuild,
- profiles for warm-root-heavy benchmarks are dominated by page work rather than
  rebuild orchestration,
- native-path hook or counter assertions prove the warm fast path did not fall
  back to rebuild-oriented machinery except where the threshold policy allows
  it,
- warm apply rebuild fallback counters are zero on sparse-delta warm benchmarks,
- warm apply per-key retention-lookup fallback counters are zero on the focused
  warm benchmarks.

## R5. Native Batch Planner + Batch Ingest

### Scope

Make `InsertBatch` and related batch paths emit root-local sorted runs directly.

### Tests first

Add failing tests for:

- native `InsertBatch` run emission for primary/index-state/secondary roots,
- ordered result preservation despite internal sorting,
- buffered and persisted uniqueness conflicts on the native path,
- `CreateIndex` backfill using native grouped publish,
- indexed delete coherence on the native path,
- canonical unique probe prefix planning without full secondary entry-key
  materialization for conflict checks,
- no batch-local duplicates surviving the fail-fast planning stage.

### Implementation

- rewrite batch ingest planning to produce root-local runs,
- eliminate remaining translation through generic batch entry slices,
- keep single-document APIs behaviorally stable, even if they still delegate to
  batch planning internally,
- ensure planner work is ordered as: dedupe/probe plan first, heavy payload
  construction second.

### Perf gates

Add focused benchmarks for:

- no-index `InsertBatch`,
- indexed `InsertBatch`,
- checkpointed indexed `InsertBatch`,
- mixed insert/delete batch.

Unified-bench alignment gate for this phase:

- re-run the write-path anchor bundle under `fast` and `wal_on_fast`,
- compare no-index collection batch ingest against raw TreeDB `batch_write`
  throughput on the same host and aligned settings,
- compare checkpointed collection behavior against `-suite flushdrain` so wins
  are not explained only by deferred work.

### Exit criteria

- native batch ingest is the intended fast path,
- batch throughput clearly exceeds the oracle baseline,
- profiles no longer show replay translation helpers as first-order costs,
- profiles no longer show probe planning dominated by fallback maps or
  per-prefix iterator creation.

## R6. Single-Document Wrappers and Parity

### Scope

Bring `Insert`, `Delete`, and `Upsert` onto the new planner and publisher while
preserving exact semantics.

### Tests first

Add failing tests for:

- single-document parity between oracle and native behavior,
- unique conflict parity,
- reopen and maintenance parity,
- cached same-handle visibility parity.

### Implementation

- route single-document ops through the native planner where appropriate,
- retain a direct tiny-op path only if it is measurably better and does not
  break the architectural rules.

Single-document wrappers must not inherit batch-scale allocations or sorting
machinery unless measurement proves that path better. Stack-bounded scratch and
small fixed-shape planning should be the default expectation for one-document
operations.

### Perf gates

Capture focused single-document benchmarks only after batch path is stable.

This phase MUST include the benchmark coverage needed for final parity
decisions:

- no-index single-document write,
- no-index single-document read,
- no-index parallel point-read throughput.

Unified-bench alignment gate for this phase:

- no material regression beyond documented noise on:
  - `write_rand`
  - `delete_rand`
  - `random_read`
  - `random_read_parallel_acquire_snapshot`
- collection single-document benchmarks should move toward the same direction as
  the corresponding raw TreeDB point-operation anchors,
- no-index single-document write must be evaluated against the comparable raw
  point-write anchor,
- no-index single-document read must be evaluated against `random_read`,
- no-index parallel point-read must be evaluated against
  `random_read_parallel_acquire_snapshot`.

### Exit criteria

- single-document APIs are native-path clean,
- the collection benchmark suite contains the no-index point-read and parallel
  point-read coverage required for final cutover decisions,
- remaining oracle-dependent behavior is external comparison only.

## R7. Default Flip and Cleanup

### Scope

Make the native path the default and delete transitional machinery.

### Tests first

Add failing tests for:

- default path selection being native,
- oracle comparison remaining external-only,
- removal of forbidden translation hooks from the native path.

### Implementation

- flip the internal default,
- delete dead overlay and replay code,
- keep only minimal compatibility or debug hooks if still needed.

### Perf gates

Run the full acceptance and benchmark matrix on the native default path.

Unified-bench gate for this phase:

- run the full agreed raw TreeDB anchor bundle under both `fast` and
  `wal_on_fast`,
- include at least one deferred-work capture via `-suite flushdrain`,
- include at least one settled-after-checkpoint capture for read or scan paths.

### Exit criteria

- all acceptance gates are green,
- native path is the default,
- oracle branch can remain frozen as historical reference only.

## 6. Acceptance Matrix

Every rewrite PR should run the smallest relevant subset first, then the full
acceptance sweep before handoff.

Minimum recurring correctness suite:

- `GOWORK=off go test ./TreeDB/collections -count=1`
- `GOWORK=off go test ./TreeDB/... -run 'TestCollection|TestReopen|TestInvariant|TestIDGeneration' -count=1`
- `GOWORK=off go test ./TreeDB/db -run 'multi_root|Txn|Commit|TestHasPrefixAtRoot' -count=1`
- `GOWORK=off go test ./TreeDB/caching -count=1`
- `GOWORK=off go test ./TreeDB/docs/... -count=1` when docs or contracts change

Additional mandatory suites by phase:

- R1-R3: caching/root-domain specific tests
- R4: warm-root publish, maintenance, and value-log retention tests
- R5-R7: collection batch and cutover parity tests

When the native path introduces hook or counter assertions for forbidden
translation work, those assertions are part of the correctness suite for the
relevant phase.

## 7. Benchmark and Profiling Discipline

Every rewrite PR must capture focused benchmark artifacts, not just mixed suite
runs.

Raw TreeDB `unified-bench` runs are the reference anchor for TreeDB fast-path
alignment. Collection-local benchmarks are necessary but not sufficient by
themselves.

Minimum focused captures:

- `BenchmarkCollectionInsertBatchProvidedID`
- `BenchmarkCollectionInsertBatchWithSecondaryIndexes`
- `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes`

Minimum recurring `unified-bench` anchors:

- write-path anchor:
  - `write_seq`
  - `write_rand`
  - `batch_write`
  - `batch_random`
  - `batch_delete`
  - `delete_rand`
- read and snapshot anchor:
  - `random_read`
  - `random_read_parallel_acquire_snapshot`
  - `full_scan`
  - `prefix_scan`
- deferred-work anchor:
  - `-suite flushdrain`

Additional focused captures by phase:

- R1: root-domain snapshot and run-promotion microbenches
- R2: sorted key/prefix probe microbenches
- R3: grouped cold publish microbenches
- R4: warm-root apply microbenches
- R5+: mixed collection batch workloads

Each performance-facing phase should capture both:

- mixed-under-debt behavior,
- settled-after-checkpoint behavior.

Where the phase can materially shift deferred work, it should also capture a
checkpoint or flush-drain focused measurement.

Reporting rules:

- always report both batch/sec and docs/sec,
- always compare against the oracle baseline on the same harness where
  comparable,
- compare against raw cached TreeDB baselines when evaluating no-index batch
  ingest,
- do not claim a fast-path win from `fast` profile only; include at least one
  more production-like durability profile such as `wal_on_fast`,
- where relevant, report the matching `unified-bench` raw TreeDB anchor result
  beside the collection benchmark result,
- include profile artifact paths in PR notes.

### 7.1 Noise margins and expectations

Performance gating should distinguish between:

- scaffolding phases, which may legitimately show no improvement,
- performance phases, which are expected to improve their target benchmark
  family.

Default guidance:

- raw TreeDB anchor regressions larger than a small documented noise margin
  should block the phase,
- targeted performance phases should show a material improvement in at least one
  relevant collection benchmark or `unified-bench` metric before advancing,
- if a phase is prerequisite-only and does not improve throughput yet, that must
  be stated explicitly in the phase notes.

### 7.2 Canonical unified-bench capture

Unless a phase has a narrower focused need, the standard raw TreeDB anchor
capture should use `-profile-dir` so `benchprof` artifacts are comparable across
PRs.

Suggested write/read anchor capture:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile fast \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -test write_seq,write_rand,batch_write,batch_random,batch_delete,delete_rand,random_read,random_read_parallel_acquire_snapshot,full_scan,prefix_scan \
  -checkpoint-between-tests \
  -read-require-hit \
  -profile-dir "$OUT" \
  -progress=false
```

Suggested deferred-work anchor capture:

```bash
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)

./bin/unified-bench \
  -dbs treedb \
  -profile wal_on_fast \
  -suite flushdrain \
  -keys 500000 \
  -valsize 100 \
  -batchsize 8000 \
  -profile-dir "$OUT" \
  -progress=false
```

Phases may narrow or scale these commands, but deviations should be explained in
PR notes so results remain comparable.

## 8. Performance Exit Criteria

The rewrite is not done until all of the following are true:

1. no-index collection batch ingest is within `2x` of raw cached TreeDB batch
   write throughput on the same harness,
2. no-index collection single-document write is within `3x` of the comparable
   raw cached TreeDB point-write anchor on the same harness,
3. no-index collection single-document read is within `2x` of raw cached TreeDB
   `random_read` on the same harness,
4. no-index collection parallel point-read throughput is within `2x` of raw
   cached TreeDB `random_read_parallel_acquire_snapshot` on the same harness,
5. indexed collection batch ingest is within `4x` of no-index collection batch
   ingest on the same harness,
6. focused profiles for the native path are dominated by engine and hardware
   work such as page building, compression, checksums, memory copies, allocator,
   and IO,
7. focused profiles are no longer dominated by:
   - B-tree restaging,
   - iterator-to-entry conversion,
   - detached-batch replay,
   - repeated batch probe helpers,
   - steady-state full warm-root rebuild.

Missing these criteria requires explicit profiling evidence and a deliberate
spec change.

Where a phase claims elimination of a forbidden translation path, the exit
criteria also require matching hook or counter evidence in addition to wall-time
benchmarks.

## 9. Deletion Criteria for the Old Path

The old cached collections implementation can be retired as an active reference
when:

- the native path is the default,
- correctness parity remains green for at least one full benchmark and CI pass,
- benchmark superiority is demonstrated on the agreed focused suite,
- reopen, recovery, GC, rewrite, and vacuum behavior are unchanged,
- no open blocker remains that depends on oracle-only behavior,
- no remaining native-path acceptance test depends on imported transitional
  helpers
  for correctness or publication.

Until then, the old implementation stays only as an oracle and historical
reference.
