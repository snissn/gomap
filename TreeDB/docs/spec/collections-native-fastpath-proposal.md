# Collections Native Fast-Path Proposal

Status: draft proposal, non-normative.

This document describes the target architecture for a clean reimplementation of
the cached collections execution path. It is intentionally stricter than the
current implementation and is meant to replace the remaining transitional
named-root buffering and publish machinery with native cached TreeDB concepts.

This proposal does **not** redefine the public collection API or the collection
catalog model unless explicitly called out below. It primarily changes the
execution model used by cached collections writes, reads, probes, batch ingest,
and checkpoint publication.

## 1. Motivation

The current cached collections implementation is still architecture-bound.

It continues to pay substantial translation costs in hot paths:

- batch plans are converted into root-local staging structures and replay
  helpers instead of remaining native root-domain state,
- repeated batch existence and uniqueness checks still flow through point or
  prefix probe helpers,
- warm existing-root publication still performs whole-root merge/build work,
- system-root descriptor updates still re-enter batch replay plumbing.

The result is that collection batch ingest spends too much time in orchestration
and intermediate representations instead of raw TreeDB work such as page
building, compression, checksums, memory copies, and storage appends.

### 1.1 Current reference baseline

As of the reference implementation used to motivate this proposal, focused
256-document batch benchmarks are approximately:

- `InsertBatchProvidedID`: ~`722k docs/s`
- `InsertBatchWithSecondaryIndexes`: ~`119k-121k docs/s`
- `InsertBatchCheckpointWithSecondaryIndexes`: ~`2k docs/s`

These numbers are useful only as rewrite baselines. They are not the target end
state.

### 1.2 Rewrite rationale

A targeted rewrite is justified because the remaining bottlenecks are primarily
architectural, not local inefficiencies.

The rewrite should preserve:

- public collection API behavior,
- collection root catalog and root kinds,
- transactional coherence and reopen semantics,
- the existing correctness suite and benchmark harnesses as acceptance oracles.

The rewrite should replace:

- the cached named-root execution path,
- warm-root publish strategy for steady-state updates,
- batch probe shape for existence and uniqueness checks,
- residual system-root replay plumbing.

## 2. Goals

### 2.1 Primary goals

1. Make collections a native cached TreeDB client for batch ingest.
2. Remove non-native translation layers from the collections fast path.
3. Preserve the existing collection correctness contract:
   - primary + index-state + secondary roots remain coherent,
   - grouped visibility remains atomic,
   - reopen and maintenance behavior remain coherent.
4. Move the dominant cost of collection batch ingest from orchestration toward
   CPU, memory-bandwidth, and storage-bandwidth work.

### 2.2 Secondary goals

1. Preserve the current public collections API where practical.
2. Preserve the current root catalog format and per-root format bits unless a
   later change proves them insufficient.
3. Reuse the current test suite as the behavioral acceptance oracle.
4. Make performance review easier by making the fast path structurally obvious:
   root-local sorted runs, vectorized probes, grouped publish, and no replay
   translation steps.

## 3. Non-Goals

This proposal does not attempt to solve the following in the first rewrite:

1. Replacing JSON/index extraction with compiled field extractors.
2. Changing document encoding or collection API semantics.
3. Adding migration tooling for old cached collection implementations.
4. Guaranteeing parity with raw KV throughput for indexed document ingest.

JSON/index extraction is expected to remain visible in profiles after this work,
but the TreeDB-specific overhead should fall significantly.

## 4. Terms

- **Root domain**: a first-class cached write domain associated with one logical
  root, such as the system root, a collection primary root, an index-state
  root, or a secondary index root.
- **Mutable run**: the active in-memory ordered accumulation structure for one
  root domain.
- **Immutable run**: a sealed ordered structure queued for publication.
- **Publish group**: one atomic publish unit spanning the exact set of roots and
  descriptors touched by one logical collection mutation batch.
- **Grouped publish**: one commit boundary that updates one publish group.
- **Warm root**: a root that already has a persisted backend root page id.
- **Cold root**: a root with no persisted backend root page id yet.
- **Canonical unique probe key**: the minimal key or prefix used to detect a
  unique-value conflict without materializing full secondary entry keys that
  include document ids.

## 5. Architectural Principles

### 5.1 Root domains are first-class cached domains

The cached layer MUST treat the system root and named roots as native cached
root domains, not as overlays or sidecar state.

Each root domain MUST own:

- a stable logical identity,
- current published backend root id,
- root format,
- mutable ordered state,
- queued immutable ordered state,
- a version counter used by collection runtime caches.

The cached layer MUST be able to enumerate pending work across all root domains
through one scheduler without special named-root-only or system-root-only flush
plumbing.

For the native path, root domains MUST be the authoritative cached state.

A legacy overlay, side map, or replay buffer MAY exist only as a compatibility
wrapper around the native root-domain implementation during bring-up. It MUST
NOT remain the source of truth for reads, flush scheduling, or publication.

### 5.2 Batch ingest emits root-local native state

Collection batch ingest MUST emit root-local native state directly:

- one primary-root mutation stream,
- one index-state mutation stream,
- one secondary-root mutation stream per affected index,
- system-root descriptor mutations only when root metadata changes.

The fast path MUST NOT materialize `[]batch.Entry` just to move data between
layers that already know the data is ordered and root-scoped.

### 5.3 Publication is run-native, not replay-native

The backend publish path MUST accept ordered root-local state directly.

The fast path MUST NOT rely on:

- detached batches,
- iterator-to-entry replay,
- entry-slice materialization,
- system-root special replay code,

when equivalent ordered root-local structures already exist.

### 5.4 Warm-root steady-state updates must avoid full rebuild semantics

Warm-root publication MUST converge on page-preserving merge/apply semantics.

Rebuilding an entire warm root from a merged iterator is acceptable only as a
threshold-based fallback or bring-up implementation, not as the steady-state
fast path.

### 5.5 The fast path must be explainable in one sentence

The desired steady-state execution model is:

`collection batch plan -> per-root sorted runs -> vectorized root probes -> native grouped publish`

Any implementation that materially resembles:

`collection plan -> B-tree restaging -> iterator scan -> []Entry materialization -> detached batch replay -> rebuild`

is out of scope for the final design.

## 6. Target Execution Model

### 6.1 Root-domain state model

Each root domain MUST expose an internal shape equivalent to:

- mutable ordered run,
- zero or more immutable ordered runs,
- published backend root id,
- root format,
- domain version.

The batch fast path MUST be representable as sorted runs without per-entry tree
insertion.

The mutable ordered run MAY use an append/sort representation or another
structure that can be sealed as an immutable run without reinserting every key
into another in-memory tree.

The first-class write representation on the native batch path MUST itself be a
run builder, append-sort buffer, or equivalent ordered-run producer.

An implementation that accumulates native batch writes in a generic mutable map
or tree and only later converts that structure into runs is not acceptable as
the steady-state native batch path.

An auxiliary point-read or tiny-write index MAY exist for engineering reasons,
but it MUST NOT define the steady-state batch ingest fast path.

The immutable state MUST be a list of sorted runs or tables that are mergeable
without reinsertion into a mutable B-tree.

Mutable B-tree restaging MAY exist only as an explicitly documented tiny-batch
fallback. It MUST NOT be the dominant path for steady-state batch ingest.

#### 6.1.2 Tiny-batch fallback policy

If a tiny-batch fallback exists, the implementation MUST define all of the
following explicitly:

- maximum key-count threshold,
- maximum payload-byte threshold,
- whether the threshold is evaluated per root or per publish group,
- whether the fallback is allowed for single-document wrappers only or also for
  internal micro-batches,
- whether the fallback is disabled for benchmark and profile builds.

The native batch-ingest path targeted by this rewrite MUST default to the
sorted-run path for the benchmark workloads used to judge the rewrite.

The implementation MUST expose counters or hooks for at least:

- tiny-batch fallback batch count,
- tiny-batch fallback key count,
- tiny-batch fallback byte count.

Performance phase PRs MUST report those counters for the targeted collection
benchmarks. A phase that claims to remove restaging cost MUST NOT advance if
the measured benchmark workload still routes materially through tiny-batch
fallback.

#### 6.1.1 Run compaction policy

Root domains MUST define a compaction or merge policy for immutable runs.

The policy MUST:

- bound read amplification,
- preserve newest-wins tombstone semantics,
- avoid unbounded run explosion under sustained batch ingest,
- preserve publish-group atomicity.

The first rewrite MAY use a simple policy such as level-free size-tiered merge,
but the policy MUST be explicit and testable.

### 6.2 Read path

`GetAtRoot`, `HasAtRoot`, `HasManyAtRoot`, `HasPrefixAtRoot`,
`HasPrefixesAtRoot`, and `IteratorAtRoot` MUST resolve against one root-domain
snapshot:

- mutable run,
- immutable runs,
- published backend root.

The read path MUST implement newest-wins semantics, including tombstones.

#### 6.2.1 Snapshot semantics

A root-domain snapshot MUST capture the tuple:

- published backend root id,
- immutable run set,
- mutable run reference.

That tuple MUST be published atomically for readers.

A collections operation that reads multiple roots from one logical snapshot MUST
see a coherent snapshot across:

- system root,
- collection primary root,
- index-state root,
- touched secondary roots.

The implementation SHOULD use RCU-style publication or an equivalent mechanism.

Lock ordering and publish rules MUST prevent observing mixed generations of
system and named roots from one logical snapshot.

### 6.3 Probe path

Collection batch ingest MUST perform probe planning before mutation planning.

The target probe flow is:

1. dedupe document ids inside the batch,
2. dedupe unique index probe prefixes inside the batch,
3. probe the primary root once for all distinct ids,
4. probe each unique secondary root once for all distinct probe prefixes,
5. merge probe results with in-batch conflicts.

The TreeDB fast path MUST NOT perform one tree probe per document when a batch
probe can answer the same question.

#### 6.3.1 Probe API requirements

TreeDB MUST expose vectorized probe APIs for the native batch path, equivalent
to:

- `HasManySortedAtRoot(root, keysSortedUnique) -> bitmap`
- `HasPrefixesSortedAtRoot(root, prefixesSortedUnique) -> bitmap`

These APIs MUST:

- require sorted, deduped inputs,
- execute as one monotonic merged scan over the root-domain snapshot and base
  root,
- preserve newest-wins tombstone semantics,
- avoid per-item iterator seeks as the normal batch path,
- return exact answers,
- return results in a representation that does not allocate per probed key or
  prefix on the steady-state fast path.

A tombstoned base prefix MUST NOT count as a conflict.

Per-item probe helpers that loop over keys or prefixes one-at-a-time are
acceptable only as correctness fallbacks. They are not acceptable native
fast-path implementations.

The implementation MUST expose counters or hooks for:

- per-item key probe fallback count,
- per-item prefix probe fallback count.

Performance phase PRs that target batch probes MUST show those counters at zero
for the focused native batch benchmarks, unless the phase is explicitly marked
as scaffolding-only.

#### 6.3.2 Unique conflict keys

Uniqueness checks MUST operate on canonical unique probe keys or prefixes.

The fast path MUST NOT build full secondary entry keys of the form
`prefix || encodedValue || docID` solely to determine whether a unique value is
already present.

The batch planner SHOULD sort and dedupe in-batch uniqueness probes using
byte-order operations rather than `map[string]...` keyed materialization when
possible.

### 6.4 Batch planning

For each batch, the planner SHOULD produce:

- ordered primary inserts or deletes,
- ordered index-state updates,
- ordered secondary inserts or deletes,
- system-root descriptor deltas only when required.

The planner MAY still compute index-state bytes and uniqueness metadata using
current collections logic in the initial rewrite, but the resulting storage plan
MUST remain root-local and ordered.

The planner MUST preserve caller-visible batch result ordering even if it sorts
internally for execution.

The planner MUST detect obvious batch-local conflicts before expensive payload
construction where practical, including:

- duplicate document ids inside the same batch,
- duplicate unique probe prefixes inside the same batch.

The steady-state fast path MUST NOT allocate full mutation payloads for entries
that can be rejected during this fail-fast planning stage.

### 6.5 Flush and checkpoint

Root domains MUST participate in one cached flush scheduler.

Checkpoint behavior MUST:

1. seal mutable root-domain runs,
2. publish pending immutable root-domain runs through one grouped publish path,
3. atomically update system-root descriptors that point at the new root ids.

There MUST NOT be a separate named-root snapshot-and-replay phase outside the
generic cached flush pipeline.

Root domains themselves MUST be the flush units or the direct source of flush
units for the native path.

#### 6.5.1 Publish groups

The unit of publication for collection writes MUST be a publish group.

For one collection mutation batch, the publish group MUST contain exactly the
set of touched roots:

- primary root,
- index-state root,
- touched secondary roots,
- system-root descriptor table when root metadata changes.

The scheduler MUST track dirty work by publish group id, not merely by
independent root id.

The scheduler MUST NOT publish a strict subset of one publish group.

Failure or retry semantics MUST preserve the full publish group until the entire
group is durably published.

The scheduler MUST define:

- queueing order for pending publish groups,
- fairness rules across collections or root domains,
- backpressure interaction when publish groups accumulate,
- whether adjacent publish groups may be coalesced before publish.

If coalescing is allowed, it MUST preserve:

- atomicity of each original publish group,
- per-group failure isolation,
- pinned multi-root snapshot coherence.

The steady-state scheduler MUST NOT starve one publish group indefinitely while
newer groups continue to make progress.

Grouped publish MUST consume stable run or table views directly.

The native grouped-publish path MUST NOT re-materialize one publish group into:

- copied per-root entry slices,
- copied replay buffers,
- detached batch transport structures.

### 6.5.2 Multi-root snapshot pinning

The backend and cached layers MUST support a pinned multi-root snapshot view for
operations that read more than one root from one logical collection snapshot.

The native path MUST NOT satisfy multi-root collection reads by acquiring a new
root snapshot independently for each root access if that can expose mixed
generations from the same publish group.

### 6.6 Cold-root publish

Cold-root publish SHOULD bulk-build directly from ordered root-domain runs.

This is already close to the desired shape and SHOULD remain the default cold
path.

### 6.7 Warm-root publish

Warm-root publish is the critical algorithmic target.

The steady-state behavior MUST:

- read only the necessary base-tree structure,
- merge ordered delta runs against the published root,
- bound work to touched or changed structure rather than full-keyspace scans,
- preserve unchanged pages where possible,
- write only changed pages and changed outer-leaf or value-log data,
- publish a new root id and retire replaced pages.

A whole-root base scan followed by full rebuild MUST NOT be the steady-state
warm-root path.

A full warm-root rebuild MAY exist only as an explicitly documented threshold
fallback.

#### 6.7.1 Warm publish decision policy

The implementation MUST define a documented threshold policy that chooses
between:

- targeted warm apply,
- threshold fallback rebuild.

The policy SHOULD consider at least:

- changed key ratio,
- estimated changed page ratio,
- available delta ordering information,
- memory budget.

The threshold policy MUST be benchmarked and visible in tests or docs.

#### 6.7.2 Warm apply mechanism

The preferred warm-root publish mechanism is a zipper-native apply or
page-aware sorted-delta merge.

The implementation MUST avoid an `Iterator(nil, nil)`-equivalent full base-tree
scan on the steady-state warm path.

The implementation MUST bound work to the touched-page frontier plus the
required search or descent structure needed to reach it. A steady-state warm
apply implementation that walks most of the tree while only changing a small
delta is not acceptable.

The implementation MUST avoid broad page-id enumeration whose cost is
asymptotically equivalent to full warm-root rebuild, even if it is described as
retirement bookkeeping.

The implementation MUST derive retired or replaced pages directly from the warm
apply algorithm. It MUST NOT require a full page enumeration pass such as
collecting every existing page id to determine retirement on the steady-state
warm path.

#### 6.7.3 Value-log delta derivation

Warm-root publish MUST derive old-vs-new value-log retention deltas inline while
applying sorted deltas.

The fast path MUST NOT issue per-key base-tree lookups merely to discover old
value pointers for retention bookkeeping.

The intended mechanism is that the warm apply path observes both the old entry
and the new delta while rewriting touched structure, and emits value-log
retention deltas directly from that merge context.

#### 6.7.4 Warm apply counters

The implementation MUST expose counters or hooks sufficient to prove the warm
path stayed on the intended algorithm, including at least:

- warm apply pages visited,
- warm apply pages rewritten,
- warm apply rebuild fallback count,
- warm apply per-key retention-lookup fallback count.

Performance phase PRs that target warm apply MUST report those counters for the
focused warm benchmarks and for checkpointed indexed-batch benchmarks.

### 6.8 System-root descriptor publish

System-root descriptor updates MUST be part of the same grouped publish model as
named roots.

The grouped publish unit is:

- zero or more named-root run sets,
- optional ordered system-root run set,
- one commit boundary.

System-root descriptor publication MUST use ordered system-root run or table
state directly.

System-root descriptor publication MUST NOT fall back to detached-batch replay
when ordered system-root state is already available.

### 6.9 Ownership and copy semantics

The rewrite MUST define explicit ownership rules for batch inputs and internal
storage plans.

The public collections API SHOULD preserve the current safe ownership contract
unless explicitly changed.

Internally, the implementation MAY distinguish between:

- view-based inputs that are valid only for the call,
- owned inputs or buffers that may be retained or published later.

The fast path SHOULD prefer explicit owned/view APIs over implicit defensive
copying.

Any internal buffer stealing MUST be explicit at the API boundary.

## 7. Required Invariants

The new implementation MUST preserve these invariants:

1. The public collections API remains behaviorally compatible unless an
   intentional v2 change is documented.
2. Primary, index-state, and secondary roots remain atomically coherent.
3. Reopen, crash recovery, and maintenance operations preserve collection
   coherence.
4. Root-format bits remain honored for live writes and maintenance.
5. Cached same-handle visibility before checkpoint remains correct.
6. Snapshot isolation remains correct across system and named roots.
7. Unique-index conflicts remain exact; no false negatives are allowed.
8. Publish-group failure MUST NOT expose partial visibility across roots.
9. Value-log retention state MUST remain coherent with grouped publish results.
10. Multi-root logical reads MUST be satisfiable from one pinned coherent
    snapshot view.
11. Root domains MUST remain the authoritative cached state for the native path.

## 8. Forbidden Patterns

The rewritten fast path MUST NOT do the following on the normal batch-ingest
path:

1. Materialize `[]batch.Entry` from ordered root-local state.
2. Replay ordered root-local state through detached batches.
3. Issue per-document point probes for existence checks.
4. Issue per-document prefix probes for uniqueness checks.
5. Rebuild an entire warm root from a merged iterator as the steady-state path.
6. Publish system-root descriptor updates through a separate replay-only path.
7. Restage steady-state batch ingest through mutable B-tree insertion per key.
8. Perform per-key base-tree lookups to derive warm-publish value-log deltas.
9. Build full secondary entry keys solely to answer uniqueness checks.
10. Publish a strict subset of a collection publish group.
11. Use per-item probe loops or per-prefix iterator creation as the normal batch
    fast path.
12. Use `map[string]...`-style keyed materialization as the steady-state
    dedupe/probe mechanism for large batch uniqueness checks.

These may exist temporarily behind tests during bring-up, but they are not
acceptable as the final steady-state architecture.

## 9. Public API and Compatibility

### 9.1 Public API

The first rewrite SHOULD preserve:

- `Collection.Insert`
- `Collection.InsertBatch`
- `Collection.Delete`
- `Collection.Get`
- `Collection.FindByIndex`
- collection manager lifecycle APIs

The rewrite MAY add internal-only fast-path interfaces used by cached TreeDB,
but SHOULD avoid exposing those until the design stabilizes.

### 9.2 On-disk model

The first rewrite SHOULD preserve:

- collection metadata encoding,
- root descriptor encoding,
- per-root format bits,
- dedicated primary/index-state/secondary root model.

If the rewrite proves a format change is necessary, it MUST update:

- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/contracts.md`
- `TreeDB/docs/spec/verification.md`

and MAY break compatibility because TreeDB remains pre-alpha.

### 9.3 Compatibility strategy

The current implementation SHOULD remain available behind an internal switch or
isolated path until the new execution path demonstrates correctness and
performance superiority.

The old path SHOULD serve only as:

- a semantic reference,
- an acceptance oracle,
- a benchmark comparison baseline.

## 10. Implementation Phasing

The rewrite SHOULD be implemented behind an internal switch or isolated path so
the current implementation remains available as a correctness reference.

Suggested phases:

### Phase A: Root-domain run model

- introduce native root-domain mutable and immutable run structures,
- make reads resolve against run snapshots,
- keep publication behavior functionally equivalent during bring-up.

### Phase B: Vectorized batch probes

- add root-local vectorized key and prefix probe APIs,
- route collection batch existence and uniqueness checks through them,
- remove per-document batch probe behavior from the new path.

### Phase C: Grouped native publish

- publish named roots and system-root tables through one grouped native path,
- remove detached-batch replay from named-root and system-root publication.

### Phase D: Warm-root merge

- replace full warm-root rebuild as the steady-state publish path,
- preserve correctness and performance parity for cold roots and checkpoint
  behavior,
- add threshold fallback policy explicitly.

### Phase E: Cutover

- run correctness parity against the existing suite,
- run benchmark parity and regression checks,
- switch the default internal execution path once the new path is clearly
  superior.

### 10.1 Backend API sketch

The rewrite SHOULD converge on backend/internal APIs conceptually equivalent to:

- `PublishRootsFromRuns(group, rootRuns, systemRun, formats) -> publishedRoots`
- `ApplyWarmDeltaRun(root, baseRootID, deltaRuns, format) -> newRootID`
- `PublishSystemFromRun(systemRun) -> newSystemRootID`
- `HasManySortedAtRoot(root, keysSortedUnique) -> bitmap`
- `HasPrefixesSortedAtRoot(root, prefixesSortedUnique) -> bitmap`
- `AcquireRootSnapshotView(groupOrSeq) -> view`

Exact names MAY differ, but the APIs MUST accept ordered run or table state
rather than generic detached batch replay inputs.

For the native fast path, those APIs MUST consume either:

- stable run or table views, or
- explicitly owned buffers transferred to the backend,

so that per-entry defensive copying does not become the transport mechanism.

The native path SHOULD expose hooks or counters sufficient to prove that it did
not regress into:

- iterator-to-entry replay,
- detached-batch replay,
- steady-state full warm rebuild,
- per-key warm-publish base lookups.

The native path SHOULD also expose counters for:

- tiny-batch fallback usage,
- per-item probe fallback usage,
- grouped publish replay or copy fallback usage.

## 11. Test Plan

The existing collection correctness suite SHOULD become the acceptance suite for
the rewrite.

Required gates:

- collection lifecycle tests,
- id-generation tests,
- secondary-index lifecycle/query/conflict/update tests,
- transaction/coherence tests,
- reopen/recovery matrix tests,
- maintenance/GC/rewrite/vacuum named-root tests,
- cached same-handle visibility tests,
- cached failure/retry tests,
- consistency fuzz tests.

Additional rewrite-specific tests SHOULD be added for:

1. root-domain run promotion without semantic drift,
2. vectorized key and prefix probes with tombstones and newest-wins semantics,
3. grouped publish of named roots plus system root,
4. warm-root merge preserving unchanged pages,
5. value-log retention delta correctness during warm apply,
6. removal of forbidden translation paths in the final fast path,
7. run compaction or merge policy correctness,
8. publish-group failure and retry semantics.
9. pinned multi-root snapshot coherence across system and named roots.
10. no fallback into per-item probe loops on the native batch path.

## 12. Performance Gates

The rewrite SHOULD be evaluated against both the current collections path and
raw cached TreeDB baselines.

Minimum benchmark set:

- `BenchmarkCollectionInsertBatchProvidedID`
- `BenchmarkCollectionInsertBatchWithSecondaryIndexes`
- `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes`

Supplementary benchmarks:

- raw cached TreeDB batch write throughput,
- raw cached TreeDB random read throughput,
- focused warm-root publish microbench,
- focused cold-root publish microbench,
- focused sorted key probe microbench,
- focused sorted prefix probe microbench,
- mixed-under-debt and settled-after-checkpoint variants of the collection batch
  path,
- current collections branch throughput before rewrite.

Required architectural counters for performance-relevant phases:

- tiny-batch fallback batch or key count,
- per-item key probe fallback count,
- per-item prefix probe fallback count,
- detached-batch replay fallback count,
- warm apply rebuild fallback count,
- warm apply per-key retention-lookup fallback count.

### 12.1 Unified-bench anchor runs

Phase and cutover decisions SHOULD be grounded in `unified-bench` runs for raw
TreeDB, not only in collection-local microbenchmarks.

At minimum, the rewrite SHOULD maintain a standard set of raw TreeDB anchor
runs:

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

These anchor runs SHOULD be captured with:

- `-profile fast` to approximate throughput ceiling behavior,
- `-profile wal_on_fast` to validate the same direction under a more
  production-like TreeDB durability profile,
- `-read-require-hit` for read-heavy anchors so wins cannot come from misses or
  silent contract drift,
- `-profile-dir` so CPU, allocation, checkpoint, and contention artifacts are
  available for `benchprof`.

Where scan or read comparisons matter, runs SHOULD distinguish:

- mixed-under-debt behavior,
- settled-after-checkpoint or settled-before-scan behavior.

The rewrite is not considered aligned with TreeDB fast paths if collection
benchmarks improve while these raw TreeDB anchor runs regress materially
without explicit rationale.

### 12.2 Collection-to-raw alignment rules

When collection throughput is compared against raw TreeDB `unified-bench`
results, comparisons SHOULD use aligned workload settings where practical:

- comparable payload sizes,
- comparable batch sizes,
- comparable durability profiles,
- the same benchmark host and harness generation.

Collection-focused benchmarks remain the direct measure of the new path, but
`unified-bench` provides the reference ceiling for how close the implementation
is getting to TreeDB’s native fast paths.

The performance contract is explicit:

- for no-index collection operations, the native path is expected to stay as
  close as practical to raw cached TreeDB throughput on the same host and
  profile,
- for indexed collection operations, exact parity is not expected, but the
  implementation must remain a bounded multiplier above the no-index path rather
  than regressing into architecture-bound overhead.

The rewrite therefore has both:

1. a **north star** of raw TreeDB parity for no-index reads and writes, and
2. a **minimum ship gate** defined by the ratio targets below.

Benchmark reporting SHOULD include both batch/sec and docs/sec for collection
benchmarks.

Claims about the native fast path SHOULD NOT rely only on the most aggressive
throughput profile. At least one focused acceptance capture SHOULD also run
under a more production-like durability profile such as `wal_on_fast`, plus a
smaller durable-boundary workload when practical.

Focused single-benchmark CPU and allocation profiles MUST be captured for at
least:

- indexed batch ingest,
- warm-root publish,
- checkpointed indexed batch ingest,
- vectorized probe microbenches.

Where hooks or counters exist, the native fast path SHOULD also prove absence of
forbidden translation work during focused runs.

Success criteria for TreeDB-specific overhead:

1. no-index batch ingest SHOULD move materially closer to raw cached TreeDB
   batch write throughput on the same harness,
2. indexed batch ingest SHOULD stop being dominated by translation-layer costs,
3. profiles SHOULD be dominated by page building, compression/checksum, memory
   copy, allocator, or IO work rather than orchestration helpers.

The rewrite is not considered complete while profiles are still dominated by:

- root-local B-tree restaging,
- iterator-to-entry conversion,
- detached-batch replay,
- repeated batch probe helpers,
- whole-root warm rebuilds.

### 12.3 Target ratios

The initial rewrite is performance-motivated by raw TreeDB parity, but the
minimum cutover gates are:

- no-index collection batch ingest within `2x` of raw cached TreeDB batch write
  throughput on the same harness,
- no-index collection single-document write within `3x` of the comparable raw
  cached TreeDB point-write anchor on the same harness,
- no-index collection single-document read within `2x` of raw cached TreeDB
  `random_read` on the same harness,
- no-index collection parallel point-read throughput within `2x` of raw cached
  TreeDB `random_read_parallel_acquire_snapshot` on the same harness,
- indexed collection batch ingest within `4x` of no-index collection batch
  ingest on the same harness,
- indexed point-read and lookup paths within a documented bounded multiplier of
  the corresponding no-index collection reads on the same harness.

These ratios are intentionally aggressive. Missing them requires explicit
profiling evidence and rationale.

These are minimum gates, not the ambition ceiling. Passing them does not by
itself prove that the implementation is close enough to hardware-limited
behavior; it only means the rewrite is no longer obviously disqualified on
throughput grounds.

### 12.4 Phase-gating model

Relevant rewrite phases SHOULD use a two-layer performance gate:

1. raw TreeDB no-regression gate on the applicable `unified-bench` anchors,
2. collection-path improvement or no-regression gate on the focused collection
   benchmarks for that phase.

Scaffolding phases MAY be allowed to show no material improvement, but they
SHOULD still satisfy raw TreeDB no-regression gates within a documented noise
margin.

Where a phase touches a collection benchmark family with a direct raw TreeDB
analog, the phase notes SHOULD state:

- the raw TreeDB anchor it is trying to track,
- whether the phase is expected to improve parity or merely avoid regression,
- whether the collection benchmark moved closer to or farther from that raw
  anchor.

Performance-oriented phases SHOULD demonstrate at least one material improvement
in the benchmark family they are intended to change, or they SHOULD not advance
without an explicit rationale that the phase is prerequisite-only.

Where architectural counters exist, performance-oriented phases SHOULD also
demonstrate that the targeted forbidden-path counter moved to zero or declined
materially in the focused benchmark family.

## 13. Cutover Rule

The current implementation SHOULD remain available until all of the following
are true:

1. correctness parity is green,
2. reopen, recovery, and maintenance parity is green,
3. benchmark results clearly beat the current path on the same harness,
4. the dominant profile costs move from orchestration to engine or hardware
   work.

At that point, the transitional cached collections path can be orphaned and
deleted.
