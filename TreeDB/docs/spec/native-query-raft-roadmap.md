# Native Query and Raft Roadmap

Status: draft proposal, non-normative.

This document defines the proposed query feature roadmap for the TreeDB native
protocol and explains when Raft/distributed execution should enter the stack.
It assumes the native wire protocol described in
`TreeDB/docs/spec/native-wire-protocol.md` and the implementation discipline in
`TreeDB/docs/spec/native-wire-implementation-guidelines.md`.

## 1. Recommendation

Raft should be early in the architecture, but not first in implementation.

TreeDB should first define and test:

1. native wire framing,
2. versioned command schemas,
3. deterministic command-entry encoding,
4. single-node native server behavior for the core collection surface.

After that, Raft should be added for the small deterministic write surface before
the system grows rich distributed query features such as fanout aggregation or
server-side WASM callbacks.

Raft should not wait until the full query roadmap is complete. Rich queries
should be designed with distributed execution in mind, but the first distributed
system should replicate boring, deterministic metadata and mutation commands.

## 2. Design Principles

1. The wire protocol is not the Raft log.
2. Every replicated write command must have deterministic command-entry bytes.
3. Reads are not Raft-log entries unless they mutate state.
4. Query features should be organized by distributed complexity.
5. Aggregations should expose partial-result formats before cross-node fanout is
   implemented.
6. Server-side callbacks must be bounded, deterministic where required, and
   feature-gated.
7. Cursor resources must be cancellable and budgeted from v1.
8. Every roadmap round must end with a dedicated performance pass before the
   next round starts.

User-command WAL is local crash-recovery durability state and is not a Raft log
entry. Applying a committed deterministic command to TreeDB must satisfy the
local command WAL publish rule from `user-command-wal.md` when the command
mutates local state. A node must not report `locally_recoverable`, advance
durable applied-index/idempotency metadata, or return
`ack_policy=raft_committed` success from that node until the local command
effects and `AppliedLSN` are durable, unless a later Raft stable-store recovery
spec explicitly guarantees replay before serving reads.

## 3. Phase-Close Performance Passes

Each roadmap round, such as R0, R1, R2, and R3, MUST reserve its final sprint
slice for aggressive benchmarking, profiling, and optimization. This final slice
is not a feature-expansion sprint. It exists to measure the new surface, isolate
regressions, profile the dominant costs, optimize the hot path where the data is
clear, and leave behind benchmark artifacts that future rounds can compare
against.

The expected benchmark hierarchy is:

1. fast local microbenchmarks for codec, parser, dispatch, state-machine, or
   query hot paths;
2. workload-coupled benchmarks that exercise the feature through its real
   product path;
3. periodic larger-system validation runs once a phase touches storage,
   transport, or distributed execution behavior.

A phase-close performance pass SHOULD include:

- before/after benchmark runs against the previous phase or agreed baseline,
- CPU, allocation, block, mutex, and trace profiles for the dominant workload,
- pprof/inlining/escape-analysis review when a hot path is unclear,
- allocation guard tests or regression tests for optimized hot paths,
- explicit native-wire benchmark labels that do not overlap with direct
  collection, Mongo driver, or Mongo raw-wire labels,
- a short report or PR note listing commands, artifact directories, benchmark
  deltas, profile bottlenecks, optimizations made, and intentionally deferred
  follow-ups.

A roadmap round SHOULD NOT advance to the next round while its phase-close pass
has unexplained material regressions in the primary benchmark family.
Prerequisite-only rounds may close with no optimization, but they still need
benchmark evidence proving the new scaffolding did not introduce avoidable hot
path cost.

## 4. Implementation Order

### R0. Protocol Spec, Registry, and Codecs

Define:

- frame header,
- body sections,
- command IDs,
- command versions,
- schema registry and drift-test policy,
- error model,
- byte-vector encoding,
- typed scalar encoding,
- ack and consistency policies,
- deterministic command-entry envelope.

Suggested slices:

- R0a: protocol constants, schema registry, and golden byte fixtures,
- R0b: frame/section codec, fuzzing, and malformed-frame tests,
- R0c: deterministic-entry encoder for the mutation allowlist,
- R0d: nativewire codec/schema benchmark suite and reusable hot-path scratch
  APIs.

Acceptance:

- versioned schema registry for frame, section, command, feature, policy, and
  error-code allocations,
- generated Go constants or drift tests proving constants match the registry,
- codec package with golden byte fixtures,
- schema-validator tests for required, optional, repeated, ordered, and
  deterministic sections,
- feature-negotiation rejection tests,
- unknown-section and unknown-flag compatibility tests,
- fuzz tests for frame, section, byte-vector, and command decoding,
- deterministic-entry canonicalization and rejection tests,
- max-frame, malformed-frame, and bounded-allocation tests,
- nativewire microbenchmarks for fixed headers, command headers, section
  envelopes, byte vectors, decode+validate, and deterministic-entry encoding,
- benchmark coverage for every command schema marked `BenchmarkRequired`,
- allocation guard tests proving reusable decode, validation, and canonical
  entry paths stay allocation-free after scratch warmup.

### R1. Single-Node Native Server MVP

Implement the native server for:

- `hello`,
- `create_collection`,
- `create_index`,
- `insert_batch`,
- `delete_batch`,
- `get_many`,
- `index_lookup`,
- `index_range`,
- `open_scan`,
- `cursor_next`,
- `cursor_close`,
- `flush_collection`,
- `checkpoint`,
- `stats`.

Suggested slices:

- R1a: server lifecycle, `hello`/`hello_ok`, `ping`/`pong`, `goaway`, and
  `stats`,
- R1b: collection metadata commands and connection-local handles,
- R1c: read commands and pull-cursor lifecycle,
- R1d: mutation commands with ack-policy handling,
- R1e: TCP, Unix socket, and in-process benchmark transports over the same
  dispatch path,
- R1f: phase-close native server benchmarking, profiling, and hot-path
  optimization.

Acceptance:

- parity tests against direct `TreeDB/collections` calls,
- native-wire TCP and in-process benchmark modes,
- clear benchmark separation from Mongo raw-wire modes,
- cursor cancellation and resource-limit tests,
- a phase-close benchmark/profile report comparing direct collection,
  native-wire in-process, and native-wire TCP paths, including per-frame,
  decode, dispatch, encode, cursor, and collection-adapter overhead.

R1 closeout evidence is recorded in
`TreeDB/docs/spec/native-wire-r1-closeout.md`.

### R2. Deterministic Command Entry v1

Implement a canonical command-entry encoder for mutating commands:

- collection metadata changes,
- index metadata changes,
- insert/replace/delete batches,
- flush/checkpoint only if a cluster policy decides they are replicated
  commands rather than local maintenance barriers.

Acceptance:

- golden canonical-entry byte fixtures for every replicated command version,
- deterministic encoding tests,
- cross-process/cross-map-order stability tests,
- idempotency-key tests,
- same logical mutation encoded from shuffled sections, different request IDs,
  acknowledgement policies, consistency policies, deadlines, compression
  choices, response-shaping flags, and trace metadata produces the same
  canonical entry digest,
- rejection tests for non-deterministic sections, duplicate singleton sections,
  unsupported command versions, local handles, and missing guards.

### R3. Raft MVP for Writes

Add Raft around the deterministic write set only.

The current R3a planning slice (#1654, with executable children #3037-#3043)
is the local state-machine boundary before networked Raft is selected:
committed deterministic `CommandEntryV1` bytes are decoded directly, validated
against command digest/target/idempotency rules, lowered to the local
user-command WAL `CommandEnvelope` payload, applied through the normal executor,
and only then allowed to advance future apply-progress metadata according to the
selected local command-WAL/`AppliedLSN` recoverability boundary.

Replicate:

- collection create/drop metadata,
- index create/drop metadata,
- insert/replace/delete batches,
- deterministic schema/catalog guards,
- idempotency records.

Do not replicate:

- ordinary reads,
- local cursor state,
- tracing/deadlines,
- physical maintenance commands unless the cluster policy explicitly requires
  them.

Acceptance:

- leader writes commit and apply on followers,
- duplicate client requests are deduplicated by `client_id + sequence` or a
  stable idempotency key,
- duplicate idempotency identity with the same digest returns the original
  outcome,
- duplicate idempotency identity with a different digest fails deterministically,
- catalog guard races commit in one log order and replay to the same success or
  guard-failure results on every replica,
- failed leadership changes do not double-apply mutations,
- restart tests preserve Raft log, stable metadata, and applied collection state,
- a node distinguishes `consensus_committed`, `locally_applied`, and
  `locally_recoverable`; client `raft_committed` success is not returned until
  the responding node satisfies the selected local apply durability rule,
- persistent applied-index, idempotency-result, and catalog-guard outcome
  metadata cannot advance past a collection mutation unless the corresponding
  local command-WAL frame, normal executor effects, and selected
  root/`AppliedLSN` boundary are recoverable locally, or a later stable-Raft
  replay design proves the entry is replayed before serving;
- the same committed entry sequence applied to fresh DBs in separate processes
  produces the same logical state digest,
- snapshot restore plus log-tail replay produces the same logical state digest
  as full log replay, with `RecoveryStatusV1` reporting `tail_pending`,
  `tail_complete`, and `ready_applied_index` from local durable evidence rather
  than claiming production snapshot transfer, log truncation, or rejoin support,
- mixed-version clusters refuse to advertise or append command versions that any
  voting replica cannot decode and apply.

### R4. Read Consistency Modes

Implement explicit read policies:

```text
local_stale
leader_read
linearizable
lease_read
```

`local_stale` may read from any node and may be behind the leader.

`leader_read` routes to the current leader but does not necessarily perform a
fresh read-index barrier.

`linearizable` performs the consensus read barrier required by the selected Raft
implementation before reading.

The current nativewire bridge for `linearizable` is a contract-only substrate:
a configured read-index provider must return a read-index proof marked with
production evidence provenance, then the existing applied-index waiter must
prove local state covers every TreeDB command at or below that read index.
Harness and other test-only evidence can exercise lower-level composition but
must not satisfy nativewire `linearizable`. If either proof is unavailable,
non-production, missing quorum, targets the wrong node/group, or local apply
lags the read index, the read fails closed. A production provider whose
read-index proof can land on no-op/config Raft log entries must add applied
Raft-index tracking or translate the proof to the latest TreeDB command index at
or below the proof before relying on the command FSM progress.

The first routed slice is intentionally fail closed. One nativewire `get_many`
document ID and a Mongo exact-`_id` find derive the catalog token and owner, but
both public paths reject before local collection observation. A real production
Raft integration must structurally bind the exact serving collection store or
manager identity to the same owner proof before either path can be enabled.
`GroupRoutedReadIndexCoordinator` remains internal contract scaffolding for
owner selection and read-index-before-apply ordering; its static synthetic
benchmark is not an enabled-path, storage, network, or quorum measurement.
Consequently this issue makes no enabled routed-read latency claim.

Non-shard-key, secondary/unique-index, multi-ID, scatter, follower, and remote
data-plane reads remain deferred to explicit later work. All token/ring
document mutations also fail closed until authoritative collection and index
metadata is structurally bound to the exact owner route proof. A gateway-local
collection-manager copy is not sufficient evidence, even when it currently
reports no indexes. Collection-placement mutations remain supported.

`lease_read` is allowed only if leader leases are implemented and the server can
prove the lease is valid.

Acceptance:

- response metadata reports the actual consistency mode used,
- stale follower reads are explicitly labeled,
- linearizable reads fail or redirect when the node cannot prove leadership or
  freshness.

### R5. Local Advanced Queries

Implement richer single-node reads only after the core read/write and cursor
model is stable:

- projection,
- server-side predicate filters over bounded candidate sets,
- count-only index queries,
- local `count`, `min`, `max`, `sum`, `avg`,
- local group-by over indexed scalar fields,
- local top-k over bounded/indexed ranges,
- `explain` and query stats.

Acceptance:

- all advanced queries have resource limits,
- all query plans can be explained,
- aggregation responses can be encoded as partial results.

### R6. Distributed Query Execution

Add distributed query execution after Raft write replication and read consistency
policies are working.

Initial distributed query features:

- scatter/gather get-many by partition routing,
- scatter/gather index lookup,
- fanout range scan with coordinator-side merge,
- partial aggregation merge,
- distributed cursor cancellation,
- shard-level and coordinator-level memory/time budgets.

Acceptance:

- coordinator reports shard participation and partial failures,
- global limits and ordering are correct,
- canceled distributed cursors release shard resources,
- partial aggregation merge is deterministic.

### R7. Server-Side WASM or Callback Execution

WASM should be last or feature-gated behind explicit experimental flags.

Read-only callbacks may come first:

- bounded map/filter over scan batches,
- no host state mutation,
- deterministic imports only,
- fuel, time, memory, and result-size limits,
- module hash in request metadata.

Write-producing callbacks require a stricter policy:

- callback execution must expand to deterministic mutation batches before Raft
  commit, or
- the Raft log must record a deterministic invocation plus module hash and all
  deterministic inputs needed to replay exactly.

The safer default is to log the expanded mutation batch, not arbitrary callback
execution, until determinism and audit tooling are mature.

## 5. Query Feature Tiers

### Tier 0: Core Collection Surface

These features are required before Raft:

- collection metadata commands,
- index metadata commands,
- insert/replace/delete batches,
- primary get/get-many,
- secondary equality lookup,
- secondary range lookup,
- bounded primary scans,
- cursor pagination,
- cancellation,
- flush/checkpoint barriers,
- stats.

These map directly onto current collection and ordered-root concepts.

### Tier 1: Local Query Quality

These features may be implemented before or after Raft, but they should not
delay Raft MVP:

- projection,
- count-only queries,
- bounded filters,
- explain plans,
- response-side query stats,
- server-side limits for documents, bytes, time, and intermediate rows.

Projection and count-only queries are high-value because they reduce network
bytes without introducing hard distributed semantics.

### Tier 2: Local Aggregations

Local aggregation should use partial-result encodings from the start.

Initial aggregations:

- `count`,
- `min`,
- `max`,
- `sum`,
- `avg` as `sum + count`,
- group by one indexed scalar,
- top-k over an indexed range.

Aggregation requests MUST include explicit limits for:

- scanned candidates,
- groups,
- output bytes,
- execution time.

### Tier 3: Distributed Queries

Distributed queries require a partition/shard policy. The roadmap should not
pretend global queries are simple until that policy exists.

Open design areas:

- collection partitioning by primary key hash or range,
- secondary-index partitioning,
- coordinator selection,
- shard-local cursor ownership,
- global ordering,
- partial failure semantics,
- retry and resume tokens,
- query admission control.

Distributed aggregation should be implemented as:

```text
shard query -> typed partial result -> coordinator merge -> final response
```

The partial result format should be deterministic and independent of the wire
transport frame.

### Tier 4: Server-Side WASM and Callbacks

Server-side callbacks are powerful but easy to make unsafe or non-deterministic.

Required policy before enabling:

- module hash and optional module registry,
- deterministic import set,
- no wall-clock, random, network, filesystem, or process-global access,
- fuel limit,
- memory limit,
- result-size limit,
- explicit read-only vs write-producing mode,
- stable error and timeout behavior,
- observability for callback CPU/memory/result bytes.

For distributed mode:

- read-only callbacks can run shard-local and return partial outputs,
- write-producing callbacks must produce deterministic mutation batches before
  consensus or be replayable byte-for-byte from the Raft log.

## 6. Raft Policy Boundaries

### 6.1 Writes

The leader validates client mutation commands and appends deterministic command
entries to Raft. Followers apply committed command entries to their local TreeDB
state machine.

The command entry should include:

- command ID and version,
- collection or metadata target,
- mutation input bytes,
- expected catalog/schema guard when applicable,
- idempotency key,
- deterministic command flags.

The command entry should not include:

- request ID,
- stream ID,
- connection-local collection handle,
- tracing,
- deadline,
- negotiated compression,
- response page size,
- client socket metadata.

### 6.2 Reads

Reads are governed by consistency policy, not by command-entry replication.

The server may satisfy `local_stale` reads from a follower. Stronger reads must
be routed or proved according to the cluster policy.

Read responses in cluster mode SHOULD include:

- serving node ID,
- leader ID if known,
- applied log index,
- consistency mode requested,
- consistency mode actually used.

### 6.3 Metadata

Collection and index metadata changes are replicated writes. They should use the
same deterministic command-entry path as document mutations.

Open question: whether global metadata is one Raft group or whether each
collection/shard has its own group plus a separate metadata group. The first
Raft MVP SHOULD use the simplest model that can prove correctness.

### 6.4 Maintenance

Local physical maintenance, such as value-log rewrite, GC, leaf-generation pack,
or index vacuum, should not become user-visible replicated commands by default.

Cluster-visible maintenance policy is needed only when physical layout choices
affect routing, snapshots, or catch-up behavior.

## 7. TODO List

### Protocol TODO

- Keep command IDs and section IDs synchronized between
  `native-wire-protocol.md`, the schema registry, and drift tests.
- Keep scalar, byte-vector, result-set, presence-bitmap, and cursor-limit
  encodings covered by fixtures and conformance tests.
- Define error-code mapping from current collection errors.
- Define authentication placeholder fields.
- Keep benchmark mode labels synchronized with the benchmark harness and report
  renderers.

### Query TODO

- Specify projection syntax.
- Specify bounded predicate filter syntax.
- Specify count-only query response.
- Specify local aggregation request/response.
- Specify partial aggregation encoding.
- Specify explain-plan format.
- Specify query stats fields.

### Raft TODO

- Choose Raft library or implementation boundary.
- Define `CommandEntryV1` bytes.
- Define idempotency record storage.
- Finalize the stable-store byte format for the required applied-index and
  idempotency ordering rule: those metadata records must not advance past local
  command WAL recoverability and `AppliedLSN` publication for local mutations.
- Define stable store and log store mappings.
- Define snapshot/export/restore format.
- Define read-index or equivalent linearizable-read mechanism.
- Define initial cluster metadata model.

### WASM TODO

- Decide whether WASM is needed before distributed query MVP.
- Define deterministic host imports.
- Define module hash and registry policy.
- Define fuel/memory/result limits.
- Define whether write callbacks log invocation or expanded mutations.

## 8. Open Questions

1. Should raw ordered-KV commands be part of the first native protocol, or should
   v1 stay collection-only?
2. Should Raft be single group for the first MVP, or should the protocol reserve
   collection/shard group IDs immediately?
3. Should `flush` and `checkpoint` be client-visible in cluster mode, or treated
   as local maintenance/admin commands only?
4. Should distributed query partial failures return partial results, fail the
   entire query, or be policy-selectable?
5. Should read-only WASM be allowed before distributed queries, or should it wait
   until shard-local resource accounting exists?
