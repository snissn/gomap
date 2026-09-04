# Minima native execution contract (#4614)

Status: implementation contract frozen by #4615 against
`c2781c147afe824620d0f3fe662bdaa9e5e81bf9`. This is **not** a claim that mutable
Minima is already supported by `column_graph`. The feature gates below must
land before the new route can qualify. The existing public `native_runtime`
strategy is unchanged here; its documentation-only deprecation is #4621.

## Actual application boundary

`TreeDB/cmd/treedb_rag_benchmark/main.go -workload=minima` generates, validates,
and combines evidence. `scripts/bench_minima_qualification.sh` executes the
workload using `benchmarks/vector_db_compare/minima_treedb_runner.py`, the
Python `TreeDBClient`, and `TreeDB/documentservice`. Changing a Go benchmark
helper or adding a native-wire operation alone does not change this path.

The frozen workload is filtered **dense** cosine search: eight-dimensional
vectors, TopK 5, batches of 256, four readers and one writer, 32 warmup and
1,024 timed searches. Scalar filters are equality on `meta.user_id` and
conjunction with `meta.fpath`. It includes insert/search overlap, updates,
deletes, source replacement, reopen, and final-state checks. Text postings must
remain correct, but text/hybrid query throughput is a separate regression lane,
not a claim made by the dense benchmark.

## Data ownership and write contract

| Logical field | Selected authoritative representation | Consumers |
|---|---|---|
| document ID | existing primary/typed-row identity | lookup, tombstones, deduplication, result identity |
| `embedding` | `float32_vector`, `typed_column_part`, fixed dimensions | graph construction, ANN distance, typed exact scoring |
| `meta.user_id`, `meta.fpath` | declared `string` fields in typed-row assets initially | native scalar postings and typed predicate checks |
| `content` | declared `string` in typed-row assets | existing text analyzer/postings and result materialization |
| other unindexed payload | existing non-column retained-payload storage | requested result fields only |

Typed-row ownership for variable strings is deliberate: these are point/batch
lookup and maintenance fields, not column aggregation. Do not add dictionary or
secondary authoritative storage to this workload without measured need. The
existing typed-column dictionary representation remains available for other
access patterns. Graph adjacency and scalar/text postings are **derived index
state**, not a second authoritative copy of document fields.

M1 (#4616) extends the existing collection write planner's `columnWriteDocument`
declared values and trusted projection seam to a caller-validated typed batch.
It does not create a parallel datastore or bypass collection validation. The
current `InsertBatchWithStatsValidatedFloat32Projection` only accepts one vector
column, with restrictions on retained payload and scalar indexes; it is not yet
the required general typed mutation API.

The typed boundary must enforce before command admission:

- Equal batch lengths; nonempty IDs; duplicate-ID behavior identical to the
  corresponding insert/upsert API, including duplicates within a batch.
- Schema/collection identity and field ownership; no name-only match across
  drop/recreate or schema changes.
- Fixed vector width, finite representable FP32 components, nonzero cosine
  norm. Conversion from an external numeric representation is once at ingress.
- Present strings, explicit null, and omitted fields remain distinct. Minima
  requires all four indexed fields and rejects null/missing or wrong types;
  the general API must not collapse those states into empty string or zero.
- Atomic replacement/update planning and uniqueness checks; removed values
  disappear from scalar, text, and vector results under the same visible view.
- Indexed values are supplied once. Optional retained payload must exclude
  declared paths (including nested `meta` paths), or be checked against them at
  ingress; conflicting copies cannot be independently authoritative.

Public untrusted HTTP/native-wire decoders validate input; an internal trusted
entry point is not a validation bypass for remote callers. Async staging owns
its bytes until publication/replay no longer needs them. Borrowed request
buffers cannot escape the request lifetime; clone once at an ownership boundary,
not repeatedly in every index consumer. Unsupported schema combinations fail
before admission, not after silently selecting document reconstruction.

JSON control metadata and external JSON ingress/egress are allowed. Native
indexed-field extraction from retained JSON during ingest, maintenance, replay,
graph build, filtering, or scoring is not. Typed result materialization may
encode requested fields back to the client representation after ranking.

## Reuse decisions and implementation owners

| Operation / current seam | Reuse | Required work and owner |
|---|---|---|
| Service ingest, collection planner | `columnWriteDocument.declaredValues`, trusted vector projection, normal validation/write domain | General typed vector/scalar/text batch and typed updates/replacement; M1 |
| Command admission/replay | shared command WAL, collection replay dispatcher, catalog identities and `AppliedLSN` | Versioned typed payload carrying accepted values; replay without JSON extraction; M1. Mutable graph coverage/recovery integration; M3 |
| Typed persistence | existing base/delta/tombstone manifests and typed-row/typed-column parts | Consume latest visible generation; do not invent a second durable overlay log; M2/M3 |
| ANN base | prepared `column_graph` readers, typed FP32 sections, generic list adjacency, mapped-resource pins | Preserve fast base traversal; extend row/vector sources beyond current insert-only restriction; M2 |
| Mutable graph | existing bounded live-delta search/merge algorithms | Adapt algorithms to column-backed base and typed mutations; bounded rows **and bytes**, tombstones, view consistency; M2 |
| Fold/reopen | graph builder, manifest/root publication, WAL coverage, resource closure and GC pins | Atomic base/overlay cutover and replay-derived overlay; M3 |
| Scalar/text maintenance | existing collection indexes/analyzer and mutation planner | Supply typed values and old typed state, not reconstructed retained documents; M1/M4 |
| Client/service/runner | existing document service and Python client, native-wire codecs where useful | Wire the actual supported public typed route and report transport; M4 |

`vector_index_live_delta.go` is not a drop-in implementation: its current
`ensureLiveDeltaLocked` clones runtime scalar state, and its fold inserts into a
heap-resident native base. Reuse algorithms, not the whole `native_runtime`
object under a new name. Likewise `reconcileVectorIndexes` currently fetches
stored documents through a JSON materializer for registered runtime indexes;
the native typed branch must bypass this document reconstruction for **all**
insert, update, delete and replacement callers, not just the benchmark insert.

The existing typed assets already preserve authoritative changed values and
tombstones. The graph overlay is derived from that lineage plus accepted WAL
commands newer than the published frontier. Persist a new derived accelerator
only if reopen measurements justify it, with bounded identity-checked recovery;
never make it the sole record of an acknowledged mutation.

## Durability, publication and error boundary

The canonical contract is [write-path-and-durability.md](write-path-and-durability.md)
§0.2. Minima uses `command_wal_durable`, with no load-time relaxed downgrade.
Successful supported writes wait for stable complete command-frame closure,
then have consistent process-visible collection/index state. `Flush` drains
visibility; it is not a file/directory sync guarantee. Checkpoint and clean
close establish sealed roots covering their captured frontier.

This is existing infrastructure, not a future durability mechanism:
`NewTrustedCommandWALIntent` sets durable publication from the resolved profile;
`AppendStagedCommandWALIntent` applies that requirement before staging;
`command_wal_test.go` exercises indexed staged writes with `AppliedLSN` lagging
their WAL frame. `typed_column_publication_test.go` covers reopen and replay
without checkpoint; `column_publish_write_path_test.go` covers mutation assets
and invalid declared values rejected before WAL append. These tests do not
prove the not-yet-implemented typed Minima graph overlay.

M1 must retain typed accepted values in a versioned command payload rather than
only keeping them in transient trusted projections. M3 binds graph coverage to
the same accepted/published frontier, collection/schema identity and manifest
generation. Fold builds privately, validates its base, publishes the complete
root/asset closure atomically, retains newer mutations, and releases the old
view only after reader pins and both recoverable roots allow reclamation.

Pre-admission errors expose no mutation. Failure after a command may be durable
is commit-ambiguous: preserve the existing recovery-required/error convention,
do not return a false rollback or retry blindly. If M2 cannot expose its mutable
feature without losing acknowledgements on restart, keep it internal/gated or
merge M2 and M3 as one correctness-complete change. No public intermediate
acknowledged-but-unrecoverable mode is permitted.

## Query and materialization contract

Each response pins one coherent collection/schema/base/overlay view. A typed
scalar allow-set must be from that same view, including update/delete effects.
Filter isolation, live-ID uniqueness, score ordering, recall and deterministic
tie rules are unchanged. Typed exact scoring of allow-sets up to 4,096 is a
deliberate separately labeled route, not ANN evidence. The 4,097 boundary must
exercise ANN; intended broad/base and mutation-overlay scenarios need positive
base/overlay work. Empty filters return empty results without a document scan.
No `native_runtime`, whole-document-scan or undeclared exact fallback is allowed.

M0's bounded-50k baseline exposed a concrete regression target: 1,000 eligible
`broad_10pct` IDs, zero returned IDs, `complete_finite_ann`, 2,064 visited/scored.
The existing runtime exact cap is 512; larger complete sets do not receive the
eligible-region seeding used by `vector_aligned_ann`. Preserve this failing
fixture. M2/M4 must test 512/513/1000/4096/4097 allow-set cardinalities, dispersed
eligible nodes, and base/overlay mutations; passing the larger baseline does not
discharge the small-set regression.

The historical Python search call returns full client documents, then separately
retrieves the winning IDs one by one via `filter_documents`. Search latency
therefore already includes top-K materialization and response decoding; the
separate retrieve phase repeats required caller work. The replacement must
preserve both boundaries and requested fields. Batch retrieval is allowed if
the caller actually uses it and the comparison reports that change. An IDs-only
engine microbenchmark cannot stand in for the application response boundary.

## Evidence and acceptance

Frozen full v1 manifests/artifacts remain unchanged. Bounded 50K/250K fixtures
count **total generated corpus rows across scenarios** (initial plus overlapping
insert batches, excluding later lifecycle replacements), not rows per scenario. They are
diagnostic only: 4,097 eligible rows at less than 1% selectivity needs more than
409,700 rows in that scenario alone. Bounded fixtures retain the cutoff and
selective/broad/empty/mixed predicates, but cannot certify the full sparse case.

The native evidence schema must distinguish unavailable from measured zero.
Required semantic counters, once their product support lands, cover:

- indexed-field JSON reads by ingest/search/mutation/replay; retained-payload
  decode counts separately;
- strategy, actual ANN/exact/empty route, base and overlay candidates, scalar
  and text maintenance, documents fetched and copied bytes;
- overlay rows/bytes, fold debt and completion, graph readiness and coverage;
- per-process CPU, allocation bytes/counts, live heap and actual peak RSS.

Missing product support is an explicit unavailable reason, not zero-valued
proof. The M0 harness may reject the new strategy as unavailable until M4. A
new contract cannot weaken the frozen native-runtime validator.

Report load, graph build, readiness, maintenance drain, warmup, steady search,
restart/open/readiness, client/generator/transport, and qualification-only scroll
separately. Load-to-query-ready includes deferred construction; moving work to
first search or restart does not meet a load target. Diagnostics-on/off timing
is a matched bounded characterization, not a product speedup. Allocation
comparisons use B/row and allocs/row for load/maintenance, B/query and allocs/query
for search, with setup and retained state separately identified. Until measured,
numeric allocation budgets are unavailable; zero indexed-field reconstruction
is the structural gate, not an invented allocation measurement.

The historical TreeDB-only partial full run at the base above took 4,184.834 s
for initial load and 42.486 s for restart/open/readiness. Its largest sampled
phase-end RSS was 16,680,931,328 bytes, **not peak RSS**; `rss_bytes` summed
positive endpoint growth across process lifetimes and is also not a peak.
New Linux RSS evidence uses each process's `VmHWM`, identifies every service
lifetime, and takes the maximum across lifetimes, never their sum. This measures
server process-lifetime peak, not a phase-specific or whole-host peak. Client
memory and Go live heap need separate measurements; absence is unavailable.

Prospective full-run targets remain: load plus readiness <=1,200 s,
restart/open/readiness <=20 s, server peak RSS <=8.5 GiB, steady storage growth
<=10%, and matched-quality search median regression <=10% versus the matched
retained baseline. These are acceptance targets, **not achieved results** or
direct comparisons to incorrectly labeled legacy peaks. Use identical work,
durability, dimensions, concurrency, warmed/cold state and response projection;
three bounded counterbalanced repetitions characterize noise before numerical
allocation budgets are frozen. Do not lower gates after seeing a failed
candidate. Full qualification follows the landed product and separately frozen
harness, and captures artifact-only evidence (#4620).
