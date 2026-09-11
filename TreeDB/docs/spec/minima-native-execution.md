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

M4's incremental service schema selection is `typed_input=true` on create.
It persists the declared vector, scalar-string and indexed-content ownership
defined below and is echoed from collection metadata by create/open, including across
independent managers. Reopening with incompatible ownership is a conflict, not
a migration or permission to use retained JSON. Selection is separate from
process-local graph admission and is not a readiness claim. Selected HTTP writes
use one `UpsertTypedBatch`, with declared carriers for indexed values and JSON
serialization only for residual metadata. Initial load does not rebuild per batch.
`optimize` with positive `column_graph_serving` options explicitly builds then
admits; `column_graph_action=ensure` re-admits without rebuilding, while `fold`
and `renew` use the existing admitted limits. Compatible create with those options
re-admits after reopen. Limits are process-local and immutable until DB close.
Missing admission fails closed at search; no request-side reconciliation or
native_runtime/document-scan fallback is used. Public `route=ann` permits the
typed engine's bounded exact filter plan; legacy document-scan `route=exact` is
unsupported for selected input. Search and full fetch share the returned owner.

Serving admission prepares full materializer metadata through the same manifest
decoder used by ordinary fetches. The existing publication owner retains this
snapshot-free metadata, including typed-part sort keys, under the cold decoded
metadata budget; temporary record-adapter headers include the identity entry.
Each accepted mutation and Fold prepares complete current metadata before root
publication; unchanged base metadata remains shared. Read admission checks the
exact installed catalog and roots, then binds its own full config, snapshot and
manifest-root header. Generation-wide reconstruction validation runs during
preparation, so full fetch can binary-search immutable row and typed-part refs
without rebuilding request maps. Decoded payloads remain local to each reader.
Asset-cache integrity or namespace invalidation and Close clear prepared reuse
and retain the ordinary full-loader behavior.

The ordinary Python client and benchmark runner select this lifecycle through
HTTP controls and native 64/v2 dense, 65/v1 typed upsert and 50/v2 GetMany.
Selected dense HTTP/native responses expose versioned owned `dense_work`:
executed graph/filter work, captured owner identities/coverage and requested
output materialization, including service error prefixes. Dispatch tags alone
remain insufficient. Process-lifetime diagnostics cover indexed JSON, replay,
scans and separate GetMany work; per-query proofs do not sample process totals.
The final artifact validator and workload qualification remain separate gates;
this product contract does not certify final phase evidence or performance.

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

FP32 graph construction, node-query distance and FP32 node reranking use FP64
inverse norms and half the squared difference of normalized components. This
preserves small angular distances that a rounded dot product subtracted from
one can erase. Indexed construction diversity keeps FP32 dot comparisons only
when their separation exceeds the dimension-dependent error bound; ambiguous
comparisons use the same stable distance and count the additional work. The
bound assumes the default IEEE floating-point mode. This changes arithmetic
and newly built adjacency, not stored vectors or the graph file format; existing
graphs need an explicit rebuild to benefit from the construction repair.

Typed Minima serving explicitly selects the private strict-score policy. Its
positive `SearchCandidates` bounds actual ANN score invocations, including
repeated upper-layer greedy scores and distinct layer-0 scores. Upper descent
uses the same allowance before layer 0; the typed caller reserves suffix work
before calling the base. Exhaustion returns an error without partial successful
output. `base_ann_scored` records all actual base score calls;
`base_candidates` remains the distinct layer-0 count. Zero internal candidate
limit keeps ordinary unbudgeted traversal. A finite full-corpus allowance is
permission for work, not a guarantee of sufficient budget or exact ANN recall;
upper repeats consume allowance even when the row domain fits within the cap.
The shared pack default retains the partition router's distinct layer-0 cap,
entry-at-layer-0 traversal, and permitted approximate success at that cap.

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

Frozen full v1 manifests/artifacts remain unchanged. Bounded 50K/250K/500K/1M fixtures
count **total generated corpus rows across scenarios** (initial plus overlapping
insert batches, excluding later lifecycle replacements), not rows per scenario. They are
diagnostic only: 4,097 eligible rows at less than 1% selectivity needs more than
409,700 rows in that scenario alone. Bounded fixtures retain the cutoff and
selective/broad/empty/mixed predicates, but cannot certify the full sparse case.

Native evidence distinguishes unavailable from measured zero. Product work
producers cover:

- indexed-field JSON reads by ingest/search/mutation/replay; retained-payload
  decode counts separately;
- strategy, actual ANN/exact/empty route, base and overlay candidates, scalar
  and text maintenance, documents fetched and copied bytes;
- overlay rows/bytes, fold debt and completion, graph readiness and coverage;
- per-process CPU, allocation bytes/counts, live heap and actual peak RSS.

Missing producer support is an explicit unavailable reason, not zero-valued
proof. Historical M0 evidence does not carry the measured contract. A new
contract cannot weaken the frozen native-runtime validator.

Report load, graph build, readiness, maintenance drain, warmup, steady search,
restart/open/readiness, client/generator/transport, and qualification-only scroll
separately. Load-to-query-ready includes deferred construction; moving work to
first search or restart does not meet a load target. Diagnostics-on/off timing
is a matched bounded characterization, not a product speedup. Allocation
comparisons use B/row and allocs/row for load/maintenance, B/query and allocs/query
for search, with setup and retained state separately identified. Until measured,
numeric allocation budgets are unavailable; zero indexed-field reconstruction
is the structural gate, not an invented allocation measurement.

The historical combined TreeDB/Qdrant `c2781c147` run passed its then-current
correctness/lifecycle contract; the individual raw envelopes were partial inputs
to that comparison. TreeDB initial load took 4,184.834 s. Its 42.486 s restart
interval excluded Ensure, which occurred in the separate 125.494 s post-reopen
phase. These timings do not establish the new restart boundary. Its largest
sampled phase-end RSS was 16,680,931,328 bytes, **not peak RSS**; `rss_bytes`
summed positive endpoint growth across lifetimes and was also not a peak.
Historical diagnostic `VmHWM` samples cover their recorded live endpoints.
Measured TreeDB evidence requires exclusive Linux `wait4` through each owned
exit, takes the maximum across lifetimes, and never sums their peaks. Client
memory and Go live heap remain separate measurements.

The measured schema is `treedb_rag_application/minima_measured_v1`. Its externally
pinned freeze binds source, natural binaries, clients, manifest bytes/semantics
and actual backend options. Full evidence requires reviewed bounded calibration;
pending calibration is valid only for nonqualifying bounded runs. Complete raw
TreeDB and Qdrant artifacts must satisfy the complete measured contract even
when their envelopes are marked partial. Historical validators are unchanged.
Missing fields, null counters, duplicate keys, unavailable producers, unjoined
requests, inconsistent lifetime/phase intervals and incomplete terminal/exit
coverage cannot stand in for measured zero. The public ledger includes actual
batch writes, readiness, visibility fetch/count and maintenance controls, with
causal completion checks; concurrent searches retain their permitted overlap.

Measured Qdrant restart CPU/RSS/disk deltas are endpoint-only: the old
sample precedes stop, and the new baseline is the actual post-startup-ready
sample also retained by the aggregate resource segment. Shutdown/startup
resource costs between those samples are unavailable. The raw
`resource_availability.restart` records this gap and marks through-exit peak
RSS unavailable; sampled `VmHWM` is still only a lifetime highwater through its
live endpoint. The restart wall timer includes stop/open/reconnect/readiness.
TreeDB retains its derived numeric restart origin (zero CPU/RSS and old-end
disk), independently of actual first-work snapshots and owned `wait4` evidence.
These resource conventions do not change either backend's wall-time boundary.

The five adopted inclusive full-scale caps use these exact raw boundaries:

| Gate | Cap | Raw measurement |
| --- | ---: | --- |
| Load plus readiness | 1,200,000,000,000 ns | Before initial batch preparation through durable load and Build/readiness |
| Restart readiness | 20,000,000,000 ns | Before shutdown through open/replay, reconnect and Ensure |
| Server peak RSS | 9,126,805,504 bytes | Maximum owned-process `wait4` peak through exit |
| Timed search median | 2,127,956 ns | Nearest-rank p50 of all 1,024 outer client calls |
| Final storage | 2,967,728,546 bytes | Live final regular-file logical lengths before cleanup |

The median cap is the integer 10% allowance over the historical 1,934,506 ns
outer-call population; the storage cap is the same allowance over 2,697,935,042
bytes. The historical pooled inner-call median is not that population. Keep all
scenarios, errors and tails. A failed semantic or completion gate cannot yield a
qualifying subset. The live final disk endpoint includes WAL, persistent value
log, indexes and metadata and excludes symlinks; extra GC/checkpoints, settling
waits or post-cleanup measurements cannot replace it. Qdrant remains mandatory
matched evidence but is not the denominator of these two relative caps.

Prospective full-run targets remain: load plus readiness <=1,200 s,
restart/open/readiness <=20 s, server peak RSS <=8.5 GiB, final storage <=10% above
the adopted endpoint, and matched-quality search median regression <=10% versus the matched
retained baseline. These are acceptance targets, **not achieved results** or
direct comparisons to incorrectly labeled legacy peaks. Use identical work,
durability, dimensions, concurrency, warmed/cold state and response projection;
three bounded counterbalanced repetitions characterize noise before numerical
allocation budgets are frozen. Do not lower gates after seeing a failed
candidate. Full qualification follows the landed product and separately frozen
harness, and captures artifact-only evidence (#4620).
