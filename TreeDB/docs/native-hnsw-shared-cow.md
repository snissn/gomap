# Ordinary HNSW on the shared COW owner

Ordinary BSON collections using `Open` plus `NativeSemanticBackend` can declare
`native_runtime` vector indexes and use the maintained HNSW builder, `Search`,
`SaveSnapshot`, and `LoadVectorIndexSnapshot`. This adapter uses the existing
HNSW algorithm and graph record codec. It does not introduce a second graph
engine or a new WAL format.

Graph saves replace the named vector domain, so omitted records cannot survive
shrink or empty-image publication. Each saved domain carries an interpretation
and the source document generation. Physical materialization writes those two
values with the graph root in the same system publication. Reopen imports the
saved identity; canonical rebuild replay writes the same version record.

A runtime authenticates collection incarnation, schema, graph interpretation and
document generation. Search checks before and after evaluation. Save refuses
stale coverage before assignment; assigned failures remain ambiguous. An exact,
clean, already-saved runtime may satisfy Load without constructing a duplicate.
A dirty or unsaved runtime cannot satisfy that cache path. An old captured read
retains its original graph and value pointers through replacement and checkpoint.

Ordinary Flush consults canonical vector declarations, including when its handle
predates vector DDL. It reuses covered registered runtimes, or builds the current
canonical rows through the maintained builder before saving. This repair does
not mark a stale runtime as current. The existing DB-scoped schema coordinator
owns the shared derived runtimes. Mutation notification includes sibling handles
and handles opened before vector DDL; accepted mutation ACKs follow graph
reconciliation. Manager Close flushes registered graphs; DB Close releases their
receipts after active public Search readers finish. A graph may outlive the
manager that originally built it while the DB and a sibling manager remain open.
Vector Create/Drop on a handle whose manager has closed refuse before producer
preflight or publication, even while another manager keeps the DB usable.

A post-accepted reconciliation failure keeps its ambiguous mutation result and
the canonical committed rows. It retires invalid graph payloads and their schema
and allocation receipts after active graph readers finish, retaining registered
receiver identity. Search remains unavailable until an explicit rebuild succeeds.
After any invalid canonical vector row is corrected, the same receiver's
`Rebuild` uses the existing authenticated builder and ownership swap to recover;
no failed partial graph is marked current. Reconciliation holds one runtime use
lock at a time and releases it before retiring a failed payload.

Public receiver `Rebuild` preserves receiver identity while the maintained builder
constructs a separately admitted replacement. The installation swaps graph backing,
schema ownership and allocation receipts explicitly; it never copies mutexes.
`UnregisterVectorIndex` removes the shared registration and retires graph storage
after active graph readers finish. The detached receiver refuses Search and
mutation until explicit registration rebuilds canonical rows. Registration refuses
another DB, a replaced runtime, and dropped or recreated declarations. Ordinary
collection Status and Rebuild use this runtime family; source-import and typed
graph declarations retain their separate dispatch. `SaveNativeDeltaSnapshot`
uses full native domain replacement with the existing Save acknowledgment.

Scalar Create/Drop and changes to other vector declarations use the same DDL
owner and shared mutation gate. Before assignment, they prepare an admitted
replacement for each surviving registered ordinary HNSW receiver. The maintained
snapshot/load codec clones the actual base and live-delta planes separately;
current scalar columns are reconstructed from the original captured primary cut
and proposed schema. The original graph is never folded or rebuilt from canonical
rows during this preflight. Manual tombstones, node history, receiver identity,
tuning, dirty/persisted bookkeeping, coverage and interpretation remain intact.
The codec validates edge records using its existing saved-load rules; original
entry/max-level state is retained rather than adopting load's normalized values.

A successful logical DDL installation transfers the prepared payload and schema
receipt into the same receiver before releasing the mutation/use boundary. A
completion error after installation still performs that transfer and remains
ambiguous. Admission or conflict refusal before installation leaves original
metadata, runtime and receipts unchanged. This can require a temporary second
graph and may refuse finite capacity before assignment. Charges are recomputed
from current node/column storage, so obsolete scalar charge does not accumulate
across DDL. A previously stale document generation remains stale; DDL does not
bless incomplete graph coverage. Full schema authentication remains exact.
This preparation performs work proportional to the surviving graph and scalar
rows, and excludes queries on those receivers until DDL completes. It is a
correctness and ownership tradeoff for index DDL, not a DDL speed improvement.

Manual Insert and Tombstone share the coordinator mutation gate, runtime use lock
and graph lock with ordinary mutation notification. Insert admits retained row
growth on the original runtime receipt before graph allocation. Filtered Search
copies candidate IDs and immutable query configuration while graph storage is
protected, then releases the runtime use lock before invoking user code. Its
original captured read and admitted workspace remain live through reranking,
exact fallback and final validation. A callback that changes the captured document
or graph interpretation makes the outer Search unavailable. CheckRecall uses the
maintained serial exact path for native owners rather than allocating the legacy
whole-corpus matrix without admission.

Runtime storage is admitted before allocation against the existing finite COW
budget. Retained row admission uses the deterministic HNSW level, one owned
vector/ID image, explicitly doubling mutable node descriptors, fixed degree-plus-one
mutable adjacency, and the current/replacement immutable image capacities. Native
views keep dirty-row sharing but do not retain an obsolete spare or publish
intermediate images during one mutation; legacy reader-view recycling is unchanged.
Native float32 insertion keeps per-node immutable vectors instead of the optional
contiguous matrix, avoiding cross-view matrix-growth aliases. Legacy matrix storage
is unchanged. A separate high-water term on the same runtime receipt covers construction scratch,
including independent frozen-prefix worker workspaces before a live-delta fold.
Column offsets/presence and payload growth are charged even for missing values.
Build, Load, manual insertion, reconciliation, schema cloning and status decoding
use this same model. Scratch remains charged while runtime fields retain it. Unchanged vectors and deletions do not accumulate new node
charges; changed vectors retain their actual graph history until rebuild replaces
it. Decoder and persistence receipts have operation lifetimes. Save borrows its
immutable encoded run table through COW preparation, rather than copying another
complete encoded graph. It releases the obsolete snapshot reservation before
preparation. Public Search admits a separate workspace per concurrent query and
uses operation-local graph scratch and live-delta result buffers rather than
retaining a query pool. The ordinary query reuses its initial captured read for
final document attachment, pre-sizes bounded candidate scratch, and copies selected
IDs through the maintained output arena. Discarded candidate references are cleared
before returning caller-owned results. Default underfill and empty searches reuse the maintained
exact scan and ranking with a single native capture. Persisted M and EF settings
must match the normalized canonical declaration before runtime decoding; the
per-runtime rebuild ratio is preserved when finite and in (0, 1]. Returned
IDs and document bytes remain caller-owned outputs under the ordinary read
contract. These charges bound engine-owned allocations, not RSS or GC behavior.

The focused public controls cover all three profiles, second reopen, shrink,
empty replacement, large pointer-backed graph records, stale saves, stale-handle
Flush, and process exit without Close for the two WAL profiles. A representative
2,048-node, eight-dimensional, M16 graph saves and reopens under the unchanged
64 MiB core in-flight limit. Capacity refusals remain possible when retained
captures or a genuinely different second runtime exhaust that finite budget.
These controls do not establish unrestricted graph size or concurrency.

The [canonical public harness](../../cmd/mongo_gateway_bench/README.md) documents
the portable guardrails in `cmd/mongo_gateway_bench/standalone_vector_bench_test.go`:
`BenchmarkStandalonePublicVectorBuildSaveLoad`,
`BenchmarkStandalonePublicVectorSearch`, and
`BenchmarkStandalonePublicVectorSaveReopenLoad`. They distinguish logical Save
and clean-runtime reuse from physical checkpoint, Close and reopened decoding.
Timing requires a coordinated quiet host slot. This is a compatibility guardrail,
not a vector-optimization claim.

This slice does not qualify ColumnGraph/partition-live codecs, text/hybrid
search, template graph materialization, legacy catalog upgrade, all filtered
query paths, or the complete Mongo capability inventory. Existing specialized
routes keep their separate contracts. The parent adoption issue remains open
until its remaining public capabilities and performance gates are satisfied.
