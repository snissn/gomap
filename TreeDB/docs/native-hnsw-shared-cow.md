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

Runtime storage is admitted before allocation against the existing finite COW
budget. Row charges use the actual deterministic HNSW level and maintained
layer capacities. Unchanged vectors and deletions do not accumulate new node
charges; changed vectors retain their actual graph history until rebuild replaces
it. Decoder and persistence receipts have operation lifetimes. Save borrows its
immutable encoded run table through COW preparation, rather than copying another
complete encoded graph. It releases the obsolete snapshot reservation before
preparation. Public Search admits a separate workspace per concurrent query and
uses operation-local graph scratch and live-delta result buffers rather than
retaining a query pool. Default underfill and empty searches reuse the maintained
exact scan and ranking with a single native capture. Persisted tuning parameters
must match the normalized canonical declaration before runtime decoding. Returned
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
