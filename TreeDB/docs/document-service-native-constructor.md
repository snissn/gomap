# Document-service native constructor seam

`CollectionManager.CreateCollectionWithStatus` is the plain catalog create API
for callers that need to distinguish a newly created collection from a compatible
existing collection. It uses the same ordinary create planner as
`CreateCollection`. The document service uses this API; it does not have an
externally assigned command intent or a publish preflight callback.

For a native owner, independent collection managers share one create lock on the
existing DB-scoped coordinator. The create planner takes it before its manager
mutex and derives `alreadyExisted` from the captured catalog under that owner.
Catalog conflict validation still protects against other logical writers. Fresh
names refused before native admission do not retain namespace coordinators;
create serialization does not hold the coordinator's registry mutex.
A compatible plain no-op returns owned metadata without assigning a WAL command,
advancing the logical sequence or replacing the collection incarnation. An
incompatible request preserves the native unsupported error identity and reports
an incompatible schema. A failed preassignment create reports no successful
creation. If publication reports logical installation followed by an error, the
API returns installed metadata with an ambiguity-classified error; callers must
not treat that error as proof that nothing was created.

The existing prepared-intent, externally assigned intent and publish-preflight
APIs retain their contracts and callbacks. This plain API does not grant those
capabilities to the native facade. The selected native canonical apply wrapper
continues to use the original native planner and its assigned-command result.

The public owner's `DeferredVectorBuildMaintenance()` capability gives the
service the same database's existing deferred-build control. A scoped service
keeps its own epoch identity; the capability neither opens another database nor
extends its lifetime. Close the service before closing the original owner.
This accessor adds no acknowledgement or durability boundary.

## Service vector strategy selection

New ordinary service indexes retain the original metric-based strategy when
`vector_index_options.strategy` is omitted: cosine selects `column_graph`; other
supported metrics select `native_runtime`. `IndexInfo.vector_strategy` reports
the selected declaration. Explicit native-runtime HNSW remains available within
its supported capability. Typed input, normalized representation and quantized
declarations retain their ColumnGraph contracts.

Ordinary native-storage ColumnGraph supports the captured graph, text, scalar
and final-document reads used by the service. Omitted cosine requests retain
ColumnGraph without algorithm substitution or legacy fallback. Typed input,
normalized representation and quantized serving keep their separate admission
contracts. Public default and performance qualification remain parent gates.

An idempotent Create with omitted strategy derives only the stored strategy from
the existing service declaration. Existing ColumnGraph indexes keep their schema
and strategy, including an index created by the previous cosine default. Metric,
dimension, scalar declarations and tuning still pass the ordinary compatibility
checks; omission does not copy arbitrary stored options. An explicit strategy
mismatch returns conflict before staging a replacement. One existing collection
lookup supplies both this choice and ColumnStore compatibility handling.

The Hybrid consumer selects production counters for an actual native-runtime
declaration, reusing the maintained graph-only buffered executor. ColumnGraph
keeps its diagnostics route, and explicit public full-diagnostics requests against
native-runtime indexes retain their refusal. Ordinary ColumnGraph is supported
on the native owner; typed, normalized and quantized declarations retain their
separate specialized capability and admission contracts.

Dense route selection preserves valid arbitrary metadata filters: an omitted
route uses ANN for a supported declared scalar plan and the existing exact
executor otherwise. Explicit ANN retains its scalar validation errors; typed
input remains on its admitted graph route. The deterministic choice is returned
in `route`, before query execution. The compiler probe adds a second scalar-plan
compilation for declared filtered ANN requests; its feature cost remains to be
measured. This does not add a new document-scan or allocation-governor guarantee.

## Command storage owner

`cmd/treedb-document-service` and `cmd/treedb-native-server` select the public
TreeDB owner with `MemtableMode=cow_btree` by default. They activate the native
semantic backend before creating the collection manager. HTTP, optional native
wire, diagnostics and deferred vector-build maintenance borrow that same owner.
Profiles, finite budgets and the existing mutation acknowledgement boundaries
remain in force. Upserts use `InsertBatchWithStats`; deferred Optimize uses the
same owner's maintenance capability.

The command capability matrix preserves the underlying profile contracts:

| Capability | `command_wal_durable` | `command_wal_relaxed` / `no_wal_fast` |
| --- | --- | --- |
| Plain indexed native-wire CRUD | Supported | Supported |
| Ordinary omitted-cosine ColumnGraph, either storage owner | Supported | Refused before publication |
| Current ordinary ColumnGraph ANN / mixed Hybrid after vector mutations | Explicit Optimize required | Unsupported declaration |

Native ordinary non-typed ColumnGraph retains full JSON even without scalar
fields, as required by its existing constructor. The legacy zero-scalar layout
keeps noncolumn retained JSON. Compatible repeated Create and reopening reuse
stored options; this does not migrate existing collections or change typed lanes.

Deferred service writes and raw wire vector mutations update document/text/scalar
state without publishing a fresh ordinary graph. Immediate keyword and filtered
exact Dense reads can observe those changes. Unfiltered ANN and mixed Hybrid
refuse with `index_unavailable` while the captured graph source is unresolved or
stale; they do not serve an older graph or rebuild during a query. Native wire
transports that service refusal as `consistency unavailable`, without successful
route or result attribution. A row materialization or Flush/Checkpoint can persist
the new source but cannot make an older graph current.

For the HTTP command, explicitly call the existing Optimize endpoint after these
mutations, then issue ANN/Hybrid. The standalone native server has no graph
Optimize wire command: stop it orderly, open the same directory/profile through
the public command owner/API, call `Service.OptimizeIndex`, close that owner, and
restart the server. Its process regression retains the original raw vector insert,
checks pending refusal before and after Flush/Checkpoint, performs this offline
maintenance, and then checks the original Dense/reopen result. No automatic
Optimize, new query engine or wire maintenance endpoint is implied.

Both commands accept `-legacy-storage` as an explicit process-wide choice for
specialized typed, normalized and quantized configurations. Select it before
opening the original directory with its original profile. Ordinary ColumnGraph,
including omitted cosine and compatible persisted declarations, is supported on
the native owner and does not require this flag. Omitted strategy on compatible
Create preserves the stored algorithm; explicit mismatch still conflicts. A
capability refusal never retries another owner or converts the declaration.
Typed serving still requires its complete positive engine limits on admission
after reopen. The switch neither manufactures those limits nor expands eligibility.

Partition and replicated-vector programs retain their existing authority,
generations, fences and replica ACK contracts. Neither command registers
`VectorPartitionOperations` or HTTP partition routes; `-legacy-storage` adds no
endpoint or replication runtime. Existing eligible native typed/column operations,
specialized imports and explicitly configured non-vector `NativeCollections`
replicated groups retain their narrower documented contracts. Ordinary native
correctness, cost and default qualification remain separate obligations.

Shutdown drains application/native listeners and diagnostics, then closes the
service, manager and original database. Borrowed backend handles are not closed
independently. Startup failures after Open use the same cleanup path; cleanup
errors make the process fail. The command prints `TreeDB storage: shared_cow` or
`legacy`, and HTTP diagnostics expose the selected owner's memtable mode.
Native retained-byte/lease counters do not substitute for specialized admission.
No general migration or performance acceptance follows from owner selection.

The native command synced-ACK regression sends an indexed JSON mutation with
`AckSynced`, kills the process immediately after acknowledgement without Flush,
Checkpoint or graceful shutdown, and checks document/scalar recovery in a fresh
process. Explicit Flush/Checkpoint are exercised separately after recovery.

Native-wire metadata v7 carries `TextIndexes` through the maintained catalog
codec. The standalone command lifecycle fixture intentionally combines a
service-seeded Dense index with a separate wire-created CRUD collection; this
composition does not express a metadata transport limitation. The HTTP command's
optional native listener queries the HTTP-created service index on the same owner.

## Ordinary wire queries

Negotiated command 64/v5 follows the public ordinary service query policy.
Unfiltered omitted-cosine ColumnGraph uses its maintained ANN search and
same-search projected document bytes. Its prepared-reader, search-pack and delta
counters come from the actual search result. Native ordinary ColumnGraph's
prepared pack reports `search_pack_search=1` and `column_graph_prepared_search=0`;
that physical route does not imply native_runtime base-plus-live-delta execution.
Filtered ordinary ColumnGraph uses the existing exact
scan, scoring, tie order and projection, reporting `route=exact`, `exact=true`
and its actual `documents_scanned`. This intentional exact route is not reported
as an ANN fallback or as HNSW. Supported native_runtime ANN remains explicit in
its attribution. The original strict HNSW, typed, quantized and normalized wire
versions retain their independent admission and proof contracts.

## Specialized-owner continuity proof

`cmd/internal/documentserviceowner.TestOwnerLegacyAdvancedAndCloseErrors`
selects the explicit legacy owner, admits typed ColumnGraph with the maintained
positive limits, replaces a bounded source and serves filtered Dense plus mixed
Hybrid. A fresh owner reopens without reseeding and re-admits the same limits;
an injected original-database Close error remains visible. A paired native-owner
attempt at the identical typed declaration must preserve sequence, physical token,
next command LSN, empty catalog and filesystem namespace/file lengths on refusal.
This is an owner/public-service composition control, not a claim that every
specialized request is transported by either command protocol. Its exact-source
execution and the maintained specialized matrices remain required separately.
