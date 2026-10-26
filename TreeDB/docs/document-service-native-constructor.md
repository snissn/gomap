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

Ordinary native-storage ColumnGraph service adoption remains unmet. New omitted
cosine requests on an activated native owner refuse unsupported capability;
there is no algorithm substitution or legacy fallback after activation. The
native constructor is a capability boundary, not proof of general service
adoption or a requirement to change the existing algorithm default.

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
native-runtime indexes retain their refusal. No server storage constructor or
strategy fallback is changed here. Explicit ColumnGraph/typed/normalized/quantized
native storage eligibility remains a separate capability obligation.

Dense route selection preserves valid arbitrary metadata filters: an omitted
route uses ANN for a supported declared scalar plan and the existing exact
executor otherwise. Explicit ANN retains its scalar validation errors; typed
input remains on its admitted graph route. The deterministic choice is returned
in `route`, before query execution. The compiler probe adds a second scalar-plan
compilation for declared filtered ANN requests; its feature cost remains to be
measured. This does not add a new document-scan or allocation-governor guarantee.
