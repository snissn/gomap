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

This seam is provisional constructor integration. It does not switch either
server default, change the service's cosine ColumnGraph strategy, certify its
buffered, typed or hybrid search tails, or qualify owner/maintenance wiring. The
public service fixture retains both default cosine ColumnGraph under its
supported durable profile and explicit ordinary HNSW under all three profiles.
The complete tail must be observed rather than inferred from successful catalog
creation.

The current public discriminator advances ordinary HNSW through service create
and idempotent create in all three profiles, then stops at service Upsert:
`InsertBatchWithStats` requests stats that the native insert executor explicitly
refuses. Default cosine ColumnGraph still stops at create because its combined
column, scalar and text schema is outside the selected native eligibility. The
four equivalent legacy cells complete their count, filtered keyword and dense
retrieval tails. These are remaining capability gates, not accepted exclusions.
