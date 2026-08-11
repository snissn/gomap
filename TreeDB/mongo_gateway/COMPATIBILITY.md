# Mongo Gateway Compatibility Matrix

Status: current compatibility inventory and gap harness for issue #1493,
including MongoDB desktop-client metadata and DDL gaps from issue #1473.

TreeDB's Mongo gateway is a deliberately small MongoDB-compatible subset. It is
intended to let common MongoDB drivers exercise TreeDB collection workloads and
to make TreeDB-vs-MongoDB benchmarks comparable. It is not a claim of full
MongoDB server compatibility.

## Harness

The executable matrix is:

```sh
GOWORK=off go test ./TreeDB/mongo_gateway -run TestMongoCompatibilityMatrix -count=1
```

That test seeds a BSON-backed gateway collection and probes representative
supported and rejected paths. Broader protocol coverage lives in the existing
server and official-driver tests:

```sh
GOWORK=off go test ./TreeDB/mongo_gateway -run 'TestServer(OfficialGoDriver|Find|Index|Update|Insert|Cursor)|TestMongoCompatibilityMatrix' -count=1
```

The matrix is a gap finder, not a conformance certification. A row marked
`supported subset` means the named shape works, but MongoDB's full semantics for
that command or operator are intentionally out of scope.

## Test-Backed Matrix

This table is generated from the versioned capability manifest in
`capability_manifest.go`. Do not edit the generated block by hand; update the
manifest and its executable probe, then regenerate all factual capability docs
with:

```sh
GOWORK=off go test ./TreeDB/mongo_gateway \
  -run TestMongoCompatibilityMatrixDocumentationUpToDate \
  -update-mongo-capability-docs
```

`TestMongoCompatibilityMatrixDocumentationUpToDate` fails if this table or the
gateway README summary drifts from the manifest. `TestMongoCompatibilityMatrix`
fails if any capability ID is duplicated, lacks an executable probe, or has
an extra probe outside the manifest.

The BSON/storage matrix below is likewise generated from canonical rows in
`capability_manifest.go`; it separates native ordered-BSON scalar support from
bridge-only regex/code limitations.

<!-- mongo-compatibility-matrix:begin -->
| Category | Feature | Status | Capability ID |
|---|---|---|---|
| wire | hello command | supported | `wire.hello-command` |
| wire | ping command | supported | `wire.ping-command` |
| wire | connectionStatus command (#1473) | supported subset | `wire.connectionstatus-command` |
| wire | hostInfo command (#1473) | supported subset | `wire.hostinfo-command` |
| wire | buildInfo command (#1473) | supported subset | `wire.buildinfo-command` |
| crud | insert explicit _id | supported | `crud.insert-explicit-id` |
| crud | find by _id equality | supported | `crud.find-by-id-equality` |
| query | indexed equality and range predicates | supported subset | `query.indexed-equality-and-range-predicates` |
| query | $in on indexed scalar fields | supported subset | `query.in-on-indexed-scalar-fields` |
| query | bounded BSON v2 compound and descending index planning | supported subset | `query.compound-descending-bson-v2-planner` |
| query | top-level $or expressions | supported subset | `query.top-level-or-expressions` |
| query | projection, sort, skip, and limit | supported subset | `query.projection-sort-skip-and-limit` |
| query | bounded $ne/$nin/$exists, field $not, and top-level $nor | supported subset | `query.negative-existence-and-top-level-nor` |
| cursor | getMore and killCursors | supported | `cursor.getmore-and-killcursors` |
| read concern | local/available readConcern maps to local_stale | supported subset | `read-concern.local-available-readconcern-maps-to-local-stale` |
| read concern gap | majority, linearizable, and snapshot readConcern | rejected | `read-concern-gap.majority-linearizable-and-snapshot-readconcern` |
| write concern | standalone absent/default, w:1, and journal acknowledgement (#4060) | supported subset | `write-concern.standalone-w1-and-journal` |
| write concern gap | standalone w:0, replica acknowledgement, and positive wtimeout | rejected | `write-concern-gap.unacknowledged-replica-and-timeout` |
| crud | updateOne $set by _id | supported subset | `crud.updateone-set-by-id` |
| crud | delete by _id | supported subset | `crud.delete-by-id` |
| metadata | listCollections | supported subset | `metadata.listcollections` |
| metadata | listDatabases | supported subset | `metadata.listdatabases` |
| metadata | create collection | supported subset | `metadata.create-collection` |
| session | logical session handshake and endSessions | supported subset | `session.logical-session-handshake-and-endsessions` |
| metadata | createIndexes, listIndexes, and dropIndexes (ordered BSON scalar v2: one to four components, asc/desc) | supported subset | `metadata.createindexes-listindexes-and-dropindexes` |
| document | native BSON storage mode | supported subset | `document.native-bson-storage-mode` |
| query | bounded document-only dotted projection and sort | supported subset | `query.bounded-document-only-dotted-projection-and-sort` |
| update subset | natural-order arbitrary-filter update, delete, and findAndModify | supported subset | `update-subset.natural-order-arbitrary-filter-update-delete-and-findandmodify` |
| update | exact _id upsert | supported subset | `update.exact-id-upsert` |
| crud | bounded multi insert, update, and delete with ordered or unordered indexed batch errors | supported subset | `crud.bounded-multi-write-and-batch-ordering` |
| update | $inc | supported subset | `update.inc` |
| update | $unset | supported subset | `update.unset` |
| update | nested $set/$unset/$inc and bounded array modifiers (no numeric array-index paths) | supported subset | `update.nested-set-unset-inc-and-bounded-array-modifiers-no-numeric-array-index-paths` |
| update | ReplaceOne by exact _id | supported subset | `update.replaceone-by-exact-id` |
| index | BSON v2 index without treedbValueType | supported subset | `index.bson-ordered-v2-without-treedbvaluetype` |
| read command | aggregate match/project/sort/skip/limit/count | supported subset | `read-command.aggregate-match-project-sort-skip-limit-count` |
| diagnostics | bounded standalone serverStatus | supported subset | `diagnostics.serverstatus` |
| diagnostics | namespace-scoped top command counters | supported subset | `diagnostics.top` |
| diagnostics | bounded standalone dbStats and collStats | supported subset | `diagnostics.dbstats-and-collstats` |
| read command | count filter/skip/limit | supported subset | `read-command.count-filter-skip-limit` |
| read command | distinct top-level field with filter | supported subset | `read-command.distinct-top-level-field-with-filter` |
| read command | explain queryPlanner and executionStats for bounded standalone read plans | supported subset | `read-command.explain-bounded-read-plans` |
| read command gap | maxTimeMS on aggregate/count/distinct | rejected | `read-command-gap.maxtimems-on-aggregate-count-distinct` |
| update subset | findAndModify exact _id no-match | supported subset | `update-subset.findandmodify-exact-id-no-match` |
| transaction gap | transactions and retryable writes | not implemented | `transaction-gap.transactions-and-retryable-writes` |
| security | SCRAM-SHA-256 authentication | supported subset | `security.authentication-scram-sha-256` |
| security | durable built-in role authorization (#4059) | supported subset | `security.authorization-built-in-roles` |
| security | TLS transport and safe remote listen (#4057) | supported subset | `security.transport-tls-and-safe-remote-listen` |
| cluster gap | replica-set and sharding advertisement | not implemented | `cluster-gap.replica-set-and-sharding-advertisement` |
<!-- mongo-compatibility-matrix:end -->

For a naive TreeDB-vs-MongoDB throughput smoke that connects to both targets and
fails on unsupported gateway operations:

```sh
scripts/mongo_gateway_compat_smoke.sh
```

The smoke wrapper defaults to a small BSON-backed driver workload and writes the
usual compare bundle with `report.md`, `summary.tsv`, `matrix.tsv`, and raw JSON
phase output. Use an already-running MongoDB instead of Docker with:

```sh
MONGO_MODE=external MONGO_URI=mongodb://127.0.0.1:27017 scripts/mongo_gateway_compat_smoke.sh
```

The underlying `mongo_gateway_compare.sh` harness is the larger benchmark path;
the smoke script is only the sprint-friendly supported/unsupported and naive
throughput check.

## Status Keys

| Status | Meaning |
|---|---|
| `supported` | Covered by the gateway and exercised by tests. |
| `supported subset` | Covered for the listed narrow shape; broader MongoDB semantics remain gaps. |
| `rejected` | Explicitly rejected to avoid silent semantic drift. |
| `not implemented` | Command/operator is not handled by the gateway. |
| `benchmark-only` | Available in benchmark helpers, not part of the Mongo compatibility surface. |

## Cluster Token/Ring Read And Index Policy

The standalone compatibility rows below do not imply sharded Mongo behavior.
When a cluster route provider reports `token` or `ring` placement, the current
policy is deliberately narrower:

- an exact single-predicate `_id` equality `find` is encoded with the same
  primary-key bytes and `DocumentIDTokenV1` function as nativewire, and the
  catalog is allowed to resolve exactly one owner;
- the Mongo gateway then returns a stable `NotWritablePrimary` route rejection
  before opening or reading the local collection, because it does not yet wire
  that owner target to a structurally bound owner Raft proof and collection
  store;
- non-`_id` filters, secondary-index equality/range reads, `_id` `$in`, scans,
  and other possible scatter shapes remain query-route rejections. There is no
  default scatter or global result coordination;
- all token/ring document mutations fail before cluster submission until
  authoritative collection and index metadata is structurally bound to the
  exact owner route proof. Gateway-local collection metadata is not
  authoritative for a remote owner, including when it reports no indexes;
- `listCollections`, `listDatabases`, and `listIndexes` fail closed in routed
  cluster mode instead of returning gateway-local metadata; and
- `createIndexes` and `dropIndexes` remain local-mutation rejections in cluster
  mode. No sharded secondary-index DDL or global unique-index claim is made.

Nativewire recognizes one `get_many` document ID and resolves its catalog owner,
but its public token/ring read path also fails closed before invoking a
coordinator or observing local collection state. It records:

```text
treedb.native_wire.cluster_read_route.requests_total
treedb.native_wire.cluster_read_route.errors_total
treedb.native_wire.cluster_read_route.unsupported_total
treedb.native_wire.cluster_read_route.owner_store_unbound_total
```

The static in-process `GroupRoutedReadIndexCoordinator` is internal downstream
scaffolding. It validates owner selection and read-index-before-apply ordering,
but has no structural binding to the serving `CollectionManager`, so it does
not enable public reads. Its synthetic benchmark is not an enabled read-path,
storage, network, quorum, or production horizontal-scale measurement; no
enabled-path latency claim is made.

## Command And Wire Matrix

| Area | Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|---|
| Wire | `OP_QUERY` `isMaster` / legacy handshake | `supported subset` | `TestServerHandlesQueryHello`, official-driver tests | Handshake fields are minimal. |
| Wire | `OP_MSG` body command | `supported` | `TestServerHandlesMsgPing`, `TestMongoCompatibilityMatrix` | Exhaustive wire flag coverage is not a goal. |
| Wire | `OP_MSG` document sequence for inserts | `supported` | `TestServerInsertAndFindByID` | Document sequences are only accepted where the command supports them. |
| Wire | `OP_COMPRESSED` | `rejected` | `TestServerRejectsCompressedMessages` | Compression negotiation is not implemented. |
| Command | `hello` / `isMaster` | `supported subset` | `TestMongoCompatibilityMatrix`, official-driver tests | Minimal server metadata only. |
| Command | `connectionStatus` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestSCRAMSHA256EstablishesConnectionIdentityAndGatesCommands`, authorization tests | Returns the authenticated connection identity plus its effective built-in roles and safe privilege/resource projections; unauthenticated calls return an empty `authInfo` projection. |
| Command | `hostInfo` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerHandlesHostInfo` | Returns minimal local runtime and OS metadata only. |
| Command | `buildInfo` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerHandlesBuildInfo` | Returns minimal MongoDB-compatible gateway version/build metadata only. |
| Command | `ping` | `supported` | `TestMongoCompatibilityMatrix` | None for MVP. |
| Command | `insert` / `insertMany` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverBasicCRUD`, `TestStandaloneServerOfficialGoDriverWriteConcernW1AndJournaled` | Standalone absent/default, `{w: 1}`, `j: false`, and `wtimeout: 0` use the configured profile's ordinary acknowledgement boundary. `j: true` closes the explicit TreeDB sync boundary described below. Cluster submitter mode remains authoritative: absent/default and `{w: 1}` request visible ack, `{w: "majority"}` requests Raft-committed proof, and unsupported options reject before submit. |
| Command | `find` | `supported subset` | `TestMongoCompatibilityMatrix`, find planner tests | Query language is intentionally limited. |
| Command | `getMore` / `killCursors` | `supported subset` | `TestMongoCompatibilityMatrix`, cursor tests | Server cursor state is in-memory only. |
| Command option | `readConcern` on `find`, `aggregate`, `count`, `distinct`, `getMore`, `listCollections`, `listDatabases`, and `listIndexes` | `supported subset` / `rejected` | `TestMongoReadConcernAcceptsLocalStaleReadSurfaces`, `TestMongoReadConcernRejectsStrongLevelsBeforeServingData`, `TestMongoAggregateCountDistinctRejectUnsupportedSurface`, `TestMongoCompatibilityMatrix` | Absent/empty, `{level: "local"}`, and `{level: "available"}` are accepted and map to local_stale reads. `majority`, `linearizable`, `snapshot`, cluster-time fields, unknown options, malformed documents, bad `level` types, and duplicate `level` are rejected before serving data. |
| Command | `update` / `updateOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, update tests, `TestMongoFirstWriteConcurrentUpdateUpserts`, `TestMongoFirstWriteLateExistingMutationsWait`, `TestMongoFirstWriteUnrelatedColdMutationsDoNotWait`, `TestMongoFirstWriteStalePendingNamespaceDoesNotWait` | Exact `_id` retains its direct path. Supported non-`_id` filters select the first natural-order match within the scan cap and recheck at mutation; non-`_id` upsert fails closed. Competing in-process first-write upserts serialize collection creation and its first mutation after an initial miss; unrelated namespaces do not share the mutation gate, and existing-collection duplicate handling is unchanged. |
| Command | `delete` / `deleteOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, CRUD tests | Exact `_id` accepts legacy limit `0` or `1`; supported non-`_id` filters require `limit: 1`, select natural order, and recheck before deletion. |
| Command | `listCollections` | `supported subset` standalone; `rejected` in routed cluster mode | `TestMongoCompatibilityMatrix`, metadata tests, `TestMongoRoutedMetadataReadsFailClosedBeforeLocalCatalogObservation` | Minimal filtering and response fields; routed mode has no authoritative catalog binding. |
| Command | `create` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerCreateCollectionCommand` | Creates a plain TreeDB collection catalog entry; existing collections are treated as idempotent no-op success with a response note instead of MongoDB `NamespaceExists`; capped collections and other MongoDB collection options are rejected. |
| Command | `createIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests, compound planner tests | Ordered BSON scalar v2 accepts one through four ascending/descending key components without `treedbValueType`; the explicit `treedbValueType` path remains single-field ascending. Standalone `find` can select compatible BSON-v2 definitions; routed mode rejects before local observation. |
| Command | `listIndexes` | `supported subset` standalone; `rejected` in routed cluster mode | `TestMongoCompatibilityMatrix`, metadata tests, `TestMongoRoutedMetadataReadsFailClosedBeforeLocalCatalogObservation` | Emits TreeDB-specific `treedbValueType` only when local metadata is authoritative. |
| Command | `dropIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | No broad collection/database DDL surface. |
| Command | `aggregate` | `supported subset` standalone; `rejected` in cluster mode | `TestMongoCompatibilityMatrix`, `TestMongoReadCommandsAggregateCountDistinct`, `TestStandaloneServerOfficialGoDriverAggregateCountDistinct` | Ordered initial `$match` plus a compatible top-level compound `$sort`, top-level inclusion/exclusion `$project`, `$skip`, `$limit`, and `$count` stages are supported with bounded materialization and normal cursors. The exact `$group`/`$sum: 1` shape emitted by the pinned Go driver's `CountDocuments` is also supported. Expressions, non-initial planner sort, other `$group` shapes, write/output stages, `maxTimeMS`, and other options fail closed. |
| Command | `serverStatus` | `supported subset` standalone; `rejected` routed | diagnostics direct-wire/official-driver tests | Numeric `opcounters`, bounded `metrics.treedb.commands`, connections/cursors, and standalone command-WAL inventory only; unavailable fields are omitted. |
| Command | `top` | `supported subset` standalone; `rejected` routed | diagnostics direct-wire/official-driver tests | Mongo-shaped namespace `total.time` (microseconds) and `total.count` only. |
| Command | `dbStats`, `collStats` | `supported subset` standalone; `rejected` routed | diagnostics direct-wire/official-driver tests | Exact catalog/document/index counts only within MaxFindScanDocuments; dbStats shares one request budget across collections. Byte/storage totals are omitted. |
| Command | `count`, `countDocuments`, `estimatedDocumentCount` | `supported subset` standalone; `rejected` in cluster mode | `TestMongoCompatibilityMatrix`, `TestStandaloneServerOfficialGoDriverAggregateCountDistinct` | `count` supports the shared filter subset plus non-negative skip/limit; `CountDocuments` uses the bounded aggregate count shape; `EstimatedDocumentCount` uses the proper count command but currently scans rather than reading metadata. `maxTimeMS` and other unsupported options fail closed. |
| Command | `distinct` | `supported subset` standalone; `rejected` in cluster mode | `TestMongoCompatibilityMatrix`, `TestMongoDistinctTopLevelArrayNumericEqualityAndOrder` | Top-level fields, optional shared filters, scalar/array flattening, missing/null handling, stable BSON numeric equality, and first-seen ordering are supported within document/value and Decimal128 work bounds. Dotted fields, `maxTimeMS`, and other unsupported options fail closed. |
| Command | `findAndModify` | `supported subset` | `TestFindAndModifyReturnsAtomicBeforeAndAfterImages`, `TestFindAndModifyInsertConflictAppliesToExistingDocument`, `TestMongoFirstWriteConcurrentFindAndModifyUpserts`, `TestStandaloneServerOfficialDriverConcurrentFirstWriteFindAndModifyUpserts`, `TestStandaloneServerOfficialGoDriverFilterWrites`, `TestMongoCompatibilityMatrix` | Exact `_id` retains its direct path; supported non-`_id` filters select natural order and recheck at mutation. Replacement and the shared dotted `$set`/`$unset`/`$inc`, `$setOnInsert`, bounded `$push`, and bounded `$addToSet` modifier subset are available; pre-image by default, `new:true` post-image, optional top-level `fields`, and exact-`_id` upsert. `$setOnInsert` applies only to insertion. Competing in-process first-write upserts serialize collection creation and its first mutation after an initial miss. A same-`_id` upsert losing the initial insert retries as an update only for `ErrDocumentExists`, returning `updatedExisting: true` without `upserted`; other duplicate conflicts fail. Cluster/routed, remove, sort, dotted projection, positional/array-filter paths, and transaction/retry markers are rejected. |
| Command | collection/database drop | `not implemented` | Command falls through to `CommandNotFound` | Collection lifecycle beyond create and index metadata is not exposed. |
| Command | logical sessions / `endSessions` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverLogicalSession` | Advertises `logicalSessionTimeoutMinutes` and accepts `endSessions`; session IDs are accepted for driver compatibility only. |
| Command | transactions / retryable writes | `not implemented` | `TestMongoCompatibilityMatrix` rejects transaction and retryable-write markers on supported commands and covers `commitTransaction` absence | Depends on local transaction/WAL/idempotency roadmap. |
| Command | cluster-mode `hello` primary advertisement | `supported subset` | `TestClusterHelloReflectsAdmissionWritablePrimary` | Uses cluster admission status to avoid advertising writable primary on followers or unavailable admission. |
| Command | SCRAM-SHA-256 authentication / built-in authorization | `supported subset` | `TestSCRAMSHA256EstablishesConnectionIdentityAndGatesCommands`, `TestAuthorizationEverySupportedCommandHasExplicitPrivilege`, `TestAuthorizationDropRecreateRevokesStaleConnectionAndCursor`, role/cursor/admin/reopen tests, official-driver SCRAM and authorization tests | Versioned scoped `read`, `readWrite`, `dbAdmin`, `userAdmin`, and `serverAdmin` grants authorize supported standalone commands before protected observation or mutation. Durable account incarnations bind sessions and cursors: drop/recreate revokes the prior identity while password rotation preserves it. Version-2 verifier/grant payloads require a nonzero incarnation; legacy version-1 state fails closed and requires a pre-alpha rebuild or offline repair. Growing records use TreeDB's persistent ValueLog with durable pointer publication. Lists are scope-filtered, user mutations recheck actor and target grants under the catalog lock, the last usable server administrator is protected, unusable non-empty catalogs require offline repair, and unbound routed access fails closed. Multi-record create/update operations use fail-closed partial-success ordering rather than rollback atomicity. No external identity mapping or distributed role catalog is claimed. |

## Desktop Client Check

On 2026-08-01, MongoDB Compass 1.49.12 on macOS 26.2 was tested against the
gateway at commit `03e7a26e56100964f14f603f0248a1a6ccc50a68`, using
`mongodb://127.0.0.1:27130/?directConnection=true`. Compass refreshed the
database tree, listed the database, opened `compass_e2e.docs`, and rendered
three BSON documents.

This certifies only the tested connection-and-browse flow, not full Compass or
MongoDB compatibility. The optional Compass Performance view remains
non-blocking for basic connection and browsing: `top`, `serverStatus`,
`dbStats`, and `collStats` now provide the bounded standalone subsets described
below; its `currentOp` aggregate pipeline still uses an unsupported stage.

## Query Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| `_id` equality | `supported` standalone; `rejected` for cluster token/ring | `TestMongoCompatibilityMatrix`, `TestClusterRoutePreflightMongoShardKeyFindMapsTokenThenFailsClosed` | Cluster mode resolves the single token but has no structural owner-proof/collection-store identity binding, so it does not serve local data. |
| `_id` `$in` | `supported subset` | find planner tests | No full query planner cost model. |
| Indexed scalar equality | `supported subset` | `TestMongoCompatibilityMatrix` | Requires single-field TreeDB secondary index. |
| Indexed scalar `$in` | `supported subset` | `TestMongoCompatibilityMatrix` | Null/missing has special scan behavior. |
| Indexed scalar ranges `$gt`, `$gte`, `$lt`, `$lte` | `supported subset` | `TestMongoCompatibilityMatrix` | Range behavior is typed by `treedbValueType`. |
| Top-level `$and` | `supported subset` | `TestMongoCompatibilityMatrix` | Planner flattens supported subexpressions only. |
| Top-level `$or` | `supported subset` | `TestMongoCompatibilityMatrix`, direct-wire and official-driver `$or` tests | One or more document branches using equality, range, `$in`, and `$and`; sibling predicates are ANDed. `$or` uses the bounded scan fallback; no index union. |
| `$nor`, `$not` | `not implemented` | Unsupported operators reject | Needs explicit planner semantics. |
| Regex/text/geospatial predicates | `not implemented` | Unsupported operators reject or command is absent | Out of MVP scope. |
| Dotted predicates into nested objects/arrays | `supported subset` | dotted predicate tests | Projection and sort do not support dotted fields. |
| BSON document/array equality | `supported subset` | `TestRawValuesEqualHandlesDeepNestedBSON`, `TestRawValuesEqualHandlesWideBSON`, `TestDocumentMatchesPlanBoundsDecimal128EqualityWork`, `TestServerQueryAndFilterWriteRejectOverBudgetDecimal128Equality` | Query `$eq`/`$in` equality shares a per-candidate budget of 1,024 potential finite Decimal128 normalizations across predicates and `$or` branches; an over-budget candidate returns `BadValue`. Byte-identical numeric encodings use the exact-value fast path. |
| Projection include/exclude top-level fields | `supported subset` | `TestMongoCompatibilityMatrix` | Cannot mix include/exclude except `_id`; dotted projection rejected. |
| Top-level sort | `supported subset` | `TestMongoCompatibilityMatrix`, compound planner tests | One or more top-level terms are accepted. A compatible one-term BSON-v2 compound index order (or its complete reverse) streams with stable `_id` ties. Multi-term and all other supported sorts use bounded in-memory Mongo sorting before pagination, because missing/null values can occupy distinct physical index runs. Dotted sort is rejected. |
| `skip`, `limit`, `batchSize`, `singleBatch` | `supported subset` | cursor and find planner tests | Behavior is bounded by gateway scan/message limits. |
| Aggregate pipeline | `supported subset` | `TestMongoReadCommandsAggregateCountDistinct`, official-driver tests | Bounded ordered stages only: `$match`, `$project`, `$sort`, `$skip`, `$limit`, `$count`, plus the exact driver count `$group`; no general expressions or output stages. |
| Count / distinct | `supported subset` | `TestMongoAggregateCountDistinctEnforceScanBounds`, official-driver tests | Both use bounded reads; estimated count is exact rather than metadata-fast, and distinct is top-level only. |
| Collation | `not implemented` | Not parsed | String comparison is binary/default TreeDB behavior. |

## Update And Delete Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| Single-document `updateOne` / `ReplaceOne` | `supported subset` | `TestMongoFilterWritesSelectOneAcrossUpdateDeleteAndFindAndModify`, `TestMongoFilterWritesScanCapFailsWithoutMutation`, `TestMongoFilterUpdateSupportedLogicalFilters`, and driver update tests | Standalone only. Exact `_id` retains its direct fast path; equality, range, `$in`, `$and`, and top-level `$or` select the first natural-order match within the scan cap and recheck before mutation. |
| Ordered/unordered insert, update, and delete command batches | `supported subset` | `TestMongoInsertBatchOrderedAndUnorderedStableIndices`, `TestMongoMultiWriteOrderedAndUnorderedStableIndices`, official-driver bounded multi-write test | Commands preparse every item; structural/unsupported items reject before mutation. Runtime item failures return `ok:1` with stable `writeErrors` indices. Ordered stops at the first error; unordered continues within the shared command budget. Per-document atomicity only; no transaction. |
| `multi: true` update and `limit: 0` delete | `supported subset` | `TestMongoMultiWriteUpdateManyDeleteManyAndParseBeforeExecute`, `TestMongoFilterUpdateManyRechecksPredicateAfterDeterministicDrift`, `TestMongoMultiWriteMaxTimeMSBoundsCommandDeadline` | Natural-order selection is bounded command-wide by examined documents, retained targets, error entries, and a five-second deadline; a positive `maxTimeMS` can only shorten that deadline. Each retained target is predicate-rechecked immediately before its atomic mutation. Native BSON insert batches are one non-interruptible checked granule. Pipelines, collation, hints, positional/array updates, transactions, retryable writes, and routed multi-document writes reject. |
| Exact-`_id` `upsert: true` | `supported subset` | `TestMongoCompatibilityMatrix`, direct-wire and driver upsert tests | Modifier and replacement upserts return `n: 1`, `nModified: 0`, and typed `upserted` entries; cluster/routed upserts are rejected. |
| Nested update modifiers | `supported subset` | `TestMongoMutationNestedOperators`, `TestMongoMutationSetOnInsertOnlyAppliesToInsertion`, `TestMongoMutationRejectsWideStoredBSONBeforeDecode`, `TestMongoMutationRejectsDeepCodeWithScopeBeforeDecode`, `TestMongoMutationSharesDecodeBudgetAcrossOperands`, `TestMongoMutationAddToSetRejectsExpensiveDecimal128ComparisonsBeforeMutation`, `TestMongoMutationAddToSetChargesNestedDecimal128LeavesBeforeMutation`, `TestMongoMutationAddToSetSharesDecimal128BudgetAcrossTargets`, `TestMongoMutationAddToSetChargesDecimal128LeavesOnBothSides`, `TestStandaloneServerOfficialGoDriverFilterWrites`, `TestMongoMutationCommandWALValueLogPointersReopen`, `TestMongoCompatibilityMatrix` | Dotted `$set`, `$unset`, `$inc`, `$setOnInsert`, and scalar or at-most-256-item `$each` forms of both `$push` and BSON-equality `$addToSet` are shared by exact-ID and supported filter writes. `$addToSet` membership deduplicates numeric NaNs, unlike query equality. An update targets at most 256 fields; each dotted path and raw BSON container nesting, including CodeWithScope scope documents and the constructed result, have at most 100 levels; one slow mutation's stored document and all decoded operands together admit at most 65,536 raw elements and 16 MiB of raw BSON; `$addToSet` duplicate work is capped per mutation at 65,536 comparisons, 8 MiB of worst-case BSON value bytes, and 1,024 potential Decimal128-normalizing leaf comparisons. `$setOnInsert` applies only when an exact-ID upsert inserts; empty operator specifications and empty `$each` are no-ops. `_id`, numeric array-index components, positional/array-filter paths, empty path segments, ancestor/descendant conflicts, pipelines, unsupported modifiers, excessive BSON nesting, and over-budget slow decoded documents reject before mutation. `$inc` supports int32/int64/double only; null/non-numeric targets reject. |
| Cluster/routed generic, replacement, and upsert updates | `rejected` | cluster submitter tests | Only existing standalone semantics are supported; cluster accepts its native BSON `$set` route only. |
| Single-document `delete` | `supported subset` | `TestMongoFilterWritesSelectOneAcrossUpdateDeleteAndFindAndModify`, `TestMongoFilterWritesScanCapFailsWithoutMutation`, and direct-wire delete tests | Exact `_id` accepts legacy limit `0` or `1`; supported non-`_id` filters require `limit: 1`, use natural-order selection, and recheck before deletion. |

## BSON And Storage Matrix

<!-- mongo-bson-storage-matrix:begin -->
| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| _id as TreeDB primary key | `supported` | CRUD tests | _id array rejected; exact Mongo key ordering is scoped to gateway encoding. |
| Auto-generated _id | `supported subset` | insert tests | Generated ObjectId is gateway-local. |
| Native BSON collection storage | `supported subset` | TestMongoCompatibilityMatrix, BSON storage tests | Preferred current gateway storage path. |
| JSON / template-v1 bridge storage | `supported subset` | update/materializer tests | Some BSON types are rejected before JSON bridge storage. |
| BSON string, bool, int32/int64, double, null, ObjectId | `supported` | CRUD/find tests | Ordered BSON scalar v2 indexing accepts supported scalar values without treedbValueType; legacy homogeneous indexes require it. |
| Decimal128, date, timestamp in native BSON ordered-v2 indexes | `supported subset` | TestMongoCompoundPlanScalarSortMatchesBoundedComparator, TestMongoCompoundPlanDecimal128SortMatchesBoundedComparator, TestCompareRawValuesScalarOrderMatchesBSONV2Codec | Compatible one-term BSON-v2 sort selection is supported standalone; legacy treedbValueType indexes remain limited to string, bool, int64, and double. |
| Arrays and nested documents | `supported subset` | document validation and dotted predicate tests | No multikey index compatibility claim. |
| Binary and other non-JSON BSON types | `supported subset in native BSON mode` | TestMongoCompatibilityMatrix covers binary insert in BSON mode | JSON/template bridge rejects unsupported BSON types. |
| Regex and code | `storage-format dependent` | native BSON storage and bridge-rejection tests | Native BSON may retain unindexed values; JSON/template bridge and regex/code query or index semantics remain unavailable. |
<!-- mongo-bson-storage-matrix:end -->

## Index Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| Built-in `_id_` index metadata | `supported subset` | `listIndexes` tests | Backed by primary key, not a user-created secondary index. |
| Single-field ascending secondary index | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | `TestMongoCompatibilityMatrix`, `TestClusterRoutePreflightMongoRejectsNonShardAndSecondaryIndexReads`, token/ring mutation policy tests | Cluster mode has no owner-bound shard-local index metadata or scatter policy. |
| Unique single-field secondary index | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | metadata/update tests, token/ring mutation policy tests, `TestClusterSubmitterRejectsIndexDDLNoLocalMutation` | Global unique coordination and owner-bound index metadata are not implemented. |
| Supported `treedbValueType` values | `supported subset` | metadata tests | `string`, `bool`, `int64`, `double`. |
| Ordered BSON scalar v2 compound index (one through four components) | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | `TestCreateAndListCompoundDescendingBSONIndex`, compound planner tests, `TestMongoCompatibilityMatrix` | Standalone planner selects canonical top-level `$and` equality/`$in` prefixes plus at most one range suffix when ordering is compatible. Arrays/multikey metadata reject; unsupported query shapes fall back to bounded scan or reject when hinted. |
| Ordered BSON scalar v2 descending index | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | `TestCreateAndListCompoundDescendingBSONIndex`, compound planner tests, `TestMongoCompatibilityMatrix` | Standalone planner selects compatible direct or complete-reverse order with stable `_id` ties. Hashed, text, wildcard, and geospatial indexes remain unavailable. |
| Automatic legacy scalar type inference for indexes | `rejected` | `TestMongoCompatibilityMatrix` covers missing `treedbValueType` on the legacy path | Ordered BSON scalar v2 is the default BSON index format; legacy homogeneous types must be declared explicitly. |

## Durability And Transactions

| Surface | Status | Current gap |
|---|---|---|
| Mongo `writeConcern` | `supported subset` standalone; cluster submitter contract preserved | Standalone accepts absent/empty, `{w: 1}`, boolean `j`, and `wtimeout: 0`. Ordinary acknowledgement follows the selected profile: `command_wal_durable` already returns after its durable command-frame boundary, while `command_wal_relaxed` and `no_wal_fast` promise only their configured ordinary visible boundary. `j: true` first drains collection publishing, then closes a dependency-complete contiguous applied-prefix boundary for command-WAL profiles or calls `Checkpoint` for `no_wal_fast`. A relaxed suffix orders persistent value-log dependencies, appends and syncs a durable-prefix barrier, and publishes the barrier as a root-neutral applied command; an already-durable prefix is reused without another barrier or file sync. The boundary is also closed before returning an `ok: 0` handler response because multi-item and DDL commands can fail after a partial mutation; the original command error remains authoritative, while a sync failure adds explicit durability uncertainty. `w: 0`, `w: "majority"`, numeric `w > 1`, tags, positive `wtimeout`, deprecated `wtimeoutMS`, unknown fields, and malformed shapes reject before collection creation or mutation. Any standalone write OP_MSG with `moreToCome` receives no reply but is rejected before dispatch without mutation, including crafted absent/default or `{w: 1}` commands as well as `{w: 0}`; the connection remains usable. Positive timeouts fail closed because the current TreeDB sync boundary is not interruptible. A post-mutation sync failure returns code 64 `writeConcernError`, `TreeDBWriteConcernUncertain`, and explicit mutation/durability uncertainty fields while preserving the command's original `ok` status. Diagnostics expose per-concern requests, logical writes, visible/journal acknowledgements, completed physical sync boundaries, acknowledgement/sync nanoseconds, timeout/malformed/unsupported rejections, and classified sync failures. |
| Mongo `readConcern` | `supported subset` / `rejected` | Absent/empty, `{level: "local"}`, and `{level: "available"}` map to local_stale reads; stronger levels, cluster-time fields, unknown options, malformed documents, and duplicate `level` are rejected before serving data. |
| Logical sessions | `supported subset` | Advertised for driver compatibility; `lsid` is accepted but does not add causal consistency, retryable-write, or transaction semantics. The gateway presents as standalone and does not advertise a replica-set `setName`. |
| Multi-document transactions | `not implemented` | Transaction markers are rejected on supported read/write/metadata commands; full support is blocked on TreeDB collection transaction and collection WAL work. |
| Retryable writes / idempotency | `not implemented` | Forced `txnNumber` command markers are rejected before read/write/metadata execution rather than silently pretending idempotency bookkeeping exists; support needs explicit idempotency metadata and error contract. |

## Benchmark-Only Surfaces

| Surface | Status | Notes |
|---|---|---|
| `cmd/mongo_gateway_bench -client-mode direct` | `benchmark-only` | Bypasses Mongo wire/driver to measure collection-engine ceiling. |
| `raw-wire` and `raw-wire-tcp-pipeline` modes | `benchmark-only` | Estimate gateway/server ceiling without official driver CRUD helper overhead. |
| `fastclient` package | `benchmark-only` | Narrow raw BSON helper for focused benchmark paths. |
| `scripts/mongo_gateway_compat_smoke.sh` | `benchmark-only` | Sprint-friendly TreeDB-vs-Mongo smoke wrapper around `mongo_gateway_compare.sh`; nonzero exit means the compared workload failed. |

## Next Gap-Closing Candidates

1. Decide whether aggregate-backed `currentOp` is worth implementing; basic
   connection and browsing do not depend on it.
2. Decide whether unsupported filters should always fail closed or allow bounded
   scans behind a feature flag.
3. Add stronger readConcern modes only after a documented TreeDB
   snapshot/causal-read boundary exists; durable standalone acknowledgement
   does not imply a stronger readConcern or replica majority.
4. Add metadata-fast estimated counts and index-assisted distinct only when the
   collection metadata/index contracts can preserve the documented bounded
   semantics.
5. Keep multi-document transactions blocked until the local collection
   transaction and collection WAL tracks are implemented.
