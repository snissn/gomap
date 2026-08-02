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

This table is generated from `TestMongoCompatibilityMatrix`. Do not edit the
generated block by hand; update the test rows and regenerate with:

```sh
GOWORK=off go test ./TreeDB/mongo_gateway -run TestMongoCompatibilityMatrixDocumentationUpToDate -update-mongo-compatibility-docs
```

`TestMongoCompatibilityMatrixDocumentationUpToDate` fails if this generated
block drifts from the executable matrix rows.

<!-- mongo-compatibility-matrix:begin -->
| Category | Feature | Status |
|---|---|---|
| wire | hello command | supported |
| wire | ping command | supported |
| wire | connectionStatus command (#1473) | supported subset |
| wire | hostInfo command (#1473) | supported subset |
| wire | buildInfo command (#1473) | supported subset |
| crud | insert explicit _id | supported |
| crud | find by _id equality | supported |
| query | indexed equality and range predicates | supported subset |
| query | $in on indexed scalar fields | supported subset |
| query | top-level $or expressions | supported subset |
| query | projection, sort, skip, and limit | supported subset |
| cursor | getMore and killCursors | supported |
| read concern | local/available readConcern maps to local_stale | supported subset |
| read concern gap | majority, linearizable, and snapshot readConcern | rejected |
| crud | updateOne $set by _id | supported subset |
| crud | delete by _id | supported subset |
| metadata | listCollections | supported subset |
| metadata | listDatabases | supported subset |
| metadata | create collection | supported subset |
| session | logical session handshake and endSessions | supported subset |
| metadata | createIndexes, listIndexes, and dropIndexes | supported subset |
| document | native BSON storage mode | supported subset |
| query gap | dotted projection | rejected |
| update | exact _id upsert | supported subset |
| update gap | multi update | rejected |
| update | $inc | supported subset |
| update | $unset | supported subset |
| update | ReplaceOne by exact _id | supported subset |
| index gap | compound index | rejected |
| index gap | index without treedbValueType | rejected |
| command gap | aggregate | not implemented |
| command gap | serverStatus | not implemented |
| command gap | top | not implemented |
| command gap | dbStats | not implemented |
| command gap | count | not implemented |
| update subset | findAndModify exact _id no-match | supported subset |
| transaction gap | transactions and retryable writes | not implemented |
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
| Command | `connectionStatus` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerHandlesConnectionStatus` | Returns unauthenticated `authInfo` users, roles, and privileges; no auth or authorization support. |
| Command | `hostInfo` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerHandlesHostInfo` | Returns minimal local runtime and OS metadata only. |
| Command | `buildInfo` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerHandlesBuildInfo` | Returns minimal MongoDB-compatible gateway version/build metadata only. |
| Command | `ping` | `supported` | `TestMongoCompatibilityMatrix` | None for MVP. |
| Command | `insert` / `insertMany` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverBasicCRUD` | Standalone writeConcern durability remains minimal. In cluster submitter mode, absent/default and `{w: 1}` request visible ack, `{w: "majority"}` requests Raft-committed proof, and unsupported writeConcern options are rejected before submit. |
| Command | `find` | `supported subset` | `TestMongoCompatibilityMatrix`, find planner tests | Query language is intentionally limited. |
| Command | `getMore` / `killCursors` | `supported subset` | `TestMongoCompatibilityMatrix`, cursor tests | Server cursor state is in-memory only. |
| Command option | `readConcern` on `find`, `getMore`, `listCollections`, `listDatabases`, and `listIndexes` | `supported subset` / `rejected` | `TestMongoReadConcernAcceptsLocalStaleReadSurfaces`, `TestMongoReadConcernRejectsStrongLevelsBeforeServingData`, `TestMongoCompatibilityMatrix` | Absent/empty, `{level: "local"}`, and `{level: "available"}` are accepted and map to local_stale reads. `majority`, `linearizable`, `snapshot`, cluster-time fields, unknown options, malformed documents, bad `level` types, and duplicate `level` are rejected before serving data. |
| Command | `update` / `updateOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, update tests | Only `_id`-targeted updateOne with accepted update shapes. |
| Command | `delete` / `deleteOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, CRUD tests | Only `_id`-targeted deletes. |
| Command | `listCollections` | `supported subset` standalone; `rejected` in routed cluster mode | `TestMongoCompatibilityMatrix`, metadata tests, `TestMongoRoutedMetadataReadsFailClosedBeforeLocalCatalogObservation` | Minimal filtering and response fields; routed mode has no authoritative catalog binding. |
| Command | `create` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerCreateCollectionCommand` | Creates a plain TreeDB collection catalog entry; existing collections are treated as idempotent no-op success with a response note instead of MongoDB `NamespaceExists`; capped collections and other MongoDB collection options are rejected. |
| Command | `createIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | Single-field ascending indexes only, with `treedbValueType`. |
| Command | `listIndexes` | `supported subset` standalone; `rejected` in routed cluster mode | `TestMongoCompatibilityMatrix`, metadata tests, `TestMongoRoutedMetadataReadsFailClosedBeforeLocalCatalogObservation` | Emits TreeDB-specific `treedbValueType` only when local metadata is authoritative. |
| Command | `dropIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | No broad collection/database DDL surface. |
| Command | `aggregate` | `not implemented` | `TestMongoCompatibilityMatrix` | No aggregation pipeline; Compass Performance `currentOp` uses this command. |
| Command | `serverStatus` | `not implemented` | `TestMongoCompatibilityMatrix` | Optional Compass Performance metadata is unsupported; this does not block basic connection or browsing. |
| Command | `top` | `not implemented` | `TestMongoCompatibilityMatrix` | Optional Compass Performance metrics are unsupported; this does not block basic connection or browsing. |
| Command | `dbStats` | `not implemented` | `TestMongoCompatibilityMatrix` | Database statistics are unsupported; this does not block basic connection or browsing. |
| Command | `count`, `countDocuments`, `estimatedDocumentCount` | `not implemented` | `TestMongoCompatibilityMatrix` covers `count` command absence | Future fast count work should be explicit. |
| Command | `distinct` | `not implemented` | Command falls through to `CommandNotFound` | No distinct scan/index planner. |
| Command | `findAndModify` | `supported subset` | `TestFindAndModifyReturnsAtomicBeforeAndAfterImages`, `TestMongoCompatibilityMatrix` | Exact `_id` query with top-level `$set`/`$inc`/`$unset` or replacement; pre-image by default, `new:true` post-image, optional top-level `fields`, and upsert. Cluster/routed, remove, sort, dotted projection, and transaction/retry markers are rejected. |
| Command | collection/database drop | `not implemented` | Command falls through to `CommandNotFound` | Collection lifecycle beyond create and index metadata is not exposed. |
| Command | logical sessions / `endSessions` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverLogicalSession` | Advertises `logicalSessionTimeoutMinutes` and accepts `endSessions`; session IDs are accepted for driver compatibility only. |
| Command | transactions / retryable writes | `not implemented` | `TestMongoCompatibilityMatrix` rejects transaction and retryable-write markers on supported commands and covers `commitTransaction` absence | Depends on local transaction/WAL/idempotency roadmap. |
| Command | cluster-mode `hello` primary advertisement | `supported subset` | `TestClusterHelloReflectsAdmissionWritablePrimary` | Uses cluster admission status to avoid advertising writable primary on followers or unavailable admission. |
| Command | auth / authorization | `not implemented` | Command falls through to `CommandNotFound` | Out of MVP scope. |

## Desktop Client Check

On 2026-08-01, MongoDB Compass 1.49.12 on macOS 26.2 was tested against the
gateway at commit `03e7a26e56100964f14f603f0248a1a6ccc50a68`, using
`mongodb://127.0.0.1:27130/?directConnection=true`. Compass refreshed the
database tree, listed the database, opened `compass_e2e.docs`, and rendered
three BSON documents.

This certifies only the tested connection-and-browse flow, not full Compass or
MongoDB compatibility. The optional Compass Performance view remains
non-blocking for basic connection and browsing: `top` and `serverStatus` return
`CommandNotFound`, and its `currentOp` request uses unsupported `aggregate`.
The small driver seeding run likewise reached document insertion but its final
`dbStats` request returned `CommandNotFound`; database statistics are not
implemented.

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
| Projection include/exclude top-level fields | `supported subset` | `TestMongoCompatibilityMatrix` | Cannot mix include/exclude except `_id`; dotted projection rejected. |
| Sort by one top-level field | `supported subset` | `TestMongoCompatibilityMatrix` | Compound sort and dotted sort rejected. |
| `skip`, `limit`, `batchSize`, `singleBatch` | `supported subset` | cursor and find planner tests | Behavior is bounded by gateway scan/message limits. |
| Collation | `not implemented` | Not parsed | String comparison is binary/default TreeDB behavior. |

## Update And Delete Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| `updateOne` / `ReplaceOne` by exact `_id` | `supported subset` | `TestMongoCompatibilityMatrix`, direct-wire and driver update tests | Standalone BSON/JSON/template-v1 only. Modifiers are top-level `$set`/`$inc`/`$unset`; replacement documents preserve an omitted `_id`, allow the same `_id`, and reject a changed `_id`. |
| Batched distinct-ID `$set` updates | `supported subset` | update batch tests | Unique-index conflicts may fall back to ordered singles; generic/replacement updates stay ordered. |
| `multi: true` | `rejected` | `TestMongoCompatibilityMatrix` | No multi-update planner. |
| Exact-`_id` `upsert: true` | `supported subset` | `TestMongoCompatibilityMatrix`, direct-wire and driver upsert tests | Modifier and replacement upserts return `n: 1`, `nModified: 0`, and typed `upserted` entries; cluster/routed upserts are rejected. |
| Top-level `$set`, `$inc`, `$unset` | `supported subset` | `TestMongoCompatibilityMatrix`, mutation tests | `$inc` supports int32/int64/double only; null/non-numeric targets reject. Dotted fields, `$push`, pipelines, and other operators are rejected. |
| Cluster/routed generic, replacement, and upsert updates | `rejected` | cluster submitter tests | Only existing standalone semantics are supported; cluster accepts its native BSON `$set` route only. |
| `delete` by `_id`, limit `0` or `1` | `supported subset` | `TestMongoCompatibilityMatrix` | Non-`_id` filters rejected. |

## BSON And Storage Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| `_id` as TreeDB primary key | `supported` | CRUD tests | `_id` array rejected; exact Mongo key ordering is scoped to gateway encoding. |
| Auto-generated `_id` | `supported subset` | insert tests | Generated ObjectId is gateway-local. |
| Native BSON collection storage | `supported subset` | `TestMongoCompatibilityMatrix`, BSON storage tests | Preferred current gateway storage path. |
| JSON / template-v1 bridge storage | `supported subset` | update/materializer tests | Some BSON types are rejected before JSON bridge storage. |
| BSON string, bool, int32/int64, double, null, ObjectId | `supported` | CRUD/find tests | Indexing requires declared `treedbValueType`. |
| Arrays and nested documents | `supported subset` | document validation and dotted predicate tests | No multikey index compatibility claim. |
| Binary and other non-JSON BSON types | `supported subset` in native BSON mode | `TestMongoCompatibilityMatrix` covers binary insert in BSON mode | JSON/template bridge rejects unsupported BSON types. |
| Decimal128, date, timestamp, regex, code | `not implemented` / storage-format dependent | Unsupported bridge types reject; native BSON may store unindexed bytes | No query/index compatibility claim. |

## Index Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| Built-in `_id_` index metadata | `supported subset` | `listIndexes` tests | Backed by primary key, not a user-created secondary index. |
| Single-field ascending secondary index | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | `TestMongoCompatibilityMatrix`, `TestClusterRoutePreflightMongoRejectsNonShardAndSecondaryIndexReads`, token/ring mutation policy tests | Cluster mode has no owner-bound shard-local index metadata or scatter policy. |
| Unique single-field secondary index | `supported subset` standalone; token/ring reads and all token/ring writes are rejected | metadata/update tests, token/ring mutation policy tests, `TestClusterSubmitterRejectsIndexDDLNoLocalMutation` | Global unique coordination and owner-bound index metadata are not implemented. |
| Supported `treedbValueType` values | `supported subset` | metadata tests | `string`, `bool`, `int64`, `double`. |
| Compound index | `rejected` | `TestMongoCompatibilityMatrix` | Needs collection index design work. |
| Descending, hashed, text, wildcard, geospatial indexes | `rejected` / `not implemented` | Invalid index commands reject or command absent | Out of MVP scope. |
| Automatic type inference for indexes | `rejected` | `TestMongoCompatibilityMatrix` covers missing `treedbValueType` | Type must be declared explicitly. |

## Durability And Transactions

| Surface | Status | Current gap |
|---|---|---|
| Mongo `writeConcern` | `not implemented` as Mongo semantics | Gateway success currently follows the underlying TreeDB collection API and durability profile. |
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

1. Decide whether optional Compass Performance metadata (`serverStatus`, `top`,
   and aggregate-backed `currentOp`) is worth implementing; basic connection
   and browsing do not depend on it.
2. Decide whether unsupported filters should always fail closed or allow bounded
   scans behind a feature flag.
3. Extend explicit `writeConcern` handling beyond the current cluster submitter
   subset, and add stronger readConcern modes only after a documented TreeDB
   snapshot/causal-read boundary exists.
4. Add count/distinct only after the desired TreeDB collection count/index
   semantics are clear.
5. Keep multi-document transactions blocked until the local collection
   transaction and collection WAL tracks are implemented.
