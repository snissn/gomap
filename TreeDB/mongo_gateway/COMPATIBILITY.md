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
| query | projection, sort, skip, and limit | supported subset |
| cursor | getMore and killCursors | supported |
| crud | updateOne $set by _id | supported subset |
| crud | delete by _id | supported subset |
| metadata | listCollections | supported subset |
| metadata | create collection | supported subset |
| session | logical session handshake and endSessions | supported subset |
| metadata | createIndexes, listIndexes, and dropIndexes | supported subset |
| document | native BSON storage mode | supported subset |
| query gap | $or | rejected |
| query gap | dotted projection | rejected |
| update gap | upsert | rejected |
| update gap | multi update | rejected |
| update gap | $inc | rejected |
| index gap | compound index | rejected |
| index gap | index without treedbValueType | rejected |
| command gap | aggregate | not implemented |
| command gap | count | not implemented |
| command gap | findAndModify | not implemented |
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
| Command | `insert` / `insertMany` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverBasicCRUD` | Write concern is not Mongo-compatible durability semantics yet. |
| Command | `find` | `supported subset` | `TestMongoCompatibilityMatrix`, find planner tests | Query language is intentionally limited. |
| Command | `getMore` / `killCursors` | `supported subset` | `TestMongoCompatibilityMatrix`, cursor tests | Server cursor state is in-memory only. |
| Command | `update` / `updateOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, update tests | Only `_id`-targeted updateOne with accepted update shapes. |
| Command | `delete` / `deleteOne` helper path | `supported subset` | `TestMongoCompatibilityMatrix`, CRUD tests | Only `_id`-targeted deletes. |
| Command | `listCollections` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | Minimal filtering and response fields. |
| Command | `create` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerCreateCollectionCommand` | Creates a plain TreeDB collection catalog entry; existing collections are treated as idempotent no-op success with a response note instead of MongoDB `NamespaceExists`; capped collections and other MongoDB collection options are rejected. |
| Command | `createIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | Single-field ascending indexes only, with `treedbValueType`. |
| Command | `listIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | Emits TreeDB-specific `treedbValueType`. |
| Command | `dropIndexes` | `supported subset` | `TestMongoCompatibilityMatrix`, metadata tests | No broad collection/database DDL surface. |
| Command | `aggregate` | `not implemented` | `TestMongoCompatibilityMatrix` | No aggregation pipeline. |
| Command | `count`, `countDocuments`, `estimatedDocumentCount` | `not implemented` | `TestMongoCompatibilityMatrix` covers `count` command absence | Future fast count work should be explicit. |
| Command | `distinct` | `not implemented` | Command falls through to `CommandNotFound` | No distinct scan/index planner. |
| Command | `findAndModify` | `not implemented` | `TestMongoCompatibilityMatrix` | No atomic find/update command surface. |
| Command | collection/database drop | `not implemented` | Command falls through to `CommandNotFound` | Collection lifecycle beyond create and index metadata is not exposed. |
| Command | logical sessions / `endSessions` | `supported subset` | `TestMongoCompatibilityMatrix`, `TestServerOfficialGoDriverLogicalSession` | Advertises `logicalSessionTimeoutMinutes` and accepts `endSessions`; session IDs are accepted for driver compatibility only. |
| Command | transactions / retryable writes | `not implemented` | `TestMongoCompatibilityMatrix` rejects transactional and retryable-write markers and covers `commitTransaction` absence | Depends on local transaction/WAL/idempotency roadmap. |
| Command | auth / authorization | `not implemented` | Command falls through to `CommandNotFound` | Out of MVP scope. |

## Desktop Client Check

Issue #1473 identified a MongoDB desktop-client connection failure on:
`unsupported MongoDB gateway command: connectionStatus`. The gateway now handles
that command with a minimal unauthenticated `authInfo` response and the
compatibility matrix keeps it covered.

The same desktop-client path later exposed
`unsupported MongoDB gateway command: hostInfo`. The gateway now handles that
command with minimal local runtime and OS metadata and keeps it covered in the
matrix.

The client path then exposed `unsupported MongoDB gateway command: buildInfo`.
The gateway now handles that command with minimal MongoDB-compatible version
and build metadata and keeps it covered in the matrix.

The client path then exposed `unsupported MongoDB gateway command: create`.
The gateway now handles plain collection creation as a TreeDB collection catalog
entry, treats existing collections as idempotent no-op success, and rejects
unsupported MongoDB collection options such as capped collections. That duplicate
`create` behavior is an intentional GUI-compatibility deviation from MongoDB's
`NamespaceExists` error.

The client path then exposed a driver-side
`Current topology does not support sessions` error. The gateway now advertises
logical session timeout metadata in `hello` and accepts `endSessions`; this
unblocks ordinary session-bearing driver commands without adding transaction
semantics.

This does not yet certify a full desktop GUI connection flow. If a client gets
past `connectionStatus` / `hostInfo` / `buildInfo` / `create` / logical
sessions and then asks for other metadata or DDL commands such as
`listDatabases` or `serverStatus`, add those commands as explicit matrix rows
before deciding whether to implement or reject them.

## Query Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| `_id` equality | `supported` | `TestMongoCompatibilityMatrix` | None for MVP. |
| `_id` `$in` | `supported subset` | find planner tests | No full query planner cost model. |
| Indexed scalar equality | `supported subset` | `TestMongoCompatibilityMatrix` | Requires single-field TreeDB secondary index. |
| Indexed scalar `$in` | `supported subset` | `TestMongoCompatibilityMatrix` | Null/missing has special scan behavior. |
| Indexed scalar ranges `$gt`, `$gte`, `$lt`, `$lte` | `supported subset` | `TestMongoCompatibilityMatrix` | Range behavior is typed by `treedbValueType`. |
| Top-level `$and` | `supported subset` | `TestMongoCompatibilityMatrix` | Planner flattens supported subexpressions only. |
| Top-level `$or`, `$nor`, `$not` | `rejected` / `not implemented` | `TestMongoCompatibilityMatrix` covers `$or` rejection | Needs explicit planner semantics. |
| Regex/text/geospatial predicates | `not implemented` | Unsupported operators reject or command is absent | Out of MVP scope. |
| Dotted predicates into nested objects/arrays | `supported subset` | dotted predicate tests | Projection and sort do not support dotted fields. |
| Projection include/exclude top-level fields | `supported subset` | `TestMongoCompatibilityMatrix` | Cannot mix include/exclude except `_id`; dotted projection rejected. |
| Sort by one top-level field | `supported subset` | `TestMongoCompatibilityMatrix` | Compound sort and dotted sort rejected. |
| `skip`, `limit`, `batchSize`, `singleBatch` | `supported subset` | cursor and find planner tests | Behavior is bounded by gateway scan/message limits. |
| Collation | `not implemented` | Not parsed | String comparison is binary/default TreeDB behavior. |

## Update And Delete Matrix

| Surface | Status | Harness / evidence | Current gap |
|---|---|---|---|
| `updateOne` by `_id` with `$set` | `supported subset` | `TestMongoCompatibilityMatrix` | Top-level fields only; `_id` mutation rejected. |
| Batched distinct-ID `$set` updates | `supported subset` | update batch tests | Unique-index conflicts may fall back to ordered singles. |
| `multi: true` | `rejected` | `TestMongoCompatibilityMatrix` | No multi-update planner. |
| `upsert: true` | `rejected` | `TestMongoCompatibilityMatrix` | No upsert semantics. |
| `$inc`, `$unset`, `$push`, pipeline updates | `rejected` / `not implemented` | `TestMongoCompatibilityMatrix` covers `$inc` rejection | Only `$set` is implemented. |
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
| Single-field ascending secondary index | `supported subset` | `TestMongoCompatibilityMatrix` | Requires `treedbValueType`. |
| Unique single-field secondary index | `supported subset` | metadata/update tests | Unique conflict behavior is TreeDB-backed, not exhaustive Mongo parity. |
| Supported `treedbValueType` values | `supported subset` | metadata tests | `string`, `bool`, `int64`, `double`. |
| Compound index | `rejected` | `TestMongoCompatibilityMatrix` | Needs collection index design work. |
| Descending, hashed, text, wildcard, geospatial indexes | `rejected` / `not implemented` | Invalid index commands reject or command absent | Out of MVP scope. |
| Automatic type inference for indexes | `rejected` | `TestMongoCompatibilityMatrix` covers missing `treedbValueType` | Type must be declared explicitly. |

## Durability And Transactions

| Surface | Status | Current gap |
|---|---|---|
| Mongo `writeConcern` | `not implemented` as Mongo semantics | Gateway success currently follows the underlying TreeDB collection API and durability profile. |
| Mongo `readConcern` | `not implemented` | No read concern parser or server-side snapshot API mapping. |
| Logical sessions | `supported subset` | Advertised for driver compatibility; `lsid` is accepted but does not add causal consistency, retryable-write, or transaction semantics. |
| Multi-document transactions | `not implemented` | Transactional write markers are rejected before mutation; full support is blocked on TreeDB collection transaction and collection WAL work. |
| Retryable writes / idempotency | `not implemented` | `txnNumber` write markers are rejected before mutation; support needs explicit idempotency metadata and error contract. |

## Benchmark-Only Surfaces

| Surface | Status | Notes |
|---|---|---|
| `cmd/mongo_gateway_bench -client-mode direct` | `benchmark-only` | Bypasses Mongo wire/driver to measure collection-engine ceiling. |
| `raw-wire` and `raw-wire-tcp-pipeline` modes | `benchmark-only` | Estimate gateway/server ceiling without official driver CRUD helper overhead. |
| `fastclient` package | `benchmark-only` | Narrow raw BSON helper for focused benchmark paths. |
| `scripts/mongo_gateway_compat_smoke.sh` | `benchmark-only` | Sprint-friendly TreeDB-vs-Mongo smoke wrapper around `mongo_gateway_compare.sh`; nonzero exit means the compared workload failed. |

## Next Gap-Closing Candidates

1. Run a real desktop GUI client after `connectionStatus` and add any next
   unsupported command as a matrix row before implementing it.
2. Decide whether unsupported filters should always fail closed or allow bounded
   scans behind a feature flag.
3. Add explicit `writeConcern`/`readConcern` handling that either rejects
   unsupported values or maps them to documented TreeDB durability boundaries.
4. Add count/distinct only after the desired TreeDB collection count/index
   semantics are clear.
5. Keep multi-document transactions blocked until the local collection
   transaction and collection WAL tracks are implemented.
