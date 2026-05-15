# MongoDB-Compatible Gateway Plan

Status: planning, work log, early wire-protocol prototype, first
collection-backed command path, CRUD MVP, and collection/index metadata MVP.

This folder tracks the product expansion effort to expose TreeDB collections
through a MongoDB-compatible gateway. The near-term goal is not to claim full
MongoDB compatibility. The goal is a small, honest driver-compatible subset that
lets existing MongoDB clients try TreeDB, and lets us benchmark TreeDB against a
local MongoDB server on comparable document workloads.

## Framing

TreeDB collections are already close to the product shape of a document
database: named collections, JSON-like documents, primary document lookup, and
secondary indexes. A MongoDB-compatible gateway is attractive because it gives
users a familiar model and avoids inventing a custom client protocol before we
know whether this product direction has pull.

The scope has to stay deliberately narrow. MongoDB compatibility includes wire
protocol details, BSON semantics, query language behavior, update operators,
cursors, aggregation, indexes, sessions, write concern, auth, and transactions.
Full compatibility is a long-term product, not an MVP.

## MVP Compatibility Target

The first milestone should be a gateway that common MongoDB drivers can connect
to and use for simple collection workflows.

Required MVP operations use MongoDB command names. Common drivers expose helper
methods such as `insertOne` / `insertMany`, `updateOne`, and `deleteOne` /
`deleteMany` on top of these commands.

- `hello` / `isMaster` enough for driver handshake.
- `insert`.
- `find` with simple predicates.
- `getMore` and `killCursors` for bounded result sets.
- `update` with `$set`.
- `delete`.
- `createIndexes` and `dropIndexes`.
- `listCollections` and `listIndexes`.

TreeDB-backed `createIndexes` rejects attempts to create the built-in `_id`
index. All user-created single-field indexes require the gateway-specific
`treedbValueType` option. Supported values are `string`, `bool`, `int64`, and
`double`; the gateway forwards that type into collection secondary-index
metadata instead of inferring from existing documents.

MVP query support:

- equality predicates on `_id` and indexed scalar fields.
- range predicates on indexed scalar fields: `$gt`, `$gte`, `$lt`, `$lte`.
- `$in` on `_id` and indexed scalar fields.
- top-level `$and`.
- projection include/exclude for top-level fields.
- sort, limit, and skip when backed by a usable index or a bounded in-memory
  fallback.

MVP BSON/document support:

- `_id` required or auto-generated.
- MongoDB `_id` maps to the TreeDB collection document ID / primary key for the
  MVP.
- BSON object, string, bool, integer, double, null, array, and nested document
  values preserved on round trip.
- Clear rejection for unsupported BSON types rather than lossy conversion.
- The initial gateway may translate BSON into an existing collection document
  format, but it should measure that cost directly before treating re-encoding
  as acceptable.

## Non-Goals For The First PR Stack

- Full MongoDB wire protocol coverage.
- Aggregation pipeline.
- Transactions and sessions.
- Replication, change streams, and oplog behavior.
- Sharding.
- Auth and authorization.
- Text, geospatial, hashed, wildcard, or compound index parity unless a benchmark
  explicitly needs them.
- Exact MongoDB error codes for every edge case.

## Proposed Layout

Implementation should stay isolated until the compatibility surface is proven.

- `TreeDB/mongo_gateway/`
  - gateway package, protocol adapters, planner, tests, and this planning file.
- `TreeDB/cmd/mongo_gateway/`
  - eventual CLI entrypoint for local testing and benchmarks.
- `docs/benchmarks/`
  - benchmark reports once MongoDB comparison runs are reproducible.

Avoid coupling the gateway directly to low-level pager or value-log APIs. The
first implementation should depend on TreeDB collections and only introduce
lower-level hooks when a measured workload needs them.

## Architecture Sketch

1. Wire protocol layer
   - Accept MongoDB driver connections.
   - Decode OP_MSG commands and encode MongoDB-like replies.
   - Keep compatibility behavior in one package so TreeDB collection code stays
     protocol-neutral.

2. BSON/document layer
   - Convert BSON documents to the collection document representation.
   - Preserve enough type information for round trips and indexed scalar
     comparisons.
   - Define unsupported-type behavior before benchmarks begin.
   - Keep the storage boundary explicit: start with JSON or template-v1 where it
     is sufficient, but plan a native BSON collection document format if
     benchmarks show BSON re-encoding or BSON-to-JSON conversion is a material
     ingest, update, read, or index-extraction bottleneck.

3. Query planner
   - Map `_id` equality to TreeDB primary-key lookup.
   - Map supported indexed predicates to collection secondary-index lookup.
   - Use bounded scan fallback only when it is explicit and visible in metrics.
   - Expose planner stats so benchmark output can distinguish indexed paths from
     fallback scans.

4. Index mapping
   - Map `createIndexes` to collection index metadata where possible.
   - Start with single-field ascending indexes.
   - Record unsupported index declarations and return clear errors.

5. Benchmark harness
   - Use the same document corpus and operation mix for TreeDB and MongoDB.
   - Track ops/sec, p50/p95/p99 latency, disk bytes after load, disk bytes after
     checkpoint/vacuum/compact, and correctness verification.
   - Attribute time spent in BSON decode, document re-encoding, index extraction,
     TreeDB writes, and response encoding so format work is driven by measured
     bottlenecks.

## Wire Protocol Library Survey

Decision: implement a small TreeDB-owned wire protocol layer for the MVP. Do
not depend on a third-party MongoDB wire protocol package for the first
prototype. If we add a MongoDB dependency, prefer the public
`go.mongodb.org/mongo-driver/v2/bson` package for BSON documents and ObjectId
helpers, not the driver's internal wire protocol package.

Sources checked:

- MongoDB wire protocol reference:
  <https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/>
- MongoDB handshake spec:
  <https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/>
- Official Go driver internal `wiremessage` package:
  <https://pkg.go.dev/go.mongodb.org/mongo-driver/v2/x/mongo/driver/wiremessage>
- Official Go driver public `bson` package:
  <https://pkg.go.dev/go.mongodb.org/mongo-driver/v2/bson>
- FerretDB extracted wire package:
  <https://pkg.go.dev/github.com/FerretDB/wire>
- Teleport MongoDB proxy protocol package:
  <https://pkg.go.dev/github.com/zmb3/teleport/lib/srv/db/mongodb/protocol>
- Cybergarage `go-mongo` server package:
  <https://pkg.go.dev/github.com/cybergarage/go-mongo/mongo>

Findings:

- Modern MongoDB command traffic is centered on `OP_MSG`, and MongoDB 5.1
  removes the old CRUD opcodes in favor of `OP_MSG`. However, driver handshakes
  can still start with legacy `isMaster` over `OP_QUERY` before switching to
  `OP_MSG` when `maxWireVersion` indicates support. The MVP therefore needs
  `OP_QUERY` handshake support plus `OP_REPLY`, not only `OP_MSG`.
- The official Go driver's `wiremessage` package has the low-level constants and
  append/read helpers we would want, but its docs say it is internal,
  experimental, and has no backward-compatibility guarantee. It is useful as a
  reference, not a dependency boundary.
- FerretDB/wire is the closest standalone fit: it reads/writes messages and
  covers `OP_MSG`, `OP_QUERY`, and `OP_REPLY`. Its README currently says "Please
  do not use it yet", and the latest module declares Go 1.24 while this repo is
  still `go 1.23.0`. It is worth re-evaluating later, but not as the MVP
  foundation.
- Teleport's protocol package covers the right message subset for a proxy, but
  it is embedded in a large proxy product tree rather than published as a small
  standalone protocol module. Pulling it in would import product assumptions we
  do not need.
- Cybergarage/go-mongo exposes server/message-handler abstractions, but it is a
  broader server framework with low import adoption. For this effort, it is more
  useful as prior art than as an initial dependency.

Direct MVP wire scope:

- Read and validate the 16-byte standard message header.
- Enforce a conservative maximum message length.
- Support `OP_QUERY` only for initial `hello` / `isMaster` handshake, and reply
  with `OP_REPLY`.
- Support `OP_MSG` for normal commands with exactly one kind 0 body section.
  Add kind 1 document sequences only when bulk payloads require them.
- Do not advertise compression; reject or close on `OP_COMPRESSED` until a
  benchmark or driver compatibility test requires it.
- Populate response headers with `responseTo` set to the client request ID.
- Use `bson.Raw` / `RawValue` for command parsing and response construction
  where the public driver BSON API is sufficient.

Revisit this decision if FerretDB/wire becomes explicitly supported for external
use, if direct wire parsing starts to sprawl beyond the small subset above, or if
driver compatibility tests expose edge cases that a maintained protocol package
already handles cleanly.

## Document Identity Mapping

MVP decision: MongoDB `_id` should be the TreeDB collection primary key. This
matches the current collection API shape: callers insert with a document ID,
primary lookup reads by that ID, and delete removes by that ID. It also gives the
cleanest MongoDB benchmark mapping because MongoDB's `_id` index corresponds to
TreeDB's primary-root lookup instead of an extra secondary index.

Expected behavior:

- If an inserted BSON document has no `_id`, the gateway generates one before
  writing.
- The generated `_id` should use MongoDB-compatible ObjectId semantics unless a
  benchmark or driver compatibility test gives a reason to choose otherwise.
- The stored document should still contain `_id` so BSON round trips preserve the
  caller-visible document shape.
- Updates that change `_id` should be rejected. If a future compatibility mode
  permits them, it should be implemented as explicit delete+insert behavior, not
  as an in-place primary-key mutation.
- The primary-key encoding must be canonical and type-aware. It should not rely
  on lossy stringification of BSON values.

Remaining risk to measure: arbitrary or large `_id` values can make primary keys
and secondary-index postings larger, and random ObjectIds may have different
write-locality behavior than an internal monotonic row ID. Keep the alternative
design, internal row ID plus unique `_id` index, as a fallback only if benchmark
evidence shows `_id`-as-primary-key dominates disk usage or write cost.

## Collection Document Format Expansion

TreeDB collections currently have JSON and template-v1 document formats. The
MongoDB gateway should explicitly evaluate whether BSON deserves to become a
third collection document format, for example `DocumentFormatBSON`.

Do not assume BSON storage is required for the first gateway milestone. It adds
format surface area, type-ordering rules, index extraction logic, and migration
pressure. It becomes high priority if benchmarks show that repeated
BSON-to-JSON, JSON-to-BSON, or BSON-to-template-v1 conversion is a material part
of end-to-end cost.

If benchmark evidence supports it, a BSON document format should aim to:

- store raw or lightly normalized BSON bytes in the primary collection root.
- extract secondary index values directly from BSON without converting to JSON.
- preserve MongoDB scalar typing and ordering semantics where the MVP query
  subset depends on them.
- return stored BSON to the wire protocol with minimal re-encoding.
- coexist with JSON and template-v1 in the document-format benchmark matrix.

## Benchmark Questions

The comparison should be honest about product shape. TreeDB is currently best
positioned as an embedded/local durable document store, while MongoDB is a
mature networked database with a broad query engine.

Priority workloads:

- Bulk insert of medium JSON/BSON documents with one and two secondary indexes.
- Indexed point lookup by `_id`.
- Indexed point lookup by one secondary scalar field.
- Indexed range scan with limit.
- Mixed insert/read workload with checkpointing enabled.
- Disk usage after load, after checkpoint, and after maintenance.

## Work Log

- PR 1088 added OP_MSG document sequences plus collection-backed `insert` and
  exact `_id` equality `find`, with a real Go driver insert/find smoke test.
- PR 1089 adds exact `_id` equality `update`/`delete` command support for the
  gateway CRUD MVP.
- Current metadata slice adds `listCollections`, `createIndexes`,
  `listIndexes`, and `dropIndexes` for single-field ascending collection
  secondary indexes.
- Current find-planner slice adds `_id` `$in`, indexed scalar equality/`$in`,
  top-level `$and`, bounded scan fallback for range predicates, single-field
  sort, skip, limit, and top-level projection.
- Current cursor slice adds in-memory server cursor state for batched `find`,
  `getMore`, and `killCursors`.
- Current benchmark slice adds `cmd/mongo_gateway_bench`, a reusable comparison
  runner in `scripts/mongo_gateway_compare.sh`, and
  `cmd/mongo_gateway_compare_report` for regenerating Markdown and TSV reports
  from raw TreeDB/MongoDB benchmark JSON.
- Current compatibility slice adds `COMPATIBILITY.md` and
  `TestMongoCompatibilityMatrix`, a compact issue-1493 matrix that probes
  representative supported and rejected Mongo gateway paths.
- Current desktop-client compatibility slice handles the issue-1473
  `connectionStatus`, `hostInfo`, `buildInfo`, `create`, and logical-session
  topology failures with minimal metadata/DDL/session responses and keeps them
  covered in the matrix.
- Current smoke slice adds `scripts/mongo_gateway_compat_smoke.sh`, a small
  TreeDB-vs-MongoDB driver workload wrapper around the comparison harness that
  reports naive throughput and exits nonzero if the supported path breaks.

Metrics to record for every run:

- total operations/sec.
- p50, p95, and p99 latency by operation type.
- BSON decode, document re-encode, index extraction, and response encode time
  where instrumentation can measure them without distorting the benchmark.
- database directory bytes.
- index/data split where available.
- TreeDB leaf-vlog, value-vlog, and `index.db` bytes.
- MongoDB collection/index/storage bytes from `dbStats` and filesystem `du`.
- correctness counts for documents inserted, found, updated, and deleted.

## Open Design Questions

- Should the gateway continue bridging through canonical Extended JSON, store
  raw BSON bytes, or use a typed internal document encoding?
- If raw BSON storage is useful, should it be a collection-level document format
  beside JSON and template-v1, or a gateway-local optimization hidden behind the
  existing collection API?
- What canonical `_id`-to-primary-key byte encoding should support ObjectId,
  string, numeric, binary, and other MVP BSON scalar types?
- Should the gateway impose an `_id` size limit or warning threshold to protect
  secondary-index posting size?
- Which BSON types need indexed ordering in the MVP?
- Should unsupported filters fail closed, or fall back to bounded scans behind a
  feature flag?
- How much MongoDB error-code compatibility matters for common drivers.
- Whether the first benchmark should use the real MongoDB wire protocol or a
  lower-level adapter that exercises equivalent operations.

## Todo

- [x] Survey Go MongoDB wire protocol libraries and decide whether to use one or
      implement the small OP_MSG subset directly.
- [x] Write a short compatibility matrix for commands, query operators, update
      operators, and BSON types.
- [x] Prototype the small TreeDB-owned wire layer with OP_QUERY handshake,
      OP_REPLY handshake response, and OP_MSG command request/response tests.
- [x] Prototype a minimal gateway server loop that answers `OP_QUERY`
      `hello` / `isMaster` and `OP_MSG` `ping` without collection storage.
- [x] Prototype OP_MSG document-sequence parsing for insert-style driver
      payloads.
- [x] Define the initial BSON-to-TreeDB document encoding as a canonical
      Extended JSON bridge, with native BSON storage still benchmark-gated.
- [x] Define canonical `_id` primary-key encoding and generated ObjectId
      behavior for inserts.
- [ ] Instrument BSON decode, document re-encoding, index extraction, and
      response encoding so the benchmark can prove whether a native BSON
      collection format is needed.
- [ ] If re-encoding is material, draft `DocumentFormatBSON` alongside the
      existing JSON and template-v1 collection formats.
- [x] Prototype driver handshake with the official MongoDB Go driver.
- [x] Implement `insert` and `_id` lookup against TreeDB collections.
- [x] Implement single-field index creation and indexed `find`.
- [x] Add protocol-level compatibility tests using a real MongoDB driver.
- [x] Add a reproducible MongoDB-vs-TreeDB benchmark harness.
- [x] Add a sprint-friendly MongoDB-vs-TreeDB compatibility smoke that reports
      naive throughput and fails on unsupported benchmark-path operations.
- [x] Publish first benchmark report with disk usage and ops/sec.
- [ ] Revisit scope after the first benchmark report and decide whether to
      expand compatibility or keep the gateway benchmark-only.

## Work Log

- 2026-04-28: Created the planning folder and initial spec. Agreed framing:
  pursue a MongoDB-compatible gateway/subset for product exploration and
  benchmarking, not a claim of full MongoDB compatibility.
- 2026-04-28: Added native BSON collection-format planning as a benchmark-driven
  follow-up if BSON re-encoding proves expensive relative to TreeDB operations.
- 2026-04-28: Decided the MVP should map MongoDB `_id` to the TreeDB collection
  primary key, with remaining work focused on canonical key encoding and
  benchmark validation of key-size/write-locality costs.
- 2026-04-28: Completed the Go wire-protocol package survey. Decision: implement
  a small TreeDB-owned wire layer for `OP_QUERY` handshake, `OP_REPLY`, and
  `OP_MSG`; use public BSON APIs where helpful, but avoid internal/unstable or
  not-yet-supported wire protocol dependencies for the MVP.
- 2026-04-28: Added `TreeDB/mongo_gateway/wire`, a focused protocol-framing
  package that validates message headers, handles OP_QUERY handshake parsing,
  builds OP_REPLY responses, parses/builds single-body OP_MSG messages, rejects
  checksum-bearing and document-sequence OP_MSGs until needed, and uses the
  official driver v2 `bson.Raw` type for BSON document validation.
- 2026-04-28: Added a minimal `mongogateway.Server` that serves one message or
  a connection loop, responds to legacy `OP_QUERY` handshake requests with
  `OP_REPLY`, responds to `OP_MSG` `ping` with an `OP_MSG` reply, and rejects
  compressed messages until compression is implemented.
- 2026-04-28: Added the first collection-backed command path. `insert` accepts
  either a `documents` array or an OP_MSG `documents` document sequence,
  auto-creates the TreeDB collection namespace, maps BSON `_id` to a type-aware
  collection primary key, stores documents through a canonical Extended JSON
  bridge, and `find` supports `_id` equality with a Mongo-style cursor response.
  This is intentionally a bridge format until benchmarks prove whether native
  BSON collection storage is needed.
- 2026-04-29: Added the reusable Mongo gateway comparison harness and checked in
  `docs/benchmarks/mongo_gateway_compare_2026-04-29/`, a six-cell TreeDB vs
  MongoDB report bundle with raw JSON, matrix TSV, summary TSV, physical disk
  `du` bytes, and ops/sec ratios.
