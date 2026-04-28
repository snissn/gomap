# MongoDB-Compatible Gateway Plan

Status: planning and work log only. No compatibility code lives here yet.

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

Required MVP operations:

- `hello` / `isMaster` enough for driver handshake.
- `insert` / `insertOne` / `insertMany`.
- `find` with simple predicates.
- `getMore` and cursor close for bounded result sets.
- `update` / `updateOne` with `$set`.
- `delete` / `deleteOne` / `deleteMany`.
- `createIndexes` and `dropIndexes`.
- `listCollections` and `listIndexes`.

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
   - Map `_id` equality to primary lookup.
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

- Should the gateway store raw BSON bytes, canonical JSON, or a typed internal
  document encoding?
- If raw BSON storage is useful, should it be a collection-level document format
  beside JSON and template-v1, or a gateway-local optimization hidden behind the
  existing collection API?
- Should `_id` be the TreeDB collection primary key, or should it live as a
  normal field with an internal primary key?
- Which BSON types need indexed ordering in the MVP?
- Should unsupported filters fail closed, or fall back to bounded scans behind a
  feature flag?
- How much MongoDB error-code compatibility matters for common drivers.
- Whether the first benchmark should use the real MongoDB wire protocol or a
  lower-level adapter that exercises equivalent operations.

## Todo

- [ ] Survey Go MongoDB wire protocol libraries and decide whether to use one or
      implement the small OP_MSG subset directly.
- [ ] Write a short compatibility matrix for commands, query operators, update
      operators, and BSON types.
- [ ] Define the initial BSON-to-TreeDB document encoding.
- [ ] Instrument BSON decode, document re-encoding, index extraction, and
      response encoding so the benchmark can prove whether a native BSON
      collection format is needed.
- [ ] If re-encoding is material, draft `DocumentFormatBSON` alongside the
      existing JSON and template-v1 collection formats.
- [ ] Prototype driver handshake with the official MongoDB Go driver.
- [ ] Implement `insert` and `_id` lookup against TreeDB collections.
- [ ] Implement single-field index creation and indexed `find`.
- [ ] Add protocol-level compatibility tests using a real MongoDB driver.
- [ ] Add a reproducible MongoDB-vs-TreeDB benchmark harness.
- [ ] Publish first benchmark report with disk usage and ops/sec.
- [ ] Revisit scope after the first benchmark report and decide whether to
      expand compatibility or keep the gateway benchmark-only.

## Work Log

- 2026-04-28: Created the planning folder and initial spec. Agreed framing:
  pursue a MongoDB-compatible gateway/subset for product exploration and
  benchmarking, not a claim of full MongoDB compatibility.
- 2026-04-28: Added native BSON collection-format planning as a benchmark-driven
  follow-up if BSON re-encoding proves expensive relative to TreeDB operations.
