# TreeDB MongoDB Gateway

This package contains the small MongoDB-compatible gateway used to expose TreeDB
collections to MongoDB clients.

The standalone server can be run directly from this directory-level entrypoint:

```sh
GOWORK=off go run ./TreeDB/mongo_gateway/server.go \
  -addr 127.0.0.1:27017 \
  -dir /tmp/treedb-mongo-gateway \
  -profile command_wal_durable \
  -document-format bson
```

`GOWORK=off` keeps `go run` from resolving the gateway library through a parent
workspace that replaces `github.com/snissn/gomap` with another checkout.

Equivalent Makefile targets from the repository root:

```sh
make build-mongo-gateway
make run-mongo-gateway
```

Useful Makefile overrides:

```sh
MONGO_GATEWAY_ADDR=127.0.0.1:27018 \
MONGO_GATEWAY_DIR=/tmp/treedb-mongo-gateway-dev \
MONGO_GATEWAY_PROFILE=command_wal_durable \
make run-mongo-gateway
```

The server prints a MongoDB URI when it starts. MongoDB clients should connect
with direct/single-server mode when their driver supports it, for example:

```text
mongodb://127.0.0.1:27017/?directConnection=true
```

<!-- mongo-capability-summary:begin -->
## Executable capability summary

Manifest: `treedb.mongo-gateway.capability-manifest/v1/sha256:3bced062c485678a4ea0dd4aae5fe4eef8c36ae63cb1110cef5b6a5bc525118f`

| Surface | Status | Boundary |
|---|---|---|
| Standalone CRUD | supported subset | Explicit-ID and bounded single-document shapes; broader Mongo semantics remain intentionally limited. |
| Aggregation, count, and distinct | supported subset | Bounded standalone subsets only; unsupported stages, dotted distinct keys, and maxTimeMS reject. |
| Administrative diagnostics | not implemented | serverStatus, top, and dbStats are not implemented. |
| Logical sessions | supported subset | Driver-interoperability metadata only; no transaction or causal-session semantics. |
| Scalar indexes | supported subset | BSON collections default ordinary single-field ascending indexes to BSON-ordered v2; explicit treedbValueType remains the legacy homogeneous path. Compound and descending indexes remain rejected. |
| Authentication and authorization | not implemented | The current standalone gateway assumes a trusted local deployment. |
| Transactions and retryable writes | not implemented | Transaction markers reject and commitTransaction is unavailable. |
| Replica set and sharding | not implemented | Standalone hello metadata does not advertise replica-set or sharded-server behavior. |
<!-- mongo-capability-summary:end -->

This summary and the factual table in `COMPATIBILITY.md` are generated from the
same versioned executable capability manifest. Regenerate both with:

```sh
GOWORK=off go test ./TreeDB/mongo_gateway \
  -run TestMongoCompatibilityMatrixDocumentationUpToDate \
  -update-mongo-capability-docs
```

The manifest identity is also exposed by `buildInfo` and emitted by benchmark
reports so stored compatibility evidence can be tied to the exact declared
surface.

## Differential compatibility fixtures

`cmd/mongo_gateway_compat_diff` compares a deliberately small set of declared
standalone shapes with a reference MongoDB. It is compatibility evidence for
those fixtures, not a full MongoDB conformance claim. Fixture files are
versioned canonical Extended JSON in
`cmd/mongo_gateway_compat_diff/fixtures`; the runner decodes them to BSON and
preserves BSON type and field order in emitted observations.

Every fixture capability ID is checked against the consumed executable manifest
before it runs: `supported` fixtures must map to a supported/support-subset
row, and `rejected` fixtures must map to a rejected/not-implemented row.
`--smoke` selects only fixture files explicitly marked `"smoke": true` (the
initial bundled set is all smoke-marked). Post-command state is compared in
the server's returned natural order; the runner never sorts it to make a
disagreement disappear.

Ordinary package tests do not require Docker or a reference server. To run the
local smoke suite, use the wrapper, which starts the pinned `mongo:7.0.14`
image and writes `result.json`, `result.md`, and `result.tsv`:

```sh
scripts/mongo_gateway_compat_diff.sh --smoke --out /tmp/mongo-gateway-compat-diff
```

For an externally managed reference, provide its URI explicitly:

```sh
GOWORK=off go run ./cmd/mongo_gateway_compat_diff \
  -reference-uri 'mongodb://127.0.0.1:27017/?directConnection=true' \
  -out /tmp/mongo-gateway-compat-diff
```

Artifacts include `result.json`, concise `result.md`/`result.tsv`, the
capability-manifest identity, pinned reference image,
observed reference `buildInfo` version/git identity, normalized TreeDB and
reference responses/state, and per-fixture duration. Only fixture-scoped
`ignore_fields` are omitted. Error messages remain visible for diagnosis, but
equality compares error code and labels so permitted implementation wording
does not hide semantic differences. A missing reference exits with status 3
and a `reference-unavailable` artifact state, distinct from a compatibility
mismatch (status 1).

Cluster submitter mode does not turn this gateway into a sharded Mongo server.
For token/ring placement, exact `_id` equality finds are mapped to one catalog
token but fail closed before local observation because the gateway does not yet
have a production owner-proof/collection-store identity binding. Nativewire's
public token/ring read path is disabled for the same reason. Non-shard-key and
secondary-index reads remain non-scatter route rejections. All token/ring
mutations fail closed until authoritative collection and index metadata is
bound to the exact owner route proof; a gateway-local indexless copy is not
authoritative for a remote owner. Routed `listCollections`, `listDatabases`,
and `listIndexes` also fail closed instead of returning gateway-local metadata.
Collection-placement mutations remain supported. See
[`COMPATIBILITY.md`](COMPATIBILITY.md#cluster-tokenring-read-and-index-policy)
for the exact policy, counters, and internal-scaffold limitation.
