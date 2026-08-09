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

## Transport policy

Plaintext is deliberately limited to loopback listeners. A standalone listener
on `0.0.0.0`, `::`, or another non-loopback address must use `-tls-cert-file`
and `-tls-key-file`, or explicitly select `-allow-insecure-remote`; the latter
prints a warning and is only appropriate for controlled development networks.
The gateway accepts TLS 1.2 or 1.3, performs each handshake before processing a
MongoDB message, and bounds it with `-tls-handshake-timeout` (10 seconds by
default). It never prints key material or raw certificates in startup status.
Library users can obtain the selected mode and certificate expiry with
`TransportStatus`, and handshake counts plus cumulative/max nanosecond timing
with `TransportMetrics`; neither exposes client identities or certificate/key
contents. The executable capability manifest identifies the supported policy;
each compatibility-differential artifact additionally records its effective
TreeDB listener mode (the bundled harness is `plaintext-loopback`).

```sh
GOWORK=off go run ./TreeDB/mongo_gateway/server.go \
  -addr 0.0.0.0:27017 -dir /var/lib/treedb-mongo \
  -tls-cert-file /etc/treedb/tls/server.pem \
  -tls-key-file /etc/treedb/tls/server-key.pem \
  -tls-min-version 1.2
```

Clients should use normal certificate validation, for example a URI with
`tls=true` plus their trusted CA configured in the driver. Client certificate
verification is optional and uses `-tls-ca-file -require-client-cert`; it is a
transport check only, not MongoDB authentication or authorization.

<!-- mongo-capability-summary:begin -->
## Executable capability summary

Manifest: `treedb.mongo-gateway.capability-manifest/v1/sha256:74f666cad3c1939b3ecab5935cb220a94809943c710936ad52e827a4c34d0346`

| Surface | Status | Boundary |
|---|---|---|
| Standalone CRUD | supported subset | Explicit-ID and bounded single-document shapes; broader Mongo semantics remain intentionally limited. |
| Standalone write concern | supported subset | Absent/default and w:1 use the selected profile's ordinary acknowledgement boundary; j:true closes a real command-WAL or checkpoint sync boundary. Unacknowledged, replica, and interruptible-timeout semantics reject before mutation. |
| Aggregation, count, and distinct | supported subset | Bounded standalone subsets only; unsupported stages, dotted distinct keys, and maxTimeMS reject. |
| Administrative diagnostics | not implemented | serverStatus, top, and dbStats are not implemented. |
| Logical sessions | supported subset | Driver-interoperability metadata only; no transaction or causal-session semantics. |
| Transport security | supported subset | Loopback plaintext remains available; non-loopback standalone listeners require TLS unless an explicit insecure override is selected. Password authentication refuses plaintext non-loopback listeners. TLS 1.2+ with bounded handshakes is supported. |
| Scalar indexes | supported subset | BSON collections default ordinary single-field ascending indexes to BSON-ordered v2; explicit treedbValueType remains the legacy homogeneous path. Compound and descending indexes remain rejected. |
| Authentication and authorization | supported subset | Standalone SCRAM-SHA-256 establishes a connection identity from durable verifier-only records. Per-command authorization, SCRAM-SHA-1, and external identity providers remain unavailable. |
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
`--smoke` selects only fixture files explicitly marked `"smoke": true`; the
bundled full set also covers supported read, write, aggregate, distinct, and
metadata shapes. Post-command state enumerates every collection in the fixture
database in collection-name order, retaining each collection's natural document
order and deterministic collection/index metadata. It is a bounded mutation
witness, not a claim to snapshot every MongoDB catalog option or view attribute.
The runner never sorts
documents to make a disagreement disappear.

`ignore_fields` applies only to command/cursor replies, while
`ignore_state_fields` applies only to supported-case state comparison; neither
is applied to the exact pre/post state proof required for a rejected fixture.
`normalize_fields` replaces only a declared nondeterministic
value (such as an ObjectID or BSON timestamp) while retaining its BSON type and
path. `normalize_response_envelope_order` is a separate, explicit opt-in for
top-level command-reply envelope keys such as `ok` and `n`.
`normalize_cursor_envelope_order` is a separate opt-in for cursor transport
keys (`id`, `ns`, `firstBatch`/`nextBatch`), while the initial raw reply and
every raw `getMore` reply remain attributable. Nested BSON and cursor-document
order remains significant. `normalize_cursor_namespace` normalizes only the
database prefix of `cursor.ns`, retaining its collection or `$cmd` suffix.
Rejected fixtures require an exact TreeDB error code, a successful reference
command, and unchanged bounded captured state: collections, natural documents,
and index specifications.

Ordinary package tests do not require Docker or a reference server. To run the
local smoke suite, use the wrapper, which starts the pinned `mongo:7.0.14`
image and writes `result.json`, `result.md`, and `result.tsv`:

```sh
scripts/mongo_gateway_compat_diff.sh --smoke --out /tmp/mongo-gateway-compat-diff
```

Omit `--smoke` to run the full diagnostic fixture set. Full mode exits 1 for a
compatibility mismatch and intentionally retains the differing BSON path, type,
and value in the artifacts; it may expose known gateway gaps. This is evidence,
not a claim that every declared shape is already identical to MongoDB.

For an externally managed reference, provide its URI explicitly:

```sh
GOWORK=off go build -o /tmp/mongo_gateway_compat_diff ./cmd/mongo_gateway_compat_diff
/tmp/mongo_gateway_compat_diff \
  -reference-uri 'mongodb://127.0.0.1:27017/?directConnection=true' \
  -out /tmp/mongo-gateway-compat-diff
```

Artifacts include `result.json`, concise `result.md`/`result.tsv`, the
capability-manifest identity, pinned reference image,
observed reference `buildInfo` version/git identity, normalized TreeDB and
reference responses/state, and per-fixture duration. Fixture-scoped reply
`ignore_fields` and state `ignore_state_fields` are omitted from their
respective normalized artifacts. Error messages remain visible for diagnosis, but
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
