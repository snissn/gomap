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

The gateway implements an intentionally narrow command subset around hello/ping,
insert, find/getMore/killCursors, update, delete, and collection/index metadata.
It provides minimal logical-session compatibility (`logicalSessionTimeoutMinutes`,
`lsid`, and `endSessions`) for driver interoperability, but not transactions,
causal consistency, or other session semantics. It also does not provide
authentication, replica set behavior, sharding, change streams, aggregation, or
full MongoDB compatibility.

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
