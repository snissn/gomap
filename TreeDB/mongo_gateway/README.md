# TreeDB MongoDB Gateway

This package contains the small MongoDB-compatible gateway used to expose TreeDB
collections to MongoDB clients.

The standalone server can be run directly from this directory-level entrypoint:

```sh
GOWORK=off go run ./TreeDB/mongo_gateway/server.go \
  -addr 127.0.0.1:27017 \
  -dir /tmp/treedb-mongo-gateway \
  -profile durable \
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
MONGO_GATEWAY_PROFILE=fast \
make run-mongo-gateway
```

The server prints a MongoDB URI when it starts. MongoDB clients should connect
with direct/single-server mode when their driver supports it, for example:

```text
mongodb://127.0.0.1:27017/?directConnection=true
```

The gateway implements an intentionally narrow command subset around hello/ping,
insert, find/getMore/killCursors, update, delete, and collection/index metadata.
It does not provide authentication, replica set behavior, sharding, sessions,
transactions, change streams, aggregation, or full MongoDB compatibility.
