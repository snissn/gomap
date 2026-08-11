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

## In-process connection ownership

`ServeConn` owns its cursor and authentication lifecycle. In-process callers
that dispatch a long-lived logical connection with `ServeOneWithOwner` or
`ServeOneWithOwnerBuffered` must call `ReleaseOwner(owner)` when it closes and
before reusing that owner value; this removes both retained cursors and the
connection-bound authenticated identity. Owner values must be nonzero;
`ReleaseOwner(0)` is a harmless no-op. `ReleaseOwner` is idempotent and safe to
call concurrently.

<!-- mongo-capability-summary:begin -->
## Executable capability summary

Manifest: `treedb.mongo-gateway.capability-manifest/v1/sha256:6e9425a7b60720800cfb0d7590c9378751420382cf5eeb7a765a43c34b1b28af`

| Surface | Status | Boundary |
|---|---|---|
| Standalone CRUD | supported subset | Explicit-ID CRUD plus bounded multi-update/delete and ordered or unordered insert/update/delete batches; a positive maxTimeMS shortens the shared five-second command deadline; per-document atomicity only, never a transaction. |
| Standalone write concern | supported subset | Absent/default and w:1 use the selected profile's ordinary acknowledgement boundary; j:true closes a real command-WAL or checkpoint sync boundary. Unacknowledged, replica, and interruptible-timeout semantics reject before mutation. |
| Aggregation, count, and distinct | supported subset | Bounded standalone subsets only. Explain reports stable primary, secondary, compound_index_scan, bounded-scan, and adaptive_candidate_selection vocabulary for find, count, distinct, and aggregate pipelines whose planner prefix is match/sort/skip/limit; later match or sort stages reject rather than being misreported as find-plan work. Writes, routed reads, and unsupported verbosity reject. |
| Administrative diagnostics | supported subset | Standalone-only subset: serverStatus exposes standard numeric opcounters plus bounded gateway command metrics, connection/cursor counters, and a read-only command-WAL inventory when standalone-owned; top emits Mongo-shaped namespace total.time/count only. dbStats and collStats cap live documents at MaxFindScanDocuments; diagnostics separately caps total primary-source work (tombstones and merged-run/shadow work included) at twice that value. dbStats shares both budgets across its collections and, after exact exhaustion, admits only metadata-proven empty trailing collections. Physical/logical byte totals and unsupported fields are omitted rather than estimated. Routed mode rejects before local observation. |
| Logical sessions | supported subset | Driver-interoperability metadata only; no transaction or causal-session semantics. |
| Transport security | supported subset | Loopback plaintext remains available; non-loopback standalone listeners require TLS unless an explicit insecure override is selected. Password authentication refuses plaintext non-loopback listeners. TLS 1.2+ with bounded handshakes is supported. |
| Scalar indexes | supported subset | Standalone find/count/distinct and initial aggregate match/sort prefixes can select ordered BSON v2 indexes with one through four ascending or descending components for canonical equality prefixes, bounded $in fanout, and one range suffix. A compatible one-term sort streams index order (or its complete reverse); multi-term sorts use the bounded in-memory Mongo comparator before skip/limit because missing and null can occupy distinct physical BSON-v2 runs. All other shapes use their existing bounded scan path, while a supplied missing, malformed, incompatible, or unsupported hint rejects before data reads. Every planned scan is capped by MaxFindScanDocuments and direct scalar work is capped at 64 physical entries per allocated result slot; materialized BSON is capped by MaxCursorRetainedBytes. Explicit treedbValueType remains the legacy homogeneous single-field ascending path. Ordered BSON v2 compound and descending definitions reject multikey metadata before catalog mutation; sparse, partial, TTL, collation, and hidden options also reject before catalog mutation. |
| Authentication and authorization | supported subset | Standalone SCRAM-SHA-256 identities use versioned durable account incarnations plus read, readWrite, dbAdmin, userAdmin, and serverAdmin grants, spilling growing records to TreeDB's persistent ValueLog, with pre-execution command checks, filtered catalog visibility, incarnation-bound sessions and cursors, and last-admin safeguards. Drop/recreate revokes the prior incarnation while password rotation preserves it. Cluster/routed protected commands fail closed without authoritative resource binding; SCRAM-SHA-1 and external identity providers remain unavailable. |
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

## Standalone authorization policy

Enabling authentication also enables fail-closed per-command authorization.
The first verifier created in an empty catalog is the bootstrap
`serverAdmin`; subsequent users receive no privileges until durable grants are
assigned. A non-empty catalog with no usable administrator does not
auto-escalate a new verifier; trusted tooling reports that offline repair is
required. The built-in roles are deliberately bounded: `read` permits data
reads, `readWrite` adds mutations, `dbAdmin` owns collection/index metadata and
DDL, `userAdmin` owns user management within its scope, and `serverAdmin` owns
the full standalone server. Grants may be server-, database-, or
collection-scoped. A collection-scoped `userAdmin` may manage and observe only
identities whose non-empty current and requested grants stay inside that
collection; empty grant sets require database scope. A server-scoped
`userAdmin` may grant non-administrator server roles, but only `serverAdmin`
may grant `serverAdmin`. Every user mutation rechecks the actor and target's
current/requested grants while holding the same backend catalog lock.
Missing and protected out-of-scope `updateUser`/`dropUser` targets return the
same generic denial to every narrower `userAdmin`; only `serverAdmin`, which can
manage every valid current grant, receives explicit user-not-found results for
an unknown identity. Out-of-scope identities do not expose duplicate-user
results, and an orphan-verifier collision is visible only to `serverAdmin`.

Authorization runs before collection/index lookup, route resolution, cursor
creation, or mutation. Catalog lists are filtered, retained cursors are bound
to the durable account incarnation that created them, and grant revocation is
observed at the next command boundary. Dropping and recreating the same
`(authDB, username)` generates a new incarnation, revokes stale authenticated
connections, and prevents a new login from resuming an old cursor. Password
rotation preserves the incarnation, so already authenticated connections keep
their identity; verifier disable applies to subsequent authentication attempts.
The catalog rejects malformed
durable records on reopen and prevents disabling, demoting, or dropping the
last enabled server administrator. Routed/cluster protected commands reject
when the gateway lacks an authoritative resource binding.
User-management commands reject transaction markers, standalone `w:0`, and
OP_MSG `moreToCome` before durable mutation. Wire `createUser` publishes the
requested verifier and exact roles under one catalog lock and never invokes
trusted bootstrap escalation. A mixed `updateUser` rotates the verifier before
publishing roles: if the second durable write fails, the old credential is
invalid while the new credential retains the old grants. These multi-record
operations are fail-closed but not rollback-atomic. Growing versioned verifier
and grant records use TreeDB's persistent ValueLog with durable pointer
publication; it is independent of the redo WAL. A failed
or ambiguous grant publication invalidates the immutable authorization
snapshot so the next protected command reloads durable state or denies closed.
The version-2 verifier and grant payloads require a nonzero account incarnation;
legacy version-1 records fail closed and, in this pre-alpha format, require a
database rebuild or explicit offline repair rather than online migration.
`AuthorizationMetrics` reports only low-cardinality allowed/denied totals; it
does not expose secrets or query/document payloads.

## Explain contract

`queryPlanner` is selector-only and always reports `namespace`, `winningPlan`
(`stage`, optional `indexName`, `residualFilter`, and `inMemorySort` when
applicable), `usableIndexes`, `rejectedIndexes`, `scanBounds`, `sort`,
`maxScanDocuments`, and `cursorWork`. Scan bounds use field/operator/type,
cardinality, inclusivity, and fixed privacy-safe value fingerprints; literal
filter values and TreeDB storage addresses are never returned. `executionStats`
adds `nReturned`, gateway-owned `candidateDocumentsExamined`,
`candidateDocumentsMaterialized`, `cursorDocumentsMaterialized`, `scanCap`,
and `executionTimeMillis`. A capped execution returns the same planner plus
`truncated: true` and `rejectionReason: scan_cap_exceeded`; other fail-closed
execution errors retain the planner with `truncated: false` and a stable
rejection reason. These gateway-owned counters are not MongoDB
`totalDocsExamined` metrics and include bounded adaptive candidate work.
When `winningPlan.stage` is `adaptive_candidate_selection`, `candidatePlans`
is additionally present: each entry has gateway-owned `stage`, `indexName`,
and `field` values for an executable candidate probe. It is omitted for all
other planner stages.

## Differential compatibility fixtures

`cmd/mongo_gateway_compat_diff` compares a deliberately small set of declared
standalone shapes with a reference MongoDB. It is compatibility evidence for
those fixtures, not a full MongoDB conformance claim. Fixture files are
versioned canonical Extended JSON in
`cmd/mongo_gateway_compat_diff/fixtures`; the runner decodes them to BSON and
preserves BSON type and field order in emitted observations.

The explain fixture proves the cross-target wire invariant that a nested
`explain`/`find` command over the seeded collection is accepted and returns a
successful command envelope. It deliberately ignores `queryPlanner`: MongoDB's
planner document is implementation-specific and the gateway publishes its own
stable vocabulary, which is specified and tested by the gateway contract tests
rather than claimed as MongoDB planner parity.

The version-1 fixture contract is intentionally unauthenticated: it has one
shared seed/command flow and no per-target authentication bootstrap. Therefore
it does not claim differential evidence for SCRAM identities or authorization
policy. Adding an unauthenticated authorization fixture would be misleading,
while enabling authentication only on one target would turn setup differences
into false compatibility results. Authorization compatibility is instead
covered by the executable capability-matrix probe, official-driver tests, and
the role, mutation-boundary, list-filtering, cursor-ownership, persistence, and
cluster fail-closed package tests. A future authenticated differential contract
must bootstrap equivalent users and grants independently on both targets.

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
