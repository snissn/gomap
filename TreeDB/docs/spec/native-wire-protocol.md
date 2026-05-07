# TreeDB Native Wire Protocol v1

Status: draft proposal, non-normative.

TreeDB is pre-alpha. This document defines the target native network protocol
shape for TreeDB collections and raw ordered-key operations. It is intended to
guide implementation, benchmark work, and the future Raft/distributed database
surface. It does not describe current shipped server behavior.

Normative keywords are conformance requirements for code that advertises
native-wire v1. Until a phase lands, unmatched requirements are design
constraints. Any PR that changes frame layout, command IDs, section IDs,
deterministic encoding, benchmark labels, or observability keys MUST update this
document, the roadmap, relevant codec/golden tests, and benchmark documentation
in the same change.

## 1. Decision

TreeDB SHOULD use a native binary protocol for its production data plane.

OpenRPC, OpenAPI, Swagger, and JSON-RPC style descriptions are intentionally
not the canonical TreeDB network protocol. They can be useful for generated
admin clients or human-readable documentation, but their JSON object model,
field-name repetition, generic map decoding, HTTP request shape, and awkward
streaming/backpressure semantics are the wrong default costs for the TreeDB hot
path.

The native protocol MUST be designed so the same logical command schema can also
produce future deterministic Raft command entries. Transport fields, tracing,
deadlines, negotiated compression choices, and socket-level request identifiers
MUST NOT be part of the replicated command identity.

## 2. Goals

1. Provide a fast collection-native network protocol.
2. Keep command payloads vectorized for insert/get/query batches.
3. Avoid JSON/BSON command envelopes on the hot path.
4. Preserve TreeDB's existing collection document formats:
   - JSON document bytes,
   - native BSON document bytes,
   - template-v1 stored documents and template records.
5. Make durability and consistency semantics explicit per request.
6. Leave room for streaming reads, backpressure, cancellation, compression,
   authentication, and future cluster routing.
7. Define a clean boundary between client wire frames and future Raft log
   entries.

## 3. Non-Goals

The v1 protocol does not initially require:

1. MongoDB compatibility.
2. Redis/RESP compatibility.
3. OpenRPC or JSON-RPC compatibility.
4. Arbitrary SQL execution.
5. Cross-node distributed query execution.
6. Server-side WASM execution.
7. A stable public compatibility promise before TreeDB exits pre-alpha.

MongoDB and Redis compatibility layers MAY continue to exist as adapters, but
they MUST NOT define the native TreeDB protocol or future Raft log format.

## 4. Layering

The protocol has three layers:

1. **Transport frame:** connection-local framing, request IDs, stream IDs,
   frame type, and frame flags.
2. **Command payload:** versioned command IDs and typed sections.
3. **Deterministic command entry:** a canonical subset of the command payload
   suitable for future Raft replication.

Only layer 3 is eligible for Raft log storage. Layer 1 MUST be excluded from
Raft entries. Layer 2 MAY contain optional client/server convenience sections
that are also excluded from deterministic entries unless explicitly marked as
deterministic command input.

Protocol compatibility is based on explicit version and feature negotiation, not
best-effort decoding of unknown required fields. Unknown required frame flags,
section flags, command IDs, command versions, or critical sections MUST fail
fast with a structured error. Unknown advisory fields MAY be ignored.

## 5. Transport

The primary transports are:

- TCP,
- Unix domain sockets,
- in-process benchmark transport.

TLS and authentication are negotiated above the raw connection or through a
future authenticated transport wrapper. The frame format does not require HTTP.

### 5.1 Fixed Frame Header

All fixed-width integers are little-endian. Variable-length integers use unsigned
base-128 varints.

```text
offset  size  field
0       4     magic = "TDB1"
4       2     header_len
6       2     version_major
8       2     version_minor
10      2     frame_type
12      4     frame_flags
16      8     stream_id
24      8     request_id
32      8     body_len
```

The initial fixed header length is 40 bytes. `header_len` allows future fixed
header extensions. Receivers MUST reject frames whose `header_len` is smaller
than 40 or larger than their configured maximum fixed-header length.

The frame body begins at byte offset `header_len`. v1 senders MUST set
`header_len=40` unless a fixed-header extension has been negotiated. Receivers
that accept `header_len > 40` MUST skip bytes `[40, header_len)` before reading
the body. Unknown fixed-header extensions are invalid unless explicitly
negotiated.

`body_len` is the number of bytes following the header. Receivers MUST enforce a
negotiated maximum frame size.

`frame_flags` use the low 16 bits for required semantics and the high 16 bits
for advisory semantics. A receiver MUST reject a frame with an unknown required
flag bit. A receiver MAY ignore unknown advisory flag bits. Frame flags are
transport metadata and MUST NOT be copied into deterministic command entries
unless a command schema explicitly promotes the same logical meaning into a
deterministic command flag.

### 5.2 Frame Types

Initial frame types:

```text
1  hello
2  hello_ok
3  request
4  response
5  data
6  error
7  cancel
8  ping
9  pong
10 goaway
```

`request_id` identifies one client request on a connection. Servers MAY process
pipelined requests out of order unless the command explicitly requires ordering.
Clients MUST match responses by `request_id`.

`stream_id` identifies a logical stream or cursor. Non-streaming requests and
initial cursor-open requests use `stream_id=0`. For v1 cursors,
`cursor_next`/`cursor_close` requests name the server-assigned cursor ID in
`stream_id`, and responses MAY echo it. Negotiated push-streaming extensions MAY
use the same `request_id` with a non-zero `stream_id`.

v1 cursor delivery is pull-based. `open_scan` returns a server-assigned cursor
ID in `cursor_meta.stream_id` and MAY return an initial batch. Each
`cursor_next` or `cursor_close` is a new request with a new `request_id` and the
cursor ID in `stream_id`. The server returns at most one batch per
`cursor_next`. EOF, `cursor_close`, idle timeout, cancel, or any terminal cursor
error releases the cursor. The `data` frame type is reserved for a separately
negotiated push-streaming extension and MUST NOT be used for v1 cursors.

A `cancel` frame targets the `request_id` in its header. `stream_id=0` cancels
the whole request; non-zero `stream_id` cancels that cursor or stream. Cancel is
best-effort: clients MUST tolerate a successful response racing with cancel.
After observing cancel, servers SHOULD stop work promptly, release cursor state,
and return `canceled` if no terminal response has already been sent.

v1 read backpressure is expressed by `cursor_next` maximum item count,
`cursor_next` maximum response bytes, negotiated maximum frame size, and
negotiated maximum in-flight requests and cursors. Servers MUST NOT send
unbounded unsolicited frames.

`goaway` indicates that the peer will accept no new requests on the connection.
Its body MUST include `last_accepted_request_id` and an optional error code.
Clients MAY retry requests with IDs greater than `last_accepted_request_id`
when the command and idempotency policy allow retry.

Each request moves through frame decode, schema validation, admission, dispatch,
engine execution, response encode, and terminal cleanup. A request MUST emit at
most one terminal response or error. Cursor-open commands create server-owned
cursor state only after admission succeeds. EOF, `cursor_close`, cancel, idle
timeout, connection close, or terminal cursor error MUST release that state.
Mutation dispatch MUST resolve connection-local handles and catalog guards before
execution. Distributed mode MUST canonicalize and admit deterministic command
bytes before applying the mutation.

### 5.3 Body Sections

Frame bodies are a sequence of typed sections:

```text
section_id     uvarint
section_flags  uvarint
section_len    uvarint
section_bytes  bytes[section_len]
```

`section_flags & 1` marks a section as critical. Unknown critical sections MUST
fail the frame. Unknown non-critical sections MUST be ignored. Unknown
`section_flags` bits are required semantics and MUST fail the frame unless the
command schema or negotiated feature explicitly defines them.

Sections SHOULD be ordered by increasing `section_id`, but receivers MUST NOT
depend on order unless a command schema says a repeated section is ordered.
Deterministic command-entry encoders MUST sort deterministic sections by
`section_id` and MUST reject duplicate non-repeatable deterministic sections.

Each command schema MUST list required sections, optional sections, allowed
multiplicity, ordering significance, and whether each section is deterministic
command input. Duplicate singleton sections and missing required sections MUST
be rejected as `invalid_command`.

### 5.4 Common Sections

Initial common section IDs:

```text
1  command_header
2  error
3  capability_set
4  deadline
5  trace_context
6  ack_policy
7  consistency_policy
8  idempotency_key
9  checksum
10 compression
11 response_meta
12 cursor_meta
```

Command-specific sections start at `100`.

### 5.5 Byte Vectors

Batch-oriented fields use byte-vector encoding:

```text
count          uvarint
lengths[count] uvarint
payload_bytes  bytes[sum(lengths)]
```

This avoids per-item maps and keeps IDs, documents, keys, and values compact.
Implementations SHOULD parse byte vectors into offset tables or borrowed slices
instead of allocating one object per element.

Byte vectors do not encode nulls. Optional or missing results MUST use a
separate presence bitmap or status vector whose item count exactly matches the
byte-vector count required by the command. Decoders MUST reject length
overflows, truncated payloads, extra bytes, and byte vectors whose declared
lengths do not sum to the remaining payload size.

### 5.6 Response Metadata

Successful responses SHOULD include `response_meta` when the command touches
durability, consistency, catalog state, or cluster routing. Response metadata is
not deterministic command input.

Initial response metadata fields:

```text
actual_ack_policy        uvarint optional
actual_consistency       uvarint optional
commit_seq              uvarint optional
catalog_version         uvarint optional
applied_log_index       uvarint optional
serving_node_id         bytes optional
leader_node_id          bytes optional
```

Single-node implementations may omit cluster fields. Cluster implementations
SHOULD report `applied_log_index` and serving/leader node identity for reads
whose freshness matters.

## 6. Handshake

The client MUST send `hello` before ordinary requests. The server replies with
`hello_ok` or `error`.

`hello` advertises:

- client protocol versions,
- maximum frame size,
- supported compression codecs,
- supported document formats,
- desired authentication mode,
- optional client name and driver version.

`hello_ok` returns:

- selected protocol version,
- server maximum frame size,
- selected compression policy,
- supported command IDs,
- supported document formats,
- durability/consistency modes,
- server feature bits.

Feature negotiation MUST be explicit. A client MUST NOT assume support for a
command, document format, compression codec, query operator, or consistency mode
that the server did not advertise.

A connection has exactly one selected transport protocol version. `hello` and
`hello_ok` for this specification use header version 1.0. After `hello_ok`, all
frames on the connection MUST carry the selected version. Receivers MUST reject
ordinary frames with a different major version and MUST reject an unnegotiated
minor version. Major versions are incompatible. Minor versions are additive only
when explicitly negotiated.

## 7. Command Header

Every `request` frame has a `command_header` section:

```text
command_id       uvarint
command_version  uvarint
command_flags    uvarint
```

Command versions are per-command schema versions. A server MAY support multiple
versions of the same command during pre-alpha development, but the selected
schema MUST be explicit in every request.

Once a `command_id + command_version` is admitted to a deterministic command
entry, its deterministic semantics are immutable for that Raft log lineage.
Section IDs MUST NOT be reused with different meanings. Semantic changes,
canonical encoding changes, or changed default behavior require a new
`command_version` or `entry_version`. Mixed-version clusters MUST only advertise
command versions that every voting replica can decode and apply
deterministically.

`command_flags` are command payload metadata, not transport metadata. Flags that
affect mutation semantics MUST be part of deterministic command-entry encoding.
Flags that only affect response shaping, tracing, or pagination MUST NOT be
replicated.

## 8. Document Formats

Document format codes:

```text
0 default
1 json
2 bson
3 template_v1
```

The native protocol carries stored document bytes without wrapping them in a
JSON command object.

For BSON collections, the server SHOULD validate BSON once while decoding the
request and then call the trusted BSON insertion path.

For template-v1 collections, the protocol MUST support both:

- current self-contained `TD1I` per-document insert envelopes,
- a batch-level template-record section that lets clients send each template
  record once per batch.

The batch-level template section is preferred for native clients because it
matches the collection root model: template records, primary documents,
index-state, and secondary-index postings are one logical collection mutation
group.

Deterministic entries replicate logical mutation input: collection identity,
document IDs, stored document bytes, template records when required, command
flags, and catalog guards. Secondary-index postings, index-state deltas, backend
root IDs, value-log offsets, flush artifacts, and other node-local derived
storage state MUST be regenerated by the state machine and MUST NOT be
replicated unless a future command version explicitly freezes that
representation.

## 9. Collection References and Metadata

`collection_ref` supports two forms:

```text
1 collection_name
2 connection_local_handle
```

Collection names are stable command input. Connection-local handles are a
transport optimization returned by `open_collection`; they are never valid
outside the connection that received them and MUST NOT appear in deterministic
command entries.

Collection metadata commands SHOULD use the same logical fields as the current
`CollectionMeta` and `IndexDefinition` model:

```text
collection_name
document_format
allow_array_values_in_index
data_root_storage_policy
index_state_storage_policy
buffered_indexed_write_policy
index_name
index_field
index_value_type
index_unique
index_multi_key
index_storage_policy
```

Metadata mutation responses SHOULD include the resulting catalog version or
equivalent schema guard. Later mutation requests MAY use
`expected_catalog_version` to fail fast when a client planned against stale
metadata.

For distributed mode, collection and index mutation entries MUST include a
deterministic catalog guard: either `expected_catalog_version` or an equivalent
catalog epoch plus stable collection/index IDs resolved from committed metadata.
Connection-local handles and unguarded client names MUST be resolved before
append and MUST NOT be stored in the deterministic entry.

## 10. Initial Command Set

The v1 implementation SHOULD start with the smallest surface that can replace
the current Mongo raw-wire benchmark path for native clients.

### 10.1 Control Commands

```text
10 create_collection
11 list_collections
12 create_index
13 list_indexes
14 drop_index
15 open_collection
16 close_collection
17 drop_collection
```

`open_collection` MAY return a connection-local collection handle. Handles are
an optimization only. The deterministic command entry MUST use stable collection
names or stable collection IDs from committed metadata, not connection-local
handles.

### 10.2 Mutation Commands

```text
30 insert_batch
31 replace_batch
32 delete_batch
33 flush_collection
34 flush_all
35 checkpoint
```

`insert_batch`, `replace_batch`, and `delete_batch` are logical mutation
commands. `flush_collection`, `flush_all`, and `checkpoint` are local durability
barriers in v1 and MUST NOT be replicated as Raft state-machine commands unless
a future distributed barrier command gives them explicit consensus semantics.

`insert_batch` sections:

```text
100 collection_ref
101 document_format
102 document_ids byte_vector
103 documents byte_vector
104 template_records optional byte_vector
105 expected_catalog_version optional
```

`replace_batch` sections:

```text
100 collection_ref
101 document_format
102 document_ids byte_vector
103 documents byte_vector
104 template_records optional byte_vector
105 expected_catalog_version optional
106 replacement_mode
```

`delete_batch` sections:

```text
100 collection_ref
102 document_ids byte_vector
105 expected_catalog_version optional
```

Duplicate IDs in one mutation batch MUST be rejected unless a future command
schema explicitly defines ordered same-ID semantics.

In distributed mode, mutation entries MUST carry either `idempotency_key` or
`client_id + client_sequence`. The identity is scoped to the cluster/database
and is part of state-machine deduplication state. Reuse of the same identity
with the same canonical command digest MUST return the prior outcome. Reuse with
a different digest MUST fail with `idempotency_conflict`. Transport `request_id`
MUST NOT participate in this identity.

Mutation responses SHOULD include:

```text
matched_count optional
modified_count optional
inserted_count optional
deleted_count optional
result_ids optional byte_vector
per_item_status optional status_vector
response_meta optional
```

`per_item_status` is required when a command can partially classify items while
still returning an overall command error or partial-success result in a later
schema. The initial v1 mutation commands SHOULD remain all-or-nothing unless a
command version explicitly defines partial success.

### 10.3 Read Commands

```text
50 get_many
51 index_lookup
52 index_range
53 open_scan
54 cursor_next
55 cursor_close
56 explain
57 stats
```

`get_many` returns results in request order. Missing documents are represented
with an explicit presence bitmap or result-status vector; missing values MUST
not be confused with present empty documents.

`index_lookup` is equality lookup over one typed secondary index value.

`index_range` is a bounded range over one typed secondary index. Bounds are
encoded as typed scalar sections, not as JSON filter objects.

`open_scan` creates a cursor over a primary or secondary ordered range. Cursors
MUST have server-side byte, document-count, and idle-time limits.

Read responses SHOULD use:

```text
presence_bitmap optional
document_ids optional byte_vector
documents optional byte_vector
values optional byte_vector
truncated optional bool
cursor_meta optional
response_meta optional
```

For `get_many`, result order MUST match request key order. For index and scan
commands, result order MUST match the ordered root or index order selected by
the command.

`cursor_meta` SHOULD include:

```text
stream_id
batch_items
batch_bytes
has_more
server_cursor_deadline optional
```

`cursor_next` requests MUST include a maximum item count, maximum response bytes,
or both. Servers MAY return fewer results than requested. A response with
`has_more=false` is terminal and releases the cursor. A `cursor_not_found` error
is terminal for the named cursor but not for the connection.

## 11. Typed Scalars

Index and query scalar codes:

```text
1 string
2 bool
3 int64
4 double
5 bytes
6 null
```

Index query commands MUST use the index definition's declared value type. The
wire scalar encoding is logical. The internal secondary-index key encoding
remains an implementation detail unless a future server-to-server command
explicitly freezes it.

## 12. Durability and Consistency Policies

The `ack_policy` section requests the minimum acknowledgement boundary for a
mutation:

```text
1 visible
2 flushed
3 synced
4 raft_committed
```

`visible` means the mutation is visible to reads through the serving process or
owning write domain. It is not necessarily crash-durable.

`flushed` means collection write-domain state has been published to backend
roots for that collection or all touched roots.

`synced` means the operation reached the strongest local durability boundary
available under the server's configured TreeDB durability mode.

`raft_committed` is reserved for distributed mode. It means the command has been
committed by consensus and applied according to the cluster's state-machine
policy.

If `ack_policy` is absent, the server uses its advertised default. The initial
single-node native server SHOULD default to `visible` to match current
collection API behavior, but clients that need a durability boundary SHOULD ask
for `flushed` or `synced` explicitly.

A server MUST either satisfy the requested minimum policy or fail the request.
It MUST NOT silently downgrade `synced` to a relaxed or checkpoint-only
guarantee. Successful responses SHOULD report `actual_ack_policy`.

Read `consistency_policy` values:

```text
1 local_stale
2 leader_read
3 linearizable
4 lease_read
```

Single-node servers MAY treat `leader_read`, `linearizable`, and `lease_read` as
equivalent when no cluster is configured, but they MUST report the actual mode in
the response.

## 13. Error Model

Errors use stable numeric codes plus machine-readable fields:

```text
code        uvarint
retryable   bool
message     string
detail_kv   repeated string/string or typed values
```

Initial error classes:

```text
1 malformed_frame
2 unsupported_version
3 unsupported_feature
4 auth_required
5 permission_denied
6 invalid_command
7 collection_not_found
8 index_not_found
9 duplicate_document_id
10 document_exists
11 unique_index_conflict
12 catalog_version_mismatch
13 read_only
14 timeout
15 canceled
16 resource_exhausted
17 internal
18 durability_unavailable
19 consistency_unavailable
20 cursor_not_found
21 catalog_changed
22 idempotency_conflict
```

Errors that map to current collection duplicate-key conditions SHOULD preserve a
stable duplicate-key class so clients can handle inserts and unique-index
conflicts uniformly.

Error frames MUST carry the target `request_id` when the error is
request-scoped and the target `stream_id` when stream-scoped. A request-scoped
error is terminal for that request. A stream-scoped error is terminal for that
stream or cursor.

`malformed_frame`, invalid header length, frame-size violations, and unsupported
post-handshake versions are connection-fatal. The server MAY send `goaway` or an
`error` frame before closing when safe.

## 14. Deterministic Command Entry v1

Future Raft log entries SHOULD use a deterministic command-entry envelope:

```text
entry_magic = "TDC1"
entry_version
command_id
command_version
client_id optional
client_sequence optional
deterministic_sections
```

Deterministic entries MUST use one canonical encoding:

- little-endian fixed-width integers,
- minimal unsigned base-128 varints for variable integers,
- sections sorted by `section_id`,
- command-defined ordering for repeated sections,
- no duplicate non-repeatable sections,
- exact bytes for document IDs, document payloads, template records, and scalar
  query values,
- no map iteration order.

Unknown, ignored, transport-only, or non-critical convenience sections MUST NOT
be copied into the deterministic entry. Optional fields MUST either be omitted
or encoded with explicit defaults according to the command schema; both forms
MUST NOT be accepted for the same `command_version`.

The deterministic Raft command set is an allowlist. Logical metadata mutations
and logical collection mutations MAY be replicated. Reads, cursors, explain,
stats, open/close handles, `ack_policy`, `consistency_policy`, deadlines,
cancellation, checksums, compression framing, flush, checkpoint, and other local
durability or response-shaping controls MUST NOT be replicated as command
identity. If a distributed barrier is needed, define it as a separate
deterministic no-op or barrier command with explicit semantics.

The following MUST NOT be included in deterministic command entries:

- connection-local request IDs,
- stream IDs,
- transport deadlines,
- trace context,
- negotiated compression choices,
- non-deterministic server timestamps,
- response-shaping hints that do not change state.

The following SHOULD be included when present:

- collection metadata mutation input,
- collection mutation IDs and document bytes,
- expected catalog version or equivalent conflict guard,
- idempotency key or `client_id + client_sequence`,
- deterministic command flags.

In distributed mode, mutating entries MUST include the idempotency identity and
catalog guard rules described above. Clients may omit those sections for a
single-node pre-alpha server only when the advertised command schema permits it.

Commands that are read-only, cursor-only, or response-shaping only SHOULD NOT be
encoded as Raft entries. Write-producing future features, such as server-side
callbacks, MUST either log their fully expanded deterministic mutation batch or
define a deterministic replay envelope with module hash and all deterministic
inputs needed to reproduce the same writes.

Raft implementations MAY store deterministic entries directly or wrap them in a
larger consensus-layer entry that also carries term/index metadata.

## 15. Implementation Conformance

Implementation guidance is expanded in
`TreeDB/docs/spec/native-wire-implementation-guidelines.md`. The rules below are
part of the protocol contract for code that advertises native-wire v1.

### 15.1 Append-Time Determinism Gate

In distributed mode, the leader MUST pass every mutating request through an
append-time determinism gate before Raft append:

1. decode the wire request using the negotiated command schema,
2. reject unsupported command versions, unknown required sections, duplicate
   singleton sections, and non-deterministic sections,
3. resolve connection-local handles and client names to committed stable catalog
   identities,
4. verify that the command is in the deterministic command allowlist,
5. require an idempotency identity and catalog guard,
6. encode the command with the canonical command-entry encoder,
7. compute the canonical command digest over the exact entry bytes,
8. append only those canonical bytes to Raft.

Followers MUST apply committed command-entry bytes directly. They MUST NOT
reconstruct entries from wire frames, negotiated connection features, local
defaults, map iteration order, timestamps, handles, or transport metadata.

### 15.2 Canonical Encoder Registry

Each replicated `command_id + command_version` MUST have exactly one canonical
encoder and decoder. Generic section copying is not sufficient for Raft entry
construction.

The command schema MUST define deterministic sections, field order inside each
section, optional-field defaults, repeated-section ordering, scalar encoding,
and rejection rules. A command version MUST NOT accept both omitted and
explicit-default encodings for the same logical value.

Canonical encoders MUST reject inputs that cannot be represented with one stable
byte form. Any future replicated scalar that admits multiple encodings, such as
floating NaN values, MUST either define a canonical representation or be
rejected.

### 15.3 Catalog Guards and Stable IDs

Catalog guards are evaluated by the state machine at apply time. Leader-side
validation is only a preflight optimization. A committed command whose guard
does not match the applied catalog state MUST produce the same deterministic
failure on every replica and MUST still update idempotency state for that
command identity.

Create/drop collection and create/drop index commands MUST encode deterministic
existence guards, stable names, and any stable IDs assigned by committed catalog
state. Replicas MUST NOT allocate catalog IDs from local randomness, wall-clock
time, process-local counters outside the log, or map iteration order.

### 15.4 State-Machine Boundary

Raft entries replicate logical state-machine input only. Applying an entry may
derive secondary-index postings, index-state deltas, value-log writes, backend
roots, flush artifacts, and physical file layout locally. Those derived
artifacts MUST NOT influence later logical command results except through
committed logical state.

The apply path MUST NOT depend on wall-clock time, process-global randomness,
goroutine scheduling, map iteration order, local value-log offsets, local flush
timing, or local maintenance decisions. Snapshots MUST include logical catalog
state, collection contents, index definitions, and idempotency records; they
need not preserve byte-identical local storage layout.

### 15.5 Local-Only Commands

`open_collection`, `close_collection`, cursors, reads, `explain`, `stats`,
`flush_collection`, `flush_all`, `checkpoint`, value-log GC, value-log rewrite,
and physical maintenance commands are local-only in v1.

Cluster servers MUST NOT satisfy `ack_policy=raft_committed` by appending these
commands to Raft. They MUST either execute them as local operations with
response metadata naming the serving node, reject them as `invalid_command`, or
define a future deterministic distributed barrier command with explicit
consensus semantics.

### 15.6 Mixed-Version Cluster Admission

In cluster mode, `hello_ok` MUST distinguish locally implemented command
versions from cluster-admitted command versions. A leader MUST only admit
deterministic entries whose `entry_version`, `command_id`, `command_version`,
and required feature bits can be decoded and applied by every current voting
replica for the Raft group.

Rolling upgrades MUST keep new deterministic command versions disabled until the
membership and feature floor prove that all voting replicas can replay them. A
node that cannot decode an already committed entry version MUST refuse to join
as a voting replica for that log lineage.

## 16. Benchmark and Observability Requirements

The first implementation MUST add native client modes beside the existing Mongo
gateway client modes:

```text
native-wire-tcp
native-wire-inproc
```

Benchmark reports MUST label native-wire results separately from:

- direct in-process collection calls,
- Mongo driver paths,
- Mongo raw-wire paths.

Native-wire benchmarks SHOULD include:

- insert load throughput,
- get/get-many throughput,
- equality/range index lookup throughput,
- cursor scan throughput,
- allocation profile,
- per-frame byte overhead,
- server encode/decode/dispatch overhead,
- frames and bytes in/out,
- request count and item count,
- cursor count,
- error and cancellation totals.

Native-wire benchmark PRs MUST update `cmd/unified_bench` labels and tests before
publishing native-wire results. Native-wire TCP and in-process results MUST NOT
be reported under `native-fastpath`.

The native server SHOULD expose stable `treedb.native_wire.*` stats through the
same stats path consumed by `unified-bench` and `benchprof`. Minimum counters are
connections opened/closed, frames in/out, bytes in/out, malformed frames,
requests started/completed/failed/canceled/timed out, in-flight requests, open
cursors, cursor closes/timeouts, per-command request/error counts, and
encode/decode/dispatch nanoseconds.

## 17. Open Questions

1. Whether v1 should support raw ordered-KV commands alongside collection
   commands, or keep raw-KV access as a later capability.
2. Whether connection-local collection handles are worth the complexity in v1.
3. Which compression codecs should be allowed on the wire initially.
4. How authentication and authorization should map onto collections, indexes,
   and future cluster routing.
5. Whether `visible` should remain the standalone default once native clients
   move beyond benchmark and compatibility work.
