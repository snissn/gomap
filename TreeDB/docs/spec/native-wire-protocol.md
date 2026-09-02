# TreeDB Native Wire Protocol v1

Status: normative for code that advertises native-wire v1. Distributed/Raft
behavior remains target/non-normative until cluster mode lands.

TreeDB is pre-alpha. This document defines the target native network protocol
shape for TreeDB collections and raw ordered-key operations. It is intended to
guide implementation, benchmark work, and the future Raft/distributed database
surface. Sections marked distributed or Raft target do not describe current
single-node server behavior.

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

User-command WAL is local crash-recovery durability state and is not a Raft log
entry. Native-wire deterministic command entries describe logical mutations;
each node that applies such a command must still satisfy the local
user-command-WAL recoverability, root-publication, and applied-LSN rule before
reporting local or Raft-backed success for collection mutations. Local command
WAL payloads should reuse or wrap these deterministic command-entry schemas so
the single-node WAL and future Raft paths do not grow separate mutation
encoders.

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

Primitive payload encodings:

- `uvarint`: unsigned base-128, least-significant 7-bit group first, high bit set
  on every non-final byte. Senders MUST use the shortest encoding; receivers
  MUST reject encodings that overflow `uint64` or exceed 10 bytes.
- `varint`: signed `int64` encoded by zig-zag mapping to `uvarint`
  (`n >= 0 => n*2`, `n < 0 => (-n*2)-1`).
- `bool`: one byte, `0=false`, `1=true`; all other values are malformed.
- `string`: `uvarint` byte length followed by UTF-8 bytes. Command-specific
  validators may impose a narrower grammar, such as collection or index names.
- `bytes`: `uvarint` byte length followed by exactly that many opaque bytes,
  except where a containing structure already supplies an explicit length.
- Optional fields are represented by section or field presence, not by an
  implicit null encoding. A section schema that contains optional scalar fields
  MUST define an explicit presence mechanism, such as a field-count prefix,
  fixed presence bitmap, or command-specific sentinel, and MUST define the
  default value used when the field is absent.

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
`cursor_next`/`cursor_close` requests MUST carry the server-assigned cursor ID
in a `cursor_ref` section. Clients SHOULD also set `stream_id` to the same
cursor ID for transport observability; servers MUST reject a request where a
non-zero `stream_id` disagrees with `cursor_ref`. Responses MAY echo the cursor
ID in `stream_id`. Negotiated push-streaming extensions MAY use the same
`request_id` with a non-zero `stream_id`.

v1 cursor delivery is pull-based. `open_scan` returns a server-assigned cursor
ID in `cursor_meta.stream_id` and MAY return an initial batch. Each
`cursor_next` or `cursor_close` is a new request with a new `request_id`, a
`cursor_ref` body section, and normally the cursor ID in `stream_id`. The server
returns at most one batch per `cursor_next`. EOF, `cursor_close`, idle timeout,
cancel, or any terminal cursor error releases the cursor. The `data` frame type
is reserved for a separately negotiated push-streaming extension and MUST NOT be
used for v1 cursors.

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
Its body is a `response_meta` section encoded as a string map. The map MUST
include `last_accepted_request_id` encoded as an unsigned decimal string and MAY
include `error_code` encoded as an unsigned decimal string plus an optional
human-readable `message`. Clients MAY retry requests with IDs greater than
`last_accepted_request_id` when the command and idempotency policy allow retry.

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

`ack_policy`, `consistency_policy`, deadlines, tracing, compression, and
response-shaping sections are common request/transport policy. They are not
deterministic command input. Schema registries must mark `ack_policy`
non-deterministic for every command version, and deterministic-entry encoders
must strip it before canonical entry construction.

Command-specific sections start at `100`.

Initial command-specific section IDs used by v1 commands and responses:

```text
100 collection_ref
101 document_format
102 document_ids              byte_vector
103 documents                 byte_vector
104 template_records optional byte_vector
105 expected_catalog_version
106 replacement_mode
107 collection_meta
108 index_definition
109 index_name
110 collection_handle
111 index_value
112 index_lower_bound
113 index_upper_bound
114 cursor_ref                uvarint cursor_id
115 cursor_limits             uvarint max_items, uvarint max_bytes
116 presence_bitmap           bitset, least-significant bit first
117 truncated                 bool
118 status_vector             byte_vector of per-item status records, reserved
119 catalog_guard             CatalogGuardV1
120 existence_guard           ExistenceGuardV1
121 update_field_names        byte_vector
122 update_field_values       byte_vector of BSON typed raw values
```

`cursor_limits` encodes both fields. A zero `max_items` or `max_bytes` means
"no client-specified limit" for that dimension; servers still enforce
negotiated and configured limits. `presence_bitmap` has exactly
`ceil(result_count/8)` bytes, where bit `i` says whether result `i` is present.
`status_vector` is reserved for a future command version that defines partial
success records; v1 all-or-nothing commands MUST NOT emit it.

`update_field_names` and `update_field_values` are used by `update_bson_set`.
They MUST have identical item counts and at least one item. `update_field_names`
items are UTF-8 field-name bytes. v1 `update_bson_set` supports top-level BSON
field names only: names MUST NOT be empty, `_id`, contain `.`, contain NUL, or
start with `$`. Each `update_field_values` item is one byte of BSON type followed
by that type's raw BSON value bytes, matching `bson.RawValue{Type, Value}`.

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
byte-vector count required by the command. After a decoder reads `count` and
then exactly `count` length varints, the remaining bytes in that same
byte-vector payload are `payload_bytes`. Decoders MUST reject length overflows,
truncated payloads, extra bytes, and any byte vector where `sum(lengths) !=
len(payload_bytes)`.

### 5.6 Response Metadata

Successful responses SHOULD include `response_meta` when the command touches
durability, consistency, catalog state, or cluster routing. Response metadata is
not deterministic command input.

Initial response metadata fields:

```text
actual_ack_policy        uvarint optional
actual_consistency       uvarint optional
commit_state             uvarint optional
durability_mode          uvarint optional
commit_seq              uvarint optional
catalog_version         uvarint optional
applied_log_index       uvarint optional
serving_node_id         bytes optional
leader_node_id          bytes optional
```

`commit_state` values:

```text
0 unknown
1 not_committed
2 committed_recoverable
3 committed_or_unknown_after_commit
```

`durability_mode` values:

```text
1 durable
2 wal_on_relaxed
3 wal_off_relaxed
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

Initial response-shaping flags:

```text
bit 0 omit_result_ids
bit 1 omit_response_meta
```

`omit_result_ids` asks successful mutation responses to omit result ID vectors
when the command can otherwise report success through `response_meta`. Servers
MAY still return result IDs to older clients or for commands that require them;
clients that set this flag MUST NOT depend on IDs being present.

`omit_response_meta` asks successful responses to omit advisory response
metadata when the client only needs success/error signaling. Servers MUST still
satisfy the requested ack/consistency policy before returning success; this flag
only shapes the success response body and MUST NOT change command semantics.

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

Collection names are request input and API/display text. They are not replay
identity for replicated mutating commands. Connection-local handles are a
transport optimization returned by `open_collection`; they are never valid
outside the connection that received them and MUST NOT appear in deterministic
command entries. Before a mutating command is appended as a deterministic entry,
names and handles must be resolved to `CatalogGuardV1` stable IDs.

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
vector_indexes
quantized_indexes
scalar_u8_calibration
```

The current nativewire collection metadata frame version is `5`; version `5`
includes persisted scalar_u8 calibration semantics for quantized vector indexes.
Metadata mutation responses SHOULD include the resulting catalog version or
equivalent schema guard. Later single-node mutation requests MAY use
`expected_catalog_version` to fail fast when a client planned against stale
metadata. Replicated deterministic mutation entries MUST use `CatalogGuardV1`.

For distributed mode, collection and index mutation entries MUST include a
deterministic catalog guard with stable IDs resolved from committed metadata.
Connection-local handles and unguarded client names MUST be resolved before
append and MUST NOT be stored in the deterministic entry.

Canonical guard sections:

```text
CatalogGuardV1 {
    CollectionUID           uuid128
    CollectionGeneration    uint64
    SchemaEpoch             uint64
    LogicalCatalogDigest    bytes32
    ExpectedName            string optional diagnostic/existence guard
    IndexGuards             repeated {
        IndexUID            uuid128
        IndexGeneration     uint64
        DefinitionDigest    bytes32
    }
}

ExistenceGuardV1 {
    TargetKind              collection | index
    ExpectedState           absent | present
    StableName              string
    ExistingUID             uuid128 optional
    AssignedUID             uuid128 optional for create
    CatalogEpoch            uint64
}
```

Metadata commands must include deterministic existence guards and stable
assigned IDs when they are encoded as replicated commands. Guard mismatch is a
deterministic state-machine result and must update idempotency state with the
failure outcome.

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
36 update_bson_set
```

`insert_batch`, `replace_batch`, `delete_batch`, and `update_bson_set` are
logical mutation commands. `flush_collection`, `flush_all`, and `checkpoint` are
local durability barriers in v1 and MUST NOT be replicated as Raft state-machine
commands unless a future distributed barrier command gives them explicit
consensus semantics.
An `ack_policy` common section may be present on mutation requests, but it is
request/response policy only and is not part of deterministic command identity.

`insert_batch` sections:

```text
100 collection_ref
101 document_format
102 document_ids byte_vector
103 documents byte_vector
104 template_records optional byte_vector
105 expected_catalog_version optional
119 catalog_guard required for distributed mode
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
119 catalog_guard required for distributed mode
```

`delete_batch` sections:

```text
100 collection_ref
102 document_ids byte_vector
105 expected_catalog_version optional
119 catalog_guard required for distributed mode
```

`update_bson_set` sections:

```text
100 collection_ref
102 document_ids byte_vector, exactly one item
105 expected_catalog_version optional
121 update_field_names byte_vector
122 update_field_values byte_vector
119 catalog_guard required for distributed mode
```

`update_bson_set` applies one or more top-level BSON `$set` field assignments
to exactly one BSON document. Missing IDs return `matched_count=0` and
`modified_count=0`; unchanged values return `matched_count=1` and
`modified_count=0`.

Duplicate IDs in one mutation batch MUST be rejected unless a future command
schema explicitly defines ordered same-ID semantics.

In distributed mode, mutation entries MUST carry either `idempotency_key` or
`client_id + client_sequence`. The identity is scoped to the cluster/database
and is part of state-machine deduplication state. Reuse of the same identity
with the same canonical command digest MUST return the prior outcome. Reuse with
a different digest MUST fail with `idempotency_conflict`. Transport `request_id`
MUST NOT participate in this identity.

Single-node mutation retries are not exactly-once unless the server persists a
durable idempotency record with the logical mutation outcome. If a client times
out or disconnects after commit but before response, retry may observe duplicate
document IDs or unique-index conflicts from the prior commit. Non-idempotent
updates must be guarded by application-level version/compare predicates or a
durable idempotency key before clients can treat blind retry as safe.

Mutation responses SHOULD include:

```text
matched_count optional
modified_count optional
inserted_count optional
deleted_count optional
result_ids optional byte_vector, omitted when omit_result_ids is honored
per_item_status optional status_vector
response_meta optional, omitted when omit_response_meta is honored
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

`cursor_next` requests MUST include `cursor_ref` and `cursor_limits` sections.
The `cursor_limits` section MUST include a maximum item count, maximum response
bytes, or both. `cursor_close` requests MUST include `cursor_ref`. Servers MAY
return fewer results than requested. A response with
`has_more=false` is terminal and releases the cursor. A `cursor_not_found` error
is terminal for the named cursor but not for the connection.

### 10.4 Vector Search Commands

```text
58 vector_status
59 vector_search_strict
60 vector_search_fast
61 vector_pin_search_snapshot
62 vector_search_pinned
63 vector_close_pinned_snapshot
```

These are connection-local read commands and never deterministic mutation or
command-WAL entries. Every command requires the generic `deadline` section and
uses the existing native-wire framing, bounded operation context,
connection limits, cancellation, and error frames. Their direct binary sections
are:

```text
123 vector_search_request
124 vector_fast_options
125 vector_pin_options
126 vector_search_response
127 vector_fast_evidence
128 vector_status
```

The request carries float32 query values, generation, metric, search budgets,
limits, and deadline directly; the response carries ordered document IDs,
float32 scores, counters, and stage timings directly. JSON, reflection, generic
maps, and string-form float conversion are not part of this route. One
`vector_pin_search_snapshot` is owned by its connection and MUST be released by
the close command or by connection teardown. Strict, fast, and pinned searches
retain their public `vectorpartition.OperationsV1` consistency and validation
semantics; native wire changes only the transport representation.

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
owning write domain. For V1 WAL-on collection modes this also requires local
command-WAL recoverability: the command frame and required external refs are
recoverable, and the normal executor has installed the mutation in the
process-visible write domain. It does not require root publication or
`AppliedLSN` advancement. For WAL-off relaxed mode this is process-local
visibility only.

`flushed` means all touched collection state for the command has been published
to backend roots, and WAL-backed commands have `AppliedLSN` advanced in the same
backend commit.

`synced` means `flushed` plus an fsync-capable local durability boundary. A
server running a mode that cannot provide fsync for the touched state MUST fail
with `durability_unavailable`; it must not reinterpret `synced` as a relaxed
checkpoint.

`raft_committed` is reserved for distributed mode. It means consensus commit
plus the cluster-defined local apply/recoverability rule. It is not local WAL
append and is not part of deterministic command identity.

If `ack_policy` is absent, the server uses its advertised default. The initial
single-node native server SHOULD default to `visible`, but clients that need a
publication or sync boundary SHOULD ask for `flushed` or `synced` explicitly.

A server MUST either satisfy the requested minimum policy or fail the request.
It MUST NOT silently downgrade `synced` to a relaxed or checkpoint-only
guarantee. Successful responses SHOULD report `actual_ack_policy`.
Local policies are ordered only within the local family:
`visible < flushed < synced`. `raft_committed` is a named cluster policy and
must not be treated as a numeric extension of local durability ordering unless a
future cluster spec explicitly defines the implied local durability level.

If validation succeeds but required external-ref preparation/protection or
command-WAL append fails before a complete command frame becomes recoverable, the
server returns `durability_unavailable`, `retryable=true`,
`commit_state=not_committed`, and the mutation must not be visible.

If a complete command frame reached the required local boundary but root
publication, `AppliedLSN` advancement, visible install, flush, checkpoint, or
response construction fails, the server returns `commit_ambiguous`,
`retryable=false` unless a durable idempotency record makes replay safe, and
`commit_state=committed_or_unknown_after_commit`. The server must not report
`not_committed` after a complete command frame may be recovered and replayed.

If a command requested `ack_policy=flushed` or `ack_policy=synced` and the
logical mutation committed but the requested barrier failed, the error must
still expose the post-commit state. It must not look like an ordinary mutation
rejection.

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

The current token/ring cluster implementation recognizes `get_many` with
exactly one document ID and maps it with `DocumentIDTokenV1` to one catalog
owner. The public nativewire read then fails closed with route class
`owner_store_unbound` before invoking a read coordinator or observing the local
collection. A catalog owner and read-index proof do not establish that the
serving `CollectionManager` is the exact applied store for that owner; no
production Raft/store identity binding currently exists.

`GroupRoutedReadIndexCoordinator` is internal downstream scaffolding only. It
can validate owner selection and read-index-before-apply ordering, but it does
not bind collection-store identity and cannot enable public reads. Therefore
there is no enabled-path latency claim or benchmark for token/ring reads.
Multi-ID reads, non-route-key queries, scans, secondary/unique-index reads, and
cross-shard reads also fail closed; no scatter, follower-read, lease-read,
global ordering, or global unique coordination is implied.

All token/ring document mutations fail closed until authoritative collection
and index metadata is structurally bound to the exact owner route proof.
Gateway-local collection metadata is not authoritative for a remote owner,
including when it reports no indexes. Routed `list_collections`,
`list_indexes`, and `open_collection` requests also fail closed instead of
returning gateway-local metadata. Collection-placement mutations remain
supported. Rejected public read routes expose only request/error/unsupported
and `owner_store_unbound` counters under
`treedb.native_wire.cluster_read_route.*`; no success, read-index, leader, or
follower-path counter is emitted for this disabled path.

## 13. Error Model

The v1 `error` section uses stable numeric codes plus a retry hint and a
human-readable message:

```text
code       uvarint
retryable  bool
message    string
```

The `string` encoding is the base protocol string encoding from section 3.
Machine-readable request or stream metadata MAY be carried in a sibling
`response_meta` string map. Typed error detail values are not frozen in v1; a
future command or negotiated feature must define a separate critical detail
section before typed details are emitted.

Default error messages and metadata must be redacted. They must not include raw
documents, raw user keys, raw document IDs, raw collection names, raw index
names, raw root names, tenant-sensitive path components, or absolute host paths.
Use stable error codes, counts, sizes, collection UIDs, file IDs, offsets,
checksums, and keyed hashes by default. Raw diagnostic values require an
explicit local admin/debug mode outside the default wire response.

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
23 commit_ambiguous
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

Future Raft log entries SHOULD use a deterministic command-entry envelope. The
R2 implementation includes a v1 encoder/decoder, and compatible implementations
MUST follow the byte layout below.

```text
entry_magic[4] = "TDC1"
entry_version = uvarint(1)
command_id = uvarint
command_version = uvarint
command_flags = uvarint
section_count = uvarint
repeat section_count:
  section_id = uvarint
  section_len = uvarint
  section_payload[section_len]
```

Deterministic entries MUST use one canonical encoding:

- minimal unsigned base-128 varints for variable integers,
- sections sorted by `section_id`,
- command-defined ordering for repeated sections,
- no duplicate non-repeatable sections,
- exact bytes for document IDs, document payloads, template records, and scalar
  query values,
- no map iteration order.

The v1 envelope itself carries only deterministic command flags. R2/v1 defines
no deterministic command flags yet; compatible v1 encoders MUST write
`command_flags = 0`, and v1 decoders MUST reject non-zero `command_flags` as
reserved for future versions. Response shaping flags such as omitted result IDs
or omitted response metadata are stripped before entry encoding because they do
not change logical state.

Unknown, ignored, transport-only, or non-critical convenience sections MUST NOT
be copied into the deterministic entry. Optional fields MUST either be omitted
or encoded with explicit defaults according to the command schema; both forms
MUST NOT be accepted for the same `command_version`.

`ack_policy` is a common request section, but it is never deterministic command
input. Encoders must strip it before deterministic-entry construction.

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
- `ack_policy` and `consistency_policy`,
- negotiated compression choices,
- non-deterministic server timestamps,
- response-shaping hints that do not change state.

The following SHOULD be included when present:

- collection metadata mutation input,
- collection mutation IDs and document bytes,
- `CatalogGuardV1` or legacy `expected_catalog_version` conflict guard,
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

R0d MUST establish codec-level nativewire microbenchmarks before native
client/server modes publish end-to-end numbers. These package benchmarks SHOULD
use the `BenchmarkNativewire...` prefix and cover:

- fixed frame-header encode/decode,
- command-header encode/decode,
- section-envelope encode/decode,
- byte-vector encode/decode across ID and document-shaped batches,
- request body decode plus schema validation for every command schema marked
  `BenchmarkRequired`,
- deterministic command-entry encoding for every replicated benchmark case.

R0d benchmarks SHOULD report `B/op`, `allocs/op`, bytes processed per second,
and `wire_B/item` where a command carries a batch. Reusable hot paths for
section decoding, byte-vector decoding, schema validation, and deterministic
entry encoding SHOULD have allocation guard tests proving zero allocations after
scratch warmup. Allocating compatibility APIs MAY remain available, but
benchmark labels MUST distinguish allocating and scratch/reuse paths.

Every later native-wire implementation round MUST close with a benchmark and
profile pass before publishing the next feature round. R1 closeout MUST compare
direct collection calls, native-wire in-process dispatch, and native-wire TCP
dispatch. Later closeouts MUST benchmark the primary feature path introduced by
that round, capture profiles for the dominant workload, and document any
material regression, optimization, or deferred performance follow-up.

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
