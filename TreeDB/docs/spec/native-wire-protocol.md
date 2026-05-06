# TreeDB Native Wire Protocol v1

Status: draft proposal, non-normative.

TreeDB is pre-alpha. This document defines the target native network protocol
shape for TreeDB collections and raw ordered-key operations. It is intended to
guide implementation, benchmark work, and the future Raft/distributed database
surface. It does not describe current shipped server behavior.

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

`body_len` is the number of bytes following the header. Receivers MUST enforce a
negotiated maximum frame size.

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

`stream_id` identifies a logical stream or cursor. Non-streaming requests use
`stream_id=0`. Streaming result frames use the same `request_id` and a non-zero
`stream_id` assigned by the server.

### 5.3 Body Sections

Frame bodies are a sequence of typed sections:

```text
section_id     uvarint
section_flags  uvarint
section_len    uvarint
section_bytes  bytes[section_len]
```

`section_flags & 1` marks a section as critical. Unknown critical sections MUST
fail the frame. Unknown non-critical sections MUST be ignored.

Sections SHOULD be ordered by increasing `section_id`, but receivers MUST NOT
depend on order unless a command schema says a repeated section is ordered.

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
```

Command-specific sections start at `100`.

### 5.5 Byte Vectors

Batch-oriented fields use byte-vector encoding:

```text
count uvarint
repeat count:
  length uvarint
bytes concatenated_payloads
```

This avoids per-item maps and keeps IDs, documents, keys, and values compact.
Implementations SHOULD parse byte vectors into offset tables or borrowed slices
instead of allocating one object per element.

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

## 9. Initial Command Set

The v1 implementation SHOULD start with the smallest surface that can replace
the current Mongo raw-wire benchmark path for native clients.

### 9.1 Control Commands

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

### 9.2 Mutation Commands

```text
30 insert_batch
31 replace_batch
32 delete_batch
33 flush_collection
34 flush_all
35 checkpoint
```

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

### 9.3 Read Commands

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

## 10. Typed Scalars

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

## 11. Durability and Consistency Policies

The `ack_policy` section controls when a mutation response is sent:

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

## 12. Error Model

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
```

Errors that map to current collection duplicate-key conditions SHOULD preserve a
stable duplicate-key class so clients can handle inserts and unique-index
conflicts uniformly.

## 13. Deterministic Command Entry v1

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

Raft implementations MAY store deterministic entries directly or wrap them in a
larger consensus-layer entry that also carries term/index metadata.

## 14. Benchmark Requirements

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
- server decode/dispatch overhead.

## 15. Open Questions

1. Whether v1 should support raw ordered-KV commands alongside collection
   commands, or keep raw-KV access as a later capability.
2. Whether connection-local collection handles are worth the complexity in v1.
3. Which compression codecs should be allowed on the wire initially.
4. Whether `synced` should be rejected or downgraded under relaxed durability
   modes, or accepted with a response field describing the weaker actual
   guarantee.
5. How authentication and authorization should map onto collections, indexes,
   and future cluster routing.
