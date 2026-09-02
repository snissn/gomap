# Native Wire Protocol Implementation Guidelines

Status: draft playbook, non-normative.

This document is the implementation operating guide for:

- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`

The wire protocol spec defines the bytes and semantics. This playbook defines
how implementation work should keep code, tests, benchmarks, and future Raft
behavior aligned with that spec.

## 1. Purpose

The native protocol should not become a pile of hand-decoded commands whose
behavior is only discoverable from server dispatch code. The implementation
should make protocol rules explicit, testable, and hard to bypass.

The first implementation goal is not public compatibility. TreeDB is pre-alpha.
The goal is internal discipline: every command, section, error, feature bit, and
canonical Raft entry rule should have one obvious place in code and one obvious
set of tests.

## 2. Source of Truth

Until a machine-readable schema exists, the Markdown spec is the human source of
truth and the Go schema registry is the executable source of truth. A future
registry file, for example `TreeDB/docs/spec/native-wire-v1.registry.json`, can
become the shared source for generated constants and documentation tables once
manual duplication becomes risky.

Every PR that changes the wire protocol MUST update all affected artifacts in
the same change:

- `native-wire-protocol.md`,
- `native-query-raft-roadmap.md` when sequencing or distributed policy changes,
- the command/section schema registry,
- golden byte fixtures,
- negative conformance tests,
- benchmark labels or report parsing when new benchmark modes are added.

Command IDs, section IDs, error codes, feature bits, and protocol versions MUST
not be invented inline in server handlers. They should be declared in one schema
package with names that match the spec. Go constants used by the codec, server,
tests, and benchmarks should either be generated from the registry or checked
against it by a drift test.

When the schema grows enough that manual duplication becomes risky, generate
either constants or documentation tables from the registry. Do not introduce a
generator until it removes real drift risk.

## 3. Package Boundaries

The exact package names can change, but the implementation should preserve these
boundaries:

- **Frame codec:** encodes and decodes fixed headers, frame types, frame flags,
  section envelopes, byte vectors, compression framing, checksums, and resource
  limits. It should not depend on collections or Raft packages.
- **Schema registry:** describes supported commands, versions, sections,
  multiplicity, deterministic status, required feature bits, and validation
  rules.
- **Command normalization:** converts validated wire sections into typed request
  structs. It resolves defaults and rejects ambiguous encodings before execution.
- **Server dispatch:** handles connection state, handshake, authentication,
  request routing, cursor lifecycle, cancellation, response metadata, and error
  mapping.
- **Collection adapter:** maps normalized commands onto `TreeDB/collections`
  behavior and owns parity tests against direct collection calls.
- **Deterministic entry codec:** encodes and decodes canonical Raft command-entry
  bytes. It must not depend on connection state, negotiated compression, request
  IDs, cursor state, deadlines, tracing, local durability policy, or response
  shaping. It strips `ack_policy`, `consistency_policy`, deadlines, tracing,
  compression, response shaping, request IDs, stream IDs, and local handles
  before deterministic-entry construction.
- **Conformance fixtures:** stores golden wire frames, canonical entries, and
  rejection cases that can be reused by future clients or compatibility tests.

Do not let the server dispatch path be the only validator. The codec and schema
layers should reject malformed or unsupported input before collection execution.

## 4. Decode and Validation Pipeline

Each request should pass through the same staged pipeline:

1. validate frame header, frame size, negotiated version, and frame flags;
2. decode sections with maximum count and maximum byte limits;
3. enforce unknown critical section and unknown required flag rules;
4. look up `command_id + command_version` in the negotiated schema;
5. enforce required sections, singleton multiplicity, and repeated-section
   ordering rules;
6. decode command-specific fields into typed structs;
7. apply explicit defaults from the command schema;
8. reject duplicate IDs, stale catalog guards, unsupported document formats, and
   unsupported ack or consistency policies;
9. execute the command or, in distributed mode, pass it through the append-time
   determinism gate before execution.

Decoders should reject before allocating large buffers whenever possible.
Length fields, section counts, byte-vector counts, and cursor limits should be
checked against negotiated or configured maximums at the boundary.

## 5. Schema Registry Rules

Each command schema should record:

- command name, ID, and version,
- minimum protocol version,
- required feature bits,
- required and optional sections,
- allowed section multiplicity,
- whether repeated section order is meaningful,
- deterministic sections and deterministic command flags,
- optional-field default rules,
- request and response section layouts,
- whether the command is local-only, read-only, or replicated,
- allowed ack and consistency policies,
- error mappings for expected collection failures.

Adding a command version is preferred over changing existing semantics. Once a
command version can appear in a deterministic command entry, its deterministic
meaning is immutable for that Raft log lineage.

Each command should also have an implementation-status row recording:

- whether it is in the v1 target surface,
- first implementation phase,
- implementation state,
- whether it can produce a deterministic entry,
- whether a benchmark is required.

The command list in `native-wire-protocol.md` is the v1 target surface. The
roadmap decides which subset is implemented in each phase.

## 6. Conformance Tests

R0 implementation should land conformance tests before or with codecs. Store
native-wire fixtures under package `testdata`, for example
`TreeDB/internal/nativewire/testdata/v1/`.

Required positive fixtures:

- `hello` and `hello_ok`,
- a minimal request/response,
- `insert_batch` with byte-vector document IDs and documents,
- `get_many` with a presence bitmap,
- an error frame,
- `open_scan` plus `cursor_next`,
- a canonical deterministic entry for every replicated command version.

Each fixture should include:

- fixture name and schema version,
- negotiated feature set,
- hex-encoded frame bytes,
- expected decoded summary,
- expected error code for rejection fixtures,
- expected deterministic-entry bytes for mutating command fixtures.

Required rejection fixtures:

- bad magic,
- invalid `header_len`,
- frame larger than the negotiated maximum,
- unnegotiated protocol version,
- unknown required frame flag,
- unknown critical section,
- unknown required section flag,
- duplicate singleton section,
- missing required section,
- byte-vector length overflow, truncation, and extra bytes,
- non-minimal canonical varint in deterministic entries,
- local handle inside a deterministic entry,
- missing idempotency identity or catalog guard for distributed mutation.

Changing an existing golden fixture is a protocol change. Prefer adding a new
fixture or a new command version unless the old fixture intentionally documents
a pre-alpha break.

Fuzz tests should target frame and section decoding, byte vectors, compression
boundaries, and command normalization. Fuzzers must use memory and frame-size
caps so malformed input cannot turn into unbounded allocation.

## 7. Determinism Tests

Before Raft, add deterministic-entry tests that prove:

- the same logical mutation encoded with different request IDs, deadlines, trace
  metadata, compression choices, acknowledgement policies, consistency policies,
  response-shaping flags, and section order produces the same canonical entry
  digest;
- `ack_policy=visible`, `flushed`, `synced`, and absent/default do not produce
  different deterministic command bytes, while rejected single-node
  `raft_committed` handling remains a request-validation/server-admission error
  rather than deterministic-entry semantics;
- shuffled Go map iteration and cross-process execution produce the same bytes;
- unsupported command versions and non-deterministic sections are rejected before
  append;
- replaying the same canonical entry sequence into fresh databases produces the
  same logical catalog, document, index, and idempotency state;
- snapshot restore plus log-tail replay produces the same logical state digest
  as full log replay.

The logical state digest should be independent of local file layout, value-log
offsets, flush timing, or maintenance decisions.

## 8. Performance Practices

The hot path should stay binary and vectorized:

- parse byte vectors into offset tables or borrowed slices,
- avoid reflection and generic map decoding in request parsing,
- avoid per-document allocation where a batch view is enough,
- keep JSON/OpenRPC/admin description layers out of the data-plane codec,
- profile decode, validation, dispatch, and collection execution separately,
- track per-frame overhead in benchmark reports.

Benchmarks should keep native-wire results separate from direct collection calls,
Mongo driver paths, and Mongo raw-wire paths.

Every native-wire roadmap round should close with a benchmark/profile sprint.
This sprint should freeze the current feature surface, run the relevant
microbenchmarks and workload-coupled benchmarks, capture CPU, allocation, block,
mutex, and trace profiles, and optimize the dominant costs before the next
roadmap round begins. It should not add new protocol features except
instrumentation, benchmark labels, or small optimizations directly justified by
the profile.

The closeout report for that sprint should name:

- the exact benchmark commands and baseline commit or previous roadmap slice,
- artifact directories for raw output and profiles,
- top CPU and allocation sources,
- throughput, latency, bytes/op, `B/op`, `allocs/op`, and wire-overhead deltas,
- optimizations made during the sprint,
- regressions accepted with rationale and follow-up issue or PR target.

R0d codec benchmarks should land before the native server. They should:

- use stable `BenchmarkNativewire...` names,
- cover every command schema marked `BenchmarkRequired`,
- benchmark allocating compatibility APIs separately from scratch/reuse APIs,
- report `wire_B/item` for batch-shaped command bodies,
- include allocation guard tests for warmed reusable frame/section, byte-vector,
  schema-validation, and deterministic-entry paths,
- keep benchmark fixtures representative of collection workloads without
  pulling in the collections package or storage engine.

When a benchmark exposes avoidable allocations in the codec or schema layer,
prefer a reusable scratch API over hiding the cost in the benchmark harness. The
native server should be able to use the same scratch APIs later for per-request
parse state.

## 9. Observability

The first server should expose enough counters to debug protocol behavior:

- frames and bytes in/out,
- decode failures by error code,
- command counts and latency by command ID/version,
- rejected unsupported versions/features,
- cursor opens, closes, cancellations, timeouts, and limit hits,
- in-flight requests and cursors,
- requested and actual ack/consistency policies,
- response metadata for serving node, leader node, and applied log index when
  cluster mode exists.

Trace context is transport metadata. It must not influence deterministic command
entry bytes.

## 10. PR Checklist

Every protocol implementation PR should answer:

1. Which spec sections changed or were implemented?
2. Which command/section schema entries changed?
3. Which golden fixtures were added or updated?
4. Which rejection tests prove unsupported input fails at the right layer?
5. Which parity tests compare native-wire behavior to direct collection calls?
6. Which deterministic-entry tests are needed now or deferred with a clear
   reason?
7. Which benchmark labels, profile captures, or report parsers changed?
8. If this is a phase-close sprint, what benchmark/profile artifacts and
   optimization decisions close the phase?
9. Did the roadmap need an update because implementation reality changed?

Do not merge a new replicated command version without a canonical fixture,
append-time rejection tests, and a replay determinism test.
