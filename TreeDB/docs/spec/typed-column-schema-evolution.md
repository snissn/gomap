# Typed-Column Schema Evolution and Migration Policy (#1789)

Status: current pre-alpha policy for typed-column image, descriptor, manifest,
and schema changes. This document is normative for typed-column storage changes;
it does not introduce migration scaffolding or a backwards-compatibility promise.

## Pre-alpha compatibility policy

TreeDB is pre-alpha. Typed-column part images, descriptors, manifest records,
and collection metadata fields may change between commits or releases. A DB
directory created by one binary is not guaranteed to open with another binary,
and a newer binary is allowed to reject older typed-column directories rather
than migrate them.

The required safety rule is fail closed: when typed-column metadata or bytes are
unsupported, stale, incomplete, or inconsistent, TreeDB must reject the open,
scan, reconstruction, maintenance, GC, or rewrite operation before serving data
or mutating typed assets. Unsupported typed-column formats must not be treated as
best-effort data, silently cleaned up, or rewritten into a guessed shape.

For benchmark and experiment directories, the preferred current answer is
rebuild rather than migrate. Recreate fixture DB directories with the binary and
schema being measured. Migration tooling is deferred until a separate issue
commits to a stable compatibility target.

## Fail-closed detection points

Typed-column validation should reject mismatches from small headers,
descriptors, manifest identity records, and manifest refs whenever possible. A
reader or maintenance planner should not decode a full typed-column payload, scan
all rows, or allocate per-row state merely to discover an unsupported version or
schema mismatch.

Current and future typed-column code must fail closed on at least these classes:

- **Schema identity:** collection `schema_hash` and typed asset `SchemaHash`
  must match the normalized schema/config that selected the reader. Schema hash
  drift means caches and typed-column payloads are stale.
- **Field ownership:** each logical field has one authoritative owner for a
  generation. A field expected to be `typed_column_part` must not be read from a
  typed-row asset, retained document payload, or derived accelerator as a silent
  substitute, and overlapping authoritative owners must be rejected.
- **Value type:** declared typed-storage value type must match the typed-column
  column descriptor. Authoritative `uint32_list` uses the non-null
  `uint32_list`/`raw_uint32_offsets_list` descriptor identity with `rows+1`
  offsets. Authoritative `adjacency_list` remains consumer-specific: dense
  compatibility uses the fixed-degree `uint32` shape described by
  `adjacency_degree`, while explicit offsets-list compatibility must stay
  classified separately from the generic list primitive.
- **Vector dimensions / adjacency degree:** `float32_vector` fields must have positive
  `vector_dims`; `adjacency_list` `typed_column_part` fields must be non-nullable
  and have positive `adjacency_degree`. Descriptor fixed-width element counts
  must match the declared dimensions/degree. Nullable/missing vector and
  adjacency payloads remain staged and fail-closed.
- **Fixed-width metadata and layout:** fixed-width column descriptors must match
  type, encoding, compression, element width/count, byte length, row count,
  endian mode, section range, and alignment before exposing direct typed views.
  Truncated, overlapping, out-of-bounds, or misaligned sections must fail closed
  or use only an explicitly documented safe fallback that preserves semantics.
- **Image version:** typed-column part-image magic/version values are required
  gates. Unknown image versions must fail closed before section payload decode.
- **Descriptor version:** column, dictionary, aggregate, locator, nullable, and
  future section descriptors must reject unsupported descriptor versions/kinds
  from descriptor bytes before row-level decode.
- **Manifest identity:** active manifest identity and recovery-authoritative
  identity must have supported format/version values, matching generation and
  checksum, and the collection metadata must name the expected manifest root and
  storage policy. Missing or conflicting identity state fails closed.
- **Manifest refs:** every accepted typed-column ref must match expected kind,
  namespace, generation, part id, segment file id, offset, length, checksum,
  lifecycle role, and row/range metadata. Kind/generation/part/checksum/range
  mismatches, non-canonical segments, missing segments, and out-of-bounds ranges
  fail closed.

These checks apply both to online reads and to maintenance. A GC or rewrite plan
with incomplete classification, unsupported typed-column formats, corrupt refs,
or unconvertible active pins must not delete or rewrite typed-column bytes.

## Rebuild versus migrate

Until TreeDB declares a stable typed-column compatibility window:

- benchmark and experiment DB directories should be rebuilt after typed-column
  image, descriptor, manifest, or schema semantics change;
- public docs and benchmark notes should identify the commit/schema used to
  create reusable fixtures;
- tooling may report why a directory is unsupported, but it must not perform an
  implicit migration during open, scan, GC, rewrite, or compaction; and
- tests may assert fail-closed behavior for old/unsupported versions instead of
  requiring cross-version reads.

## Future migration tooling requirements

A future migration tool must be explicitly scoped before implementation. At a
minimum it must:

1. open the source directory read-only and validate typed-column image,
   descriptor, manifest, checksum, and range closure before copying bytes;
2. identify source and target format/schema versions, including descriptor and
   manifest identity versions;
3. produce a new directory or copy-on-write generation rather than mutating the
   only durable copy in place;
4. preserve row identity, generation ordering, tombstone semantics, schema hash
   provenance, field ownership, value types, vector dimensions, nullable/missing
   semantics, and fixed-width layout invariants;
5. recompute and publish manifest identity checksums only after all target refs
   are durable and verified;
6. keep typed-row, typed-column, and derived-accelerator refs tied to their
   authoritative owner/generation;
7. provide an audit/verification report that operators can run read-only; and
8. include reopen, corruption, interruption, and rollback tests plus benchmark
   evidence for any migrated hot path.

This issue deliberately does not build that scaffolding.

## Performance compatibility policy

Typed-column decode, direct-view, scan, and reconstruction merge paths are
allocation-budgeted hot paths. A future change to typed-column image,
descriptor, schema, or manifest semantics must state whether it preserves the
current 0-alloc or near-0-alloc decode/scan path after setup. If it cannot, the
change must define an explicit, benchmarked fallback and explain why the fallback
is acceptable.

PRs that touch typed-column or typed-storage hot paths must report baseline
versus final `B/op` and `allocs/op` for the affected benchmark(s). If a profile
exposes new hot-path allocation, the PR must remove it or document a bounded follow-up
with allocation profile/top evidence. Policy or migration work must not bless
full-document reconstruction on direct typed-column scan paths, per-row heap
wrappers, maps, interface boxes, closures, or string/byte conversions in inner
loops.

Fail-closed validation is not optional for performance. Unsupported-version and
schema-mismatch checks should be header/descriptor-level and allocation-bounded,
not skipped or delayed until row materialization.
