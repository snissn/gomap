# TreeDB Storage Format

This document defines TreeDB's durable on-disk formats and local frame formats.
The native client/server wire protocol is owned by
`TreeDB/docs/spec/native-wire-protocol.md`.

TreeDB is pre-alpha; format compatibility between versions is not guaranteed.
That disclaimer does not permit fail-open handling of acknowledged durable
writes. Once a directory advertises a required storage feature such as
`command_wal_v2`, unsupported binaries must fail closed instead of serving,
cleaning, compacting, or rewriting the directory. Typed-column image,
descriptor, manifest, and schema evolution follows the fail-closed policy in
`typed-column-schema-evolution.md`.

The canonical production profiles are `command_wal_durable`,
`command_wal_relaxed`, and `no_wal_fast`; `bench_unsafe` is benchmark/test only.
The resolved profile is immutable at open. Main DB `format.json` version 4
persists exactly one canonical `durability_profile`. Public reopen, native
backend open, and offline maintenance must select the same profile. Version 4
without a profile, an unknown profile, an older unbound main-DB manifest, or a
selected/persisted mismatch returns the pre-alpha rebuild-required error rather
than attempting a mixed-profile or mixed-version mode. The gate is not disabled
by `IgnoreFormatConfig`. Internal side-store and low-level test manifests may
remain unbound version 2/3 files; they cannot open a version-4 main DB without an
explicit matching profile.

## 1. Top-Level Storage Objects

A TreeDB deployment uses:

- `index.db` (paged B+Tree index and metadata),
- commit-log segments under `wal/commit-l*.log`; future user-command WAL
  frames extend this same segment family rather than creating a second WAL file
  class,
- value-log segments under `value_vlog/value-l*.log`,
- optional split outer-leaf value-log segments under `leaf_vlog/value-l*.log`
  when `IndexOuterLeavesInValueLog` is enabled,
- transient, crash-persistent value-log delete intents beside a segment as
  `.<segment-name>.delete-<volume-id>-<object-id>/<segment-name>`, where the
  fixed-width hexadecimal ids identify the exact physical object being
  deleted,
- typed asset manager segments under
  `column_assets/<namespace>/assets/segments/segment-*.tca` for production
  typed-storage physical assets (`column_assets` remains the compatibility
  directory name),
- R3a/Raft apply metadata logs `apply-progress-v1.log` and
  `apply-results-v1.log` under the per-group Raft `apply/` metadata directory,
- optional single-group Raft provider state under
  `raftcluster/nodes/<node-id>/groups/<group-id>/` with separate `log/`,
  `stable/`, `apply/`, `snapshots/`, and `peers/<peer-id>/` directories,
- optional side-store DBs (`dictdb`, `templatedb`) using their own `index.db` files.

### Vector-partition manifests (`vector_partitions/`)

`vector_partitions/` is a persistent, Raft-snapshot-included namespace for M1
vector-partition metadata; it is not a WAL or cache. VPM1 remains the canonical
generation-manifest payload, but it is not published as a mutable standalone
file. Local lifecycle authority is an immutable VCP1 checkpoint plus a
digest-chained VLC1 delta tail under the exact hashed identity:

```text
sha256(collection)-sha256(index).lifecycle.checkpoint.<20-digit-epoch>.vlc
sha256(collection)-sha256(index).lifecycle.epoch.<20-digit-epoch>.delta.<20-digit-sequence>.vlc
```

The highest checkpoint epoch is the sole authority; corruption never falls
back to a lower epoch. BUILD creates a new checkpoint epoch. READY,
LOCAL_ACTIVATE, DEACTIVATE, DELETE_PREPARE, RECLAIM_PROGRESS, and
DELETE_COMPLETE append immutable deltas until the bounded tail is compacted
into another checkpoint. A checkpoint contains the identity, generation floor
and high water, durable activation high water, at most two live generation
states, active/retired generation, last sequence/digest, embedded canonical
VPM1 manifests, and any VPR1 reclaim debt. The generation floor records the
first accepted generation; successors must be contiguous, so an absent
generation inside the durable floor/high-water interval is exact proof of
completed deletion rather than an unrecorded gap. The activation high water
records the newest generation that ever held local activation authority
independently of the live pointers, so deletion and reclaim cannot make an
older prepared generation eligible for reactivation. The checkpoint is capped
at 30 MiB, its current tail at 4 MiB, the identity namespace at 64 MiB and
4,096 entries. Counts and lengths are checked before allocation.

Every file is installed no-replace from an exact synchronized anonymous handle,
then the parent namespace is synchronized and reopened. Exact-byte retries are
idempotent; conflicting immutable names, physical aliases, symlinks, malformed
names, gaps, cross-identity payloads, or invalid transitions fail closed.
Superseded epochs may remain as zero-length audit stubs in a live store, but
Raft export includes only the highest checkpoint and its contiguous current
tail. The archive always carries an explicit `db/vector_partitions` directory,
including when it is empty. Restore rejects a missing directory, any extra
audit epoch, or legacy mutable authority. It reads the namespace in bounded
batches, applies the 4,096-entry cap to all names, and verifies the ranges,
CRC32 values, and SHA-256 digests of every asset referenced by a non-deleting
manifest before replacing the target namespace.

VPM1 uses big-endian magic `0x56504d31` and wire version `3`, bounded
length-prefixed fields and lists, one (exactly one) router-asset frame,
canonical ordering, and an integrity digest. Version 3 has this fixed,
untagged order:

1. magic and `uint32` version;
2. eight `uint32`-length-prefixed UTF-8 strings: format, state, collection,
   index name, index-definition digest, integrity digest, balance policy, and
   ready-set digest;
3. six `uint64` values: source generation, source checksum, source schema
   hash, source row count, partition generation, and router generation,
   followed by the `uint32` partition count;
4. exactly one router asset;
5. counted placement, disjoint-membership, overlap-membership,
   representative-membership, and partition-asset lists, in that order.

Each placement is a `uint32` partition ID plus a length-prefixed group ID.
Each membership is a `uint64` source ordinal plus a `uint32` partition ID.
Every asset descriptor is a partition ID; length-prefixed logical ID,
SHA-256 checksum, and optional membership digest; a `uint64` byte length; and
a `ColumnAssetRef` containing length-prefixed kind/namespace, `uint64`
generation/part ID, `uint32` file ID, `uint64` offset/length, and `uint32`
CRC, in that order. Version 3 adds the membership-digest string between the
asset checksum and byte length.
Native partition HNSW assets require that SHA-256 digest; it binds the
generation, partition, ordered authoritative stable IDs, and home/overlap
membership kinds. There are no optional tagged fields and this pre-alpha
decoder accepts only version 3; older directories require rebuild rather than
migration.

Partition-local `hnsw_search_pack_v1` assets use wire version 3 when rebuilt
with the reachability repair. Version 3 retains the version-2 176-byte header
and required membership digest, then adds exactly two checksum-covered CSR
sections for auxiliary offsets and neighbors. The auxiliary CSR is present even
when empty and must exactly contain the deterministic branching-factor-eight
tree over native layer-0 directed-reachability roots (entry component first),
plus one directed edge from every non-root row with a persisted upper HNSW
level to its component root. Those seed anchors preserve ordinary upper-layer
descent before layer-0 reaches the component bridge. Its ordinals, offsets,
tree bridges, seed anchors, degree cap, edge total, source identity, and
membership binding validate before a reader exposes a prepared view.
Versions 1 and 2 retain their existing layouts and have no auxiliary channel.

For M3 bounded-overlap manifests, the canonical balance-policy grammar is
`m3_bounded_overlap_v1:capacity=<u64>,budget=<u64>,realized=<u64>,unspent=<u64>`
followed by the optional suffix `,build_identity=<64-lowercase-hex-sha256>`.
The optional build identity binds the fixture, source graph, assignment, overlap, and index
construction configuration without adding a sidecar; because `BalancePolicy`
is covered by the VPM1 integrity digest, editing or relabeling that identity
invalidates the manifest. Readers accept the historical form without the
suffix, but evidence paths that require an authoritative build identity reject
its absence. Noncanonical field order, additional fields, invalid digests,
zero capacity, or accounting other than `budget = realized + unspent` fail
closed. TreeDB is pre-alpha, so new M3 evidence directories may require
rebuilding when this identity is required.

VRP1 (the READY promotion payload, distinct from the VPR1 reclaim payload)
uses ASCII magic `VRP1`, big-endian wire version `2`, and this fixed,
untagged order:

1. magic, `uint32` version, and `uint64` generation;
2. the 32-byte building-manifest SHA-256 digest;
3. `uint64` router generation and the 32-byte ready-manifest SHA-256 digest;
4. the length-prefixed ready-set digest;
5. a counted, canonically ordered representative-membership list;
6. an asset list containing exactly one router asset encoded with the VPM1
   asset frame; and
7. a final 32-byte SHA-256 digest over all preceding bytes.

The payload is capped at 16 MiB. Generation/router identity, digests, mapping
order and bounds, the single router asset, trailing bytes, and canonical
re-encoding are validated before the promotion is applied. Version 2 adds the
representative mapping needed to reconstruct the ready manifest exactly.
Current pre-alpha readers reject older VRP1 versions; rebuild old DB
directories instead of migrating in place.

VPR1 is the bounded, versioned, checksummed reclaim payload that retains
original and rewritten asset debt until physical GC completes. Pre-alpha
binaries reject old `.vpm`, `.active`, `.retired`, `.inactive`, and `.deleting`
authority; there is no fallback or migration path.

### Catalog/meta Raft lifecycle snapshot

The catalog/meta Raft FSM snapshot is canonical JSON format version 1 with an
8 MiB outer limit. It stores `format`, `applied_index`, the canonical catalog
`record`, the exact `last_command`, and an optional base64-encoded
`vector_partition_lifecycle` byte payload. When the vector-partition lifecycle
feature is enabled, that inner payload is itself canonical JSON format version
1 containing ordered `records`, `mutation_fences`, and
`collection_mutation_barriers` arrays.

The lifecycle payload permits at most 4,096 generation records, 4,096
per-index mutation fences, and 4,096 collection barriers. Each collection
barrier retains at most 64 completed operation identities. These inner bounds
are enforced before the complete base64-expanded outer snapshot is checked
against the 8 MiB limit. Restore rejects unknown fields, duplicate identities,
noncanonical ordering or encoding, catalog-identity mismatches, multiple active
generations for an index or serving name, and malformed barriers or fences. A
pending per-index fence must have exactly one matching unconfirmed invalidated
generation record at the same mutation epoch and cannot coexist with an active
generation. The snapshot is installed all-or-nothing; invalid lifecycle state
never publishes the catalog record or applied index.

Before a writable public open can succeed, TreeDB establishes the complete
directory dependency chain from the outer database root through `maindb`,
enabled side-store roots, and each backend's `wal`, `value_vlog`, `leaf_vlog`,
and `column_assets` directories. Missing components are created in parent-first
order. On platforms with parent-directory sync, the distinct parents of newly
created names are synchronized deepest-first, including the parent of a newly
created outer root. Windows uses its narrower exact-child creation persistence
primitive for each new component. Fully existing initialized layouts add no
creation sync; partial initialized layouts synchronize only the parents of
names created by that open. Until a backend `index.db` exists as initialization
proof, writable open conservatively synchronizes every ancestor edge from the
requested directories through the filesystem or volume root, deepest-first and
deduplicated. This closes intermediate edges left by a failed attempt even
though the proof alone cannot recover the original pre-existing boundary. Any
namespace sync failure aborts open, and an unavailable primitive returns the
typed `ErrNamespacePersistenceUnsupported` result rather than certifying the
layout.

The old collection root-delta WAL storage class (`wal/collection-l*.log`,
`collection_wal_v1`) is deprecated before becoming the active committed format.
It is retained in `collection-wal-durability-plan.md` as historical design
context. The active target is the user-command WAL: command frames ordered by
`LSN`, checkpointed by durable `AppliedLSN`, and defined in
`user-command-wal.md`. Exact command-frame bytes, checksums, segment metadata,
cleanup records, and golden encodings must be maintained here and mapped to
tests in `TreeDB/docs/spec/verification.md` as each milestone lands.

The operator restorable file set, live backup barrier, and restore validation
procedure are defined in `TreeDB/docs/spec/backup-restore.md`. A live
filesystem-level copy without that barrier is unsupported once command-WAL
external refs can exist.

Raft provider state is separate from the main DB. The local command WAL under
`maindb/wal/` is not a Raft log, and `maindb/value_vlog/` remains persistent
value storage managed by reachability-based GC and rewrite/compaction. The
single-group provider/storage boundary is defined in
`TreeDB/docs/spec/raftcluster.md`.

The identity-encoded delete-intent directory is recovery state, not another
value-log segment or an age-based retention mechanism. It may remain after an
interrupted unlink. A writable open reconciles it before exposing segments; a
read-only open never mutates it and reports `ErrRecoveryRequired`. Because
TreeDB is pre-alpha, this protocol has no compatibility migration scaffold.

### Mongo gateway authorization raw-KV records

When the standalone Mongo gateway authentication feature is used, it owns the
following reserved raw-KV keys. The `v1` component is the key-namespace version;
the current JSON payload version is independently `2`:

- `\x00mongo-gateway/auth/v1/synthetic-secret` stores the process-independent
  SCRAM synthetic-user secret;
- `\x00mongo-gateway/auth/v1/<auth-db>/<username>` stores one SCRAM verifier,
  where both identity components are unpadded base64url encodings of their
  UTF-8 bytes; and
- `\x00mongo-gateway/authorization/v1/catalog` stores the complete standalone
  role-assignment catalog.

The synthetic-secret record is JSON with integer `version` and base64 `secret`
fields. Version 2 requires exactly 32 decoded secret bytes. A version-2 verifier
record has `version`, `username`, `auth_db`, base64 `salt`, `iterations`, base64
`stored_key`, base64 `server_key`, `enabled`, and nonzero unsigned
`incarnation` fields. The stored and server keys are 32 decoded bytes, and the
incarnation changes on account drop/recreate but not password rotation.

The version-2 authorization record is JSON with `version` and a nonempty
`users` array. Each user entry has `username`, `auth_db`, nonzero unsigned `id`
(the verifier incarnation), and a `roles` array. Each role grant contains
`role`, plus optional `database` and `collection`; a collection requires its
database, and `serverAdmin` has server scope only. User entries and grants are
stored in canonical identity/role order with no duplicates. The catalog is
bounded to 4,096 users and 64 grants per user.

Only a missing authorization-catalog key denotes the pristine bootstrap state.
A persisted empty/null/malformed catalog, an unsupported payload version, or a
verifier/assignment incarnation mismatch fails closed. Version-1 verifier and
authorization payloads are deliberately not migrated; this pre-alpha format
requires rebuild or explicit offline repair.

Writes use the selected profile's synchronous raw-KV durability boundary.
Records too large for the inline index threshold are stored in TreeDB's
persistent ValueLog and the raw-KV entry stores the durable `ValuePtr`; the
ValueLog is not a WAL or temporary side store. Verifier and authorization
records remain separate durable writes, so user-management operations are
fail-closed but not rollback-atomic. A grant snapshot is published only after
the authorization record succeeds; ambiguous publication invalidates the
snapshot, and abandoned appended pointers release their pending GC pins.

## 1.1 Opt-in external-version MVCC key subspace

Issue [#3670](https://github.com/snissn/gomap/issues/3670) defines a pre-alpha,
opt-in physical-key codec for a later caller-assigned MVCC layer. The codec is
implemented in `TreeDB/internal/mvcckey`; existing raw TreeDB reads, writes,
`EntryRevision`, page formats, and command-WAL frames do not invoke it.

Version 1 physical keys use this byte layout:

```text
00 54 44 42 4d 56 43 43 01  escaped-logical-key  00 00  u64be(^timestamp)
|--------- "TDBMVCC" v1 ---------|                       |----- 8 bytes -----|
```

Encoding rules:

- logical keys are arbitrary byte strings, including the empty key;
- each logical `00` byte is escaped as `00 ff`; every other byte is copied;
- `00 00` terminates the escaped logical key;
- the caller-assigned timestamp is a nonzero `uint64`; zero is reserved and
  rejected;
- the timestamp suffix is the big-endian bitwise complement of the timestamp;
- malformed escapes, missing/truncated/extra suffix bytes, timestamp zero, and
  keys outside the exact versioned marker fail explicitly;
- the format envelope is at most `65535` encoded bytes and is checked before
  allocation. TreeDB's 4096-byte pages and the selected leaf/value shape may
  impose a smaller usable physical-key limit.

The escape is order-preserving. If one logical key is a prefix of another, its
`00 00` terminator sorts before either a copied nonzero extension byte or the
`00 ff` encoding of a zero extension byte. For equal logical keys,
`u64be(^timestamp)` sorts larger timestamps first. Physical byte order is
therefore `(logical key ascending, timestamp descending)`.

The marker reserves the half-open physical range
`[00 54 44 42 4d 56 43 43 01, 00 54 44 42 4d 56 43 43 02)` when the opt-in
layer owns a TreeDB keyspace. Logical-prefix bounds use the marker plus an
escaped, unterminated logical prefix. All-version bounds for one logical key use
the marker plus the escaped key and `00 00` terminator. The exclusive upper
bound is the lexicographic successor of that physical prefix. Exact-key bounds
enforce the same full-key size envelope as `Encode`; logical-prefix bounds are
limited only by their own encoded bound size.

The `TreeDB/mvcc` owner stores a one-byte record-kind envelope as the physical
value:

```text
01 <logical value bytes>   // present value; the payload may be empty
02                         // tombstone; trailing bytes are malformed
```

An empty physical value or unknown record-kind byte is malformed. Logical
tombstones are values in this reserved subspace rather than raw TreeDB delete
operations so historical deletion markers remain seekable. This record format,
like the key codec, is pre-alpha and may require rebuilding experimental DB
directories after a format change.

This namespace does not make raw TreeDB keys non-empty or silently reserve a
prefix in existing raw APIs. The MVCC owner must prevent unrelated raw writes
from entering its reserved physical range. The namespace marker makes MVCC
physical keys non-empty even when the logical key is empty.

The same opt-in owner stores its global discard floor under an explicit
metadata marker that cannot be mistaken for a physical-key codec version:

```text
00 54 44 42 4d 56 43 43 00 4d 01 64 66
|--------- "TDBMVCC" ---------| M v1 d  f
```

Its value is `01 u64be(discard_floor)`. A zero floor is represented by absence;
an encoded zero, unknown record version, or wrong record length is corruption.
The floor is the greatest external timestamp declared discardable. It is
monotonic and is persisted before physical version deletion starts. This
metadata key sorts outside the half-open version-1 scan namespace, so retained-
version iterators cannot surface it as a logical record. The zero marker plus
`M` identifies metadata and leaves nonzero codec version markers available for
future versioned-key formats.

## 1.2 Query-Ready Typed-Column Base Generation V3

A query-ready base generation (`QRBG` V3) is a rebuildable, non-authoritative
container for a snapshot-visible set of immutable typed-column part images. It
is a local derived format: it is not an index root, WAL record, recovery
selector, primary document store, or GC/rewrite owner. Until the authoritative
asset-root lifecycle publishes this format, the embedded typed-column images
remain the recoverable source state and a missing QRBG may be rebuilt.

All integer fields are little-endian. The 80-byte header is:

```text
offset  size  field
0       4     magic = "QRBG" (bytes 51 52 42 47)
4       2     version = 3
6       2     reserved = 0
8       8     exact base generation (nonzero)
16      32    exact collection schema SHA-256 (nonzero)
48      4     part count
52      4     header CRC-32C
56      4     part-table CRC-32C
60      4     reserved = 0
64      8     payload offset
72      8     total container bytes
```

The header checksum is CRC-32C (Castagnoli) over all 80 header bytes with
bytes `[52,56)` zeroed. The table checksum is CRC-32C over exactly
`part_count * 144` bytes beginning at offset 80.

Each 144-byte part-table entry is:

```text
offset  size  field
0       8     source generation
8       8     typed-column part ID
16      8     embedded image offset
24      8     embedded image byte length
32      8     row count
40      8     typed-column manifest byte length
48      32    SHA-256 of the complete embedded image
80      8     signed primary-ID base, encoded as two's-complement uint64
88      1     primary-ID mode
89      7     reserved = 0
96      8     query-ready execution sidecar offset
104     8     query-ready execution sidecar byte length
112     32    SHA-256 of the complete execution sidecar
```

Entries are strictly ordered by `(source generation, part ID)`. Source
generations are nonzero and cannot exceed the header's base generation;
duplicate identities are invalid. Entry row count, part ID, and manifest length
must exactly match the embedded typed-column image metadata.

Primary-ID mode `0` preserves the encoded row-locator primary IDs and requires
a zero base. Mode `1` requires the embedded primary IDs to be the exact dense
part-local domain `[0,row_count)` and translates them into the logical domain
as `primary_id_base + encoded_primary_id`; negative bases and signed overflow
are invalid. This lets insert-only source parts with independently numbered
local rows remain disjoint after reopen without rewriting their encoded
payloads. Base-plus-delta visibility and tombstones operate on the translated
logical IDs.

The payload begins at `payload_offset`, aligned to 4096 bytes. For each entry,
the embedded typed-column image begins at its section alignment and is followed
by an 8-byte-aligned query-ready execution sidecar. Bytes between the part
table, images, and sidecars must be zero. Ranges may not overlap or extend
beyond `total_container_bytes`; that field must equal the exact file length, so
unaccounted trailing bytes are invalid. The final sidecar ends at the end of
the container. An empty base is represented by a zero-entry table, 4096-byte
payload offset, zero padding, and total length equal to that payload offset.

The execution sidecar (`QRXS` V1) is query-independent physical state, not a
stored operator or answer. Its 48-byte little-endian header is:

```text
offset  size  field
0       4     magic = "QRXS" (bytes 51 52 58 53)
4       2     version = 1
6       2     reserved = 0
8       8     row count
16      4     column count
20      4     reserved = 0
24      8     column-descriptor offset = 48
32      8     payload offset, aligned to 8 bytes
40      8     total sidecar bytes
```

Each 64-byte column descriptor is:

```text
offset  size  field
0       4     column-name offset
4       4     column-name byte length
8       1     kind: 1 = dictionary code, 2 = signed int64
9       1     fixed width: 1/2/4 for codes, 8 for int64
10      1     nullable/absence-bitmap flag: 0 or 1
11      1     reserved = 0
12      4     local dictionary cardinality; zero for int64
16      8     values offset
24      8     values byte length
32      8     absence-bitmap offset, or zero
40      8     absence-bitmap byte length, or zero
48      8     values plus absence bytes, excluding padding
56      8     reserved = 0
```

Descriptors and names are strictly sorted by column name; names and vector
ranges are contiguous in descriptor order except for required zero alignment
padding. Code vectors use the minimum width covering the local cardinality.
Int64 vectors are fixed-width little-endian values. An optional one-bit-per-row
bitmap records null/default absence. Logical primary-key columns are omitted.

Open requires the caller's expected generation and schema hash and fails closed
on either mismatch, an unsupported version, nonzero reserved bytes or padding,
checksum failure, malformed bounds/order, or an invalid embedded typed-column
manifest/layout. Successful file open uses a read-only mapping where supported.
Encoded source payloads, direct vectors, and absence bitmaps remain slices of
that mapping. Format validation must not require whole-part payload attachment,
re-encoding, or a query-shaped dictionary/rank/offset reconstruction.
File-backed views must be closed before their files are replaced or deleted.

The implementation and format tests are in
`TreeDB/internal/typedcolumn/query_ready_base.go` and
`TreeDB/internal/typedcolumn/query_ready_base_test.go`. The collection-level
ownership, lifecycle boundary, and performance contract are described in
`docs/architecture/query-ready-base-generation.md`.

## 1.3 Query-Ready Delta / Consolidated Base Envelope V1

A query-ready delta generation (`QRDG` V1) is a rebuildable, non-authoritative
envelope containing one QRBG V3 image and a sorted tombstone table. Kind `1`
represents one ordinary delta generation. Kind `2` represents a standalone,
bounded multipart replacement base produced by deterministic consolidation.
It is not an authoritative publication, recovery, or reclamation format.

All integer fields are little-endian. The 96-byte header is:

```text
offset  size  field
0       4     magic = "QRDG" (bytes 51 52 44 47)
4       2     version = 1
6       2     kind (1=delta, 2=consolidated base)
8       8     exact envelope generation (nonzero)
16      32    exact collection schema SHA-256 (nonzero)
48      4     tombstone count
52      4     header CRC-32C
56      4     tombstone-table CRC-32C
60      4     reserved = 0
64      8     embedded QRBG payload offset
72      8     total envelope bytes
80      8     embedded QRBG byte length
88      4     original-base part count
92      4     accumulated delta-derived part count
```

The header checksum is CRC-32C (Castagnoli) over all 96 header bytes with bytes
`[52,56)` zeroed. The tombstone checksum covers exactly
`tombstone_count * 16` bytes after the header. Each entry is `i64 primary_id`
followed by `u64 tombstone_generation`. Entries are strictly increasing by
primary ID and contain only the highest tombstone generation retained for that
ID; generations are nonzero and cannot exceed the envelope generation.

The embedded QRBG begins at a 4096-byte-aligned payload offset. Padding between
the tombstone table and payload is zero. The declared total and inner lengths
must account for the exact file length; corruption, truncation, trailing bytes,
or a malformed inner QRBG fail closed. Schema and generation identity must
match both the caller's expectation and the embedded QRBG.

Ordinary delta kind requires both lineage counts to be zero and every embedded
part source generation to equal the envelope generation. Consolidated-base
kind requires:

```text
original_base_part_count + accumulated_delta_part_count == embedded_part_count
```

Consolidation may preserve mixed historical source generations. Its output
generation is exactly the highest selected delta-prefix generation, or the
unchanged prior base generation if the prefix is empty. Repeated consolidation
carries forward tombstones and accumulated lineage; it cannot reset the
delta-derived count. A later physical base rewrite may define a new origin.

The implementation, fail-closed tests, and format mutations are in
`TreeDB/internal/typedcolumn/query_ready_delta.go` and
`TreeDB/internal/typedcolumn/query_ready_delta_test.go`. The visibility, bound,
dictionary-domain, performance, and lifecycle contract is in
`docs/architecture/query-ready-delta-generation.md`.

## 2. Index Page Basics

### 2.1 Fixed page size

- `PageSize = 4096` bytes.

### 2.2 Page header

All pages begin with a 16-byte header:

```text
u64 PageID
u32 Checksum   // CRC-32/IEEE over page with checksum bytes zeroed
u16 Flags      // low bits: page type; high bits: encoding flags
u16 Count      // entry count
```

Page types (`Flags` low bits):

- `0x01`: meta page
- `0x02`: freelist page
- `0x03`: internal page
- `0x04`: leaf page
- `0x05`: COW freelist generation page
- `0x06`: COW freelist index page
- `0x07`: COW freelist chunk page
- `0x08`: COW freelist reservation page
- `0x09`: dependency-manifest page
- `0x0a`: durable-root-record page

### 2.3 Checksum

- Checksum algorithm: CRC-32/IEEE.
- Verification may be cached unless `VerifyOnRead` forces every-read checks.

## 3. Durable Root Publication V1

TreeDB keeps exactly two fixed meta pages, at page IDs 0 and 1. A meta page is
the single authoritative publication point for one independently recoverable
root generation. Roots, allocator state, the command-WAL frontier, and external
resource reachability live in immutable pages that the meta page binds by page
identity and SHA-256 digest.

This is a pre-alpha format cutover. A non-empty legacy meta body fails open with
`ErrLegacyFormatRebuildRequired`; recovery does not reinterpret or migrate it.

### 3.1 Durable meta body V1

The 104-byte body after the common page header is:

| Body bytes | Encoding | Meaning |
| --- | --- | --- |
| `0:8` | bytes | magic `TDMETV1\0` |
| `8:10` | u16 LE | format version, `1` |
| `10:12` | u16 LE | projection version, `1` |
| `12:14` | u16 LE | body size, `104` |
| `14:16` | zero | reserved |
| `16:24` | u64 LE | `CommitSeq` |
| `24:32` | u64 LE | `DurableSeq` |
| `32:40` | u64 LE | `RootRecordPageID` |
| `40:72` | bytes | SHA-256 meta-projection digest |
| `72:104` | bytes | SHA-256 durable-root-record digest |

The projection digest hashes the magic, versions, body size, reserved bytes,
`CommitSeq`, `DurableSeq`, and `RootRecordPageID`. It deliberately excludes the
root-record digest: the root record stores the projection digest, its digest
then binds the record, and the meta stores that record digest without a hash
cycle.

`CommitSeq` is the latest visible commit covered by this root. `DurableSeq` is
the contiguous durable-publication generation within the current lineage. A
grouped publication may therefore advance `CommitSeq` by several commits while
advancing `DurableSeq` by exactly one.

### 3.2 Durable-root record V1

A durable-root record is one checksummed `0x0a` page. Bytes `16:384` are:

| Page bytes | Encoding | Meaning |
| --- | --- | --- |
| `16:24` | bytes | magic `DROOTV1\0` |
| `24:26` | u16 LE | version, `1` |
| `26:28` | u16 LE | header size, `384` |
| `28:32` | zero | reserved |
| `32:40` | u64 LE | `CommitSeq` |
| `40:48` | u64 LE | `DurableSeq` |
| `48:56` | u64 LE | user root page ID |
| `56:64` | u64 LE | system root page ID |
| `64:72` | u64 LE | durable total-page extent |
| `72:80` | u64 LE | maximum entry revision |
| `80:88` | u64 LE | applied command-WAL LSN |
| `88:96` | u64 LE | last commit height |
| `96:128` | four u64 LE | COW freelist header, generation, commit, and high-water IDs |
| `128:160` | bytes | SHA-256 COW freelist digest |
| `160:176` | two u64 LE | freelist free and retired counts |
| `176:192` | two u64 LE | dependency-manifest first page and byte length |
| `192:200` | two u32 LE | dependency-manifest entry and page counts |
| `200:232` | bytes | SHA-256 dependency-manifest digest |
| `232:248` | two u64 LE | parent record page ID and commit sequence, or zero |
| `248:280` | bytes | parent record digest, or zero |
| `280:312` | bytes | meta-projection digest |
| `312:344` | bytes | SHA-256 durable-root-record digest |
| `344:384` | zero | reserved |

Bytes `384:4096` are zero. The record digest is SHA-256 over the complete page
with both the common page checksum and record-digest fields cleared. The common
page checksum is then computed normally. The optional parent tuple identifies
the previous independently recoverable generation. Recovery reads at most that
one parent record, verifies its exact page/commit/digest binding and contiguous
durable-publication sequence, and rejects a child whose applied command-WAL LSN regresses
below the parent's frontier. The live publisher separately proves contiguous
command-WAL coverage before encoding the child. Recovery never follows the
parent recursively, so this lineage check does not authorize an unbounded
recovery walk.

`AppliedCommandLSN` and `MaxEntryRevision` are therefore selected with the
exact roots that contain their effects. No sidecar, format marker, or padded
meta bytes may supply either value independently.

### 3.3 Dependency manifest V1

The manifest is a deterministic payload split across contiguous checksummed
`0x09` pages. Each page uses this 96-byte header:

| Page bytes | Encoding | Meaning |
| --- | --- | --- |
| `16:24` | bytes | magic `DPMPGV1\0` |
| `24:26` | u16 LE | version, `1` |
| `26:28` | u16 LE | header size, `96` |
| `28:32` | u32 LE | zero-based page index |
| `32:36` | u32 LE | page count |
| `36:40` | u32 LE | manifest entry count |
| `40:48` | u64 LE | complete payload byte length |
| `48:56` | u64 LE | next page ID, or zero |
| `56:88` | bytes | SHA-256 complete-payload digest |
| `88:92` | u32 LE | this page's payload length |
| `92:96` | zero | reserved |
| `96:4096` | bytes | payload chunk, then zero padding |

The payload begins with `DPMANV1\0`, version `1`, header size `16`, and an
entry count. Each entry is length-prefixed and canonically sorted by its encoded
bytes. It binds the resource kind and logical lane; resource ID and diagnostic
path; platform/volume/object/generation identity; resource generation and
digest; durable byte/LSN/RID frontiers; root reachability fields; logical
obligations; and, when present, the retained-parent namespace identity and
create/rename obligation. Duplicate or conflicting entries fail closed.

Manifest validation is bounded by its declared 64 MiB maximum, byte length,
page count, contiguous page IDs, per-page checksums, complete-payload digest,
and deterministic re-encoding. The selected manifest is the exact external
resource closure that must remain present for that root generation.

Recovery decodes the two physical meta slots independently and attempts
checksum-valid candidates in descending commit order. Two byte-identical metas
for the same commit count as one recoverable generation, not two; the second is
rejected as a mirror so the next normal alternate-slot publication restores a
distinct fallback. Two different roots claiming the same commit sequence are a
split-brain format error and both are rejected. Recovery does not rewrite or
repair either slot while opening.

Raft snapshot installation is the explicit relocation exception. Archive
extraction recreates external files with target-replica identities, so the
installer rebinds each side-store index first, then re-encodes the main slots'
manifests with those final side-store identities. Rebind mutates a synced
sibling copy of each index and installs it with an atomic rename plus exact
parent-directory sync only after both rebound metas are stable; a failure
before that install leaves the original index unchanged. Root-record lineage
and meta digests are updated, but both distinct generations are preserved. The
scratch copy is then opened through normal bounded recovery, and any later file
replacement still fails exact identity validation. An ordinary filesystem copy
does not perform this rebind and remains unrecoverable when a selected manifest
binds copied external dependencies.

### 3.4 Publication order and ownership

One synchronous publication executes in this order:

1. validate the candidate and capture its exact dependency closure,
2. flush and sync those external dependencies,
3. materialize the COW freelist, dependency manifest, and durable-root record,
4. sync the exact candidate index file through its retained handle,
5. write the alternate meta slot exactly once,
6. sync that exact meta page through the same stable index handle,
7. install visible state and only then advance frontiers or release overwritten
   generation ownership.

Dependency, index, and meta syncs run outside DB, write, commit, and root-build
locks. The narrow root-reuse admission fence remains exclusively held: it is
not a root-construction lock, and it prevents a new reader from capturing the
old visible generation after reuse eligibility has been sampled. The fence is
released only after the durable root, allocator generation, and visible root
agree. A failure before the target meta is first mutated leaves the prior meta
authoritative and retains exact candidate ownership for retry. From the first
target-meta mutation onward, an error is ambiguous and poisons the live handle;
the process must reopen and run bounded selection.

The active stable-I/O adapters are explicit and fail closed:

| Platform | Mapped/index file barrier | Namespace barrier | Contract result |
| --- | --- | --- | --- |
| Linux | `fdatasync` for pager index publication; `fsync` for general stable files | `fsync` on the retained directory handle | supported when the syscalls succeed |
| FreeBSD, NetBSD, OpenBSD | aligned `msync(MS_SYNC)`, then `fsync` on the retained file | `fsync` on the retained directory handle | supported when the syscalls succeed |
| Darwin | aligned `msync(MS_SYNC)`, then `F_FULLFSYNC` on the retained file | `fsync` on the retained directory handle | unsupported errors are typed and fail before activation |
| Windows | `FlushViewOfFile`, then `FlushFileBuffers` on the retained file | append-only create/open uses `NtCreateFile` relative to a delete-sharing retained parent and `FlushFileBuffers` on the exact child; rename, remove, and generic parent-directory sync are unsupported | create-only obligations are supported; broader namespace mutation returns a typed unsupported result |
| Other targets | no asserted stable-file primitive | no asserted stable-namespace primitive | typed unsupported result; durable-root activation is rejected |

These are ordering primitives, not evidence that a particular device honored
volatile-cache persistence. Deterministic oracle events describe the requested
barriers and the resulting modeled stable image.

- New `command_wal_v2` directories start with `AppliedCommandLSN=0`.
- Updating `AppliedCommandLSN` without selecting the roots that contain those
  command effects is invalid.
- Selecting roots that contain command effects without the matching
  `AppliedCommandLSN` is invalid for durable root publish/checkpoint state.
- Required feature validation must fail closed before full `command_wal_v2`
  execution is enabled if a command-WAL directory is opened by code that decodes
  only the 60-byte pre-command-WAL meta body.
- `format.json` must use version 3 or newer when `required_features` contains
  `command_wal_v2`; putting required features in version 2 is invalid because
  older binaries would ignore unknown JSON fields and fail open.
- Meta-page tests cover `AppliedCommandLSN` encode/decode, durable-meta
  magic/version/body gating, alternating meta pages, paired root/frontier
  selection, and checksum validation over the complete durable body.

## 3.1.1 R3a Apply Metadata Logs

Durable R3a apply stores persist metadata beside the future Raft node state, not
inside `index.db`, `wal/`, or the value log. For the single-group Raft layout the
directory is:

```text
raftcluster/nodes/<node-id>/groups/<group-id>/apply/
```

The default file names are:

- `apply-progress-v1.log` for `raftapply.ApplyProgressStore`;
- `apply-results-v1.log` for `raftapply.ApplyResultStore`.

The parent path is intentionally narrow so the #3044 storage boundary can place
it under the node/group directory without changing the record format. These
files are part of the local Raft group storage set for backup/restore purposes,
but they are not part of the local command WAL and are not consensus-log bytes.
They record the applied-index and idempotency/result replay metadata that lets a
later Raft FSM recover after restart.

Each file starts with a 20-byte header:

```text
bytes[8] Magic     = "TDBR3A1\n"
u32      Version   = 1
u16      Kind      = 1 for progress, 2 for results
u16      HeaderLen = 20
u32      HeaderCRC = CRC-32/IEEE over bytes 0..15
```

Each appended frame has a 16-byte header followed by the payload:

```text
bytes[4] Magic      = "R3AF"
u16      Kind       = file kind
u16      Version    = 1
u32      PayloadLen
u32      FrameCRC   = CRC-32/IEEE over the first 12 header bytes plus payload
bytes    Payload
```

Progress payload:

```text
u64       ApplyTerm
u64       ApplyIndex
bytes[32] CommandDigestV1
u64       AppliedCommandLSN
bytes[32] LogicalDigestV1 // logical DB digest at this apply boundary
```

Result payload:

```text
u64       ApplyTerm
u64       ApplyIndex
bytes[32] CommandDigestV1
u64       AppliedCommandLSN
u64       IdempotencyKeyLen
bytes     IdempotencyKey
u64       ResultStatusLen
bytes     ResultStatus
bytes[32] ResultCommandDigest
u64       DeterministicErrorCodeLen
bytes     DeterministicErrorCode
i64       AffectedCount
i64       MatchedCount
bytes[32] ResultDigest   // LogicalDigestV1 bytes when apply succeeded
bytes[32] ProgressLogicalDigestV1 // logical digest to repair missing progress
```

New v1 writers include `MatchedCount`. V1 decoders MUST also accept legacy
records where `ResultDigest` follows `AffectedCount` directly; those records
decode with `MatchedCount=0`.

Open-time recovery scans complete frames and rebuilds in-memory lookup indexes.
Frame truncation, checksum mismatch, unsupported file or frame versions, kind
mismatches, malformed payloads, same-`ApplyEntryID` different-digest conflicts,
same-idempotency different-digest conflicts, non-increasing progress indexes,
and term-regressing progress records MUST fail closed. Gaps between progress
indexes MAY represent committed non-command Raft log entries. A duplicate
same-`ApplyEntryID`/same-digest frame MAY be treated as an idempotent append
retry and ignored after the first record.

Accepted records are appended after the store interface's preflight checks pass.
By default each accepted frame is fsynced before the in-memory store state is
advanced; test and benchmark code may explicitly disable that sync to isolate
CPU/allocation costs. New-file header creation also fsyncs the file and, where
supported, the parent directory. TreeDB is pre-alpha, so later binaries may
reject older metadata versions instead of migrating them.

## 3.2 Collection Document Payloads

### 3.2.1 BSON-ordered scalar secondary-index keys (v2)

An index whose persisted metadata selects `bson-ordered-v2` is valid only for a
BSON collection. Metadata version 6 stores an optional ordered `components`
list of one through four `{field,direction}` members; direction is exactly `1`
or `-1`. An explicit ordered definition persists `components`. A field-only
legacy definition omits `components` on disk and a decoder treats it as one
ascending component named by `field`.
Its ordered-root entry key is the concatenation of those frozen BSON v2 scalar
components followed by one explicit BSON v2 document-ID suffix. The components
are the self-delimiting `0xb2` ascending or `0x4d` descending encodings defined
in `bson-index-key-codec-v2.md`; the suffix has its own marker, zero escaping,
and terminator. It is not a scalar component and does not participate in scalar
ordering. The suffix makes otherwise equal compound scalar keys unique per
document.

Missing fields and BSON null have distinct scalar components. Numerically equal
Int32, Int64, Double, and Decimal128 values have the same scalar component.
Unsupported BSON scalar values fail before a document or index mutation is
published. Compound components reject arrays rather than expanding multikey
entries. Unique indexes consequently treat equal normalized numeric values
as conflicts, while missing and null follow their distinct-key policy.

The persisted index `value_type` metadata, not a byte prefix or any inference
from entry bytes, selects both component and document-ID decoding and the range
bound construction. A v2 decoder never falls back to the legacy typed format,
and a legacy decoder never attempts v2 bytes. Wrong collection document format,
unknown value type, malformed/truncated v2 component or suffix, invalid escape,
or metadata/key mismatch MUST fail closed at create/open/recovery or query time
before returning a partial index result. Existing typed-v1 index metadata and
bytes remain on their legacy path.

Collection document payload encodings are defined separately in
`TreeDB/docs/spec/collections-document-formats.md`. In particular,
template-v1 collections store compact `TD1D` primary documents and persist the
template ID map in the collection-local `<collection>/templates` ordered root.

Column-store collections using the non-column retained-payload policy default to
`semantic-stream-v1`. Insert-batch publication stores compact semantic-stream
locators in the primary collection root and writes retained scalar streams into
the collection-local retained semantic-stream side root. Explicit
`retained_payload_encoding: "template-v1"` remains a supported compatibility
encoding for non-column retained payloads.

Semantic-stream-v1 retained payload bytes are durable ordered-root values. All
integer varints below use Go `binary.PutUvarint` encodings.

Primary collection root value:

```text
bytes[9]   Magic = "crss1loc\0"
bytes[32]  BlockKey = SHA-256 of the stored side-root block value bytes
uvarint    Row = zero-based row ordinal inside that block
```

Retained semantic-stream side root:

```text
Root name: <collection>/retained/semantic-stream-v1
Key:       BlockKey
Value:     StoredBlock
```

The side-root block key is the SHA-256 digest of the exact stored block value,
not necessarily the decoded raw block. Blocks contain at most 4096 retained
documents; single-row insert/update batches still produce one side-root block
and one primary locator per retained document.

Column-store publications also atomically maintain a mutation-safe physical row
locator root:

```text
Root name: <collection>/column/row-locator
Key:       exact primary document ID bytes
Value:
  bytes[4]   Magic = "CRL1"
  u64 BE     typed asset generation
  u64 BE     typed asset part ID
  u64 BE     zero-based row index
  u64 BE     applied command LSN
```

The value is exactly 36 bytes. Insert and replace publications co-publish the
primary root, typed manifest, and locator root so one snapshot cannot observe a
primary value with a locator from another generation. Delete publications write
an ordered-root tombstone for the document ID. Column asset compaction
atomically republishes the manifest and all live locators because it rewrites
their physical generation and row coordinates. Unknown magic, the wrong value
length, an overflowing row index, or invalid physical coordinates fail closed.

Raw side-root block value:

```text
bytes[9]  Magic = "crss1blk\0"
uvarint   RowCount
uvarint   PathCount

repeated PathCount times:
  uvarint   SegmentCount
  repeated SegmentCount times:
    uvarint   SegmentByteLength
    bytes     UTF-8 JSON object path segment
  uvarint   EntryCount
  repeated EntryCount times:
    uvarint   RowDelta
    uvarint   ValueByteLength
    bytes     Raw retained JSON scalar/object/array bytes for that path row
```

Paths are sorted by their dot-joined path key. Entries within each path are
sorted by row; the first `RowDelta` is the absolute row and subsequent
`RowDelta` values are deltas from the previous row. Retained root documents must
be JSON objects after declared column paths are removed.

Stored side-root block value:

```text
bytes[9]  Magic = "crss1blk\0"
...       Raw block body above
```

or, when compression is smaller and the raw block is within the implementation
compression limit:

```text
bytes[9]  Magic = "crss1zst\0"
uvarint   DecodedRawBlockByteLength
bytes     zstd frame for the complete raw "crss1blk\0" block
```

Decoders must accept both raw `crss1blk\0` blocks and compressed `crss1zst\0`
wrappers. A `crss1zst\0` wrapper decodes to one complete raw block, and the
decoded byte length must exactly match `DecodedRawBlockByteLength`.

## 3.3 Collection Text Index Root Payloads

Collection text-index roots are ordinary ordered roots whose keys and values are
stored inline in B-tree leaves or, when the root storage policy selects it, as
existing value-log/split-leaf-log pointer-backed leaf values. Text roots do not
own a separate physical file class or GC domain.

The explicit-v1 text root payload contract is documented in
`collection-text-search.md`. The default-v2 production contract, v1 compatibility
boundary, and rollout rules are documented in `collection-text-v2-contract.md`;
this section records the canonical durable key/value bytes that have landed for
the M2 posting-block family. All integer varints below are Go `binary.PutUvarint` encodings unless
noted otherwise.

Text-v2 posting-block root name:

```text
<collection>/text-v2-posting-blocks/<indexName>
```

Posting-block keys sort by term, then first ordinal, then block identity:

```text
u8  KeyVersion = 2
u8  KeyKind    = 0x21  // text-v2 posting block
uvarint TermLen
bytes Term[TermLen]
u64 BlockStartOrdinalBE
u64 BlockIDBE
```

`BlockStartOrdinalBE` and `BlockIDBE` are non-zero. The key prefix through the
term bytes is the range-scan prefix for a single term. `BlockStartOrdinalBE` is
the first document ordinal stored in the value. `BlockIDBE` is non-zero and lets
future delta/micro-block writers and rewrite tooling publish multiple ordinary
root records for the same high-document-frequency term without rewriting one
large value per mutation.

Posting-block values contain scoring postings only; positions and offsets are
absent and belong to a separate positions lane if enabled by a later format.

```text
u8      ValueVersion  = 2
uvarint FormatVersion = 1
u8      BlockKind     // 1=sealed, 2=delta, 3=micro
u8      Flags         // currently 0
uvarint BlockStartOrdinal
uvarint BlockID

// exact summary / upper-bound metadata
uvarint FirstOrdinal
uvarint LastOrdinal
uvarint DocCount
uvarint MaxTermFrequency
uvarint FieldCount
uvarint MaxFieldTermFrequency[FieldCount]
u8      UpperBoundKind // 1=BM25F lane maxima
uvarint EntryCount

// repeated EntryCount times
uvarint OrdinalDelta  // from previous ordinal; previous starts at BlockStartOrdinal-1
uvarint Generation
u8      EntryFlags    // currently 0
uvarint TermFrequency
uvarint FieldTermFrequency[FieldCount]

u32 PayloadCRC32BE // IEEE CRC-32 over all preceding value bytes, including ValueVersion
```

`ValueVersion=1` blocks from the M2/M4 pre-M5 format did not carry the trailing
checksum. Current decoders can still read them for exact exhaustive validation,
but M5 block-max skipping requires the checksum-protected `ValueVersion=2`
payload before trusting summary bytes for skip decisions.

Required validation/fail-closed invariants:

- `FormatVersion`, `ValueVersion`, key version, known kinds, and zero flags must
  be checked before use.
- `BlockStartOrdinal`, `BlockID`, `FirstOrdinal`, `LastOrdinal`, `DocCount`,
  `EntryCount`, and every entry `Generation`/`TermFrequency` are non-zero.
- key `(BlockStartOrdinal, BlockID)` must match the value identity.
- `FirstOrdinal == BlockStartOrdinal`, ordinals are strictly increasing, and a
  builder must reject duplicate document ordinals globally before splitting
  postings into blocks so duplicates cannot straddle block boundaries.
- `DocCount == EntryCount` and must not exceed the implementation maximum
  posting count per block.
- `FieldCount` is the text index field count. Every posting has exactly that
  many field-frequency lanes, and the lane sum equals `TermFrequency`.
- `PayloadCRC32BE` must match the preceding value bytes before a reader may use
  summary metadata for block-max skip decisions.
- `MaxTermFrequency`, `MaxFieldTermFrequency`, first/last ordinal, and doc count
  must exactly summarize the decoded entries.
- `UpperBoundKind=1` records per-field lane maxima that are admissible BM25F
  upper-bound inputs for non-negative BM25F weights. A future scorer that cannot
  compute/validate an admissible block bound for a query must treat the block as
  unskippable and score exhaustively.
- Unsupported versions, checksum mismatches, flags, kinds, malformed varints,
  trailing bytes, key/value identity mismatches, field-count mismatches, corrupt
  summaries, or status generation/ordinal bounds violations make the root
  corrupt for text-v2 inspection and must fail closed.

Text-v2 optional positions root name:

```text
<collection>/text-v2-positions/<indexName>
```

The positions root is format-only unless the text index was created with
`StorePositions=true`. It remains an ordinary ordered root using the text index
root storage policy; inline values and value-log pointer-backed values are
managed by normal TreeDB root/value-log maintenance. Posting scoring blocks do
not contain positions or offsets.

Position keys identify one current-or-historical document ordinal and term:

```text
u8  KeyVersion = 2
u8  KeyKind    = 0x22  // text-v2 position/detail payload
u64 OrdinalBE
uvarint TermLen
bytes Term[TermLen]
```

Position values are versioned and fail closed. New writers use value version 2;
readers still accept legacy value version 1 for pre-alpha fixtures.

Version 2 omits the duplicate term string already present in the position key,
retains an independent key/value term binding via the exact term length plus a
40-bit SHA-256 fingerprint, and delta-codes strictly increasing positions:

```text
u8      ValueVersion  = 2
uvarint FormatVersion = 1
uvarint Ordinal
uvarint Generation
uvarint TermLen
bytes[5] TermFingerprint40  // first 5 bytes of SHA-256(term)
uvarint FieldEntryCount

// repeated FieldEntryCount times, sorted by field index
uvarint FieldIndex
uvarint FieldTermFrequency
uvarint PositionCount
uvarint FirstPosition
uvarint PositionDelta[PositionCount-1]
uvarint OffsetCount
repeated OffsetCount: uvarint Start, uvarint End
```

Legacy version 1 values include the full term string and absolute positions:

```text
u8      ValueVersion  = 1
uvarint FormatVersion = 1
uvarint Ordinal
uvarint Generation
uvarint TermLen
bytes   Term[TermLen]
uvarint FieldEntryCount

// repeated FieldEntryCount times, sorted by field index
uvarint FieldIndex
uvarint FieldTermFrequency
uvarint PositionCount
uvarint Positions[PositionCount]
uvarint OffsetCount
repeated OffsetCount: uvarint Start, uvarint End
```

Validation invariants:

- key/value ordinal and term must match, and ordinal/generation must be non-zero
  and within the text-v2 status snapshot;
- field indexes are strictly increasing and within the index field list;
- `FieldTermFrequency` is non-zero, `PositionCount == FieldTermFrequency`, and
  positions are strictly increasing within each field;
- offsets are absent when `StoreOffsets=false`, or have exactly one
  non-negative `end >= start` offset per position when `StoreOffsets=true`;
- detailed/compact materialization validates only returned final results, while
  score-only search does not read this lane.

Unsupported versions, malformed varints, trailing bytes, missing final-result
payloads for positions-enabled indexes, generation/key/value mismatches, field
frequency mismatches with the scoring posting, corrupt positions/offsets, or
entries present for an index without `StorePositions` make the text-v2 positions
lane corrupt and must fail closed when inspected or when detailed materialization
needs the affected payload.

### Text-v2 rewrite/merge records

Text-v2 rewrite/merge maintenance does not introduce another durable file or GC
format. `Collection.RewriteTextIndex` publishes ordinary ordered-root deltas to
existing v2 roots:

- old posting-block keys are deleted or overwritten in
  `<collection>/text-v2-posting-blocks/<indexName>`;
- retained live postings are written as sealed posting-block values with the
  same key/value format above;
- term `PostingBlockCount` values are updated or removed in
  `<collection>/text-v2-terms/<indexName>`;
- deleted-document docID/docmap/norm tombstones can be removed from their normal
  roots after stale postings are gone;
- `<collection>/text-v2-generations/<indexName>` receives a new status record
  with an advanced root/term and, when tombstones are purged, docmap/norm
  generation.

Consequently, old inline values or `value_vlog`/leaf-log pointer-backed payloads
are reclaimed only by normal TreeDB reachability and maintenance after snapshots
that can still see the old roots have released. There is no text-block GC
manifest, side file, or asset namespace.

## 4. Value Pointer Encoding (`page.ValuePtr`)

Base struct:

```text
u64 Offset
u32 Length
u32 FileID
```

Semantics:

- `FileID` identifies the value-log segment.
- `Offset` points to `record_start + 4` (immediately after record CRC field).
- `Length` stores record length hint plus packed flags.

### 4.1 FileID layout

`FileID` uses a marker bit and packed segment id:

- bit 31 set => value-log pointer (`IsValueLogFileID=true`),
- remaining 31 bits encode `(lane, seq)`:
  - lane bits: 8
  - sequence bits: 23

Limits:

- max lane id: 255
- max seq per lane: 8,388,607

### 4.2 `Length` bit packing

Flags:

- compressed marker bit: `0x80000000`
- grouped marker bit: `0x40000000`

Grouped pointer sub-index bits:

- low portion: bits `29..26`
- extra bit: bit `31` contributes one sub-index bit
- high portion: bits `25..24`
- top portion: bit `23`
- total encoded sub-index range: `0..255`

Record-length hint:

- grouped pointers keep a best-effort 23-bit record-length hint.
- max encodable grouped record length hint: `0x007fffff`.
- if record is larger, hint is set to zero and reader uses record header length fields.

### 4.3 Packed on-disk `ValuePtr`

Some leaf encodings optionally store packed pointer payloads:

```text
u32 Offset32
u32 Length
u32 FileID
```

- Packed size: 12 bytes.
- Requires offset to fit in `u32`.

## 5. Leaf Page Encodings

All leaf pages use slotted-page mechanics (header + directory + payload heap).

Leaf encoding flags in page header:

- `0x8000`: leaf prefix compression enabled
- `0x4000`: leaf columnar encoding enabled
- `0x2000`: prefix v2 compact header enabled
- `0x1000`: packed `ValuePtr` payload enabled
- `0x0400`: columnar v2 layout enabled
- `0x0040`: native per-entry `EntryRevision` metadata enabled

Entry flags in payload (node flags) include inline/pointer/tombstone semantics.

### 5.1 Plain leaf entry

```text
u16 KeyLen
u32 ValueLen        // ignored for pointer entries
u8  EntryFlags
bytes Key
bytes Value | ValuePtr(16) | PackedValuePtr(12)
u64 EntryRevisionLE // present only when page flag 0x0040 is set
```

### 5.2 Prefix-compressed leaf v1

```text
u16 SharedPrefixLen
u16 SuffixLen
u32 ValueLen
u8  EntryFlags
bytes KeySuffix
bytes Value | Pointer
u64 EntryRevisionLE // present only when page flag 0x0040 is set
```

### 5.3 Prefix-compressed leaf v2

```text
u8 SharedPrefixLen8
u8 SuffixLen8
u8 EntryFlags
(optional) u16 SharedPrefixLen16 + u16 SuffixLen16 when both 8-bit lengths are 0xFF
(optional) uvarint ValueLen for inline non-tombstone values
bytes KeySuffix
bytes Value | Pointer
u64 EntryRevisionLE // present only when page flag 0x0040 is set
```

Notes:

- Pointer/tombstone entries omit inline value length field.
- Restart interval for prefix reconstruction: 16 entries.

### 5.4 Columnar leaf encodings

Columnar v1 stores a compact per-entry header and keeps the key after the
entry's visible value/pointer bytes:

```text
u16 KeyLen
u32 ValueLen        // ignored for pointer entries
u8  EntryFlags
bytes Value | ValuePtr(16) | PackedValuePtr(12)
bytes Key
u64 EntryRevisionLE // present only when page flag 0x0040 is set
```

Columnar v2 stores per-entry metadata in top-of-page columns:

```text
u16 KeyOff[Count]
u16 ValOff[Count]
u8  Flags[Count]
u64 RevisionLE[Count] // present only when page flag 0x0040 is set
bytes ValueBlob
bytes KeyBlob
```

### 5.5 Columnar + prefix v2

```text
u16 KeyOff[Count]
u16 ValOff[Count]
u8  Flags[Count]
u16 PrefixLen[Count]
u64 RevisionLE[Count] // present only when page flag 0x0040 is set
bytes ValueBlob
bytes KeySuffixBlob
```

Keys reconstruct using previous key prefix within restart blocks.

### 5.6 Target raw-KV entry revisions

Target entry revisions are native entry metadata for raw key/value leaves.
They are enabled per leaf by page header flag `0x0040`. When the flag is clear,
all entries in that page decode as revision `0`. When the flag is set, every
entry in the page carries one fixed-width little-endian `u64` revision:

- plain, legacy-prefix, prefix-v2, and columnar-v1 leaf entries store the
  revision directly after the visible key/value or pointer bytes for that entry;
- columnar-v2 leaves store `RevisionLE[Count]` immediately after `Flags[Count]`;
- columnar+prefix-v2 leaves store `RevisionLE[Count]` immediately after
  `PrefixLen[Count]`.

- A live raw-KV entry may carry an `EntryRevision` token. Older entries without
  revision metadata decode as revision `0` until the format gate requires
  revision support. Revision `0` is a legacy/no-revision sentinel and is not a
  valid assignment for new mutations once versioned reads are advertised.
- Revision metadata must be stored with the visible entry data path. A per-write
  system-root sidecar, separate persistent revision map, or adapter-private
  metadata tree is not an accepted storage format for this feature.
- The stored revision is the mutation revision assigned from the directory's
  shared raw-KV revision domain. The active write authority may use command-WAL
  LSN, backend commit sequence, cached mutation sequence, or future Raft apply
  identity only if that source is seeded above the durable revision floor
  selected with the current roots. Leaf readers decode the stored token; they do
  not infer it from page position, file offset, or a second root.
- The format must persist the raw-KV revision floor/`MaxEntryRevision` in the
  same root/meta selection that publishes the entries covered by it. This is
  directory metadata, not a per-write sidecar tree.
- Inline values and value-log pointer entries must both carry revisions without
  exposing revision bytes through ordinary `Get`, iterator, or `GetMany` value
  APIs.
- Tombstones and range deletes must hide older revisions. A delete followed by a
  reinsert creates a later live revision.
- Leaf split, rebuild, prefix compression, columnar encoding, packed-pointer
  encoding, bulk/cold build, compact/vacuum, and split leaf-log storage must
  preserve revision metadata for live entries.
- Readers must validate revision metadata bounds before slicing or allocating.
  Unsupported revision encodings, impossible lengths, truncated metadata, and
  inconsistent pointer/value layout fail closed as page corruption.
- The revision lookup path for a visible entry must be the same leaf/memtable
  lookup that found the value. A second ordered-root lookup is not allowed for
  the target API because it regresses raw write/read performance.

## 6. Internal Page Encodings

Internal encoding flags:

- `0x0800`: base-delta enabled
- `0x0200`: delta width `u16` (otherwise `u32`)
- `0x0100`: exact fence bounds persisted

### 6.1 Plain internal entry

```text
u16 KeyLen
u64 ChildPageID
bytes Key
```

### 6.2 Base-delta internal entry

```text
u16 SuffixLen
u16|u32 ChildDelta
bytes KeySuffix
```

With footer payload:

```text
bytes lowFence
bytes highFence
bytes sharedPrefix
u16 lowLen
u16 highLen
u16 prefixLen
u64 baseChildID
```

Child page id reconstructs as `baseChildID + ChildDelta`.

### 6.3 Leaf-Log Child Refs (IndexOuterLeavesInValueLog)

When `Options.IndexOuterLeavesInValueLog` is enabled, B+Tree leaf pages are
stored as persistent value-log records instead of pager pages in `index.db`.
Internal pages still live in `index.db`.

Internal pages that point at leaf-log records use an explicit child-ref entry
layout:

```text
u16 keyLen
u32 fileID
u64 offset
u32 recordLengthHint
u16 subIndex
bytes key
```

Notes:

- Roots remain normal pager page IDs; a single leaf-log leaf is represented by a
  one-child internal root page.
- `recordLengthHint` is best-effort. A zero hint means readers should consult
  the value-log record header.
- `subIndex` identifies the leaf page within a grouped value-log frame.
- Current builders keep leaf-log children on internal pages that contain only
  leaf-log child refs. Base-delta page-child encoding is disabled for these
  pages.

## 7. Value-Log Record Format

Each value-log record is:

```text
u32 CRC32
u8  Version         // currently 1
u8  Flags           // bit0 = grouped record
u16 Reserved
u64 RID             // 0 for grouped container record
u32 ValueLen
bytes Payload[ValueLen]
```

`Offset` in `ValuePtr` points to header byte after CRC (`Version`).

### 7.1 Grouped payload frame format

When grouped flag is set, payload starts with frame header:

```text
u8  FrameVersion    // currently 1
u8  FrameFlags      // bit0 = compressed
u8  K               // 1..255
u8  Reserved        // block codec id for compressed block frames with dictID=0
u64 DictID
u64 RID[K]
u32 Offsets[K+1]
bytes FramePayload
```

- `Offsets` are monotonic and define raw value slices.
- If `FrameFlags` indicates compression, frame payload is decoded first.
- `DictID` selects dictionary for dict-compressed payloads.
- For compressed non-dictionary block frames (`DictID=0`), `Reserved` stores
  the block codec id:
  - `1`: Snappy
  - `2`: LZ4
  - `3`: Zstandard
  Readers must fail closed on unknown non-zero block codec ids.

### 7.2 Compact split-leaf payload format

When TreeDB writes outer leaf pages into the split `leaf_vlog` directory, it
may store them in a compact canonical payload format instead of persisting the
entire raw `4096`-byte page image. This format is used only for split
`leaf_vlog` segments, not for generic lane-255 value-log files in `value_vlog`.

Compact payload layout:

```text
u8  Magic[8]        // 8a 4c 46 50 47 01 91 3c
u16 PrefixLenLE
u16 SuffixLenLE
bytes Prefix[PrefixLen]
bytes Suffix[SuffixLen]
```

Semantics:

- `Prefix` is the live byte range from the start of the page through the end of
  the top metadata/directory region.
- `Suffix` is the live byte range from the first heap byte through the end of
  the page.
- The bytes between them are the free gap of the slotted page and are omitted
  from the stored payload.

Reconstruction rules:

- decoder allocates a full `4096`-byte page buffer,
- copies `Prefix` at the start,
- copies `Suffix` at the end,
- zero-fills the omitted middle gap.

Encoding rules:

- only valid leaf pages may use this compact format,
- the canonical compact encoding zero-fills the omitted gap and recomputes the
  page checksum before storing `Prefix` and `Suffix`,
- if compact encoding is not smaller than the raw page, TreeDB stores the raw
  page payload instead.

#### Split-leaf generation manifest

`leaf_vlog/manifest.json` is a version-2 JSON document. In addition to the
current/next generation IDs and generation records, it contains the required
non-zero `manifest_revision`. The revision identifies the immutable physical
manifest replacement, not merely the logical current generation: every
successful rewrite increments it, including GC, reconcile, and offline-vacuum
rewrites that leave `current_generation_id` unchanged.

Version 1 and version-2 documents with a zero revision are deliberately
incompatible in this pre-alpha format. Open fails with the typed incompatible
manifest error; TreeDB does not infer a revision or migrate the file in place.
Empty, truncated, malformed, or structurally invalid documents also fail open.

On platforms with retained-directory relative operations, stable replacement
writes and syncs one temporary file through its exact handle, renames it
relative to the retained `leaf_vlog` directory, verifies the destination is
the same physical object, then syncs that exact directory once. The returned
stable resource token binds the SHA-256 digest of the exact version-2 bytes and
uses `manifest_revision` as its immutable generation. The replacement also
persists an immutable `manifest.durable.<revision>.json` revision. Durable-root
publication consumes that exact stable token in `DependencyManifestV1`; the
fixed `manifest.json` path is a compatibility view, not the selected revision's
identity.

If a prepared rewrite fails before publishing its meta slot, TreeDB abandons
only that unpublished immutable revision after releasing its durable resource
ownership. It verifies the exact physical identity before deletion and restores
the prior compatibility view only when the view still names the abandoned
revision. A later view wins. Any ambiguous restore or cleanup poisons the live
handle rather than guessing.

Platforms without a retained-parent relative rename capability fail the
strict stable operation before creating a temporary file. The compatibility
replacement path returns no stable token; durable-root publication must not
accept it as certifying the strict namespace durability contract and fails
closed when that namespace obligation is required.

### 7.3 Typed Asset Manager, TCPA Typed-Row Assets, and Typed-Column Parts

Production typed-storage physical data is stored in typed asset manager segments
under the compatibility `column_assets` directory. These assets are
value-log-shaped durable payloads, but they are not ordinary row `value_vlog`
values and they are not split-leaf `leaf_vlog` records. Manifest/control roots
can live in B-tree/root metadata; typed-row payloads, typed-column part payloads,
and derived accelerator payloads such as marks, dictionaries, locators, and
aggregate metadata belong under the isolated typed asset manager namespace.

A collection namespace such as `events/column-assets` maps to:

```text
Dir/maindb/column_assets/events/column-assets/
  assets/segments/segment-000001.tca
  assets/indexes/
  prepared/
  quarantine/
  tmp/
```

Segment file names use `segment-%06d.tca`. Durable manifest part records
store typed `ColumnAssetRef` values containing kind, namespace, generation,
part id, segment file id, offset, length, and checksum. Part record version 3
and newer carries a `part_role` lifecycle value: `base`, `delta`, or
`tombstone`; version 1/2 part records omit `part_role`. Part record version 4
appends a SortKey trailer after `part_role`: `u64 column_count`, then for each
column a manifest string column name and manifest string direction (currently
empty/ascending only). Version 1/2/3 part records have no SortKey trailer. Only
`tcs1_typed_column_part` records may publish a non-empty SortKey; readers and
rewrite tooling must preserve or skip this trailer by version.
Current part refs may use `tcs1_part_image` for compatibility typed-row/TCPA
assets or `tcs1_typed_column_part` for sectioned typed-column payloads,
including scalar columns plus vector/list/adjacency payload sections described
below.
`base` parts are complete insert/base spans, `delta` parts carry update rows
layered over the older visible set, and `tombstone` parts are typed-row delete
assets with no matching typed-column payload. GC/rewrite must enumerate these
refs from manifest/control roots and snapshots; it must not scan row documents
to discover typed-storage assets.

Compatibility typed-row physical part payloads use the `TCPA` envelope:

```text
u32      Magic = "TCPA"
u16      Version
string   Collection
string   Namespace
u64      Generation
u64      PartID
u64      AppliedCommandLSN
string   Operation        // insert, update, or delete
u64      SchemaHash
u64      ColumnCount
u64      RowCount
columns  declared column descriptors
rows     versioned row payloads
```

Version 2 row payloads are:

```text
bytes    RowID
bool     Deleted
values   declared column values when Deleted=false
```

Insert/update rows must have `Deleted=false` and exactly one value per declared
`typed_row_asset` column in the row asset. Delete/tombstone rows must have
`Deleted=true` and zero column values. For layouts with `typed_column_part`
owners, a `TCPA` row asset is still published for row IDs/tombstones and any
row-owned fields; the matching `tcs1_typed_column_part` for the same non-delete
generation contains authoritative scalar, fixed-dimension `float32_vector`,
Issue #1930 dense numeric vector, and non-null variable-width `uint32_list`
typed-column values keyed by row index.

Version 7 has a compact row-locator encoding for the common
`typed_column_part` case where the row asset has zero row-owned columns and all
row ids in the part have the same byte width:

```text
string   RowEncoding = "fixed_id"
u64      RowIDWidth
bytes    RowID[RowCount]    // contiguous fixed-width row ids
```

V7 stores no per-row length prefixes and no per-row deleted flags. Insert/update
parts are all live rows; delete/tombstone parts are all deleted rows. Assets with
row-owned columns, mixed-width row ids, or legacy payloads continue to use the
generic row payload.

Version 8 extends that zero-row-owned-column case for dense 8-byte big-endian
unsigned document id ranges:

```text
string   RowEncoding = "dense_id_range"
u64      BaseRowID
```

The row id for row index `i` is synthesized as `BaseRowID + i` encoded as
8-byte big-endian `uint64`. V8 stores no per-row id bytes and derives deleted
state from the asset operation, matching the V7 live/delete all-rows contract.

Latest-visible readers resolve document identity from the typed-row
row/tombstone assets first, then read the typed-column part for the winning
non-deleted generation+row. Readers validate namespace, generation, part id,
schema hash, declared column descriptors, length, role, operation, and checksum
before accepting an asset ref.

Exact aggregate metadata assets use `ColumnAssetRef.Kind =
tcs1_aggregate_metadata` and the `TCAM` envelope:

```text
u32      Magic = "TCAM"
u16      Version
string   Collection
string   Namespace
u64      Generation
u64      PartID
u64      AppliedCommandLSN
u64      SchemaHash
string   AggregateName
string   GroupColumn
string   ValueColumn
u64      PredicateCount
preds    declared exact predicate coverage
u64      Rows
u64      EntryCount
entries  grouped aggregate entries
```

Version 1 assets did not carry predicate coverage. Version 2 adds exact
predicate coverage for equality and bounded string `IN` predicates. Version 3
keeps the v2 predicate encoding and permits grouped `count` metadata with an
empty `ValueColumn`; `min`/`max` metadata still requires a value column. Version
4 adds an `Hour` field to each entry for `group-hour-count` metadata. Each
version 4 entry stores `Group`, `Hour`, `Count`, `Min`, and `Max`; grouped
count entries use `Hour=0`, populate `Count`, and leave `Min`/`Max` at zero.
Grouped-hour count entries populate `Group`, `Hour`, and `Count`. Readers only
accept an asset when collection, namespace, generation, part id, schema hash,
aggregate name, predicate coverage, group column, value column where required,
and aggregate kind match the requested physical query.

Typed-column part descriptor column type codes are currently:

| Code | Type string | Notes |
| ---: | --- | --- |
| 1 | `int64` | Signed integer scalar and default float bit-pattern carrier. |
| 2 | `low_cardinality_code` | String dictionary code carrier. |
| 3 | `bool` | Boolean bitpack/RLE carrier. |
| 4 | `float32_vector` | Fixed-dimension dense little-endian `float32` rows. |
| 5 | `adjacency_list` | Legacy/consumer-specific dense or explicit offsets-list adjacency compatibility. |
| 6 | `float32` | Native raw little-endian IEEE-754 scalar. |
| 7 | `float64` | Native raw little-endian IEEE-754 scalar. |
| 8 | `uint32_list` | Generic non-null offsets/value list primitive added by #1985. |
| 9 | `bytes` | Generic non-null opaque byte payload primitive added by #2010. |
| 10 | `int8` | #1929 non-null raw primitive scalar. |
| 11 | `uint8` | #1929 non-null raw primitive scalar. |
| 12 | `int16` | #1929 non-null raw primitive scalar. |
| 13 | `uint16` | #1929 non-null raw primitive scalar. |
| 14 | `int32` | #1929 non-null raw primitive scalar. |
| 15 | `uint32` | #1929 non-null raw primitive scalar. |
| 16 | `uint64` | #1929 non-null raw primitive scalar. |
| 17 | `float16` | #1929 storage-only raw IEEE binary16 bit payload. |
| 18 | `bfloat16` | #1929 storage-only raw bfloat16 bit payload. |
| 19 | `uint8_vector` | #1930 non-null row-major raw `uint8` dense vector. |
| 20 | `int8_vector` | #1930 non-null row-major raw `int8` dense vector. |
| 21 | `uint16_vector` | #1930 non-null row-major little-endian `uint16` dense vector. |
| 22 | `int16_vector` | #1930 non-null row-major little-endian `int16` dense vector. |
| 23 | `uint32_vector` | #1930 non-null row-major little-endian `uint32` dense vector; not adjacency. |
| 24 | `int32_vector` | #1930 non-null row-major little-endian `int32` dense vector. |
| 25 | `uint64_vector` | #1930 non-null row-major little-endian `uint64` dense vector. |
| 26 | `int64_vector` | #1930 non-null row-major little-endian `int64` dense vector. |
| 27 | `float16_vector` | #1930 non-null row-major little-endian raw IEEE binary16-bit dense vector. |
| 28 | `bfloat16_vector` | #1930 non-null row-major little-endian raw bfloat16-bit dense vector. |
| 29 | `float64_vector` | #1930 non-null row-major little-endian IEEE-754 `float64` dense vector. |
| 30 | `fixed_bytes` | #1931 fixed row-byte payload for `byte_vector`. |
| 31 | `packed_bit_vector` | #1931 packed 1-bit unsigned code vector. |
| 32 | `packed_uint2_vector` | #1931 packed 2-bit unsigned code vector. |
| 33 | `packed_uint4_vector` | #1931 packed 4-bit unsigned code vector. |

`uint32_list` descriptors must use `raw_uint32_offsets_list` encoding,
`fixed_width_elements=0`, and uncompressed split offsets/value sections. `bytes`
descriptors must use `raw_bytes_offsets`, `fixed_width_elements=0`, and
uncompressed split offsets/value sections whose values bytes are exact opaque
payload bytes rather than text. Primitive scalar descriptors must use their
matching `raw_*` encoding and `fixed_width_elements=0`; their `column_data`
sections may request Snappy, LZ4, or plain Zstandard compression while
individual codec blocks record the actual kept compression, including raw
keep-if-smaller fallback.
Dense numeric vector descriptors added
by #1930 must use their matching raw vector encoding (`raw_uint8_vector`,
`raw_int8_vector`, `raw_uint16_vector`, `raw_int16_vector`,
`raw_uint32_vector`, `raw_int32_vector`, `raw_uint64_vector`,
`raw_int64_vector`, `raw_float16_vector`, `raw_bfloat16_vector`, or
`raw_float64_vector`), positive `fixed_width_elements`/`elements_per_row`, and
uncompressed row-major payload sections (`rows * elements_per_row * width`
bytes). Fixed-byte descriptors added by #1931 must use `raw_fixed_bytes`,
positive `fixed_width_elements`/`bytes_per_row`, `bits_per_element=0`, and
uncompressed row-major payload sections (`rows * bytes_per_row` bytes). Packed
code descriptors added by #1931 must use their matching raw packed encoding
(`raw_packed_bit_vector`, `raw_packed_uint2_vector`, or
`raw_packed_uint4_vector`), positive `fixed_width_elements`/`elements_per_row`,
matching `bits_per_element` (`1`, `2`, or `4`), zero unused high padding bits in
the final byte of each row, and uncompressed row-major payload sections
(`rows * ceil(elements_per_row * bits_per_element / 8)` bytes). Readers must
fail closed on unknown type codes rather than guessing a payload shape.
Current typed-column image version 4 directory entries carry per-section
`raw_bytes` metadata for compressed sections. Durable section compression codes
are `0` = none, `1` = Snappy, `2` = LZ4, and `3` = plain Zstandard. The
`zstd_dict` code (`4`) is not a supported durable section codec and readers must
fail closed when it appears. Row-locator, dictionary, and pruning-metadata
sections may use Snappy, LZ4, or plain Zstandard when the raw length is within
the section-specific decoder cap. Readers validate the declared raw byte count
before decompression; zstd decoders are additionally configured with a maximum
decoded-size/cap tied to that declared `raw_bytes` value so corrupt frames fail
closed before unbounded growth.

The `row_locator_contiguous` physical encoding value (`37`) and
`dictionary_dense` physical encoding value (`38`) are durable TCIM section
encodings. They must appear only on their matching section kinds and must not be
advertised as declared-column payload codecs or direct-view certification
targets.

`row_locators` sections with encoding `0` use the legacy raw payload:

```text
u32 count
repeated count times:
  i64 primary_id
  u64 part_id
  u32 part_row
  u32 granule_ordinal
  u32 row_in_granule
  u32 reserved_zero
```

`row_locators` sections with encoding `row_locator_contiguous` use exactly 32
bytes:

```text
u32 magic = 0x54434c52  // "RLCT" little-endian bytes
u16 version = 1
u16 reserved_zero
u64 part_id
u64 row_count
i64 base_primary_id
```

Readers validate the payload length, magic, version, reserved field, descriptor
part id, descriptor row count, section row count, primary-id range, and granule
row coverage, then synthesize one locator for each descriptor row:
`primary_id = base_primary_id + part_row`. Sparse primary IDs, mismatched
part IDs, mismatched row counts, invalid descriptor granules, unsupported
encodings, or unsupported compression fail closed. Writers must fall back to the
legacy raw locator payload when the part is not contiguous by physical row.

`dictionaries` sections with encoding `0` use the legacy raw payload:

```text
u32 dictionary_count
repeated dictionary_count times:
  str dictionary_name     // u32 byte length followed by UTF-8 bytes
  u32 entry_count
  repeated entry_count times:
    i64 code
    str value             // u32 byte length followed by UTF-8 bytes
```

`dictionaries` sections with encoding `dictionary_dense` use:

```text
u32 magic = 0x54434944  // "DICT" little-endian bytes
u16 version = 1
u16 reserved_zero
u32 dictionary_count
repeated dictionary_count times:
  str dictionary_name
  u32 entry_count
  repeated entry_count times:
    str value
```

Dense dictionary codes are implied by entry ordinal (`0..entry_count-1`).
Readers validate the magic, version, reserved field, duplicate dictionary names,
duplicate values, declared low-cardinality cardinality, and descriptor code
coverage before the dictionary is trusted. Writers may use
`dictionary_dense` only for dense code maps and only when the final stored
payload is smaller than the raw candidate after the configured section
compression policy.

Version 1 row payloads omitted the `Deleted` flag and represented only live
insert/update rows:

```text
bytes    RowID
values   declared column values
```

M12C and later decoders may read version 1 as `Deleted=false` for pre-v2 assets.
Writers emit version 2.

Dictionary-code derived sidecars referenced by
`ColumnAssetRef.Kind = tcs1_dictionary_codes` use asset magic `TCDC`. Version 2
keeps the manifest-style big-endian header, collection/namespace/generation,
schema, column identity, dictionary strings, cardinality, and row-count fields,
but the row-code payload is no longer a manifest `uint32` stream. Writers add
deterministic zero padding after the dictionary strings until the row payload is
4-byte aligned, then emit exactly `row_count * 4` bytes of little-endian `uint32`
local dictionary codes. Segment writers also prefix-pad dictionary-code assets so
`asset_ref.offset + payload_offset` is 4-byte aligned for mmap direct-view
consumers. Readers fail closed on non-zero payload padding, payload-length or
row-count mismatch, absolute misalignment, codes outside dictionary cardinality,
checksum mismatch when requested, or unsupported versions. Version 1
big-endian/manifest row-code payloads are intentionally rejected by current
pre-alpha readers; rebuild old DB directories instead of migrating in place.

Dense int64 value derived sidecars referenced by
`ColumnAssetRef.Kind = tcs1_int64_values` use asset magic `TCI8`. Version 2
keeps the manifest-style big-endian header, collection/namespace/generation,
schema, column identity, column index, and row-count fields, then adds
zero padding until the row-value payload is 8-byte aligned. Writers then emit
exactly `row_count * 8` bytes of little-endian two's-complement `int64` values.
Segment writers prefix-pad int64 value assets so `asset_ref.offset +
payload_offset` is 8-byte aligned for mmap direct-view consumers. Readers fail
closed on non-zero payload padding, payload-length or row-count mismatch,
absolute misalignment, checksum mismatch when requested, schema/ref/column
mismatch, or unsupported versions. Version 1 big-endian/manifest row-value
payloads are intentionally rejected by current pre-alpha readers; rebuild old DB
directories instead of migrating in place.

Sectioned typed-column part payloads are `TreeDB/internal/typedcolumn` part
images referenced by `ColumnAssetRef.Kind = tcs1_typed_column_part`. When a
collection SortKey is fully owned by `typed_column_part`, uses supported
ascending non-null bool/int64/string columns, and has at most
`typedColumnPartSortKeyMaxColumns == 8` columns, the typed-column image
descriptor SortKey and the v4 manifest part SortKey trailer must match exactly.
String SortKey columns rely on part-local dictionary codes only when those codes
are assigned in logical bytewise-ascending order and the dictionary metadata
certifies that collation. Each typed-column image stores a `sort_key_marks`
section with one validated mark per granule. Mark prefixes record lower and
exclusive-upper bounds in the same logical/certified ordered code space as the
persisted SortKey; readers reject corrupt/stale mark counts, row counts,
ordinals, prefix widths, and invalid bounds rather than pruning silently. Mixed-
owner SortKeys fall back to the synthetic `__treedb_primary_id` order and publish
no typed-column SortKey trailer; typed-column-owned unsupported, nullable,
descending, or wider-than-8 SortKeys fail closed.
The durable Issue `#1755` scalar path represents bool, int64, float32,
double/float64, and string fields. Int64 typed-column fields use `delta_varint`
by default; a non-null scalar `typed_column_part` field that explicitly sets
`fixed_width_encoding: "little_endian"` uses an uncompressed native raw
little-endian payload: `raw_int64` for `int64` (`rows * 8` bytes),
`raw_float32` for `float32` (`rows * 4` IEEE-754 bits), or `raw_float64` for
`double`/`float64` (`rows * 8` IEEE-754 bits). Issue #1929 adds non-null
primitive scalar payloads `raw_int8`, `raw_uint8`, `raw_int16`, `raw_uint16`,
`raw_int32`, `raw_uint32`, `raw_uint64`, `raw_float16`, and `raw_bfloat16` with
matching type codes above. Issue #1930 adds non-null dense numeric vector
payloads `raw_uint8_vector`, `raw_int8_vector`, `raw_uint16_vector`,
`raw_int16_vector`, `raw_uint32_vector`, `raw_int32_vector`,
`raw_uint64_vector`, `raw_int64_vector`, `raw_float16_vector`,
`raw_bfloat16_vector`, and `raw_float64_vector` with matching vector type codes
above. Issue #1931 adds `raw_fixed_bytes`, `raw_packed_bit_vector`,
`raw_packed_uint2_vector`, and `raw_packed_uint4_vector` payloads with matching
fixed/packed type codes above. Multi-byte primitive scalar and dense vector
values are little-endian;
`float16` and `bfloat16` are raw 16-bit bit payloads, not arithmetic codecs.
Native scalar float payloads preserve raw bits exactly, including NaN payloads
and signed zero. The legacy
raw-`int64` float bit-pattern carrier remains a compatibility/fallback layout
when native fixed-width encoding is not selected and must not be treated as a
native scalar float direct-view payload. Issue `#1756` adds fixed-dimension
`float32_vector` fields as uncompressed row-major little-endian dense `float32`
sections whose element count per row is `vector_dims`. Issue `#1783` adds
fixed-degree `adjacency_list` fields as uncompressed row-major little-endian
dense `uint32` sections whose element count per row is `adjacency_degree`;
that dense layout remains fallback/compatibility. Issue #1914 selected the #1901
variable-list compatibility path as an explicit `ColumnStoreValueAdjacencyList`
layout extension selected by `adjacency_layout: "uint32_offsets_list"` and the
internal encoding `raw_uint32_offsets_list`. Issue #1989 quarantines that
consumer-specific selector; the primary `uint32_list` path uses the reusable
physical mechanics:

```text
offsets []uint64  // row_count + 1, little-endian
values  []uint32  // flattened uint32 values, little-endian
```

The serialized image stores one canonical column-wide offsets section and one
column-wide values section per offsets-list column. For multi-block parts, block
payloads may use block-local offsets internally, but the image writer publishes a
single global `row_count + 1` offsets array by dropping duplicate block starts and
adding cumulative value bases; readers reconstruct block-local fallback payloads
from those global sections. The offsets-list mechanics validate exact offsets
count, `offsets[0] == 0`, monotonic offsets, final offset equal to the value
count, exact offsets/value byte lengths, Go `int` range before slicing,
little-endian identity, and separate section metadata/checksums for offsets
(8-byte elements) and values (4-byte elements). The `bytes` / `raw_bytes_offsets`
primitive uses the same `uint64` sentinel offsets shape, but its values section is
an arbitrary byte stream; final offset is a byte length, empty byte slices are
equal adjacent offsets, and NUL/non-UTF-8 bytes are preserved without string
semantics. #1915 adds the safe writer and
fallback reader into owned Go slices; #1916 adds certified direct-view readers
for paired offsets/value handles, and #1917 wires that variable adjacency reader
through typed-column adapters. #1918 recorded durable `column_graph` layer-0
adjacency sources as `raw_uint32_offsets_list` typed-column assets during
physical graph rebuilds, and later graph-source work extended manifests to record
per-layer sources. Those `column_graph` source records are legacy compatibility,
not the target datastore primitive; current primary adjacency uses `uint32_list`
vector-index state. #1984 defines `uint32_list` semantics in
`typed-column-uint32-list-semantics.md`, #1985 adds the generic runtime
primitive implementation, and #1986/#1988 own vector-index state/search
consumption.

The `column_graph` manifest keeps the row graph asset ref for compatibility and
controlled fallback. Exact returned document IDs live in vector-index
`document_ids` state when that state validates; legacy graph row ID bytes are
fallback only. The legacy all-layer source metadata is an optional compatibility
manifest trailer with magic `TCGL` and
version `1`: it records `layer_count`,
`source_count`, and then one `TCGA` v1 source record per layer in ascending layer
order. Each source
record binds the source schema/column name, value type/encoding, layer number,
source schema hash, row count, value count, offsets/value/padding byte
accounting, source `tcs1_typed_column_part` ref, base-manifest identity, graph
schema hash, and graph-asset identity. `source_count` must equal `layer_count`;
layer `i` must have `Layer=i`; layer 0 is also exposed through the legacy
optional layer-0 field for older readers. Empty rows and layers are represented
by equal adjacent offsets in the per-layer offsets array. Old graph manifests
without the trailer remain row-asset fallback readable. New graph builds leave
these `TCGA`/`TCGL` fields empty and publish typed-column `uint32_list`
vector-index state instead. Do not add new storage features to this
`TCGA`/`TCGL` compatibility path.

Issue #1986 adds a separate vector-index state control record under
`\x06vector-index-state/v1/index/<index_name>` with magic `TVIS`. Version `2`
stores the logical record directly (`1` is still accepted for pre-alpha
compatibility). When that representation exceeds the reserved inline manifest
budget, version `3` stores the decoded v2 byte length followed by a Snappy block
containing the complete v2 record. Writers cap both the decoded representation
and the final v3 envelope and reject a state that still cannot fit inline;
readers validate the declared and Snappy decoded lengths before allocation.
The logical record stores index identity, row count, base manifest identity,
expected adjacency layer count, and typed-column asset refs by logical type plus
physical encoding. Its asset roles include adjacency (`uint32_list` over
`raw_uint32_offsets_list`), inverse norms (`float32` over `raw_float32`),
optional normalized vectors (`float32_vector` over `raw_float32_vector`), row
references (`int64` over `raw_int64`), exact returned document IDs (`bytes`
over `raw_bytes_offsets`), quantized code assets (`quantized_codes` role), and
scalar_u8 per-granule alpha metadata (`quantized_alpha` role) when selected.
Legacy scalar_u8 score planes use `byte_vector` over `raw_fixed_bytes` with
asset IDs `quantized/<name>/codes`; per-granule-alpha scalar_u8 uses config-hash
asset IDs `quantized/<name>/scalar_u8/<hash>/codes` plus
`quantized/<name>/scalar_u8/<hash>/alpha` with logical type `scalar_u8_alpha`
and physical encoding `raw_float32_uint32`. Declared `rabitq_1bit` v1 score planes
use `packed_bit_vector` over `raw_packed_bit_vector` with asset IDs
`quantized/<name>/packed_codes`; prototype `brq_1bit` v1 score planes use asset
IDs `quantized/<name>/brq_1bit/packed_codes`. The vector-index-state checksum
includes the control record, but the record's base checksum excludes
vector-index derived records so stale-state checks compare against authoritative
collection data. See `vector-index-state-manifest.md` and
`vector-index-row-ref-state-1993.md` for validation and fail-closed rules.

As of the #1895 pre-alpha format update, newly written `typed_column_part` images
carry a writer-built `layout_contract` section. The contract may mark only raw
non-null uncompressed `raw_int64`, native `raw_float32`, native `raw_float64`,
Issue `#1929` raw primitive scalar sections, fixed-dimension `raw_float32_vector`,
Issue `#1930` raw dense numeric vector sections, explicit
`raw_uint32_offsets_list`, and explicit `raw_bytes_offsets`
typed-column payload sections as
`DirectViewCertified`; the adapter-internal
`__treedb_primary_id` row-locator column is not a declared-value direct-view
certification target. The contract records section/block offsets, lengths,
checksums, element size, endian, length multiple, row count, fixed elements per
row, and null/default exclusion. For `raw_uint32_offsets_list` and
`raw_bytes_offsets`, the contract records global offsets/value section identity
and leaves generic per-block combined payload offsets empty because the two
sections are discontiguous. Image
padding bytes are deterministic zero bytes
and are included in serialized-image byte accounting. When a typed-column-part
asset contains an active direct-view-certified candidate, the column asset segment
writer/appender also emits deterministic zero prefix padding as needed so the
absolute storage addresses (`asset_ref.offset + section/block payload offset`)
satisfy the declared alignment; this segment prefix padding is outside the asset
payload/checksum but is part of segment file size and appender offset accounting.
Scalar-u8 vector-index `quantized_codes` images use 64-byte section alignment and
64-byte segment placement to align the payload base. Rows remain contiguous, so
each fixed-width code row begins on a cache-line boundary only when its
dimension/byte stride is divisible by 64. Other dimensions retain base alignment
without a per-row guarantee or reader fallback. Other typed-column images retain
the 8-byte default. Readers continue to accept legacy 8-byte images; physical
asset rewrite preserves 64-byte placement when the source ref is 64-byte
aligned, and reachability accounting recognizes the deterministic zero prefix
inserted for either alignment.
Old or manually constructed typed-column assets without a valid layout contract,
or refs whose absolute offsets are misaligned, fail closed in certified/prepared
paths. TreeDB is pre-alpha, so rebuilding old DB directories is preferred over
on-disk migration scaffolding for this format change.

Nullable scalar typed-column support uses nullable int64 carrier granules for
bool, int64, float32, double/float64, and low-cardinality string fields. A
nullable scalar column uses the `nullable_int64` encoding. Each granule payload
contains a fixed header, the encoded non-null/non-default carrier values, and
two row-aligned bitmaps:

- the null bitmap marks rows whose JSON path was present with an explicit
  `null`; these rows have no stored int64 payload value and reconstruct as
  explicit JSON null;
- the default/missing bitmap marks rows whose declared path was omitted from the
  source document; these rows have no stored int64 payload value and reconstruct
  by omitting that path from the retained-payload document; and
- rows with neither bit set are present/non-null and consume one encoded carrier
  payload value in row order (`0/1` bools, int64s, float bit patterns, or string
  dictionary codes).

Null and default/missing bits are mutually exclusive. Granule metadata stores
`NullCount` and `DefaultCount`; the two counts must be non-negative and must not
exceed the row count (`DefaultCount <= Rows-NullCount`). Decoders must fail
closed on invalid count metadata, truncated or incorrectly-sized bitmaps, rows
marked both null and default/missing, or stored-value underflow/overflow. Min/max
metadata, when present, covers only stored present/non-null carrier values; null
and default/missing rows contribute no value, and all-null/all-missing blocks
omit min/max. Future native nullable scalar encodings may reuse the same
explicit-null versus missing bitmap model only after their per-type payload
format is specified.

Nullable/missing typed-column codecs are allocation-budgeted hot paths and carry
a positive optimization expectation, not only a no-regression gate. When changing
encoding, decode, scan, or reconstruction merge loops, implementations should
actively remove existing avoidable allocations and obvious local overhead in the
same touched path when the cleanup is bounded, testable, and evidenced. These
loops must use compact bitmaps/default metadata plus caller-owned scratch and
must target 0 allocs/op after setup when benchmarking the core typed-column loop
separately from document materialization. Touched inner loops must be measurably
no worse, and preferably better, on `B/op` and `allocs/op`. Implementations must
not add per-row heap wrappers, maps, interface values, closures, or string/byte
conversions in these loops; if benchmarks or profiles expose allocations in
touched functions, the PR must fix them or explicitly list why they are out of
scope with a linked follow-up recommendation. Any remaining allocation requires
baseline-versus-final `B/op` and `allocs/op` evidence plus allocation profile/top
evidence before it is accepted or explicitly deferred. Checksum, lifetime,
schema, null/missing, and fail-closed validation must not be weakened to meet the
allocation budget.

Production `float32_vector`, `uint32_list`, and `adjacency_list`
nullable/missing support remains staged and fail-closed. Authoritative
`uint32_list` typed-column fields are non-null in v1 and reject adjacency-degree
or adjacency-layout selectors; empty lists are represented by equal adjacent
offsets. Authoritative dense `adjacency_list` typed-column fields must be
non-nullable, must declare positive `adjacency_degree`, and must fail closed when
any source row length, schema descriptor, or asset payload length disagrees with
that fixed degree. The adjacency offsets-list selector is also non-nullable, must
not declare `adjacency_degree`, and uses the #1915/#1916 concrete encoding for
safe publication/reopen/fallback reconstruction and adapter direct reads.

## 8. Commit-Log Segment Format

Commit-log file is a sequence of segments.

Segment envelope:

```text
u32 LengthField      // high bit = compressed flag, remaining bits = payload length
u32 CRC32(payload_stored)
bytes PayloadStored[Length]
```

If compressed flag is set:

```text
u32 RawLen
bytes ZstdCompressedRawPayload
```

Compression is only kept when it is a strict size win.

### 8.1 Legacy Pre-Command-WAL Raw Commit Batch Payload Format

This is the legacy pre-command-WAL raw payload format. It is not a compatibility
target for `command_wal_v2`. Command-WAL raw key/value writes are
encoded as `RawKVBatch` command frames and this payload may be removed from
normal open/recovery code.

```text
u8  Version          // currently 1
u32 RecordCount
Record[RecordCount]
```

Record format:

```text
u8  Op               // 0=set RID, 1=set inline, 2=delete
u16 KeyLen
u32 ValueLen
u64 RID
u64 Seq
bytes Key[KeyLen]
bytes Value[ValueLen]
```

Validation rules:

- `OpSetRID`: `RID != 0`, `ValueLen == 0`.
- `OpSetInline`: `RID == 0`.
- `OpDelete`: `RID == 0`, `ValueLen == 0`.

## 9. Command WAL Typed Frame Format

The active format for collection and catalog durability is the user-command WAL
defined in `user-command-wal.md`. It extends the existing commit-log segment
family instead of defining a new collection WAL file class. Typed command WAL
frames must live in `wal/commit-l<lane>-<seq>.log` as the only WAL payload
format once `command_wal_v2` is enabled.

The current raw commit-log record schema is superseded, not retained as a
compatibility payload. Raw KV writes become `RawKVBatch` command frames. New
collection and catalog commands must use typed command payloads inside the
shared commit-log frame stream; they must not be encoded as physical root deltas
and must not create `wal/collection-l*.log` files.

The commit-log physical segment header remains unchanged:

```text
u32 StoredLenAndFlags
u32 StoredCRC32
bytes StoredPayload[StoredLenAndFlags & lenMask]
```

`StoredPayload`, after optional existing zstd decompression, is a command frame:

```text
bytes[4] Magic              // "TCW1"
u16      Version            // 1
u16      MinReaderVersion   // reader must support at least this version
u16      CommandKind
u16      Scope
u64      FeatureFlags       // low 32 bits are critical in PR1
u64      LSN
u64      CatalogEpoch
u64      SchemaEpoch
u64      BaseAppliedLSN
u16      PayloadFormat
u16      DurabilityClass   // 1=durable, 2=relaxed
u32      PayloadLen
u32      ExternalRefsLen
u32      PreconditionsLen
u32      ResultAssertionsLen
bytes Payload[PayloadLen]
bytes ExternalRefs[ExternalRefsLen]
bytes Preconditions[PreconditionsLen]
bytes ResultAssertions[ResultAssertionsLen]
```

Production append, scan, and reopen use frame version 2 when the
`command_wal_v2` required feature is active. There is no mixed V1/V2 append
mode. A strict V2 reader that encounters V1 returns a pre-alpha
rebuild-required error, and an unsupported version fails closed. The old
`command_wal_v1` required-feature spelling is rejected with the same rebuild
requirement rather than opening a directory under partially upgraded rules.

The active V2 boundary forbids the commit-log segment-level compressed flag.
A torn compressed segment exposes only compressed bytes after `RawLen`, so its
LSN and durability class cannot be authenticated before deciding whether it is
above the durable frontier. Strict V2 readers therefore return
`ErrCommandWALV2CompressedRecordUnsupported` for both complete and terminal
compressed records. Production V2 journals disable outer segment compression;
a later format may enable it only after introducing and verifying framing that
keeps the required terminal identity prefix uncompressed.

V2 keeps the 72-byte envelope above and assigns the former reserved header
field at bytes `[54,56)` to a little-endian durability class:

| Value | Durability class | Recovery meaning |
|---:|---|---|
| 1 | Durable V2 frame class | this complete valid frame raises the durable frontier |
| 2 | Relaxed V2 frame class | eligible for suffix discard only above the durable frontier |

Zero and unknown classes are corruption. Encoders validate the class and all
semantic fields before mutating caller-owned destination storage.
An uncompressed active terminal tail is classifiable only when frame bytes
`[0,56)` persist the complete V2 identity, LSN, and durability class. A shorter
tail fails closed with `ErrCommandWALV2TailIdentityUnavailable` joined with
`ErrCorrupt`; recovery must not infer a missing class or LSN from write intent.

The journal assigns each LSN and retains its dependency debt under the same
serialization boundary. A relaxed append flushes dependency bytes to visibility
but does not issue stable file or namespace syncs. Its ordered debt entry pins
the exact dependency handles, required byte/RID frontiers, and any rotated
command-WAL file and successor-name obligations. A later durable command or
barrier deterministically coalesces debt through its assigned prefix, syncs the
exact files, stabilizes the retained namespaces, appends the durable V2 frame,
and then syncs the command WAL. That successful final WAL sync advances the
in-memory durable WAL LSN and releases covered debt. A successful synced
publication of an already-appended relaxed staged command publishes one
contiguous applied range through the new durable barrier LSN; it must not leave
the barrier below the durable frontier but above `AppliedCommandLSN`. A
successful synced checkpoint cleanup may also advance and release through the
already-durable `AppliedCommandLSN`: durable root coverage has superseded those
command frames,
and cleanup may already have deleted their segments. Cleanup or directory-sync
failure retains the debt; a later barrier must never reopen a deleted segment
through an obsolete rotation token. A failure before a durable frame append
retains the debt for retry; a failure after append or during the final WAL sync
is commit-ambiguous and poisons the open handle with `ErrRecoveryRequired`.

Every V2 command envelope containing a `RawKVBatch` payload with at least one
`SetRID` operation has exactly one `ExternalRefFenceV1` precondition. Frames
without `SetRID` have no RID fence, including `RawKVBatchV2` frames whose
pointer operations are all `SetMaterializedRID`. Its payload is:

```text
u32 UniqueRIDCount
bytes[32] SHA256(sorted unique u64-le RIDs concatenated in order)
```

Decode recomputes this fence from the canonical decoded `RawKVBatch` payload
before any RID lookup. Missing, duplicate, malformed, count-mismatched, or
digest-mismatched fences are corruption. The stable V1 fixtures remain under
`TreeDB/internal/commitlog/testdata/command_wal_v1_*.hex`; V2 fence and barrier
fixtures use the corresponding `command_wal_v2_*.hex` names.

V2 also reserves command kind `300`, system scope, and payload format `8` for
`DurablePrefixBarrierV1`. It is a durable, empty/no-op frame whose LSN advances
the recovery frontier without carrying a user mutation.

Current command kinds:

| Value | Kind | Scope | Payload format | Status |
|---:|---|---|---|---|
| 1 | `RawKVBatch` | raw KV | `RawKVBatchV1` or `RawKVBatchV2` | typed raw key/value command batch |
| 100 | `CollectionInsertBatchByID` | collection | `CollectionInsertBatchByIDV1` | deterministic collection insert/upsert-by-id batch |
| 101 | `CollectionDeleteBatchByID` | collection | `CollectionDeleteBatchByIDV1` | deterministic collection delete-by-id batch |
| 102 | `CollectionUpdateBatchByID` | collection | `CollectionUpdateBatchByIDV1` | deterministic collection update/replace-by-id batch |
| 103 | `CollectionRebuildVectorIndex` | collection | `CollectionRebuildVectorIndexV1` | deterministic collection vector-index rebuild command |
| 200 | `CatalogCreateCollection` | catalog | `CatalogCreateCollectionV1` | deterministic catalog create-collection command; old placeholder name is an alias only |
| 300 | `DurablePrefixBarrier` | system | `DurablePrefixBarrierV1` | active V2 empty durable-frontier record used by explicit sync with no user mutation |

Current payload format IDs:

| Value | Payload format |
|---:|---|
| 1 | `RawKVBatchV1` |
| 2 | `NativeWireDeterministic` |
| 3 | `CollectionInsertBatchByIDV1` |
| 4 | `CollectionDeleteBatchByIDV1` |
| 5 | `CollectionUpdateBatchByIDV1` |
| 6 | `CatalogCreateCollectionV1` |
| 7 | `CollectionRebuildVectorIndexV1` |
| 8 | `DurablePrefixBarrierV1` |
| 9 | `RawKVBatchV2` |

`RawKVBatchV1` and `RawKVBatchV2` share this payload framing:

```text
u16 Version        // 1
u32 OpCount
Op[OpCount]

Op:
u8  Op             // 1=set, 2=delete, 3=set-by-value-log-RID, 4=delete-range,
                   // 5=set-by-materialized-value-log-RID (V2 only)
u32 KeyLen          // for delete-range: StartLen, or 0xffffffff for nil/unbounded
u32 ValueLen        // for delete-range: EndLen, or 0xffffffff for nil/unbounded
bytes Key[KeyLen]   // omitted when delete-range StartLen is 0xffffffff
bytes Value[ValueLen] // omitted when delete-range EndLen is 0xffffffff
```

`SetMaterializedRID` encodes `ValueLen >= 8`, followed by a non-zero
little-endian `u64 RID` and the exact logical value bytes. `RawKVBatchV1`
rejects this operation. `RawKVBatchV2` recovery either reuses an existing record
whose RID and bytes both match or appends the bytes under that exact RID before
publishing the pointer. A present RID with different bytes is corruption.
Materialized RID operations are self-contained and do not enter the external
RID fence; `SetRID` operations in the same V2 payload retain the normal fence
and stable dependency closure. Live encoding selects V2 only through an
explicit directly durable or durable-prefix-participant append mode and only
within the shared 64 KiB/value, 1 MiB/frame, and 256-operation bounds. An
ordinary relaxed append and a reusable intent with no declared final durability
boundary use V1 even if pointer entries retain logical value bytes. A grouped
participant's envelope remains individually relaxed; its later durable-prefix
barrier is the acknowledgement boundary that stabilizes the preceding frame.

A `RawKVBatch` command frame is one atomic command: one frame, one `LSN`, and
all contained operations decode as one batch. Delete operations require
`ValueLen=0`. Delete-range operations use half-open `[start,end)` semantics;
nil start/end bounds are unbounded and are encoded with the `0xffffffff` length
sentinel. Malformed delete-range payloads (for example bounded `start >= end` or
extra bytes after a nil-bound sentinel) fail closed as corrupt. Public APIs treat
empty or reversed range deletes as no-ops and do not emit dangerous command-WAL
mutations for them.

Target raw KV entry revisions do not require per-operation sequence fields in
`RawKVBatchV1`: one mutation revision applies to all raw keys touched by the
frame. That revision is the command frame `LSN` when the LSN allocator is seeded
above the persisted raw-KV revision floor; otherwise the command/envelope must
carry one effective mutation revision from the same domain. Target conditional
raw KV transactions should express point-read preconditions and replay result
assertions through the existing `CommandEnvelope.Preconditions` and
`CommandEnvelope.ResultAssertions` extension areas when that is sufficient. A
new raw KV payload version is allowed only if #3423 proves the envelope
extensions cannot encode the required deterministic guard data without
compromising decode speed or replay correctness.

Writers may use compact all-zero set payload variants when every operation is a
set with the same non-empty zero-filled value length:

```text
u16 Version        // 2
u32 OpCount
u32 ValueLen
ZeroOp[OpCount]

ZeroOp:
u32 KeyLen
bytes Key[KeyLen]
```

Version 3 is the same compact zero-set payload with a narrower per-key length
field and is valid only when every key length fits in `u16`:

```text
u16 Version        // 3
u32 OpCount
u32 ValueLen
ZeroOp[OpCount]

ZeroOp:
u16 KeyLen
bytes Key[KeyLen]
```

Readers expand version 2 and version 3 entries to ordinary `RawKVBatch` set
operations with a zero-filled `Value[ValueLen]`; the command frame still carries
payload format `RawKVBatchV1`.

`CatalogCreateCollectionV1` payload:

```text
u16 Version        // 1
u32 CollectionNameLen
u32 MetadataLen
bytes CollectionName[CollectionNameLen]
bytes Metadata[MetadataLen] // canonical collection metadata JSON
```

The payload name and decoded metadata name must match. Replay is idempotent only
when an existing catalog entry has identical normalized metadata; incompatible
metadata fails closed before advancing `AppliedCommandLSN`.

Collection vector-index declarations are stored in the canonical collection
metadata JSON under top-level `vector_indexes`. The current collection metadata
JSON version is `5`, which includes the persisted `scalar_u8_calibration`
semantics for quantized scalar_u8 score planes. Quantized score-plane
declarations, when present, live under `vector_indexes[].quantized_indexes` and
are declarations only until matching derived assets are built and loaded; explicit
quantized query modes must fail closed when those assets are absent or stale.

### Native-runtime vector-index roots

A `native_runtime` vector index is persisted in the catalog root
`<collection>/vector-index/<index_name>`. Values are newline-terminated JSON.
The root contains `meta`, `node/<20-digit-node-id>`,
`edge/<20-digit-node-id>/<3-digit-layer>`, `tomb/<20-digit-node-id>`, and
`doc/<document-id>` keys. Nodes and edges store the HNSW graph, tombstone keys
mark deleted nodes, and document keys map the current document ID to its node.

The `meta` value binds the graph to one collection document snapshot. Coverage
version `3` stores `source_document_generation_version` and
`source_document_generation`. The generation is the big-endian `uint64` value
of the collection system key `collections/document-generation/<collection>`;
an absent key is generation zero. A successful
document mutation advances that key atomically with its primary-root or
primary-overlay descriptor. Physical root remapping during checkpoint,
compaction, or vacuum and vector-index-only publication do not advance it.

Load and status checks reject persisted metadata as `stale_document_root`
unless the coverage version and generation match the current collection.
Search rejects a runtime whose in-memory coverage is invalid or differs from
the generation in the current search snapshot. Save rejects invalid in-memory
coverage.
Under the coverage barrier, save may advance coverage after successful graph
maintenance, including by publishing a meta-only delta when document metadata
changed but vector values did not. Older coverage versions are not migrated in
this pre-alpha format; the index must be rebuilt.

Column-enabled collection metadata is stored inside the canonical collection
metadata JSON under `options.column_store`. It is production-facing
control-plane state, not a sidecar hint. Current normalized fields are:

- `enabled`: column storage is enabled for the collection.
- `columns`, `sort_key`, and `aggregate_metadata`: declared projection schema,
  analytical ordering, and aggregate metadata definitions. Aggregate metadata
  definitions may include exact string predicate coverage; metadata assets are
  only eligible for matching physical queries with identical predicate, group,
  value, hour-bucketing where applicable, and aggregate shape.
- `retained_payload` and `reconstruction`: how non-column row bytes and column
  values reconstruct full documents. The current default retained-payload policy
  is `non-column`.
- `asset_manager`: the typed column asset manager. Current production metadata
  requires `kind="value-log"` and an isolated namespace.
- `manifest_root`: descriptor for the collection system root that owns the
  active column manifest identity record. The root name must be
  `<collection>/column/manifest`, and its storage policy must match
  `control_root_storage_policy`.
- `active_manifest`: published column manifest identity
  `{generation, format, version, checksum}`. Current format is `tcs1`, version
  `1`.
- `recovery_authoritative_manifest` and
  `recovery_authoritative_applied_command_lsn`: the manifest generation and
  command stream boundary considered safe for recovery. When `active_manifest`
  is present, the recovery-authoritative identity must also be present and must
  match it until a later format explicitly supports split active/recovery
  generations; the applied command LSN must also be present and non-zero.
- `profile_support`: current production default is `durable-only`.
  `benchmark-relaxed` is permitted only for explicit benchmark/experimental
  use under relaxed durability modes.
- `typed_column_compression`: declared typed-column block compression policy.
  Current production default is `lz4`; `none` is an explicit isolation policy,
  and unsupported codecs fail closed during metadata normalization.
- `typed_column_section_compression`: whole-image section compression policy for
  eligible `tcs1_typed_column_part` sections. Empty/default selects `zstd` when
  `typed_column_compression` is also defaulted; otherwise it follows the
  explicit typed-column compression policy. Current production default is
  `zstd`; unsupported section codecs fail closed.
- `locator`: current default strategy is `side-index`.
- `schema_hash`: normalized hash of stable column schema/config fields used for
  cache identity invalidation. Manifest generation and recovery LSN are not
  schema-hash inputs.

Issue `#1753` added `TreeDB/internal/typedcolumn` as the transplanted
`experiments/colgranule` typed-column data plane. Issues `#1754`/`#1755` connect
it to production collection metadata for opt-in scalar `typed_column_part`
owners; issue `#1756` adds fixed-dimension `float32_vector` dense sections. The
transplant and adapter boundaries are documented in `typed-column-transplant.md`
and `typed-column-adapter.md`; closeout evidence and #1736 COW-maintenance
handoff facts are recorded in `typed-storage-closeout-1758.md`.

As of issue `#2297`, normalized column-store metadata records production
typed-column compression policy. The default policy requests `lz4` for
supported typed-column block families and `zstd` for eligible typed-column image
sections whose raw length can be validated from existing metadata, including
dictionary and pruning metadata sections. Compression is kept only when it is a
strict stored-size win; unsupported field/layout families remain uncompressed
unless a benchmark override explicitly forces them, in which case writers fail
closed. `none` disables the production policy for isolation when set on both the
block and section policies and is part of the schema hash.
Plain zstd (`zstd`) is a production-supported whole-image section codec and a
decode-supported typed-column block codec for internal benchmark-relaxed storage
experiments. Public production metadata still rejects
`typed_column_compression=zstd`; production-default block writers continue to
choose `lz4` unless configured otherwise with supported production values. Zstd
dictionary mode (`zstd_dict`) is deferred and unsupported for typed-column
blocks, image sections, and public metadata.

Readers must fail closed for a column-enabled collection when:

- active manifest metadata is missing required recovery-authoritative metadata,
- active and recovery-authoritative identities disagree,
- the manifest root descriptor does not match the collection system root name/policy,
- manifest identity format/version/checksum fields are invalid,
- the recovery-authoritative applied command LSN is zero while an active manifest
  is present, or
- a durable-only column collection is opened under a relaxed durability mode.

For typed-column parts specifically, readers and maintenance planners must reject
unsupported image versions, descriptor versions, manifest identity versions,
schema-hash drift, field-owner/value-type mismatches, `vector_dims` and
fixed-width layout mismatches, and kind/generation/part/checksum/range mismatches
from headers, descriptors, manifest identities, or refs whenever possible. They
must fail closed before full payload decode or per-row allocation when those
compact records already prove the format unsupported. Rebuild benchmark and
experiment directories rather than relying on implicit migrations during the
pre-alpha period; future migration tooling requirements are owned by
`typed-column-schema-evolution.md`.

`CollectionInsertBatchByIDV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 DocumentCount
bytes Collection[CollectionLen]
Document[DocumentCount]

Document:
u32 IDLen
u32 DocumentLen
bytes ID[IDLen]
bytes Document[DocumentLen]
```

`CollectionUpdateBatchByIDV1` uses the same canonical payload layout as
`CollectionInsertBatchByIDV1`; each document is the final accepted replacement
for the listed ID after user callbacks or declarative updates have resolved.

`CollectionDeleteBatchByIDV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 IDCount
bytes Collection[CollectionLen]
ID[IDCount]

ID:
u32 IDLen
bytes ID[IDLen]
```

Collection batch payloads require a non-empty collection name and non-empty
document IDs. Encoders canonicalize entries by strictly increasing document ID
before writing the payload, and decoders reject duplicate or out-of-order IDs.

`CollectionRebuildVectorIndexV1` payload:

```text
u16 Version        // 1
u32 CollectionLen
u32 IndexNameLen
bytes Collection[CollectionLen]
bytes IndexName[IndexNameLen]
```

The collection and index names must be non-empty. The command payload names the
logical rebuild request only; it does not carry vector graph bytes, physical root
deltas, or a vector-only sidecar file. Normal execution and replay re-enter the
collection vector-index rebuild path for the named index. For `native_runtime`
indexes, that path scans the canonical collection rows and publishes a complete
`<collection>/vector-index/<index_name>` root in the native format described
above. For explicit `column_graph` indexes, that path rebuilds any legacy row
graph asset only for compatibility, publishes inverse norms, HNSW adjacency,
base row references,
returned document IDs, and declared quantized code score planes as vector-index
state assets, and records vector-index control identity in the `TVIS` state
record. Quantized assets use role `quantized_codes`; scalar_u8 assets use asset
ids `quantized/<name>/codes` with `byte_vector` / `raw_fixed_bytes`, while
`rabitq_1bit` v1 assets use asset ids `quantized/<name>/packed_codes` and
prototype `brq_1bit` v1 assets use asset ids
`quantized/<name>/brq_1bit/packed_codes` with `packed_bit_vector` /
`raw_packed_bit_vector`. Query modes that select quantized assets fail closed
when matching state is absent or stale. Old
adjacency-source refs are `#1989-quarantined` compatibility. Current graph
manifests may still contain row graph refs and legacy layer-source trailer refs
for compatibility; new derived-state refs belong in vector-index state. Replay
outcomes that are defined no-ops, such as a strategy/config drift status that no
longer requires a
physical rebuild, must still publish a no-op command-WAL boundary and advance
`AppliedCommandLSN`. Corrupt payloads, unsupported payload versions, and
undefined replay outcomes fail closed before advancing `AppliedCommandLSN`.

`ExternalRefs`, `Preconditions`, and `ResultAssertions` are length-delimited
sections so PR1 can harden framing before replay uses them. The PR1 external-ref
section starts with `u32 Count`; each ref is:

```text
u16 Class          // 1=value-log, 2=leaf-log, 3=payload file
u16 Flags
u32 PathLen
u64 FileID
u64 Offset
u64 Length
bytes[32] Digest
bytes Path[PathLen]
```

The typed `ExternalRefs` layout remains reserved and inert: current V1 and V2
encoders and decoders reject every non-empty section with
`ErrCommandWALUnsupportedExternalRef`. RawKV `SetRID` instead uses the active V2
`ExternalRefFenceV1` precondition plus exact-handle dependency debt and is not
disabled by this quarantine. Activating the general typed section still
requires its resource producers, sync/recovery ownership, and deletion policy
to land atomically.

Precondition and result-assertion sections each start with `u32 Count`; every
entry is:

```text
u16 Type
u16 ReservedZero
u32 PayloadLen
bytes Payload[PayloadLen]
```

Readers must fail closed on:

- old raw `commitlog.Record` payloads when reading as command WAL;
- unsupported required frame versions;
- unknown command kinds;
- unknown critical flags;
- malformed section lengths before allocating section-owned objects;
- corrupt complete physical segment CRC;
- duplicate command `LSN` during segment scan.

Activation must begin from a clean WAL state or an explicit rebuild. The command
WAL implementation does not need to replay old raw batch segments in command WAL
directories.

`AppliedLSN` is the durable checkpoint proof for typed command frames. It is
metadata for the same commit-log sequence stream, not a collection-specific
applied marker. Recovery skips typed command frames with `LSN <= AppliedLSN`
and replays complete frames with higher LSNs through the deterministic command
executor before serving reads.

The V1 physical storage target for `AppliedLSN` is the in-page-marked meta-page
field named `AppliedCommandLSN`, encoded by the command-WAL meta extension in
Section 3.1 at body offset 68 / page offset 84. `MaxEntryRevision` is encoded
in the same selected meta body only when the revision-extension marker at body
offset 76 is present. These fields must be selected atomically with the roots
that contain the corresponding command effects. PR1 may document a blocking
reason to revisit this before PR2 starts, but storage-format
implementation must not proceed with both meta-page and system-root storage as
live options. A sidecar cleanup file, manifest, stats record, format-config
marker, or post-commit maintenance record is not authoritative state for
recovery.

The deprecated collection root-delta WAL format (`collection_wal_v1`,
`wal/collection-l*.log`, `WALLSN`, `CollectionSeq`, and collection root-delta
frames) is not an active storage target. Its detailed design is preserved only
in `collection-wal-durability-plan.md` as historical analysis for external-ref,
crash-recovery, checkpoint, and fail-closed risks.

## 10. File Naming Conventions

Current canonical names:

- commit log: `wal/commit-l<lane>-<seq>.log`
- value log: `value_vlog/value-l<lane>-<seq>.log`
- split leaf log: `leaf_vlog/value-l<lane>-<seq>.log`

Recovery parser may accept historical value-log and commit-log file names before
`command_wal_v2` activation. Once command WAL is enabled, command frames use the
shared `commit-l<lane>-<seq>.log` segment family and old raw batch payloads are
unsupported.

## 10.1 Retained Vector-Partition Benchmark Sidecars

Two JSON files may sit at the root of a *retained* M3 benchmark database
directory (one produced with `-m3-persist-db`). They are not engine structures:
no TreeDB read or write path consults them, and an ordinary database never
contains them. They are documented here because they are immutable on-disk
files inside a database directory, and because the second one is required for
that directory to reopen through the benchmark's retained-variant path.

- `vector_partition_variant_v1.json` — the retained variant descriptor,
  `m3_persistent_variant_descriptor_v6`, capped at 1 MiB. Carries the build
  identity, source/artifact/router identities, overlap accounting, partition
  loads, and (for byte-bounded builds) the shard plan, the SHA-256 of the
  generation record, and that record's byte length.
- `vector_partition_shard_generation_v1.json` — the shard generation record,
  `treedb_vector_partition_shard_generation_v1` schema 1, capped at 256 MiB.
  Carries the byte-bounded plan, the overlap config, the full realized
  membership list, its SHA-256, and the membership-derived per-pack
  home/overlap/total row and byte summaries. Roughly 5 MB at 100k rows and
  15 MB at 250k rows with the selected 0.2 overlap ratio.

Both are written once with `O_EXCL` and `fsync`, and are never rewritten in
place; a rebuild uses a fresh directory. The pair is fail-closed on reopen: a
variant descriptor carrying a non-zero `shard_plan` must be accompanied by a
generation record whose file SHA-256 equals the descriptor's
`shard_generation_digest`, whose plan equals the descriptor's plan, and whose
membership-derived accounting matches the descriptor's ratio, capacity,
per-partition loads, realized overlap, and source row count. A descriptor with
no plan must carry neither digest nor size. Deleting or editing either file
makes the directory fail to reopen rather than reopening with unverified
evidence.

Unknown schema versions fail closed. TreeDB is pre-alpha: retained benchmark
databases from earlier descriptor versions are rebuilt, not migrated.

## 11. Storage Compaction Lifecycle

`DB.CompactStorage` is the canonical online storage compaction entry point. It
does not introduce a new on-disk format; it coordinates the existing storage
objects above into one lifecycle:

1. establish a durable checkpoint boundary,
2. rewrite live value-log records into new `value_vlog` segments,
3. run reachability-based value-log GC,
4. pack live split outer-leaf pages into new `leaf_vlog` generations,
5. run leaf-generation GC,
6. vacuum/rewrite `index.db`,
7. run settle GC passes,
8. delete untracked zero-byte `value_vlog` segment files,
9. audit the remaining storage debt.

Applied compaction is serialized with other backend maintenance for the full
multi-phase sequence. Planning mode reports debt without mutating storage and is
safe for read-only opens.

Index-vacuum planning uses bounded pager metadata: total/user page span,
freelist reclaimable pages, and collection-root span. The phase report is one of
`planned`, `not_required`, `succeeded`, `deferred`, `unsupported`, or `failed`
and carries whether work was required plus its reason. A successful online
replacement rebuilds every recovery-selectable root under one
`RecoverableRootSet`, atomically rebinds the live runtime, and is followed by a
checkpoint. Index-only replacement does not rewrite or retire persistent value
or leaf-log files. Completion flags remain false while required index or other
storage-domain debt remains. After releasing the leaf-generation guard,
CompactStorage performs one bounded leaf-GC settle and one debt-driven final
index replacement so work created by earlier maintenance phases is not hidden.

In cached mode, public maintenance wrappers checkpoint first, protect cached
value-log paths, reserve rewrite RIDs from the live cached allocator, and
reconcile cached split value-log writers after backend maintenance so later
writes advance past backend-created `value_vlog`/`leaf_vlog` segments instead of
reusing segment file names.

# Freelist Generation V1 (active durable-root allocator format)

`TreeDB/freelist.FreelistGenerationV1` is the immutable allocator view bound by
the active two-meta durable-root recovery path. It is encoded as 4096-byte pager
pages using the normal page header and CRC32 convention. Page types `0x05`
through `0x08` are reserved for the generation header, radix index, state
chunk, and candidate reservation record. `DurableRootRecordV1` binds the exact
generation header identity, counts, high-water boundary, and digest.

The sparse state tree uses 14 four-bit radix levels over 256-page chunks. An
index page contains up to 16 ordered child summaries: page identity, child CRC,
free and retired counts, and the minimum `lastReachableCommitSeq`. A chunk
contains a 256-bit free bitmap and 256 retirement sequences. Free means bitmap
bit 1 and sequence 0; retired means bit 0 and sequence non-zero; live or
reserved means both zero. A one-chunk mutation therefore emits one chunk, one
immutable index path, one or more reservation pages, and one generation header.
Reservation pages form a contiguous, ordered chain so fragmented transactions
are not bounded by one page. Unchanged paths retain their existing page
identities and bytes.

The generation header binds its exact root page identity and CRC; every index
entry recursively binds its child page CRC. The header also binds the
reservation chain digest, generation/parent identity, commit sequences, high-water
boundary, and free/retired summaries with SHA-256. The reservation chain binds
a 128-bit candidate identity and canonical extents for reused data pages,
appended data pages, target metadata pages, and replaced metadata pending
retirement. Its SHA-256 covers every ordered chain page, including page IDs and
successor links. Every decoder validates the pager ID/type/CRC, magic/version,
canonical zero tail, chain order and cardinality, sorted entries, acyclic page
graph, bounds below high-water, summary counts, and semantic digest.

Freelist metadata pages are allocated only from the candidate-owned high-water
tail. Before any metadata page is written, the process reservation ledger
atomically owns the candidate's complete contiguous metadata range as well as
its data-page allocations. A competing candidate skips an owned tail range;
its durable reservation record classifies the skipped prefix as abandoned
append space instead of silently treating those page IDs as its own data or
metadata. A candidate transaction is single-use once materialization begins:
after any page-sink failure, retry starts from the immutable base rather than
reusing a partially assigned COW tree. Once the first metadata write is
attempted, ordinary abandonment cannot release that tail. Failure converts the
complete reserved tail into process-owned burned space; a later candidate skips
it, records the skipped range as abandoned append space, and releases the
process reservation only after that replacement record is durably published.
Replaced parent header,
reservation-chain, chunk, and index pages are recorded with the parent's commit
sequence and imported as retired state by the next candidate; they are not
recursively inserted into the generation that replaces them. Physical
file-length convergence is consequently a #3681 vacuum/rewrite property, not a
claim of this high-water-only codec.

Pages enter the free set only through an explicit recovery capability. Their
`lastReachableCommitSeq` must be strictly before the oldest recoverable root,
every snapshot pin, and the retained history floor. Visible, retryable,
poisoned, and shutdown candidates retain ownership until confirmed durable
publication. Reopen reconstructs surviving ownership from the selected
generation and its durable reservation record; elapsed time, `KeepRecent`, and
the visible commit sequence alone never grant reuse.
