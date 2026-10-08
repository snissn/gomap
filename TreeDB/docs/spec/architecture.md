# TreeDB Architecture

## 1. Engine Model

TreeDB exposes one primary public engine (`treedb.Open`) with a cached write-back layer enabled by default.

The runtime consists of:

- B+Tree index in `index.db` (mmap pager, copy-on-write commits).
- Value log (`maindb/value_vlog/value-l*.log`) for out-of-line large values.
- Optional split leaf log (`maindb/leaf_vlog/value-l*.log`) for outer leaf
  generations.
- Commit log / journal (`maindb/wal/commit-l*.log`) for cached-mode replay;
  future user-command WAL frames extend this same segment family.
- Optional side stores:
  - `dictdb/` for persistent dictionary bytes,
  - `templatedb/` for template compression definitions.

TreeDB's value log is the only value storage path for values.

## 2. Component Responsibilities

### 2.1 Index (B+Tree)

- Stores keys in lexicographic order.
- Stores either:
  - inline value bytes, or
  - a fixed-size `ValuePtr` reference into the value log.
- Uses copy-on-write page rewrites and dual complete root selectors for durability.
- Fresh `no_wal_fast` stores select a top-level exact-key immutable delta
  directory over a materialized DATA B-tree base, with fixed complete A/B
  capsules in `index.db.primary`. Other fresh profiles retain DATA dual-meta
  selection. See [selected capsule format](storage-format.md#30-selected-primary-capsules-v6-construction).
- Capsule readers own immutable copied directory bytes and actual component
  references. Recovery, physical export and Snapshot restore use the same
  complete capsule authority. Vacuum commits an identity-bound private
  DATA/PRIMARY pair under writer exclusion, retains old Snapshot owners and
  refuses namespace cutover with independently retained physical cuts.
  Joint recovery runs under LOCK before legacy cleanup. Native whole-cost,
  semantic materialization progress and performance remain unqualified.

### 2.2 Value Log

- Append-only segment files.
- Stores large values and grouped frames.
- Is persistent storage, not a transient write-ahead buffer.

### 2.3 Commit Log (Journal/WAL)

- Redo metadata stream used by cached-mode recovery.
- Replays legacy raw `set inline`, `set rid`, and `delete` operations.
- Target user-command WAL adds typed command frames to the same journal rather
  than adding collection-specific WAL files.
- Is independent from value-log lifetime.

### 2.4 Cached Layer

- Buffers writes in mutable memtables, or explicitly selected immutable COW roots.
- Mutable modes rotate immutable memtables; COW freezes bounded sources on the
  write/maintenance side. Both flush through canonical backend batches.
- Writes commit-log/value-log data as part of ingest path.
- Applies adaptive backpressure and optional background checkpointing.

### 2.5 Backend Layer

- Owns pager, freelist/lifecycle, commit sequence, and snapshots.
- Applies batches via zipper merge into new page generations.
- Maintains active value-log segment set for reads.

### 2.6 Immutable COW cache

`internal/memtable` also supplies a separately owned COW writer/read-header
capability around installed tidwall/btree. It owns immutable string payloads,
private preparation and finite source-generation reservations. Explicit
`MemtableMode="cow_btree"` integrates it with one coherent cached cut: fixed
shard roots, bounded frozen sources and a retained backend snapshot basis.
Capture pins that cut without rotating shards or scanning historical records.
Preparation admits all changed roots and resource owners before command append;
publication installs the complete cut once. See
[COW cached publication](cow-cache-publication.md) for public capabilities,
limits, checkpoint handoff and refusal, and
[immutable memtable ownership](cow-memtable-ownership.md) for the precise
pre-frame preparation, lease and retirement contract; mutable `BTree.Freeze`
does not provide this capability.

## 3. Directory Layout

Public `treedb.Open(opts)` treats `opts.Dir` as a root.

Default root layout:

- `<root>/maindb/`
  - `index.db`
  - `index.db.primary` (selected PRIMARY format)
  - `LOCK`
  - `wal/`
    - `commit-l<lane>-<seq>.log`
  - `value_vlog/`
    - `value-l<lane>-<seq>.log`
  - `leaf_vlog/`
    - `value-l<lane>-<seq>.log`
- `<root>/dictdb/`
  - `index.db`
  - `LOCK`
- `<root>/templatedb/` (only when template mode enabled)
  - `index.db`
  - `LOCK`

If `DisableSideStores=true`, the main DB is opened directly at `<root>`.

## 4. Open and Locking Model

### 4.1 Read-write open

- Backend DB acquires an exclusive lock file at `<maindb>/LOCK`.
- Side stores (if enabled) acquire their own exclusive lock files.
- Only one read-write process may open a given DB directory.

### 4.2 Read-only open

- Read-only open does not acquire write locks.
- Read-only open does not run mutating recovery steps.
- Required directories must already exist.

## 5. Snapshot and Reader Model

- Readers use snapshots (`AcquireSnapshot`) with a pinned commit sequence.
- Snapshots pin the index generation and referenced value-log set.
- In cached mode, snapshots and iterators also include buffered memtable writes by reading from immutable queued memtables (newest-first) plus a backend snapshot.
- Iterators are point-in-time views and must be closed.
- Writers are serialized; readers run concurrently.

In explicit COW mode, root/source/basis ownership belongs to the same cut.
Pointer/dictionary materialization and copying run outside its publication
latch. An accepted flush retains its exact covered-prefix receipt through
handoff refusal or a reported post-acceptance error; newer sources remain
visible. Maintenance refresh uses the existing flush fence and physical
retention authorities. COW does not introduce a second value-log GC registry.

## 6. Write Path Modes

Durability mode controls WAL/journal behavior, not whether value log exists.

- `DurabilityDurable`: WAL on, sync on sync APIs.
- `DurabilityWALOnRelaxed`: WAL on; legacy sync APIs are relaxed, while command-WAL explicit sync APIs opt up to a durable V2 prefix.
- `DurabilityWALOffRelaxed`: WAL off, value log still used for pointers.

See `TreeDB/docs/spec/write-path-and-durability.md`.

## 7. Architectural Constraints for Reimplementations

A compatible implementation should preserve:

1. Lexicographic ordered index semantics.
2. Long-lived value-pointer semantics.
3. Separation of commit log durability from value-log storage lifetime.
4. Coherent recovery order:
   - index meta selection,
   - value-log RID scan,
   - commit-log replay,
   - replay log cleanup.
5. Single-writer + multi-reader snapshot behavior.


The selected V6 ordinary producer shares constructor-to-capsule machinery with
the typed bounded construction API. Its directory certificate borrows the
actual immutable arena image; its DATA certificate is reusable only for the
identical retained generation/pager/base/system identities. The existing
runtime and root owners retain physical custody. Typed codec output is reused
inside the same guarded publication, while Open/recovery validate raw physical
bytes independently. This introduces no additional ownership lifecycle or
scheduler. Native producer admission, protected semantic floor proof and
same-Request materialization progress are still construction obligations.

### Ordinary transaction ownership from logical construction

For selected V6 writes, the existing DurableRootTransaction is created by the
ordinary zipper before its immutable logical directory and components are
constructed. The arena bank retains that transaction as constructor
discoverability. The visible member, candidate and later promotion refer to the
same transaction; they do not each allocate a new control owner. After binding,
the existing resource set's owner cell controls handoff. Independent physical
root references are still necessary: the constructor holds one reference through
Finish, visible state holds its own, and durable current/parent closures acquire
their actual references at promotion.

Relaxed visibility does not select a durable slot or parent. Each logical root
remains independently immutable while later relaxed writes replace visible
state. Fresh promotion binds the actual current parent and alternate fixed slot,
encodes the complete three-page capsule, and retains its two closures in the
same transaction. The existing DATA and PRIMARY dependency syncs precede capsule
installation; the final PRIMARY fence remains separate. Durable report runs the
same transaction's ordinary Finish, detaches constructor discoverability,
releases the actual resource set and drops the constructor edge. Old readers
retain their independently owned immutable roots after that detach.

Ordinary Finish remains synchronous and uses ordinary resource callbacks.
The selected experiment does not connect the native private producer or its
typed Accepted retirement cursor. Native admission, complete accounting and a
protected semantic-floor/materialization adapter remain unqualified.

### Ordinary publication views and producer input

Selected V6 DB current state and visible runtime membership borrow the existing
DurableRootTransaction's immutable publication projection. Its candidate and
single-member group are views of the same construction owner; the resource-set
owner cell controls transferred custody. Constructor, visible-state, durable-slot,
parent and reader references remain distinct physical edges.

Store inputs describe their actual accepted logical postimage. A constant-size
pre-compaction summary survives ordinary batch and cache backend-bypass routes;
existing physical flushes retain their physical semantics. Raw or mixed input
remains unqualified. Combiner input binds to a validated owned key copy because
the queue can survive the caller's stop result. Exact floor metadata follows the
same ordinary Write or WriteSync route without changing its ACK boundary.

This ordinary prerequisite exports no Request, Capture, protected-floor issuer
or protected-version proof. It does not connect native Accepted/Finish, semantic
materialization adaptation or same-Request rank.

The SAME ordinary transaction remains the identity of its candidate/current
views through publication. After successful synchronous Finish it drops
completed callback, prepared-root, candidate, resource-set and promotion scratch
references. Independently owned state, slot, reader and cut references retain the
immutable roots and persistent value resources; a consumed transaction is not an
extra owner of its former mutable publication projection.

The published V6 logical root has no backpointer to mutable publication work.
Private constructor discovery ends at successful Bind before visibility; the
runtime and coordinator retain the same transaction until its once-only Finish.
Promotion owns an additional immutable 4096-byte RAM directory and genuine
component-group references. Coalesced dependency banks attach only to that
promotion/slot root, so an admitted relaxed reader's immutable closure cannot grow
when later durable publication selects the physical slot. The capsule codec
continues to validate the original constructor's exact logical operand; the
separate RAM address is slot custody, never an extra durable selector.
