# Value Log (WAL=Value-Store) Plan

Goal: eliminate cached-mode “double write” for large values (write to WAL, then write again to slab during flush) by turning the cached-mode WAL into a **random-access value log** whose records can be referenced directly by the backend index (`ValuePtr`), while preserving:

- fast random writes (memtable-first),
- snapshot isolation,
- bounded WAL growth for workloads dominated by small inline values, and
- the current durability contract (`SetSync`/`WriteSync` are durable; non-sync writes are best-effort).

This doc is a *plan for review* and is intentionally explicit so future agents can execute without missing context.

---

## Problem statement

Today in cached mode:

- `Set` writes a full key/value record to `wal/wal-*.log`.
- Later, flush writes the same key/value again into the backend:
  - small values are written inline into `index.db`,
  - large values are written into `data-*.slab` and referenced by `ValuePtr`.

This is correct and simple, but it doubles write bandwidth for large values (WAL + slab), which shows up as a material wall-time slowdown in write-heavy workloads (e.g. `iavl-bench` with WAL enabled).

---

## Target design (high level)

Treat cached-mode WAL segments as a **value-log**:

- each log record is self-describing + checksummed,
- record offsets are stable so the backend can point to them via `ValuePtr`,
- flush/checkpoint updates the backend index to reference those records (no value copy for large values),
- old segments can be deleted when they contain no live referenced values (or after compaction migrates live values out).

Key property: the log is still a WAL (redo source), but it is also a *value store* for out-of-line values.

---

## Current vs target behavior (by value size)

Assume `T = InlineThreshold`.

### Current system (cached mode)

- `len(value) <= T`:
  - WAL: writes key/value bytes
  - Flush: stores value inline in `index.db`
  - Result: double-write exists (WAL + index), but no slab write
- `len(value) > T`:
  - WAL: writes key/value bytes
  - Flush: writes key/value bytes to slab, then stores pointer in `index.db`
  - Result: double-write exists (WAL + slab)

### Target system (cached mode, “value log” enabled)

- `len(value) <= T` (default policy: keep inline):
  - Value log: writes key/value bytes (for recovery)
  - Flush: stores value inline in `index.db`
  - Segment deletion: once checkpointed, segments that contain no referenced values can still be deleted quickly (same operational shape as today).
- `len(value) > T`:
  - Value log: writes key/value bytes once
  - Flush: stores `ValuePtr` into `index.db` pointing into the value log record (no slab write)
  - Segment lifecycle: segments containing referenced values become “slab-like” and are reclaimed via compaction/GC rather than immediate deletion.

Optional policy variant (later): “always-pointer” (even for `<=T`) to reduce duplication further at the cost of read latency and higher long-lived value-log bytes.

---

## On-disk record format

We need:

- per-record random access by `(file, offset)`,
- explicit `Op` to distinguish delete vs set(empty),
- bounded allocations on corruption (existing slab has `MaxRecordSize` cap),
- CRC32C to align with current slab/page checksums.

### Proposed v2 record layout (value log segments)

Record bytes at file offset `start`:

- `[CRC32C u32]` (Castagnoli) over the bytes `[KeyLen..ValueBytes]` (everything except the CRC itself)
- `[KeyLen u16]`
- `[ValueLen u32]`
- `[Op u8]` (`0=Set`, `1=Delete`)
- `[Key bytes]`
- `[Value bytes]` (present iff `Op=Set`; may be length 0)

`ValuePtr` for a Set record:

- `FileID`: identifies the value-log segment file
- `Offset`: `start + 4` (points at `KeyLen`, consistent with current slab `ValuePtr.Offset` meaning)
- `Length`: `2 + 4 + 1 + len(key) + len(value)` (header fields + payload, excluding CRC)

Notes:
- This keeps the “Offset points to KeyLen (after CRC)” invariant from `TreeDB/specs/spec.md`.
- Existing slab files remain v1 (`[CRC][KeyLen][ValLen][Key][Val]`) unless/until migrated; value-log segments are v2.

---

## FileID scheme and readers

We must avoid collisions with existing slab `FileID` values (`data-*.slab` IDs start at 0 and grow).

### Proposed scheme

- Reserve the high bit of `ValuePtr.FileID` as a “kind bit”.
  - `FileID & 0x8000_0000 == 0`: regular slab (`data-*.slab`, v1)
  - `FileID & 0x8000_0000 != 0`: value log segment (v2); real segment ID is `FileID &^ 0x8000_0000`

### Required plumbing

- Extend the backend value-reading layer to dispatch by kind:
  - regular slabs continue to use `slab.SlabManager` paths
  - value-log files need a parallel manager (or an extension of slab manager) that can:
    - open segment files (likely in `Dir/wal/`)
    - `Read(ptr)` and `ReadUnsafe(ptr)` returning the value bytes
    - support snapshot pinning (so segments are not removed while referenced)

---

## Lifecycle flows

### 1) Cached write (`Set`/`Delete`)

1. Append record to value log segment (via a single writer goroutine + batching, similar to current WAL group-commit).
2. Update memtable:
   - For `Set`:
     - store the *user value* bytes for fast reads (initially keep existing behavior),
     - and store the `ValuePtr` for `len(value) > T` so flush can set the pointer without rewriting the value.
   - For `Delete`: store tombstone (no pointer).

### 2) Cached flush (immutable memtable → backend)

For each key in the memtable iterator:

- Tombstone → `backendBatch.DeleteView(key)` (or `Delete`).
- Put:
  - if `len(value) <= T` → `backendBatch.SetView(key, value)` (inline, current behavior)
  - else → `backendBatch.SetPointer(key, ptr)` (new behavior; avoids writing to slab)

This requires the cached layer to have the per-key `ValuePtr` for large values at flush time.

### 3) Cached checkpoint / trimming

Checkpoint still needs to:

- establish a durable boundary (backend `WriteSync`),
- rotate to a new active value-log segment so older segments can be considered for deletion/GC.

But **deletion policy changes**:

- Today: delete all old `wal-*.log` after checkpoint.
- Target: delete only those segments that are *known to have no live referenced values*.

Segments containing referenced values are retained and reclaimed later via compaction/GC.

### 4) Backend open / crash recovery

Backend must support:

- replaying value-log segments that exist on disk into the backend index (like today’s `replayWALIntoBackend`), but:
  - for `Set` where `len(value) > T`, the replay should install a pointer into the index *pointing to the record itself* (no slab rewrite),
  - for small values, replay can inline into index as today.

After replay:

- segments that contain no referenced values can be deleted immediately,
- segments with referenced values are retained.

### 5) Compaction / GC

To avoid unbounded growth of retained segments, add a GC path:

- Select candidate value-log segments (old, high dead ratio).
- Copy live referenced values into regular slabs (`data-*.slab`) using existing compaction machinery:
  - verify liveness by comparing index pointer to the source pointer
  - apply micro-batched pointer updates (`ApplyCompactionMicroBatches`)
- After migrating live values out, delete the old segment.

---

## The hard part: storing `ValuePtr` in the memtable

Flush needs `(key → ptr)` for `len(value) > T`. There are three viable approaches; pick one in Phase 1 and keep the others as fallback.

### Option A (recommended first): side-map per mutable shard

- Keep memtables storing raw value bytes (no interface change).
- Maintain an additional map for large values only:
  - `largePtr[shard] map[string]page.ValuePtr`
  - updated on every Set/Delete:
    - Set(large): set pointer
    - Set(small)/Delete: delete any existing pointer entry for key
- On rotation: freeze the map alongside the memtable and queue it for flush.

Pros: minimal memtable changes; easiest to get correct.
Cons: duplicates keys in a Go map; extra per-write overhead.

### Option B: store pointer bytes in memtable and read values from value log

- Memtable stores a fixed 16-byte encoding of `ValuePtr` for all Sets.
- Cached `Get` detects “pointer mode” and serves values by reading from value-log manager.

Pros: reduces memtable memory; flush is simple.
Cons: changes read path; requires value-log reads for hot keys; cannot mix with raw values without tagging.

### Option C: extend memtable implementations to store pointer metadata

- Add pointer field to internal memtable nodes/entries; iterator can expose it.

Pros: avoids key duplication and avoids extra maps.
Cons: invasive across all memtable modes; higher risk.

---

## Phased implementation plan (reviewable TODO list)

### Phase 0 — Spec + baseline gates (no behavior change)

- [ ] Write down the final record format + FileID kind scheme in `TreeDB/specs/spec.md` (add an explicit “Value log segments” section).
- [ ] Identify the thresholds:
  - `InlineThreshold` continues to govern index inlining.
  - Value-log pointer threshold initially equals `InlineThreshold`.
- [ ] Add explicit benchmark gate(s) for WAL overhead (already exists for cached write concurrency; add iavl-bench notes if needed).
- [ ] Decision checkpoint: confirm we keep “inline small values” for Phase 1.

### Phase 1 — Value-log writer/reader (v2) behind a flag

- [ ] Add a new package `TreeDB/internal/vlog` (or evolve `internal/wal`) implementing:
  - `Writer.Append(op, key, value) (ptr page.ValuePtr, err error)` for OpSet only (or return ptr + ok flag)
  - `Writer.AppendBatch(records []Record) ([]page.ValuePtr, err error)`
  - `Writer.RotateTo(path)` similar to WAL
  - `Reader` that can iterate records and also `Read(ptr)` to fetch value bytes by offset
  - record-size caps matching slab’s `MaxRecordSize` behavior
- [ ] Keep the old WAL reader/writer for `ModeBackend` until recovery is updated.
- [ ] Tests:
  - encode/decode round trip; corruption/truncation handling.

### Phase 2 — Backend value-reader dispatch for value-log `FileID`s

- [ ] Extend `slab.SlabManager` (or introduce a new `valuefiles.Manager`) to support:
  - registering/opening value-log segment files by ID
  - `Read(ptr)` / `ReadUnsafe(ptr)` for v2 format
  - snapshot pinning / refcounts like `SlabSet` so segments aren’t deleted while referenced
- [ ] Tests:
  - store a pointer to a value-log record in a leaf entry, commit, then `Get` returns correct value.

### Phase 3 — Cached layer writes value log + stores pointers (Option A)

- [ ] Replace cached-mode WAL append path with value-log append under a feature flag:
  - preserve existing group-commit structure (single writer goroutine).
- [ ] Implement `largePtr` side-maps per shard (Option A):
  - rotation queues both memtable + map
  - flush consumes both
- [ ] Tests:
  - concurrent writes, flush, then backend `Get` matches.
  - overwrite large→small and small→large transitions update pointer map correctly.

### Phase 4 — Flush uses `SetPointer` for large values

- [ ] Expose `SetPointer` on the backend batch wrapper (`TreeDB/db/batch.go`) so cached flush can type-assert it (mirrors existing `SetView` usage).
- [ ] Update cached flush to:
  - use `SetPointer` for large values (pointer sourced from `largePtr`)
  - keep `SetView` for small values
- [ ] Update backend WAL recovery (`TreeDB/db/wal_recovery.go`) to:
  - replay value-log segments and keep referenced segments instead of deleting unconditionally
- [ ] Crash tests:
  - replay recovers large values without rewriting them into regular slabs.

### Phase 5 — Segment trimming + GC/compaction integration

- [ ] Add a way to determine if a value-log segment has any live referenced pointers:
  - simplest: maintain refcounts per segment during flush/replay by observing installed pointers and overwritten pointers
  - fallback: periodic scan (expensive) if refcount is complex
- [ ] Extend background compaction to consider value-log segments as candidates:
  - copy live values into regular slabs and apply pointer updates
  - delete segments when fully migrated
- [ ] Tests:
  - after compaction, reads still work and segments are removed.

---

## Testing / validation checklist

- Correctness:
  - [ ] point reads return correct bytes across overwrites and deletes
  - [ ] iterators skip tombstones and return correct values (including pointer-backed)
  - [ ] snapshot isolation preserved (segments pinned while snapshots alive)
- Crash safety:
  - [ ] torn tail in value-log segment: repair/truncate works
  - [ ] replay produces coherent backend state
- Operational:
  - [ ] checkpoint does not delete segments that are still referenced
  - [ ] disk usage remains bounded for small-value workloads (segments deleted normally)
- Performance:
  - [ ] iavl-bench WAL-enabled approaches WAL-disabled wall time by eliminating large-value double write
  - [ ] read latency impact is measured (pointer-backed reads vs inline)

---

## Open questions (explicit review points)

1) Should we keep value-log segments in `Dir/wal/` or move them into the main dir as `data-*.slab`?
2) Do we want to support both v1 slabs and v2 value-log records indefinitely, or migrate slabs to v2 over time?
3) What is the minimum viable liveness tracking for safe segment deletion without scanning the whole index?
4) Do we eventually want an “always-pointer” mode for small values (and if so, how do we quantify read regression)?

