# Agent Plan: Persistent Value Log (No Slabs)

This document reflects the current TreeDB design: the value log is **persistent**
storage and is **not** an ephemeral WAL. The legacy "slab" storage path is
removed; large values are stored in the value log and referenced by pointers in
the index.

## Current Architecture

- **WAL (journal):** Redo log for durability/recovery. Can be disabled in
  cached mode. WAL and value log are decoupled.
- **Value log (vlog):** Persistent append-only store for large values. Value
  pointers are stored in the index. The value log has:
  - **GC** based on reachability (scans index for pointers).
  - **Rewrite/compaction** tooling (vlog rewrite) to reclaim space.
  - **Read integrity options** (checksum verification controls).
- **Index (B-Tree):** Stores inline values or value-log pointers (ValuePtr).

## Implications

- Value-log pointers are **valid long-term**; segments are not treated as
  ephemeral and must not be truncated just because they’re old.
- Pointer thresholds are safe **as long as** the value log is managed as
  persistent storage (GC/rewrite) and segments are only deleted when
  unreachable.

## Testing Strategy

Focus on pointer durability and GC correctness:

### Pointer durability after reopen
- **Setup:** `Options.ValueLog.PointerThreshold=1` (force value-log pointers).
- **Action:** Write values, `Checkpoint()` (or `WriteSync`), close, reopen.
- **Assert:** Values remain readable and pointers resolve after reopen.
- **Existing coverage:** `TreeDB/reopen_verify_test.go` (e.g. `TestReopenVerify_WALOn_Checkpoint`, `TestReopenVerify_WALOn_WriteSync`).

### GC deletes unreferenced segments
- **Setup:** Write values to the value log, delete keys, checkpoint.
- **Action:** Run `DB.ValueLogGC`.
- **Assert:** Fully-unreferenced segments are removed; referenced segments remain.
- **Existing coverage:** `TreeDB/db/vlog_gc_test.go` (`TestValueLogGC_RemovesUnreferencedSegment`).

### Leaf key compression density
- **Harness:** `TreeDB/node/leaf_density_test.go` (`BenchmarkLeafPageDensity`) measures keys/page with prefix compression on/off and enforces minimum effectiveness.

## Notes

- Any documentation describing TreeDB values being stored in "slabs" is legacy and should be updated or removed (HashDB still uses slab segments).
- If WAL is disabled, value-log writes can still be deferred to flush boundaries,
  but the value log remains persistent storage.
