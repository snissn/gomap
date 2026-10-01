# Iteration Semantics

## TL;DR

- TreeDB iterators are ordered and use bounds `[start, end)`:
  - `start` is inclusive (first key `>= start`)
  - `end` is exclusive (stop once key `>= end`)
  - `nil` means unbounded
- Iterators are point-in-time views and must be closed.

## Who Is This For?

- Anyone using scans for building snapshots, compaction, export, or replication.
- Benchmark authors (so “scan” results mean something consistent).

## TreeDB

### Ordering

- Iteration is in lexicographic key order.
- `ReverseIterator` iterates in reverse lexicographic order.

### Bounds

- `Iterator(start, end)` yields keys in the half-open range `[start, end)`.
- `ReverseIterator(start, end)` yields the same range, but in reverse order.
- If `start >= end` (both non-nil), the iterator is immediately invalid.

### Tombstones

- The backend engine does not currently persist tombstones to disk.
- The cached write-back layer uses tombstones internally and filters deleted keys from iterator output.

### Retained external-MVCC versions

`mvcc.Store.IterateVersions` pins one snapshot and returns retained logical
versions with owned `Entry` bytes and explicit logical tombstones. Set
`VersionIteratorOptions.ExactKey` to scan one logical key, intersected with
prefix/bounds and the timestamp ceiling. Nil leaves this filter disabled; use
`[]byte{}` for the empty logical key. Forward timestamps descend; reverse
timestamps ascend. Seek stays inside the original physical domain. Close the
iterator to release its full snapshot. See the [canonical MVCC contract](../../TreeDB/docs/spec/contracts.md)
for floor and source-selection fallback rules.

## HashDB

- HashDB does not expose an ordered iterator.
- HashDB exposes `ForEach(fn)` which iterates all live keys in arbitrary order.
  - For the sharded `*hashdb.HashDB` entrypoint, `ForEach` takes an exclusive snapshot (blocks writers, flushes shard caches, then iterates backend state).
