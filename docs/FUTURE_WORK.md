# Future Work

Active roadmap is tracked in `TODO.md`.

## Major Unplanned Items

### Windows Support for TreeDB
- **Current Status**: TreeDB is Unix-only (Linux/macOS) due to direct usage of `unix.Mmap` in `TreeDB/pager`.
- **Goal**: Abstract the pager implementation to support `mmap` on Windows (via `CreateFileMapping`/`MapViewOfFile`).
- **HashDB**: Already supports Windows (via `mmap-go` and fallback paths).

### TreeDB Compression
- **Current Status**: TreeDB stores values raw (inline or slab).
- **Goal**: Implement Snappy/Zstd compression for slab values (similar to HashDB).

### TreeDB Index Compaction (Online)
- **Current Status**: `CompactIndex` is a "Stop-the-World" operation that rebuilds the B-Tree.
- **Goal**: Incremental background compaction for the B-Tree index to reduce fragmentation without blocking writers for long periods.

### HashDB Ordered Scans
- **Current Status**: HashDB only supports `ForEach` (arbitrary order).
- **Goal**: Investigate if a secondary index or approximate ordering is feasible without sacrificing random write performance.