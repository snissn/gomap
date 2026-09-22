# Future Work

Active roadmap is tracked in `TODO.md`.

## Major Unplanned Items

### Windows Support for TreeDB
- **Current Status**: TreeDB supports Windows (CI includes Windows test jobs) via platform-specific mmap helpers in `TreeDB/pager`.
- **Goal**: Continue hardening Windows-specific file-locking and mmap edge cases (crash recovery, file rename semantics, and cleanup).

### TreeDB Compression
- **Current Status**: TreeDB stores values inline in the B+Tree or out-of-line in the persistent value log (`Dir/maindb/value_vlog/`). The value log supports Zstandard compression and optional dictionary training/autotune.
- **Goal**: Evaluate additional compression opportunities (e.g., page-level compression) and improve operator ergonomics (GC/rewrite tooling and observability).

### TreeDB Index Compaction (Online)
- **Current Status**: `CompactIndex` is a "Stop-the-World" operation that rebuilds the B-Tree.
- **Goal**: Incremental background compaction for the B-Tree index to reduce fragmentation without blocking writers for long periods.

### HashDB Ordered Scans
- **Current Status**: HashDB only supports `ForEach` (arbitrary order).
- **Goal**: Investigate if a secondary index or approximate ordering is feasible without sacrificing random write performance.
