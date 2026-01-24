# PR15: value-log mmap read path (reduce syscalls)

## Summary
- Add an mmap-backed fast path for value-log reads (`TreeDB/internal/valuelog`).
  - `File.Read`, `File.ReadAppend`, and `File.ReadUnsafe` now attempt to serve reads from an mmap of the segment.
  - Falls back to existing `ReadAtWithDict` decoding when mmap is unavailable/out-of-date.
- Track value-log mmap remap stats via `valuelog.Manager.RemapStats()` (was hardcoded to `0,0`).

## Why
- Mode4/value-log reads were syscall-heavy (`ReadAt`) compared to slab reads; this makes value-log workloads (especially read-heavy) slower than the mode1 baseline.
- Using mmap for value-log segments removes per-read `pread` syscalls and lets `ReadAppend` copy directly from mapped memory.

## Tests
- `go test ./TreeDB/internal/valuelog -count=1`
- `go test ./... -count=1`

## unified_bench output (sample)
Mode1 (cached, WAL off, value-log off):
`go run ./cmd/unified_bench -dbs treedb -test random_read -keys 200000 -valsize 1024 -batchsize 1000 -treedb-allow-unsafe=true -treedb-relaxed-sync=true -treedb-disable-read-checksum=true -treedb-slab-compression=none -treedb-vlog-dict-train-bytes=-1 -treedb-force-value-pointers=true -treedb-disable-wal=true -treedb-disable-journal=false -treedb-disable-value-log=true -treedb-split-value-log=false -treedb-memtable-value-log-pointers=false`

```
Random Read / TreeDB = 401,278
```

Mode4 (cached, deferred value-log, journal off):
`go run ./cmd/unified_bench -dbs treedb -test random_read -keys 200000 -valsize 1024 -batchsize 1000 -treedb-allow-unsafe=true -treedb-relaxed-sync=true -treedb-disable-read-checksum=true -treedb-slab-compression=none -treedb-vlog-dict-train-bytes=-1 -treedb-force-value-pointers=true -treedb-disable-wal=false -treedb-disable-journal=true -treedb-disable-value-log=false -treedb-split-value-log=true -treedb-memtable-value-log-pointers=false`

```
Random Read / TreeDB = 326,964
```

