# Issue 2437 Memtable/Flush Allocation Blocker

Date: 2026-06-05
Branch: `codex/2437-memtable-flush-alloc-pressure`
Base commit: `a5efbdb51`

## Summary

Issue #2437 targets allocation pressure from the cached append-only memtable and
flush path. The largest reported unified `random_write` allocation bucket was
`AppendOnly.updateLatestIndexLocked`, so the first implementation probe tested
whether pre-reserving the unordered latest-index maps would reduce allocation
without changing snapshot or flush ordering.

The allocation bucket is real and movable, but the direct reservation strategy
does not meet the merge gate: it reduces random-write alloc-space while causing a
material unified-bench throughput regression.

## Probe 1: Full Existing-Capacity Latest-Index Reservation

Implementation shape:

- Reserve latest-index map capacity from the append-only table's existing entry
  slice capacity when rebuilding or lazily creating the latest index.
- Preserve all existing ordered, sorted-run, snapshot, iterator, and flush
  semantics.

Focused benchmark added temporarily for the probe:

```sh
GOWORK=off go test ./TreeDB/internal/memtable \
  -run '^$' \
  -bench '^BenchmarkAppendOnlyRandomKey64LatestIndexAndIterator$' \
  -benchmem -count=5 -benchtime=1s \
  -memprofile /tmp/2437_candidate_memtable.mem.pprof
```

Focused benchmark result versus an `origin/main` worktree with the same temporary
benchmark:

| Variant | ns/op range | B/op range | allocs/op |
| --- | ---: | ---: | ---: |
| baseline | 10.75ms-13.66ms | 3.10MB-3.28MB | 282 |
| full reserve | 9.39ms-12.29ms | 1.60MB-1.94MB | 149 |

Focused allocation profile movement:

| Variant | Total alloc-space | Top latest-index bucket |
| --- | ---: | ---: |
| baseline | 1765.73MB | `updateLatestIndexLocked` 1286.83MB |
| full reserve | 1530.39MB | `rebuildLatestIndexLocked` 995.23MB |

This proves repeated map growth in `updateLatestIndexLocked` is a meaningful
allocation source.

## Unified-Bench Gate Failure

Commands:

```sh
TMPDIR=/mnt/fast4tb/tmp /tmp/unified-bench-2437-base \
  -dbs treedb -profile fast \
  -test sequential_write,random_write \
  -keys 10000000 -progress=false \
  -profile-dir /tmp/2437_unified_base_20260605_154128 \
  --path-label native-fastpath \
  -treedb-vlog-compression-variant=block_snappy

TMPDIR=/mnt/fast4tb/tmp /tmp/unified-bench-2437-candidate \
  -dbs treedb -profile fast \
  -test sequential_write,random_write \
  -keys 10000000 -progress=false \
  -profile-dir /tmp/2437_unified_candidate_20260605_154204 \
  --path-label native-fastpath \
  -treedb-vlog-compression-variant=block_snappy
```

Throughput:

| Variant | Sequential Write | Random Write |
| --- | ---: | ---: |
| baseline | 4,199,916 ops/sec | 514,270 ops/sec |
| full reserve | 3,668,162 ops/sec | 402,447 ops/sec |
| delta | -12.7% | -21.8% |

Random-write allocation:

| Variant | Total alloc-space | Latest-index bucket |
| --- | ---: | ---: |
| baseline | 2049.76MB | `updateLatestIndexLocked` 750.50MB |
| full reserve | 1614.71MB | `rebuildLatestIndexLocked` 387.71MB |
| delta | -21.2% | -48.3% |

The implementation reduces the target allocation bucket, but the throughput
regression is material and blocks merge under #2437.

## Probe 2: Tuned 16x Fallback Reservation

A smaller reservation cap was also tested: reserve up to `count * 16`, bounded by
the existing entry-slice capacity.

Focused benchmark:

| Variant | ns/op range | B/op range | allocs/op |
| --- | ---: | ---: | ---: |
| baseline | 10.75ms-13.66ms | 3.10MB-3.28MB | 282 |
| tuned 16x | 14.48ms-19.12ms | 2.71MB-3.28MB | 247-248 |

The tuned variant keeps most of the runtime cost while preserving most of the
allocation churn, so it was not run through unified-bench.

## Tests

The following focused correctness commands passed in the isolated #2437 worktree:

```sh
GOWORK=off go test ./TreeDB/internal/memtable ./TreeDB/caching ./TreeDB/db -count=1
GOWORK=off go test ./TreeDB/caching -run 'Snapshot|Flush|AppendOnly|Memtable' -count=1
```

Package results:

- `TreeDB/internal/memtable`: pass
- `TreeDB/caching`: pass
- `TreeDB/db`: pass

## Conclusion

This issue should not merge a naive latest-index map reservation. The targeted
allocation bucket is confirmed, but reducing it by preallocating maps shifts cost
into larger rebuild/reservation work and materially reduces end-to-end
throughput.

Recommended next directions:

- Avoid building the hash latest index for random 8-byte-key append-only
  memtables until a reader/flush actually needs it.
- Preserve sorted-run lookup longer or use a cheaper latest-index representation
  specialized for 8-byte keys.
- Reduce `buildSortedLatestSnapshotLocked` and `buildSortedLatestIndicesLocked`
  allocation/copying, since those remain visible after the latest-index probe.
- Separately investigate `getAppendOnlyEntries` and
  `getAppendOnlyDirectValueArenaChunk`; they remain large allocation buckets in
  both baseline and candidate.

