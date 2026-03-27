# Value-Log Dict Class Split Experiment (2026-03-27)

Goal:

- Test a minimal-churn split of dictionary state by payload class:
  - `single_value` (all non-outer-leaf value-log payloads)
  - `outer_leaf` (outer-leaf payloads)
- Compare against the single shared dict class baseline.
- Focus on end-of-run on-disk bytes (`dir`) and gzip sanity, with post-rewrite state as the primary target.

## Code under test

Branch: `followon/vlog-dict-class-split-experiment`

Key behavior introduced:

- New `ValueLog.DictClassMode` (`single` or `split_outer_leaf`).
- Class-aware dict current pointers (`current/<class>`) in dictdb with global fallback.
- Class-aware dict-ID selection on value-log write paths.
- `unified-bench` flag: `-treedb-vlog-dict-class-mode`.

## Reproduction

Build:

```bash
go build -o ./bin/unified-bench ./cmd/unified_bench
go build -o ./bin/treemap-local ./TreeDB/cmd/treemap
```

Run A (single class):

```bash
./bin/unified-bench \
  -profile fast \
  -dbs treedb \
  -keys 5000000 \
  -test batch_write \
  -val-pattern celestia_height_prefix_fill \
  -keep \
  -treedb-vlog-compression dict \
  -treedb-vlog-auto-policy size \
  -treedb-vlog-compression-autotune aggressive \
  -treedb-vlog-compression-variant dict \
  -treedb-force-value-pointers=true \
  -treedb-vlog-dict-train-bytes 1048576 \
  -treedb-vlog-dict-dict-bytes 32768 \
  -treedb-vlog-dict-class-mode single \
  -treedb-cache-stats-after-tests \
  -treedb-vlog-rewrite-after-run
```

Run B (split class):

```bash
./bin/unified-bench \
  -profile fast \
  -dbs treedb \
  -keys 5000000 \
  -test batch_write \
  -val-pattern celestia_height_prefix_fill \
  -keep \
  -treedb-vlog-compression dict \
  -treedb-vlog-auto-policy size \
  -treedb-vlog-compression-autotune aggressive \
  -treedb-vlog-compression-variant dict \
  -treedb-force-value-pointers=true \
  -treedb-vlog-dict-train-bytes 1048576 \
  -treedb-vlog-dict-dict-bytes 32768 \
  -treedb-vlog-dict-class-mode split_outer_leaf \
  -treedb-cache-stats-after-tests \
  -treedb-vlog-rewrite-after-run
```

Optional post-run audit per kept dir:

```bash
./bin/treemap-local vlog-audit <db-dir> -rw -frame-stats -json
```

Optional gzip sanity:

```bash
du -sb <db-dir>
tar -C <db-dir> -cf - . | gzip -c | wc -c
```

## Results

Environment capture directory:

- `/tmp/vlog_dict_class_split_capture_20260327_081816`

Primary metrics:

| mode | batch_write ops/s | rewrite before->after (MiB) | dir bytes (post rewrite) | tar+gzip bytes | write_mode frames dict | write_mode frames block |
|---|---:|---:|---:|---:|---:|---:|
| `single` | 4,534,404 | 149 -> 147 | 154,425,429 | 16,528,050 | 0 | 8,850 |
| `split_outer_leaf` | 4,558,022 | 149 -> 147 | 154,400,853 | 16,527,833 | 0 | 8,188 |

Selected diagnostics:

- Both runs published only `class=single_value` dictionaries.
- Split run additionally trained an outer-leaf-class candidate (`samples=8`, `ratio=0.005`) but did not publish an outer-leaf dict.
- `vlog_write_mode.frames.dict=0` in both runs.
- `vlog_write_mode.frames.block>0` in both runs.
- `vlog-audit -frame-stats` reported identical post-rewrite frame mode for both runs:
  - mode set: `grouped_block_snappy` only
  - `grouped_raw_payload_bytes=742,920,192`
  - `grouped_stored_payload_bytes=90,862,607`

## Interpretation

- The class-split plumbing is active and learns class-specific candidates.
- For this workload/config, effective on-disk output remains block-snappy-only after rewrite.
- Result: split class mode is effectively neutral for end-run bytes and gzip sanity in this benchmark.

## Current State (2026-03-27)

- Primary optimization target remains dict/block value-log compression for end-of-run
  bytes (`dir`) with gzip as a sanity check.
- Template pre-transform experiments were informative for isolated pointer corpora,
  but were not a clear win for full Celestia outer-leaf heavy storage paths.
- Runtime template compression is now forced off in `treedb.Open`/cached opens so it
  does not affect production/benchmark runtime behavior while dict work continues.
