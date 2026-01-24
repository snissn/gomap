# Summary
- Add deterministic value-log autotune bench suite + CLI runner.
- Introduce deterministic encode-cost hooks and virtual sink for wall-time simulation.
- Add bench runner and microbench coverage for autotune decisions.

# Tests
- `go test ./... -count=1`
- `go test ./internal/valuelog -count=1`

# Benchmarks
- `go run ./cmd/unified_bench -suite vlog_autotune -case marquee_regime_shift -validate`

```
2026/01/22 06:47:41 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=128 raw=131072 stored=8966 ratio=0.068
2026/01/22 06:47:41 treedb: value-log dict published dict_id=10224528226473514282 k=4 payload_ratio=0.053 total_ratio=0.057
2026/01/22 06:47:41 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=1024 raw=1048576 stored=91198 ratio=0.087
2026/01/22 06:47:41 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=1024 raw=1048576 stored=91198 ratio=0.087
2026/01/22 06:47:41 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=128 raw=131072 stored=8966 ratio=0.068
2026/01/22 06:47:41 treedb: value-log dict published dict_id=15797060153562598949 k=4 payload_ratio=0.053 total_ratio=0.057
suite: vlog_autotune
case: marquee_regime_shift
  mode: off          throughput=4.921 MB/s raw=6291456 stored=6291456 wall_ns=1278566400
    segment: segment_a_compressible     kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
    segment: segment_b_incompressible   kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
    segment: segment_c_compressible     kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
  mode: no_dict_fixed throughput=4.921 MB/s raw=6291456 stored=6291456 wall_ns=1278566400
    segment: segment_a_compressible     kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
    segment: segment_b_incompressible   kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
    segment: segment_c_compressible     kept=0.000 attempted=0.000 ratio=1.000 state=OFF k=0 dict_id=0 history=0
  mode: dict_fixed   throughput=12.265 MB/s raw=6291456 stored=2376022 wall_ns=512959280
    segment: segment_a_compressible     kept=1.000 attempted=1.000 ratio=0.066 state=OFF k=4 dict_id=10224528226473514282 history=40960
    segment: segment_b_incompressible   kept=0.000 attempted=0.062 ratio=1.000 state=OFF k=4 dict_id=10224528226473514282 history=40960
    segment: segment_c_compressible     kept=1.000 attempted=1.000 ratio=0.066 state=OFF k=4 dict_id=10224528226473514282 history=40960
  mode: autotune     throughput=12.265 MB/s raw=6291456 stored=2376022 wall_ns=512959280
    segment: segment_a_compressible     kept=1.000 attempted=1.000 ratio=0.066 state=ACTIVE k=4 dict_id=15797060153562598949 history=40960
    segment: segment_b_incompressible   kept=0.000 attempted=0.062 ratio=1.000 state=PAUSED k=4 dict_id=15797060153562598949 history=40960
    segment: segment_c_compressible     kept=1.000 attempted=1.000 ratio=0.066 state=ACTIVE k=4 dict_id=15797060153562598949 history=40960
  mark: segment_0_fractions              PASS (kept=1.000 attempted=1.000)
  mark: segment_0_ratio                  PASS (ratio=0.066)
  mark: segment_0_publish_order          PASS (dict_id=15797060153562598949)
  mark: segment_1_fractions              PASS (kept=0.000 attempted=0.062)
  mark: segment_1_ratio                  PASS (ratio=1.000)
  mark: segment_1_publish_order          PASS (dict_id=15797060153562598949)
  mark: segment_2_fractions              PASS (kept=1.000 attempted=1.000)
  mark: segment_2_ratio                  PASS (ratio=0.066)
  mark: segment_2_publish_order          PASS (dict_id=15797060153562598949)
  mark: marquee_seg_a_active             PASS (state=ACTIVE)
  mark: marquee_seg_a_kept_frac          PASS (kept=1.000)
  mark: marquee_seg_a_dict               PASS (dict_id=15797060153562598949)
  mark: marquee_seg_b_paused             PASS (state=PAUSED)
  mark: marquee_seg_b_kept_frac          PASS (kept=0.000)
  mark: marquee_seg_b_attempted_frac     PASS (attempted=0.062)
  mark: marquee_seg_c_active             PASS (state=ACTIVE)
  mark: marquee_seg_c_kept_frac          PASS (kept=1.000)
  mark: marquee_throughput_gain          PASS (auto=12.265 off=4.921)
marks_failed: 0
```
