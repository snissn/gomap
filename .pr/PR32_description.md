## PR32: ValueLog dict pause probes + paused sampling

### Summary
- Add **pause + probe + resume** behavior for ValueLog dictionary compression:
  - When compression is paused, periodically allow a **probe attempt** (instead of staying “blindly paused”).
  - If a probe successfully keeps compression, **clear the pause immediately**.
- Allow **paused sampling** (coarse stride) so training can restart on changing streams.
- Make dict lookup **lazy** in the append path (avoid dictdb lookups unless we actually attempt dict compression).

### Tests
- `go test ./TreeDB/caching -run TestValueLogDictPauseAndProbeResume -count=1`
- `go test ./... -count=1`

### unified_bench output
`make unified-bench && ./bin/unified-bench -suite vlog_dict -dbs treedb`

```
Unified Benchmark Runner
========================
Profile:     (none/custom)
Settings:    keys=100000 valsize=128 batchsize=1000
             range_queries=200 range_span=100
DBs:         treedb
Tests:       all
Seed:        1

treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:28:04 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=224032 ratio=0.053
2026/01/21 03:28:05 treedb: value-log dict published dict_id=4519647804969231197 k=8 payload_ratio=0.045 total_ratio=0.051
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:28:05 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=345184 ratio=0.082
2026/01/21 03:28:05 treedb: value-log dict published dict_id=5609901700592824033 k=8 payload_ratio=0.074 total_ratio=0.080
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:28:06 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=14275 ratio=0.003
2026/01/21 03:28:06 treedb: value-log dict published dict_id=1594258843066887322 k=8 payload_ratio=0.003 total_ratio=0.003
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:28:06 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=22403 ratio=0.005
2026/01/21 03:28:07 treedb: value-log dict published dict_id=4096512494762515164 k=8 payload_ratio=0.005 total_ratio=0.005
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:28:07 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=224032 ratio=0.053
2026/01/21 03:28:08 treedb: value-log dict published dict_id=14211112328819127856 k=8 payload_ratio=0.045 total_ratio=0.051
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:28:08 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=345184 ratio=0.082
2026/01/21 03:28:09 treedb: value-log dict published dict_id=14896282734163780264 k=8 payload_ratio=0.074 total_ratio=0.080
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:28:09 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=14275 ratio=0.003
2026/01/21 03:28:10 treedb: value-log dict published dict_id=12972685526753320985 k=8 payload_ratio=0.003 total_ratio=0.003
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:28:10 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=22403 ratio=0.005
2026/01/21 03:28:10 treedb: value-log dict published dict_id=7165660661696469567 k=8 payload_ratio=0.005 total_ratio=0.005
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
# unified_bench suite: vlog_dict

- seed: 1
- batchsize: 1000
- warmup bytes: 16,777,216
- measure bytes: 33,554,432

| mode | dict | pattern | valsize | ops/sec | MB/s | observed_ratio_total | observed_ratio_measure | attempted_frac | kept_frac | dict_id | k | pause_bytes | wal_commit_bytes_total | wal_value_bytes_total | wal_value_bytes_measure | wal_total_bytes_total | index_bytes | dictdb_bytes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mode3 | off | ultra_compressible_repeat | 1024 | 1,404,255 | 1,371 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | ultra_compressible_repeat | 1024 | 424,465 | 415 | 0.373981 | 0.061101 | 1.000000 | 1.000000 | 4519647804969231197 | 8 | 0 | 0 | 18,823,072 | 2,050,212 | 18,823,072 | 67,108,864 | 67,149,858 |
| mode3 | off | highly_compressible_tail64 | 1024 | 1,310,626 | 1,280 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | highly_compressible_tail64 | 1024 | 392,458 | 383 | 0.394585 | 0.092007 | 1.000000 | 1.000000 | 5609901700592824033 | 8 | 0 | 0 | 19,860,096 | 3,087,236 | 19,860,096 | 67,108,864 | 67,149,858 |
| mode3 | off | incompressible | 1024 | 1,256,464 | 1,227 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | incompressible | 1024 | 1,418,917 | 1,386 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 17801216 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | off | ultra_compressible_repeat | 16384 | 132,737 | 2,074 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | ultra_compressible_repeat | 16384 | 88,995 | 1,391 | 0.335983 | 0.003471 | 1.000000 | 1.000000 | 1594258843066887322 | 8 | 0 | 0 | 16,910,578 | 116,466 | 16,910,578 | 67,108,864 | 67,149,858 |
| mode3 | off | highly_compressible_tail64 | 16384 | 83,479 | 1,304 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | highly_compressible_tail64 | 16384 | 77,515 | 1,211 | 0.337295 | 0.005438 | 1.000000 | 1.000000 | 4096512494762515164 | 8 | 0 | 0 | 16,976,596 | 182,484 | 16,976,596 | 67,108,864 | 67,149,858 |
| mode3 | off | incompressible | 16384 | 104,077 | 1,626 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | incompressible | 16384 | 113,649 | 1,776 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 33161216 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | off | ultra_compressible_repeat | 1024 | 1,428,404 | 1,395 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | ultra_compressible_repeat | 1024 | 597,325 | 583 | 0.370213 | 0.055449 | 1.000000 | 1.000000 | 14211112328819127856 | 8 | 0 | 0 | 18,633,426 | 1,860,566 | 18,633,426 | 67,108,864 | 67,149,858 |
| mode4 | off | highly_compressible_tail64 | 1024 | 1,296,803 | 1,266 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | highly_compressible_tail64 | 1024 | 577,770 | 564 | 0.390858 | 0.086417 | 1.000000 | 1.000000 | 14896282734163780264 | 8 | 0 | 0 | 19,672,542 | 2,899,682 | 19,672,542 | 67,108,864 | 67,149,858 |
| mode4 | off | incompressible | 1024 | 1,251,698 | 1,222 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | incompressible | 1024 | 1,221,397 | 1,193 | 1.012910 | 1.019767 | 0.000000 | 0.000000 | 0 | 0 | 17801216 | 0 | 50,981,412 | 34,217,692 | 50,981,412 | 67,108,864 | 67,108,874 |
| mode4 | off | ultra_compressible_repeat | 16384 | 122,119 | 1,908 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | ultra_compressible_repeat | 16384 | 98,310 | 1,536 | 0.335797 | 0.003193 | 1.000000 | 1.000000 | 12972685526753320985 | 8 | 0 | 0 | 16,901,240 | 107,128 | 16,901,240 | 67,108,864 | 67,149,858 |
| mode4 | off | highly_compressible_tail64 | 16384 | 89,351 | 1,396 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | highly_compressible_tail64 | 16384 | 72,361 | 1,131 | 0.337109 | 0.005160 | 1.000000 | 1.000000 | 7165660661696469567 | 8 | 0 | 0 | 16,967,247 | 173,135 | 16,967,247 | 67,108,864 | 67,149,858 |
| mode4 | off | incompressible | 16384 | 98,122 | 1,533 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | incompressible | 16384 | 101,337 | 1,583 | 1.000870 | 1.000803 | 0.000000 | 0.000000 | 0 | 0 | 33161216 | 0 | 50,375,424 | 33,581,384 | 50,375,424 | 67,108,864 | 67,108,874 |
```

