## PR33: K-selection (K>8) + encode-cost term

### Summary
- Extend `compression.ChooseKForDict` to evaluate larger grouping factors (up to 32) and include an **encode-cost term** in the score.
- Stop forcing a minimum `k=8` when publishing ValueLog dict profiles; publish the profile’s chosen K (clamped).

### Tests
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
2026/01/21 03:42:44 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=224032 ratio=0.053
2026/01/21 03:42:45 treedb: value-log dict published dict_id=2893264621066961609 k=4 payload_ratio=0.038 total_ratio=0.043
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:42:45 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=345184 ratio=0.082
2026/01/21 03:42:45 treedb: value-log dict published dict_id=5776134323493819492 k=4 payload_ratio=0.068 total_ratio=0.072
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:42:46 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=14275 ratio=0.003
2026/01/21 03:42:47 treedb: value-log dict published dict_id=10344756596729887105 k=4 payload_ratio=0.003 total_ratio=0.003
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
2026/01/21 03:42:47 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=22403 ratio=0.005
2026/01/21 03:42:47 treedb: value-log dict published dict_id=4096512494762515164 k=4 payload_ratio=0.005 total_ratio=0.005
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log redo_log=on
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:42:48 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=224032 ratio=0.053
2026/01/21 03:42:48 treedb: value-log dict published dict_id=14211112328819127856 k=4 payload_ratio=0.038 total_ratio=0.043
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:42:48 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=4096 raw=4194304 stored=345184 ratio=0.082
2026/01/21 03:42:49 treedb: value-log dict published dict_id=13239510747806851721 k=8 payload_ratio=0.064 total_ratio=0.069
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:42:49 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=14275 ratio=0.003
2026/01/21 03:42:50 treedb: value-log dict published dict_id=2021462902300887510 k=4 payload_ratio=0.003 total_ratio=0.003
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
2026/01/21 03:42:50 treedb: slab compression trained dict slab=0 dict_bytes=40960 samples=256 raw=4194304 stored=22403 ratio=0.005
2026/01/21 03:42:51 treedb: value-log dict published dict_id=17252032062606859535 k=4 payload_ratio=0.005 total_ratio=0.005
treedb write_path mode=cached value_store=value_log_eager redo_log=off
treedb write_path mode=cached value_store=value_log_eager redo_log=off
# unified_bench suite: vlog_dict

- seed: 1
- batchsize: 1000
- warmup bytes: 16,777,216
- measure bytes: 33,554,432

| mode | dict | pattern | valsize | ops/sec | MB/s | observed_ratio_total | observed_ratio_measure | attempted_frac | kept_frac | dict_id | k | pause_bytes | wal_commit_bytes_total | wal_value_bytes_total | wal_value_bytes_measure | wal_total_bytes_total | index_bytes | dictdb_bytes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| mode3 | off | ultra_compressible_repeat | 1024 | 1,374,960 | 1,343 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | ultra_compressible_repeat | 1024 | 343,254 | 335 | 0.378696 | 0.068174 | 1.000000 | 1.000000 | 2893264621066961609 | 4 | 0 | 0 | 19,060,416 | 2,287,556 | 19,060,416 | 67,108,864 | 67,149,858 |
| mode3 | off | highly_compressible_tail64 | 1024 | 1,295,805 | 1,265 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | highly_compressible_tail64 | 1024 | 302,858 | 296 | 0.399303 | 0.099084 | 1.000000 | 1.000000 | 5776134323493819492 | 4 | 0 | 0 | 20,097,584 | 3,324,724 | 20,097,584 | 67,108,864 | 67,149,858 |
| mode3 | off | incompressible | 1024 | 1,312,908 | 1,282 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | on | incompressible | 1024 | 1,291,114 | 1,261 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 17801216 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode3 | off | ultra_compressible_repeat | 16384 | 120,534 | 1,883 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | ultra_compressible_repeat | 16384 | 83,288 | 1,301 | 0.336211 | 0.003814 | 1.000000 | 1.000000 | 10344756596729887105 | 4 | 0 | 0 | 16,922,072 | 127,960 | 16,922,072 | 67,108,864 | 67,149,858 |
| mode3 | off | highly_compressible_tail64 | 16384 | 89,191 | 1,394 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | highly_compressible_tail64 | 16384 | 65,753 | 1,027 | 0.337523 | 0.005780 | 1.000000 | 1.000000 | 4096512494762515164 | 4 | 0 | 0 | 16,988,073 | 193,961 | 16,988,073 | 67,108,864 | 67,149,858 |
| mode3 | off | incompressible | 16384 | 97,138 | 1,518 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode3 | on | incompressible | 16384 | 114,552 | 1,790 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 33161216 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | off | ultra_compressible_repeat | 1024 | 1,292,235 | 1,262 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | ultra_compressible_repeat | 1024 | 574,586 | 561 | 0.370213 | 0.055449 | 1.000000 | 1.000000 | 14211112328819127856 | 4 | 0 | 0 | 18,633,426 | 1,860,566 | 18,633,426 | 67,108,864 | 67,149,858 |
| mode4 | off | highly_compressible_tail64 | 1024 | 1,263,026 | 1,233 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | highly_compressible_tail64 | 1024 | 551,520 | 539 | 0.390858 | 0.086417 | 1.000000 | 1.000000 | 13239510747806851721 | 8 | 0 | 0 | 19,672,542 | 2,899,682 | 19,672,542 | 67,108,864 | 67,149,858 |
| mode4 | off | incompressible | 1024 | 1,365,589 | 1,334 | 1.016113 | 1.024300 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 51,142,656 | 34,369,796 | 51,142,656 | 67,108,864 | 67,108,874 |
| mode4 | on | incompressible | 1024 | 1,206,778 | 1,178 | 1.012910 | 1.019767 | 0.000000 | 0.000000 | 0 | 0 | 17801216 | 0 | 50,981,412 | 34,217,692 | 50,981,412 | 67,108,864 | 67,108,874 |
| mode4 | off | ultra_compressible_repeat | 16384 | 126,302 | 1,973 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | ultra_compressible_repeat | 16384 | 83,445 | 1,304 | 0.335797 | 0.003193 | 1.000000 | 1.000000 | 2021462902300887510 | 4 | 0 | 0 | 16,901,240 | 107,128 | 16,901,240 | 67,108,864 | 67,149,858 |
| mode4 | off | highly_compressible_tail64 | 16384 | 81,920 | 1,280 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | highly_compressible_tail64 | 16384 | 85,315 | 1,333 | 0.337109 | 0.005160 | 1.000000 | 1.000000 | 17252032062606859535 | 4 | 0 | 0 | 16,967,247 | 173,135 | 16,967,247 | 67,108,864 | 67,149,858 |
| mode4 | off | incompressible | 16384 | 103,589 | 1,619 | 1.001007 | 1.001007 | 0.000000 | 0.000000 | 0 | 0 | 0 | 0 | 50,382,336 | 33,588,224 | 50,382,336 | 67,108,864 | 67,108,874 |
| mode4 | on | incompressible | 16384 | 113,664 | 1,776 | 1.000870 | 1.000803 | 0.000000 | 0.000000 | 0 | 0 | 33161216 | 0 | 50,375,424 | 33,581,384 | 50,375,424 | 67,108,864 | 67,108,874 |
```

