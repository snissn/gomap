**Base:** `sprint/slabopt-pr45-largeptrmap-mutex` (PR #102)

This PR tightens the “realtime” tuning loop for ValueLog dictionary compression by making K/dict-history selection publishable and by preventing the trainer from stalling when the stream is degraded.

## Changes
- `TreeDB/internal/dictdb`: allow storing per-dict `K` up to `valuelog.MaxFrameK` (32).
- `TreeDB/caching/vlog_dict.go`: when the dict bytes are unchanged (same `DictHash`), still update `K` via dictdb (`SetK`) and refresh the in-process caches.
- `TreeDB/internal/compression/trainer.go`:
  - when degraded (pause/probe path), keep the trainer collecting instead of stalling on throttling.
  - train/evaluate multiple dict *history sizes* (candidate set derived from configured `DictBytes`, plus 16K/32K/40K when applicable) and pick the best profile using a ratio-first, encode-cost tie-break policy.
  - include `EncodeNsEstimate` and `HistoryBytes` in profiles for better selection/bench visibility.
- Tests:
  - `TreeDB/caching/vlog_dict_k_update_test.go`: regression test for “same dict, new K” publication.
  - `TreeDB/internal/compression/trainer_test.go`: ensure degraded+throttled trainer restarts collection.
  - `TreeDB/internal/dictdb/store_test.go`: validates `SetK` supports `MaxFrameK`.
- Bench:
  - `TreeDB/internal/valuelog/dict_autotune_bench_test.go`: adds `BenchmarkValueLogDictAutoTuneCPU_NoIO` showing fixed `dictBytes=40k` vs “autotune dictBytes” (plus chosen K + chosen history bytes).

## Benchmark (Apple M3, darwin/arm64)
Command:
```bash
go test ./TreeDB/internal/valuelog -run '^$' -bench BenchmarkValueLogDictAutoTuneCPU_NoIO -benchmem -count=1
```

Sample output:
```text
BenchmarkValueLogDictAutoTuneCPU_NoIO/valsize=1024/highly_compressible_tail64/fixed_dictbytes=40k-8          148482  8148 ns/op 1005.37 MB/s 1.000 attempted_frac 8.000 chosen_k 1.000 compressed_frac 40960 dict_history_bytes 1.000 kept_frac 0.06736 observed_ratio
BenchmarkValueLogDictAutoTuneCPU_NoIO/valsize=1024/highly_compressible_tail64/autotune_dictbytes-8           144182  8115 ns/op 1009.48 MB/s 1.000 attempted_frac 8.000 chosen_k 1.000 compressed_frac 32768 dict_history_bytes 1.000 kept_frac 0.06736 observed_ratio
BenchmarkValueLogDictAutoTuneCPU_NoIO/valsize=16384/medium_compressible_sparse/fixed_dictbytes=40k-8          23742 51863 ns/op 2527.27 MB/s 1.000 attempted_frac 8.000 chosen_k 1.000 compressed_frac 40960 dict_history_bytes 1.000 kept_frac 0.06764 observed_ratio
BenchmarkValueLogDictAutoTuneCPU_NoIO/valsize=16384/medium_compressible_sparse/autotune_dictbytes-8           43827 27497 ns/op 2383.38 MB/s 1.000 attempted_frac 4.000 chosen_k 1.000 compressed_frac 32768 dict_history_bytes 1.000 kept_frac 0.06806 observed_ratio
```

## Validation
```bash
go test ./... -count=1
```

