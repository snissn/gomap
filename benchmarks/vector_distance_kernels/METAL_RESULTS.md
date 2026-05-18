# Metal Benchmark Results

This file records hardware-labeled Metal benchmark notes. Add new hardware runs
as new dated sections and leave prior sections intact, so cross-machine changes
are easy to compare.

Suggested format for future entries:

```md
## YYYY-MM-DD - Hardware label - benchmark name

- Branch/commit:
- Hardware:
- OS/toolchain:
- Command:
- Scope:
- Summary:
- Results:
- Notes:
```

## 2026-05-18 - Apple M3 MacBook Air - 768 realistic index shapes

- Branch/commit: `mikers/gpu-dot-fun` at benchmark-code commit `20c41fc5838a`
- Hardware: MacBook Air `Mac15,13`, Apple M3, 8 CPU cores
  (4 performance, 4 efficiency), 10-core Apple M3 GPU, 16 GB RAM
- OS/toolchain: macOS 26.2 `25C56`, Metal 4, Go 1.25.5, `darwin/arm64`
- Package CPU label: `Apple M3`
- Raw output: `/tmp/metal_dot_query_batch_realistic_dims768_m3_20260518.txt`
  on the machine that produced this note
- Benchstat output:
  `/tmp/metal_dot_query_batch_realistic_dims768_m3_20260518_benchstat.txt`
  on the machine that produced this note

Command:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench 'BenchmarkMetalDotQueryBatchRealisticIndexShapes/.*dims_768' -benchmem -benchtime=1x -count=3
```

Scope:

- 768-dimensional query-batch shapes only.
- Default realistic shapes: `32/128/512 x 8192` and `32/128/512 x 65536`.
- `METAL_DOT_LARGE` was not set, so gated 1M-candidate cases were skipped.
- Timings below are the benchstat central values from 3 one-iteration samples.
  Benchstat reports no confidence interval with only 3 samples.

Selected results:

| Shape | Serial CPU cosine | Parallel CPU cosine | Fixed 768 Metal dot | Fixed 768 Metal cosine | MPS matmul | Block top-k cosine |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 x 8k | 14.37 ms | 5.343 ms | 8.471 ms | 5.585 ms | 2.629 ms | 24.59 ms |
| 128 x 8k | 58.27 ms | 23.37 ms | 13.50 ms | 16.99 ms | 3.990 ms | 56.81 ms |
| 512 x 8k | 369.0 ms | 123.2 ms | 33.15 ms | 31.00 ms | 5.336 ms | 174.2 ms |
| 32 x 65k | 143.5 ms | 83.07 ms | 30.13 ms | 26.28 ms | 26.63 ms | 98.80 ms |
| 128 x 65k | 580.5 ms | 300.4 ms | 69.11 ms | 67.46 ms | 27.42 ms | 340.8 ms |
| 512 x 65k | 2.565 s | 1.186 s | 249.7 ms | 243.8 ms | 62.08 ms | 1.288 s |

Interpretation:

- The parallel CPU baseline materially changes the comparison, but the 768 GPU
  paths still win strongly on larger query-batch/index-update shapes.
- MPS matrix multiply is the best dense dot baseline in this run. It dominates
  large dense matrices, especially `512 x 65k`.
- The fixed-dimension 768 Metal kernels are the strongest custom kernels. Fixed
  fused cosine is roughly tied with MPS at `32 x 65k`, but MPS pulls ahead as
  query batch size grows.
- Dynamic-dimension Metal kernels are not competitive at 768 in this benchmark.
  Specializing by dimension is the important custom-kernel optimization here.
- The current block top-k kernel proves the reduced-readback shape and passes
  correctness tests, but it is slower than dense MPS or fixed 768 kernels on
  this hardware. It needs a better selection/reduction strategy before it is a
  performance path.
