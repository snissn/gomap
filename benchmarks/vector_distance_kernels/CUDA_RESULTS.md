# CUDA Benchmark Results

This file records hardware-labeled CUDA benchmark notes. Add new hardware runs
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

## 2026-05-18 - NVIDIA GeForce RTX 3060 Ti - CUDA dot smoke

- Branch/commit: `mikers/gpu-dot-fun` with local CUDA benchmark additions on
  top of `db4d428c36caa`
- Hardware: NVIDIA GeForce RTX 3060 Ti, 8 GB VRAM, compute capability 8.6
- Driver/toolchain: NVIDIA driver `535.288.01`, CUDA runtime reported by
  `nvidia-smi` as `12.2`, `nvcc` `11.5.119`, Go 1.25.7, `linux/amd64`
- Package CPU label: `11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz`
- Raw query-batch output:
  `/tmp/cuda_dot_query_batch_dims768_rtx3060ti_cpu_stronger_throughput_20260518.txt`
- Benchstat query-batch output:
  `/tmp/cuda_dot_query_batch_dims768_rtx3060ti_cpu_stronger_throughput_20260518_benchstat.txt`
- Raw large-batch output:
  `/tmp/cuda_dot_raw_large_rtx3060ti_cpu_stronger_throughput_20260518.txt`
- Benchstat large-batch output:
  `/tmp/cuda_dot_raw_large_rtx3060ti_cpu_stronger_throughput_20260518_benchstat.txt`

Commands:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -tags cuda -run '^$' -bench 'BenchmarkCUDADotQueryBatchSizes/.*dims_768' -benchmem -benchtime=1x -count=3
GOWORK=off go test -tags cuda -run '^$' -bench 'BenchmarkCUDADotRawBatchSizes/candidates_(8192|65536)' -benchmem -benchtime=1x -count=3
```

Scope:

- 768-dimensional query-batch shapes at `8/32/128 x 8192`.
- 64-dimensional single-query raw dot shapes at `8192` and `65536`
  candidates.
- Reported CPU columns use serial `tphakala/simd` batch dot and a persistent
  `tphakala/simd` worker-pool path.
- CUDA path uses cuBLAS `cublasSgemm`, copies query/output each call, and has
  staged and resident-candidate cases.
- Timings below are the benchstat central values from 3 one-iteration samples.
  Benchstat reports no confidence interval with only 3 samples.
- Throughput columns are derived from benchmark time as:
  - `dots/s = query_count * candidate_count / sec/op`
  - `logical GiB/s = dots/s * dims * 4 / 2^30`
  - `dot_GFLOP/s = dots/s * dims * 2 / 1e9`, emitted by the benchmark output
    but omitted from the compact tables below.
- `logical GiB/s` is a kernel-comparison measure for candidate-vector payload
  touched by dot products. For resident CUDA rows, it is not PCIe transfer
  bandwidth.
- Benchmark timers now stop before benchmark teardown and sink assignment, so
  the `-benchtime=1x` rows measure the dot call rather than CUDA cleanup.

Selected 768-dimensional query-batch results:

| Shape | CPU serial time | CPU serial dots/s | CPU serial GiB/s | CPU parallel time | CPU parallel dots/s | CPU parallel GiB/s | CUDA copy time | CUDA copy dots/s | CUDA copy GiB/s | CUDA resident time | CUDA resident dots/s | CUDA resident GiB/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 x 8k | 8.144 ms | 8.047M | 23.02 | 1.362 ms | 48.10M | 137.6 | 2.064 ms | 31.75M | 90.83 | 141.6 us | 462.7M | 1324 |
| 32 x 8k | 27.35 ms | 9.586M | 27.43 | 4.256 ms | 61.60M | 176.2 | 2.107 ms | 124.4M | 355.9 | 234.2 us | 1.119G | 3202 |
| 128 x 8k | 108.0 ms | 9.710M | 27.78 | 23.75 ms | 44.16M | 126.3 | 2.505 ms | 418.5M | 1197 | 590.9 us | 1.775G | 5077 |

For these 768-dimensional rows, "CPU serial" is
`cpu_tphakala_simd_f32_dot_batch_by_query`; "CPU parallel" is
`cpu_tphakala_simd_f32_dot_batch_parallel_worker_pool`.

Selected 64-dimensional single-query results:

| Candidates | CPU serial time | CPU serial dots/s | CPU serial GiB/s | CPU parallel time | CPU parallel dots/s | CPU parallel GiB/s | CUDA copy time | CUDA copy dots/s | CUDA copy GiB/s | CUDA resident time | CUDA resident dots/s | CUDA resident GiB/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8192 | 85.09 us | 96.28M | 22.95 | 86.96 us | 94.21M | 22.46 | 194.3 us | 42.16M | 10.05 | 26.17 us | 313.0M | 74.63 |
| 65536 | 767.5 us | 85.38M | 20.36 | 410.0 us | 159.8M | 38.11 | 1.303 ms | 50.30M | 11.99 | 95.59 us | 685.6M | 163.5 |

For these 64-dimensional single-query rows, "CPU serial" is
`cpu_tphakala_simd_f32_dot_batch`; "CPU parallel" is
`cpu_tphakala_simd_f32_dot_batch_parallel_worker_pool`.

Interpretation:

- The resident-candidate cuBLAS path still wins on 768-dimensional query-batch
  shapes. It is about 9.6x faster than the parallel CPU path at `8 x 8k`,
  about 18.2x at `32 x 8k`, and about 40.2x at `128 x 8k`.
- Copying candidates every CUDA batch is slower than the parallel CPU path at
  `8 x 8k`, but wins once there is enough query-batch work to amortize PCIe
  transfer and launch overhead.
- For a single 64-dimensional query, copying candidates each CUDA call is still
  slower than the `tphakala/simd` CPU paths. The resident-candidate CUDA path
  wins once candidate data is already on-device.
- This is an intentionally simple first CUDA counterpart to the Metal work. It
  does not yet include fused cosine, top-k readback reduction, pinned host
  memory, streams, or custom kernels.
