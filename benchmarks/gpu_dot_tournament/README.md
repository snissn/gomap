# GPU Dot-Product Tournament

This standalone benchmark module compares CPU and CUDA/cuBLAS approaches for
TreeDB-shaped vector scoring:

```text
out[i] = dot(query[768], matrix[row_i][768])
```

The default package builds without CUDA and includes a CPU baseline. CUDA cases
are behind the `cuda` build tag so normal repository CI does not require an
NVIDIA driver/toolkit.

## Candidates

- `cpu_tphakala_loop`: current Go SIMD baseline, looping over rows with
  `github.com/tphakala/simd/f32.DotProduct`.
- `cuda_cublas_sgemv_device_resident`: matrix, query, and output are already on
  the GPU; measures the cuBLAS SGEMV compute path.
- `cuda_cublas_sgemv_upload_query`: matrix stays resident on GPU, but each
  operation uploads the query and downloads the output.
- `cuda_cublas_sgemm_device_resident`: same math expressed as a skinny SGEMM.

The device-resident cases are the relevant optimistic model for a future TreeDB
GPU path where vector blocks and query batches are staged on device. The
upload/download case shows PCIe overhead when only one query is evaluated at a
time.

## Run

CPU only:

```sh
cd benchmarks/gpu_dot_tournament
GOWORK=off go test -run '^$' -bench . -benchmem -count=3
```

CUDA tournament:

```sh
cd benchmarks/gpu_dot_tournament
GOWORK=off go test -tags cuda -run '^$' -bench . -benchmem -count=3
```

The CUDA benchmark uses CUDA runtime and cuBLAS via cgo:

```text
-lcudart -lcublas
```

If the NVIDIA driver/toolkit is unavailable or mismatched, CUDA cases skip with
the runtime error.

## Local CPU baseline and GPU roofline

On the 11th Gen Intel i5-11400F host used while adding this benchmark, the CPU
baseline measured:

```text
rows=128:    ~4.2-4.5 us/op  (~28-30M dots/sec)
rows=1024:   ~44-50 us/op    (~21-23M dots/sec)
rows=8192:   ~0.91-0.98 ms   (~8-9M dots/sec)
rows=65536:  ~7.9-8.1 ms     (~8M dots/sec)
```

That host has an RTX 3060 Ti installed, but the local NVIDIA driver/runtime was
mismatched while this benchmark was written, so CUDA cases skipped with:

```text
cudaGetDeviceCount: forward compatibility was attempted on non supported HW
```

The roofline for a device-resident GPU path is nevertheless useful. A 3060 Ti is
roughly a 448 GB/s memory-bandwidth device. Scoring one query against 65,536
rows of 768 float32s reads about 201 MB of row data, so a bandwidth-limited
ideal is about 2.2k such tournaments/sec, or about 145M 768d dots/sec. That is
roughly an 18x raw dot-throughput ceiling over the local CPU baseline for the
large-row case. The CUDA/cuBLAS tournament exists to replace this roofline with
measured numbers on a working CUDA host.

For native in-memory HNSW at `100k docs / 768 dims / ef_search=128`, prior
single-thread no-document search measured about `0.83-0.92 ms/search`, or about
`1.1k-1.2k searches/sec`, with roughly 63% of CPU samples in dot products. If a
future GPU path can batch the graph scoring work into device-resident row groups
and approach ~145M 768d dots/sec, the dot stage for a search that scores ~2k
vectors would be around 14 us. End-to-end search would then be dominated by CPU
frontier traversal and GPU scheduling/transfer unless those are also batched.
