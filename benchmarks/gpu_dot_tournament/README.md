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

On the same host, using an RTX 3060 Ti with matching user-space driver
libraries, the CUDA/cuBLAS candidates measured:

```text
rows=128:    device-resident SGEMV ~3.4-3.5 us/op   (~36-38M dots/sec)
             upload-query SGEMV    ~20 us/op        (~6M dots/sec)
             device-resident SGEMM ~3.4-3.5 us/op   (~36-38M dots/sec)
rows=1024:   device-resident SGEMV ~10.2-10.3 us/op (~99-100M dots/sec)
             upload-query SGEMV    ~22 us/op        (~46M dots/sec)
             device-resident SGEMM ~10.3 us/op      (~99M dots/sec)
rows=8192:   device-resident SGEMV ~65 us/op        (~125M dots/sec)
             upload-query SGEMV    ~86 us/op        (~95M dots/sec)
             device-resident SGEMM ~65 us/op        (~125M dots/sec)
rows=65536:  device-resident SGEMV ~0.50 ms/op      (~128-131M dots/sec)
             upload-query SGEMV    ~0.55 ms/op      (~119M dots/sec)
             device-resident SGEMM ~0.50 ms/op      (~131M dots/sec)
```

The GPU win is clearest for large, device-resident batches. At 65,536 rows, the
CUDA path is about 16x faster than the local CPU baseline (~0.50 ms vs ~8 ms).
At 128 rows, launch/library overhead dominates and the device-resident GPU case
is only modestly faster; uploading/downloading per query is slower than CPU.

For native in-memory HNSW at `100k docs / 768 dims / ef_search=128`, prior
single-thread no-document search measured about `0.83-0.92 ms/search`, or about
`1.1k-1.2k searches/sec`, with roughly 63% of CPU samples in dot products. If a
future GPU path can batch the graph scoring work into device-resident row groups
at ~125-131M 768d dots/sec, the dot stage for a search that scores ~2k vectors
would be around 15-16 us. End-to-end search would then be dominated by CPU
frontier traversal and GPU scheduling/transfer unless those are also batched.
