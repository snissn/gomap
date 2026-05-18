# Vector Distance Kernel Benchmarks

This standalone module isolates vector cosine-distance and FP32 dot-product hot
paths seen in TreeDB HNSW graph search profiles. The original rerank shape is
64-dimensional; the Metal query-batch benchmarks also cover 768-dimensional
embedding/index-update shapes.

It compares:

- pure Go `float64` accumulation, matching TreeDB's previous precision style
- pure Go `float32` accumulation
- `gonum.org/v1/gonum/blas/blas32.Dot`, the previous TreeDB scalar kernel
- `github.com/ashvardanian/NumKong/golang` scalar `DotF32`
- `github.com/ashvardanian/NumKong/golang` scalar `AngularF32`
- `github.com/ashvardanian/NumKong/golang` packed batch kernels
- `github.com/axiomhq/simd-go` `DotProductFloat32`
- `github.com/viterin/vek/vek32` `Dot`
- `github.com/tphakala/simd/f32` `DotProduct`, `DotProductUnsafe`, and
  `DotProductBatch`
- `github.com/ic-timon/da-hvri/simd` `DotProduct`
- Apple Metal GPU execution for FP32 dot and query-batch kernels
  (`darwin && cgo` only)

It also includes focused TreeDB rerank-shape benchmarks:

- `BenchmarkTreeDBRerankKernelCandidateBatch128` isolates scoring one query
  against 128 already-contiguous candidates.
- `BenchmarkTreeDBRerankGatherAndScoreCandidateBatch128` simulates the current
  TreeDB rerank shape that gathers resident node vectors into a contiguous
  candidate matrix before scoring.

The current TreeDB NumKong control is:

```go
packedCandidates := nk.NewPackedMatrixF32(candidateVectors, len(candidateIDs), dims)
dots := make([]float64, len(candidateIDs))
nk.DotsPackedF32(query, packedCandidates, dots, 1)
```

The benchmark compares that against:

- reusing caller-owned output buffers
- configuring the current thread once with `nk.ConfigureThread`
- pre-packing candidates once and reusing the `PackedMatrix`
- using `PackedMatrix.DotsF32WithPool` with a reusable `WorkerPool`
- using `AngularsPackedF32` directly
- looping over candidates with `axiomhq/simd-go` `DotProductFloat32`
- looping over candidates with `vek32` and `tphakala/simd/f32` fused dot
  products
- using `tphakala/simd/f32.DotProductBatch` for one query against many rows
- looping over candidates with `da-hvri` dot products

NumKong's public Go API currently allocates the packed buffer inside
`NewPackedMatrixF32`. There is no public pack-into-caller-buffer API, so the
zero-allocation NumKong case requires reusing an already packed candidate
matrix.

Run:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -bench=. -benchmem -count=5 | tee /tmp/vector_distance_kernels.txt
```

Focused run:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -bench 'BenchmarkCosineDistance(Scalar64|CandidateBatch128)' -benchmem -count=5
```

Focused TreeDB rerank run:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -bench 'BenchmarkTreeDBRerank' -benchmem -count=5 | tee /tmp/treedb_rerank_kernel.txt
```

Focused Metal GPU run on macOS:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run TestMetalDotKernelCandidateBatch128 -bench BenchmarkMetalDotCandidateBatch128 -benchmem -count=5 | tee /tmp/metal_dot_candidate_batch128.txt
```

Raw dot-product batch-size sweep:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench BenchmarkMetalDotRawBatchSizes -benchmem -count=3 | tee /tmp/metal_dot_raw_batch_sizes.txt
```

Query-batch sweep for index-build/update shapes:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench BenchmarkMetalDotQueryBatchSizes -benchmem -count=3 | tee /tmp/metal_dot_query_batch_sizes.txt
```

Query-batch dimension sweep for per-dot parallelism:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench BenchmarkMetalDotQueryBatchDimensionSizes -benchmem -count=3 | tee /tmp/metal_dot_query_batch_dimension_sizes.txt
```

Tiled-kernel tuning sweep for 768/1536-dimensional batch dots:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench BenchmarkMetalDotQueryBatchTiledVariantTuning -benchmem -count=3 | tee /tmp/metal_dot_query_batch_tiled_tuning.txt
```

Realistic index-build/update shape sweep:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench BenchmarkMetalDotQueryBatchRealisticIndexShapes -benchmem -count=3 | tee /tmp/metal_dot_query_batch_realistic_shapes.txt
```

Focused 768-dimensional realistic-shape gate:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -run '^$' -bench 'BenchmarkMetalDotQueryBatchRealisticIndexShapes/.*dims_768' -benchmem -benchtime=1x -count=3
```

Hardware-labeled benchmark notes live in `METAL_RESULTS.md`. Add new hardware
runs there as separate dated sections instead of overwriting earlier results.

The realistic-shape benchmark includes `32/128/512 x 8k/65k` at both 64 and
768 dimensions by default and defines `32/128/512 x 1M` cases behind
`METAL_DOT_LARGE=1` because the dense output matrix can require hundreds of MiB
to GiB of shared memory:

```sh
METAL_DOT_LARGE=1 GOWORK=off go test -run '^$' -bench 'BenchmarkMetalDotQueryBatchRealisticIndexShapes/dims_768_queries_32_candidates_1000000' -benchmem -count=1
```

The Metal benchmark is intentionally exploratory. The baseline kernels launch
one logical Metal thread for each output dot: one thread per candidate in
single-query cases, or one thread per `(query, candidate)` pair in query-batch
cases. That means small cases such as `1 x 128` expose command-submission
overhead, while large cases such as `32 x 1M` or `512 x 65k` launch tens of
millions of logical threads and can keep the GPU much busier. Within each
baseline thread, the dot's dimensions are still accumulated serially in
`float32`; the baseline parallelism is across dots, not inside an individual
dot. The query-batch sweep also includes a first tiled matrix-style variant that
keeps candidates resident, wraps query/output slices without copying, dispatches
8 queries by 16 candidates per threadgroup, and loads 64-dimension
query/candidate tiles through threadgroup memory. It also includes a
SIMD-reduction variant where one SIMD group owns each `(query, candidate)` dot,
each lane covers a slice of the dimension range, and lane 0 writes the reduced
dot. `BenchmarkMetalDotQueryBatchDimensionSizes` compares the baseline, tiled,
and SIMD-reduction kernels at 64, 128, 256, 768, and 1536 dimensions, plus
fixed-dimension variants for those same common embedding sizes. It also includes
fixed-dimension fused-cosine kernels and an Apple Metal Performance Shaders
matrix-multiply reference.
`BenchmarkMetalDotQueryBatchSizes` includes dynamic and fixed-dimension fused
cosine cases that keep candidate inverse norms resident, wrap query inverse
norms without copying, and write `1 - dot * queryInvNorm * candidateInvNorm`
directly from the Metal kernel.
It reports a staging-copy case, a no-copy Go-slice wrapper case, a
preloaded-candidate-buffer case, and a preloaded-candidate/no-copy-output case
so memory movement and command-submission overhead remain visible. It also has
async cases that stay inside one cgo call for `b.N` iterations, copy each query
batch into shared Metal query buffers, submit work without immediately waiting,
wait only before reusing a buffer slot, and copy each completed output matrix
back to Go before the slot is reused. The realistic-shape async cases rotate
through four distinct pre-generated query batches and benchmark queue depths
1/2/3. The no-copy cases use `newBufferWithBytesNoCopy` only inside synchronous
cgo calls: Metal wraps the Go slices, waits for the command to complete, and
releases the wrappers before returning to Go. This keeps the experiment within
Go/cgo pointer ownership rules while measuring the unified-memory path. This is
not yet a production TreeDB search path. The raw batch-size sweep includes the
existing Axiom FP32 dot loop beside the Metal cases to show how much batch size
is needed before command overhead starts to amortize. The query-batch and
realistic-shape sweeps include serial and GOMAXPROCS-parallel Axiom CPU
baselines so GPU results are not compared only against one CPU core.
`BenchmarkMetalDotQueryBatchSizes` adds larger `queries x candidates` shapes
intended to model index creation and bulk index-update scoring, where one
command can produce a full dot matrix.

`BenchmarkMetalDotQueryBatchRealisticIndexShapes` also includes a block top-k
cosine benchmark. It computes per-query, per-1024-candidate-block top-k scores
and ids on the GPU, returning `queries * ceil(candidates/1024) * k` scores and
ids instead of the full dense `queries * candidates` matrix. Correctness tests
merge those block results and compare them with full dense cosine plus CPU
top-k on small cases. The current block-top-k kernel is intentionally simple and
is not yet a faster search path; it is a readback-shape probe.

`BenchmarkMetalDotQueryBatchTiledVariantTuning` tries 4x32, 8x16, and 16x8
query/candidate threadgroup tiles crossed with DOT_TILE_D 64 and 128 at 768 and
1536 dimensions. The variants are kept in this focused tuning benchmark rather
than promoted into the primary realistic-shape sweep unless they beat the
fixed-dimension kernel or MPS reference on the target dimensions.

As of this benchmark, `go get github.com/ashvardanian/simsimd/golang@latest`
fails because the latest `github.com/ashvardanian/simsimd` module does not
contain a `golang` package. If that package becomes available, add it beside the
NumKong and Axiom cases in `vector_distance_kernels_test.go`.

`github.com/pehringer/simd` is intentionally not included in timed cases. Its
public API exposes elementwise operations such as `MulFloat32`, but not a fused
dot/reduction primitive. A multiply-into-temporary plus sum shape would add
memory traffic that TreeDB's search hot loop is specifically trying to avoid.

`github.com/ic-timon/da-hvri/simd.DotProductBatchFlat` is also intentionally
not included in the 64-dimensional TreeDB shapes because its public batch API is
fixed to `Dim = 512`. The scalar `DotProduct` path is included because it
accepts arbitrary equal-length vectors.
