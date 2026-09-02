# Vector Distance Kernel Benchmarks

This standalone module isolates the 64-dimensional scalar cosine-distance hot
path seen in TreeDB HNSW graph search profiles.

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

Focused 768-dimensional dot-product run, matching larger vector-search shapes:

```sh
cd benchmarks/vector_distance_kernels
GOWORK=off go test -bench 'BenchmarkDotProduct(768|Batch768)' -benchmem -count=5 | tee /tmp/dot768_bench.txt
```

The `snissn/simd` fork has a merged amd64 AVX512 cross-row batch4
`f32.DotProductBatch` implementation (https://github.com/snissn/simd/pull/1).
To validate TreeDB-shaped kernels against that fork before it is released or
upstreamed, use a temporary module replacement from this benchmark module:

```sh
mkdir -p ~/dev/snissn
git clone https://github.com/snissn/simd ~/dev/snissn/simd || git -C ~/dev/snissn/simd pull --ff-only
cd benchmarks/vector_distance_kernels
GOWORK=off go mod edit -replace github.com/tphakala/simd=$HOME/dev/snissn/simd
GOWORK=off go test -bench 'BenchmarkDotProduct(768|Batch768)' -benchmem -count=5 | tee /tmp/dot768_snissn_simd.txt
GOWORK=off go mod edit -dropreplace github.com/tphakala/simd
```

On the 11th Gen Intel i5-11400F test host, the fork's true 128x768 batch path
measured about 22-23 ns/dot versus about 31 ns/dot for a per-row loop.

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
