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

As of this benchmark, `go get github.com/ashvardanian/simsimd/golang@latest`
fails because the latest `github.com/ashvardanian/simsimd` module does not
contain a `golang` package. If that package becomes available, add it beside the
NumKong and Axiom cases in `vector_distance_kernels_test.go`.
