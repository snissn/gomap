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

As of this benchmark, `go get github.com/ashvardanian/simsimd/golang@latest`
fails because the latest `github.com/ashvardanian/simsimd` module does not
contain a `golang` package. If that package becomes available, add it beside the
NumKong and Axiom cases in `vector_distance_kernels_test.go`.
