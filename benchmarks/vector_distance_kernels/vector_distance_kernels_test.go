package vectordistancekernels

import (
	"fmt"
	"math"
	"testing"

	nk "github.com/ashvardanian/NumKong/golang"
	axiomsimd "github.com/axiomhq/simd-go"
	"gonum.org/v1/gonum/blas/blas32"
)

const (
	benchDims       = 64
	benchCandidates = 128
	benchQueries    = 32
)

var (
	sinkDistance32  float32
	sinkDistance64  float64
	sinkDistanceBuf []float32
	sinkDotBuf      []float64
)

func BenchmarkCosineDistanceScalar64(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	candidates, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)

	b.Run("pure_go_f64_accum", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			sum += pureGoCosineDistanceF64Accum(query, queryInvNorm, candidate, candidateInvNorms[i%benchCandidates])
		}
		sinkDistance32 = sum
	})

	b.Run("pure_go_f32_accum", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			sum += pureGoCosineDistanceF32Accum(query, queryInvNorm, candidate, candidateInvNorms[i%benchCandidates])
		}
		sinkDistance32 = sum
	})

	b.Run("numkong_dot_f32_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := nk.DotF32(query, candidate)
			sum += float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[i%benchCandidates]))
		}
		sinkDistance32 = sum
	})

	b.Run("gonum_blas32_dot_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := blas32.Dot(
				blas32.Vector{N: benchDims, Inc: 1, Data: query},
				blas32.Vector{N: benchDims, Inc: 1, Data: candidate},
			)
			sum += 1 - dot*queryInvNorm*candidateInvNorms[i%benchCandidates]
		}
		sinkDistance32 = sum
	})

	b.Run("numkong_angular_f32", func(b *testing.B) {
		b.ReportAllocs()
		var sum float64
		for i := 0; i < b.N; i++ {
			sum += nk.AngularF32(query, candidateAt(candidates, i, benchDims))
		}
		sinkDistance64 = sum
	})

	b.Run("axiomhq_dot_product_f32_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := axiomsimd.DotProductFloat32(query, candidate)
			sum += 1 - dot*queryInvNorm*candidateInvNorms[i%benchCandidates]
		}
		sinkDistance32 = sum
	})
}

func BenchmarkCosineDistanceCandidateBatch128(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	candidates, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)
	packedCandidates := nk.NewPackedMatrixF32(candidates, benchCandidates, benchDims)
	dots := make([]float64, benchCandidates)
	distances := make([]float32, benchCandidates)
	angularDistances := make([]float64, benchCandidates)

	b.ReportMetric(benchCandidates, "candidates/op")
	b.ReportMetric(benchDims, "dims")

	b.Run("pure_go_f64_accum_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				distances[j] = pureGoCosineDistanceF64Accum(query, queryInvNorm, candidateAt(candidates, j, benchDims), candidateInvNorms[j])
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("pure_go_f32_accum_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				distances[j] = pureGoCosineDistanceF32Accum(query, queryInvNorm, candidateAt(candidates, j, benchDims), candidateInvNorms[j])
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("numkong_dot_f32_scalar_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := nk.DotF32(query, candidateAt(candidates, j, benchDims))
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[j]))
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("gonum_blas32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateAt(candidates, j, benchDims)
				dot := blas32.Dot(
					blas32.Vector{N: benchDims, Inc: 1, Data: query},
					blas32.Vector{N: benchDims, Inc: 1, Data: candidate},
				)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("axiomhq_dot_product_f32_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := axiomsimd.DotProductFloat32(query, candidateAt(candidates, j, benchDims))
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("numkong_packed_dots_f32_one_query", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			for j, dot := range dots {
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[j]))
			}
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_packed_angulars_f32_one_query", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nk.AngularsPackedF32(query, packedCandidates, angularDistances, 1)
		}
		sinkDotBuf = angularDistances
	})
}

func BenchmarkCosineDistanceQueryBatch32x128(b *testing.B) {
	queries, queryInvNorms := benchQueryMatrix(benchQueries, benchDims)
	candidates, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)
	packedCandidates := nk.NewPackedMatrixF32(candidates, benchCandidates, benchDims)
	dots := make([]float64, benchQueries*benchCandidates)
	distances := make([]float32, benchQueries*benchCandidates)
	angularDistances := make([]float64, benchQueries*benchCandidates)
	pool := nk.NewWorkerPool(0)
	defer pool.Close()

	b.ReportMetric(benchQueries, "queries/op")
	b.ReportMetric(benchCandidates, "candidates/query")
	b.ReportMetric(benchDims, "dims")

	b.Run("pure_go_f64_accum_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					row[j] = pureGoCosineDistanceF64Accum(query, queryInvNorms[q], candidateAt(candidates, j, benchDims), candidateInvNorms[j])
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("axiomhq_dot_product_f32_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					dot := axiomsimd.DotProductFloat32(query, candidateAt(candidates, j, benchDims))
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("numkong_dot_f32_scalar_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					dot := nk.DotF32(query, candidateAt(candidates, j, benchDims))
					row[j] = float32(1 - dot*float64(queryInvNorms[q])*float64(candidateInvNorms[j]))
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("gonum_blas32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					candidate := candidateAt(candidates, j, benchDims)
					dot := blas32.Dot(
						blas32.Vector{N: benchDims, Inc: 1, Data: query},
						blas32.Vector{N: benchDims, Inc: 1, Data: candidate},
					)
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("numkong_packed_dots_f32", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nk.DotsPackedF32(queries, packedCandidates, dots, benchQueries)
			scaleDotRows(dots, queryInvNorms, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_packed_dots_f32_with_pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			packedCandidates.DotsF32WithPool(queries, dots, benchQueries, pool)
			scaleDotRows(dots, queryInvNorms, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_packed_angulars_f32", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			nk.AngularsPackedF32(queries, packedCandidates, angularDistances, benchQueries)
		}
		sinkDotBuf = angularDistances
	})

	b.Run("numkong_packed_angulars_f32_with_pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			packedCandidates.AngularsF32WithPool(queries, angularDistances, benchQueries, pool)
		}
		sinkDotBuf = angularDistances
	})
}

func pureGoCosineDistanceF64Accum(query []float32, queryInvNorm float32, candidate []float32, candidateInvNorm float32) float32 {
	var dot float64
	for i, left := range query {
		dot += float64(left * candidate[i])
	}
	return float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorm))
}

func pureGoCosineDistanceF32Accum(query []float32, queryInvNorm float32, candidate []float32, candidateInvNorm float32) float32 {
	var dot float32
	for i, left := range query {
		dot += left * candidate[i]
	}
	return 1 - dot*queryInvNorm*candidateInvNorm
}

func scaleDotRows(dots []float64, queryInvNorms, candidateInvNorms []float32, distances []float32) {
	for q := range queryInvNorms {
		rowStart := q * len(candidateInvNorms)
		queryInvNorm := float64(queryInvNorms[q])
		for j, candidateInvNorm := range candidateInvNorms {
			dot := dots[rowStart+j]
			distances[rowStart+j] = float32(1 - dot*queryInvNorm*float64(candidateInvNorm))
		}
	}
}

func benchQueryMatrix(count, dims int) ([]float32, []float32) {
	out := make([]float32, count*dims)
	invNorms := make([]float32, count)
	for i := 0; i < count; i++ {
		vector, invNorm := benchVector(i+101, dims)
		copy(out[i*dims:(i+1)*dims], vector)
		invNorms[i] = invNorm
	}
	return out, invNorms
}

func benchCandidateMatrix(count, dims int) ([]float32, []float32) {
	out := make([]float32, count*dims)
	invNorms := make([]float32, count)
	for i := 0; i < count; i++ {
		vector, invNorm := benchVector(i+1009, dims)
		copy(out[i*dims:(i+1)*dims], vector)
		invNorms[i] = invNorm
	}
	return out, invNorms
}

func candidateAt(candidates []float32, index, dims int) []float32 {
	index %= len(candidates) / dims
	return candidates[index*dims : (index+1)*dims]
}

func benchVector(seed, dims int) ([]float32, float32) {
	vector := make([]float32, dims)
	var norm float64
	for i := range vector {
		x := math.Sin(float64((seed+1)*(i+3))) + math.Cos(float64((seed+7)*(i+11)))
		value := float32(x * 0.5)
		vector[i] = value
		norm += float64(value * value)
	}
	if norm == 0 {
		panic(fmt.Sprintf("zero norm for seed %d", seed))
	}
	return vector, float32(1 / math.Sqrt(norm))
}
