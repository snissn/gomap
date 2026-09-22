package vectordistancekernels

import (
	"fmt"
	"math"
	"testing"

	nk "github.com/ashvardanian/NumKong/golang"
	axiomsimd "github.com/axiomhq/simd-go"
	dahvrisimd "github.com/ic-timon/da-hvri/simd"
	simdf32 "github.com/tphakala/simd/f32"
	"github.com/viterin/vek/vek32"
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

	b.Run("vek32_dot_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := vek32.Dot(query, candidate)
			sum += 1 - dot*queryInvNorm*candidateInvNorms[i%benchCandidates]
		}
		sinkDistance32 = sum
	})

	b.Run("tphakala_simd_f32_dot_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := simdf32.DotProduct(query, candidate)
			sum += 1 - dot*queryInvNorm*candidateInvNorms[i%benchCandidates]
		}
		sinkDistance32 = sum
	})

	b.Run("tphakala_simd_f32_dot_unsafe_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := simdf32.DotProductUnsafe(query, candidate)
			sum += 1 - dot*queryInvNorm*candidateInvNorms[i%benchCandidates]
		}
		sinkDistance32 = sum
	})

	b.Run("da_hvri_simd_dot_product_cached_norms", func(b *testing.B) {
		b.ReportAllocs()
		var sum float32
		for i := 0; i < b.N; i++ {
			candidate := candidateAt(candidates, i, benchDims)
			dot := dahvrisimd.DotProduct(query, candidate)
			sum += float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[i%benchCandidates]))
		}
		sinkDistance32 = sum
	})
}

func BenchmarkCosineDistanceCandidateBatch128(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	candidates, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)
	candidateRows := candidateRows(candidates, benchCandidates, benchDims)
	packedCandidates := nk.NewPackedMatrixF32(candidates, benchCandidates, benchDims)
	dots := make([]float64, benchCandidates)
	dots32 := make([]float32, benchCandidates)
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

	b.Run("vek32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := vek32.Dot(query, candidateAt(candidates, j, benchDims))
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := simdf32.DotProduct(query, candidateAt(candidates, j, benchDims))
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_unsafe_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := simdf32.DotProductUnsafe(query, candidateAt(candidates, j, benchDims))
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_batch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			simdf32.DotProductBatch(dots32, candidateRows, query)
			for j, dot := range dots32 {
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("da_hvri_simd_dot_product_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				dot := dahvrisimd.DotProduct(query, candidateAt(candidates, j, benchDims))
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[j]))
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

func BenchmarkTreeDBRerankKernelCandidateBatch128(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	candidateVectors, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)

	b.ReportMetric(benchCandidates, "candidates/op")
	b.ReportMetric(benchDims, "dims")

	b.Run("current_numkong_pack_each_query_make_dots", func(b *testing.B) {
		b.ReportAllocs()
		var lastDots []float64
		var lastDistances []float32
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			dots := make([]float64, benchCandidates)
			distances := make([]float32, benchCandidates)
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
			lastDots = dots
			lastDistances = distances
		}
		sinkDotBuf = lastDots
		sinkDistanceBuf = lastDistances
	})

	b.Run("numkong_pack_each_query_reuse_outputs", func(b *testing.B) {
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_pack_each_query_reuse_outputs_configured_thread", func(b *testing.B) {
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		unlock := nk.ConfigureThread()
		defer unlock()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_prepacked_reused", func(b *testing.B) {
		packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_prepacked_reused_configured_thread", func(b *testing.B) {
		packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		unlock := nk.ConfigureThread()
		defer unlock()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_prepacked_reused_worker_pool_1", func(b *testing.B) {
		packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		pool := nk.NewWorkerPool(1)
		defer pool.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			packedCandidates.DotsF32WithPool(query, dots, 1, pool)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("numkong_angulars_pack_each_query_reuse_output", func(b *testing.B) {
		angularDistances := make([]float64, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			nk.AngularsPackedF32(query, packedCandidates, angularDistances, 1)
		}
		sinkDotBuf = angularDistances
	})

	b.Run("numkong_angulars_prepacked_reused", func(b *testing.B) {
		packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
		angularDistances := make([]float64, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nk.AngularsPackedF32(query, packedCandidates, angularDistances, 1)
		}
		sinkDotBuf = angularDistances
	})

	b.Run("axiomhq_dot_product_f32_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := axiomsimd.DotProductFloat32(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("gonum_blas32_dot_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := blas32.Dot(
					blas32.Vector{N: benchDims, Inc: 1, Data: query},
					blas32.Vector{N: benchDims, Inc: 1, Data: candidate},
				)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("vek32_dot_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := vek32.Dot(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := simdf32.DotProduct(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_unsafe_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := simdf32.DotProductUnsafe(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_batch_reuse_output", func(b *testing.B) {
		candidateMatrix := candidateRows(candidateVectors, benchCandidates, benchDims)
		dots32 := make([]float32, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			simdf32.DotProductBatch(dots32, candidateMatrix, query)
			for j, dot := range dots32 {
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("da_hvri_simd_dot_product_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := dahvrisimd.DotProduct(query, candidate)
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[j]))
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("axiomhq_dot_product_f32_loop_make_output", func(b *testing.B) {
		b.ReportAllocs()
		var lastDistances []float32
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			distances := make([]float32, benchCandidates)
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := axiomsimd.DotProductFloat32(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
			lastDistances = distances
		}
		sinkDistanceBuf = lastDistances
	})

	b.Run("numkong_dot_f32_scalar_loop_reuse_output", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := nk.DotF32(query, candidate)
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(candidateInvNorms[j]))
			}
		}
		sinkDistanceBuf = distances
	})
}

func BenchmarkTreeDBRerankGatherAndScoreCandidateBatch128(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	nodes, nodeInvNorms := benchCandidateNodes(benchCandidates, benchDims)

	b.ReportMetric(benchCandidates, "candidates/op")
	b.ReportMetric(benchDims, "dims")

	b.Run("current_like_gather_alloc_numkong_pack_each_query", func(b *testing.B) {
		b.ReportAllocs()
		var lastDots []float64
		var lastDistances []float32
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			candidateVectors := make([]float32, 0, benchCandidates*benchDims)
			candidateInvNorms := make([]float32, 0, benchCandidates)
			for j := range nodes {
				candidateVectors = append(candidateVectors, nodes[j]...)
				candidateInvNorms = append(candidateInvNorms, nodeInvNorms[j])
			}
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			dots := make([]float64, benchCandidates)
			distances := make([]float32, benchCandidates)
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
			lastDots = dots
			lastDistances = distances
		}
		sinkDotBuf = lastDots
		sinkDistanceBuf = lastDistances
	})

	b.Run("gather_reuse_buffers_numkong_pack_each_query", func(b *testing.B) {
		candidateVectors := make([]float32, 0, benchCandidates*benchDims)
		candidateInvNorms := make([]float32, 0, benchCandidates)
		dots := make([]float64, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			candidateVectors = candidateVectors[:0]
			candidateInvNorms = candidateInvNorms[:0]
			for j := range nodes {
				candidateVectors = append(candidateVectors, nodes[j]...)
				candidateInvNorms = append(candidateInvNorms, nodeInvNorms[j])
			}
			packedCandidates := nk.NewPackedMatrixF32(candidateVectors, benchCandidates, benchDims)
			nk.DotsPackedF32(query, packedCandidates, dots, 1)
			scaleDotRow(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDotBuf = dots
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_axiomhq_dot_product_f32", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := axiomsimd.DotProductFloat32(query, nodes[j])
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_gonum_blas32_dot", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := blas32.Dot(
					blas32.Vector{N: benchDims, Inc: 1, Data: query},
					blas32.Vector{N: benchDims, Inc: 1, Data: nodes[j]},
				)
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_vek32_dot", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := vek32.Dot(query, nodes[j])
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_tphakala_simd_f32_dot", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := simdf32.DotProduct(query, nodes[j])
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_tphakala_simd_f32_dot_unsafe", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := simdf32.DotProductUnsafe(query, nodes[j])
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_tphakala_simd_f32_dot_batch", func(b *testing.B) {
		dots32 := make([]float32, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			simdf32.DotProductBatch(dots32, nodes, query)
			for j, dot := range dots32 {
				distances[j] = 1 - dot*queryInvNorm*nodeInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("direct_node_vectors_da_hvri_simd_dot_product", func(b *testing.B) {
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range nodes {
				dot := dahvrisimd.DotProduct(query, nodes[j])
				distances[j] = float32(1 - dot*float64(queryInvNorm)*float64(nodeInvNorms[j]))
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("gather_reuse_buffers_axiomhq_dot_product_f32", func(b *testing.B) {
		candidateVectors := make([]float32, 0, benchCandidates*benchDims)
		candidateInvNorms := make([]float32, 0, benchCandidates)
		distances := make([]float32, benchCandidates)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			candidateVectors = candidateVectors[:0]
			candidateInvNorms = candidateInvNorms[:0]
			for j := range nodes {
				candidateVectors = append(candidateVectors, nodes[j]...)
				candidateInvNorms = append(candidateInvNorms, nodeInvNorms[j])
			}
			for j := 0; j < benchCandidates; j++ {
				candidate := candidateVectors[j*benchDims : (j+1)*benchDims]
				dot := axiomsimd.DotProductFloat32(query, candidate)
				distances[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
			}
		}
		sinkDistanceBuf = distances
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

	b.Run("vek32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					dot := vek32.Dot(query, candidateAt(candidates, j, benchDims))
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					dot := simdf32.DotProduct(query, candidateAt(candidates, j, benchDims))
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_unsafe_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				for j := 0; j < benchCandidates; j++ {
					dot := simdf32.DotProductUnsafe(query, candidateAt(candidates, j, benchDims))
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("tphakala_simd_f32_dot_batch_by_query", func(b *testing.B) {
		candidateRows := candidateRows(candidates, benchCandidates, benchDims)
		dots32 := make([]float32, benchCandidates)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				simdf32.DotProductBatch(dots32, candidateRows, query)
				for j, dot := range dots32 {
					row[j] = 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
				}
			}
		}
		sinkDistanceBuf = distances
	})

	b.Run("da_hvri_simd_dot_product_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for q := 0; q < benchQueries; q++ {
				query := queries[q*benchDims : (q+1)*benchDims]
				row := distances[q*benchCandidates : (q+1)*benchCandidates]
				queryInvNorm := float64(queryInvNorms[q])
				for j := 0; j < benchCandidates; j++ {
					dot := dahvrisimd.DotProduct(query, candidateAt(candidates, j, benchDims))
					row[j] = float32(1 - dot*queryInvNorm*float64(candidateInvNorms[j]))
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
		dot += float64(left) * float64(candidate[i])
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

func scaleDotRow(dots []float64, queryInvNorm float32, candidateInvNorms []float32, distances []float32) {
	queryInvNorm64 := float64(queryInvNorm)
	for j, candidateInvNorm := range candidateInvNorms {
		distances[j] = float32(1 - dots[j]*queryInvNorm64*float64(candidateInvNorm))
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

func benchCandidateNodes(count, dims int) ([][]float32, []float32) {
	nodes := make([][]float32, count)
	invNorms := make([]float32, count)
	for i := 0; i < count; i++ {
		nodes[i], invNorms[i] = benchVector(i+1009, dims)
	}
	return nodes, invNorms
}

func candidateAt(candidates []float32, index, dims int) []float32 {
	index %= len(candidates) / dims
	return candidates[index*dims : (index+1)*dims]
}

func candidateRows(candidates []float32, count, dims int) [][]float32 {
	rows := make([][]float32, count)
	for i := range rows {
		rows[i] = candidateAt(candidates, i, dims)
	}
	return rows
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
