//go:build darwin && cgo

package vectordistancekernels

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"testing"

	axiomsimd "github.com/axiomhq/simd-go"
)

func TestMetalDotKernelCandidateBatch128(t *testing.T) {
	kernel, err := newMetalDotKernel(benchDims, benchCandidates)
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer kernel.Close()

	query, _ := benchVector(17, benchDims)
	candidates, _ := benchCandidateMatrix(benchCandidates, benchDims)
	dots := make([]float32, benchCandidates)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load Metal candidates: %v", err)
	}
	if err := kernel.DotLoaded(query, dots); err != nil {
		t.Fatalf("run Metal dot: %v", err)
	}
	assertMetalDots(t, query, candidates, dots)

	clear(dots)
	if err := kernel.DotNoCopy(query, candidates, dots); err != nil {
		t.Fatalf("run no-copy Metal dot: %v", err)
	}
	assertMetalDots(t, query, candidates, dots)

	clear(dots)
	if err := kernel.DotLoadedNoCopyOutput(query, dots); err != nil {
		t.Fatalf("run Metal dot with no-copy output: %v", err)
	}
	assertMetalDots(t, query, candidates, dots)
}

func TestMetalDotKernelQueryBatch(t *testing.T) {
	const queryCount = 7

	kernel, err := newMetalDotKernel(benchDims, benchCandidates)
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer kernel.Close()

	queries, queryInvNorms := benchQueryMatrix(queryCount, benchDims)
	candidates, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)
	dots := make([]float32, queryCount*benchCandidates)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load Metal candidates: %v", err)
	}
	if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
		t.Fatalf("load Metal candidate inverse norms: %v", err)
	}
	if err := kernel.QueryBatchLoaded(queries, queryCount, dots); err != nil {
		t.Fatalf("run Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchLoadedAsyncDoubleBuffered(queries, queryCount, 3, dots); err != nil {
		t.Fatalf("run async double-buffered Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchTiledLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run tiled no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchSIMDLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run SIMD no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)

	clear(dots)
	if err := kernel.QueryBatchCosineLoadedNoCopy(queries, queryInvNorms, queryCount, dots); err != nil {
		t.Fatalf("run fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchCosineForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, benchDims, dots, 1e-4)

	clear(dots)
	if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchCosineForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, benchDims, dots, 1e-4)

	clear(dots)
	if err := kernel.QueryBatchMPSLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, benchDims, dots, 1e-3)

	const topK = 10
	topKScores := make([]float32, queryCount*kernel.topKBlockCount()*topK)
	topKIndices := make([]uint32, len(topKScores))
	blockCount, err := kernel.QueryBatchCosineBlockTopKLoadedNoCopy(queries, queryInvNorms, queryCount, topK, topKScores, topKIndices)
	if err != nil {
		t.Fatalf("run block top-k no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalBlockTopKForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, benchDims, topK, blockCount, topKScores, topKIndices, 1e-4)

	clear(dots)
	if err := kernel.QueryBatchNoCopy(queries, candidates, queryCount, dots); err != nil {
		t.Fatalf("run no-copy Metal query batch: %v", err)
	}
	assertMetalQueryBatchDots(t, queries, candidates, queryCount, dots)
}

func TestMetalDotKernelQueryBatchHigherDimension(t *testing.T) {
	const (
		dims           = 256
		queryCount     = 5
		candidateCount = 97
	)

	kernel, err := newMetalDotKernel(dims, candidateCount)
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer kernel.Close()

	queries, queryInvNorms := benchQueryMatrix(queryCount, dims)
	candidates, candidateInvNorms := benchCandidateMatrix(candidateCount, dims)
	dots := make([]float32, queryCount*candidateCount)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load Metal candidates: %v", err)
	}
	if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
		t.Fatalf("load Metal candidate inverse norms: %v", err)
	}
	if err := kernel.QueryBatchTiledLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run tiled no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatchSIMDLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run SIMD no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatchCosineLoadedNoCopy(queries, queryInvNorms, queryCount, dots); err != nil {
		t.Fatalf("run fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchCosineForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, dims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchCosineForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, dims, dots, 1e-3)
}

func TestMetalDotKernelQueryBatch768(t *testing.T) {
	const (
		dims           = 768
		queryCount     = 4
		candidateCount = 128
		topK           = 10
	)

	kernel, err := newMetalDotKernel(dims, candidateCount)
	if err != nil {
		t.Skipf("Metal unavailable: %v", err)
	}
	defer kernel.Close()

	queries, queryInvNorms := benchQueryMatrix(queryCount, dims)
	candidates, candidateInvNorms := benchCandidateMatrix(candidateCount, dims)
	dots := make([]float32, queryCount*candidateCount)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load Metal candidates: %v", err)
	}
	if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
		t.Fatalf("load Metal candidate inverse norms: %v", err)
	}
	if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)

	for variant, variantName := range metalDotTiledVariantNames {
		clear(dots)
		if err := kernel.QueryBatchTiledVariantLoadedNoCopy(queries, queryCount, variant, dots); err != nil {
			t.Fatalf("run tiled variant %s no-copy loaded-candidate Metal query batch: %v", variantName, err)
		}
		assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)
	}

	clear(dots)
	if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, queryCount, dots); err != nil {
		t.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchCosineForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, dims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatchMPSLoadedNoCopy(queries, queryCount, dots); err != nil {
		t.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 1e-3)

	topKScores := make([]float32, queryCount*kernel.topKBlockCount()*topK)
	topKIndices := make([]uint32, len(topKScores))
	blockCount, err := kernel.QueryBatchCosineBlockTopKLoadedNoCopy(queries, queryInvNorms, queryCount, topK, topKScores, topKIndices)
	if err != nil {
		t.Fatalf("run block top-k no-copy loaded-candidate Metal query batch: %v", err)
	}
	assertMetalBlockTopKForDims(t, queries, queryInvNorms, candidates, candidateInvNorms, queryCount, dims, topK, blockCount, topKScores, topKIndices, 1e-3)
}

func assertMetalDots(t *testing.T, query, candidates, dots []float32) {
	t.Helper()
	for j, got := range dots {
		want := pureGoDotF32Accum(query, candidateAt(candidates, j, benchDims))
		if diff := math.Abs(float64(got - want)); diff > 1e-4 {
			t.Fatalf("dot[%d]=%g want %g diff %g", j, got, want, diff)
		}
	}
}

func assertMetalQueryBatchDots(t *testing.T, queries, candidates []float32, queryCount int, dots []float32) {
	t.Helper()
	assertMetalQueryBatchDotsForDims(t, queries, candidates, queryCount, benchDims, dots, 1e-4)
}

func assertMetalQueryBatchDotsForDims(t *testing.T, queries, candidates []float32, queryCount, dims int, dots []float32, tolerance float64) {
	t.Helper()
	candidateCount := len(candidates) / dims
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		for j := 0; j < candidateCount; j++ {
			got := dots[q*candidateCount+j]
			want := pureGoDotF32Accum(query, candidateAt(candidates, j, dims))
			if diff := math.Abs(float64(got - want)); diff > tolerance {
				t.Fatalf("dot[%d,%d]=%g want %g diff %g", q, j, got, want, diff)
			}
		}
	}
}

func assertMetalQueryBatchCosineForDims(t *testing.T, queries, queryInvNorms, candidates, candidateInvNorms []float32, queryCount, dims int, distances []float32, tolerance float64) {
	t.Helper()
	candidateCount := len(candidates) / dims
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		for j := 0; j < candidateCount; j++ {
			got := distances[q*candidateCount+j]
			dot := pureGoDotF32Accum(query, candidateAt(candidates, j, dims))
			want := 1 - dot*queryInvNorms[q]*candidateInvNorms[j]
			if diff := math.Abs(float64(got - want)); diff > tolerance {
				t.Fatalf("distance[%d,%d]=%g want %g diff %g", q, j, got, want, diff)
			}
		}
	}
}

func assertMetalBlockTopKForDims(t *testing.T, queries, queryInvNorms, candidates, candidateInvNorms []float32, queryCount, dims, topK, blockCount int, gotScores []float32, gotIndices []uint32, tolerance float64) {
	t.Helper()
	candidateCount := len(candidates) / dims
	dense := make([]float32, queryCount*candidateCount)
	runAxiomCosineBatch(queries, queryInvNorms, candidates, candidateInvNorms, queryCount, candidateCount, dims, dense)
	wantScores, wantIndices := cpuDenseTopK(dense, queryCount, candidateCount, topK)
	mergedScores, mergedIndices := mergeBlockTopK(gotScores, gotIndices, queryCount, blockCount, topK)
	for i := range wantScores {
		if diff := math.Abs(float64(mergedScores[i] - wantScores[i])); diff > tolerance || mergedIndices[i] != wantIndices[i] {
			t.Fatalf("topk[%d]=(%g,%d) want (%g,%d) diff %g", i, mergedScores[i], mergedIndices[i], wantScores[i], wantIndices[i], diff)
		}
	}
}

func runAxiomDotBatch(queries, candidates []float32, queryCount, candidateCount, dims int, dots []float32) {
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := dots[q*candidateCount : (q+1)*candidateCount]
		for j := 0; j < candidateCount; j++ {
			start := j * dims
			row[j] = axiomsimd.DotProductFloat32(query, candidates[start:start+dims])
		}
	}
}

func runAxiomCosineBatch(queries, queryInvNorms, candidates, candidateInvNorms []float32, queryCount, candidateCount, dims int, distances []float32) {
	for q := 0; q < queryCount; q++ {
		runAxiomCosineRows(queries, queryInvNorms, candidates, candidateInvNorms, q, q+1, candidateCount, dims, distances)
	}
}

func runAxiomCosineBatchParallel(queries, queryInvNorms, candidates, candidateInvNorms []float32, queryCount, candidateCount, dims int, distances []float32, workers int) {
	if workers <= 1 || queryCount <= 1 {
		runAxiomCosineBatch(queries, queryInvNorms, candidates, candidateInvNorms, queryCount, candidateCount, dims, distances)
		return
	}
	if workers > queryCount {
		workers = queryCount
	}
	rowsPerWorker := (queryCount + workers - 1) / workers
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		startQuery := worker * rowsPerWorker
		endQuery := startQuery + rowsPerWorker
		if endQuery > queryCount {
			endQuery = queryCount
		}
		if startQuery >= endQuery {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runAxiomCosineRows(queries, queryInvNorms, candidates, candidateInvNorms, startQuery, endQuery, candidateCount, dims, distances)
		}()
	}
	wg.Wait()
}

func runAxiomCosineRows(queries, queryInvNorms, candidates, candidateInvNorms []float32, startQuery, endQuery, candidateCount, dims int, distances []float32) {
	for q := startQuery; q < endQuery; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := distances[q*candidateCount : (q+1)*candidateCount]
		queryInvNorm := queryInvNorms[q]
		for j := 0; j < candidateCount; j++ {
			start := j * dims
			dot := axiomsimd.DotProductFloat32(query, candidates[start:start+dims])
			row[j] = 1 - dot*queryInvNorm*candidateInvNorms[j]
		}
	}
}

const metalDotTopKEmptyIndex = ^uint32(0)

func cpuDenseTopK(dense []float32, queryCount, candidateCount, topK int) ([]float32, []uint32) {
	scores := make([]float32, queryCount*topK)
	indices := make([]uint32, queryCount*topK)
	initTopK(scores, indices)
	for q := 0; q < queryCount; q++ {
		scoreRow := scores[q*topK : (q+1)*topK]
		indexRow := indices[q*topK : (q+1)*topK]
		for j := 0; j < candidateCount; j++ {
			insertTopK(scoreRow, indexRow, dense[q*candidateCount+j], uint32(j))
		}
	}
	return scores, indices
}

func mergeBlockTopK(blockScores []float32, blockIndices []uint32, queryCount, blockCount, topK int) ([]float32, []uint32) {
	scores := make([]float32, queryCount*topK)
	indices := make([]uint32, queryCount*topK)
	initTopK(scores, indices)
	for q := 0; q < queryCount; q++ {
		scoreRow := scores[q*topK : (q+1)*topK]
		indexRow := indices[q*topK : (q+1)*topK]
		for block := 0; block < blockCount; block++ {
			base := (q*blockCount + block) * topK
			for k := 0; k < topK; k++ {
				index := blockIndices[base+k]
				if index == metalDotTopKEmptyIndex {
					continue
				}
				insertTopK(scoreRow, indexRow, blockScores[base+k], index)
			}
		}
	}
	return scores, indices
}

func initTopK(scores []float32, indices []uint32) {
	for i := range scores {
		scores[i] = float32(math.Inf(1))
		indices[i] = metalDotTopKEmptyIndex
	}
}

func insertTopK(scores []float32, indices []uint32, score float32, index uint32) {
	last := len(scores) - 1
	if score > scores[last] || (score == scores[last] && index >= indices[last]) {
		return
	}
	pos := last
	for pos > 0 && (score < scores[pos-1] || (score == scores[pos-1] && index < indices[pos-1])) {
		scores[pos] = scores[pos-1]
		indices[pos] = indices[pos-1]
		pos--
	}
	scores[pos] = score
	indices[pos] = index
}

func reportQueryBatchMetrics(b *testing.B, queryCount, candidateCount, dims, resultCount int, resultMetric string, candidateValues, outputValues int) {
	b.ReportMetric(float64(queryCount), "queries/op")
	b.ReportMetric(float64(candidateCount), "candidates/query")
	b.ReportMetric(float64(resultCount), resultMetric)
	b.ReportMetric(float64(dims), "dims")
	if candidateValues > 0 {
		b.ReportMetric(float64(candidateValues*4)/(1024*1024), "candidate_MiB")
	}
	if outputValues > 0 {
		b.ReportMetric(float64(outputValues*4)/(1024*1024), "output_MiB")
	}
}

func BenchmarkMetalDotCandidateBatch128(b *testing.B) {
	query, queryInvNorm := benchVector(17, benchDims)
	candidateVectors, candidateInvNorms := benchCandidateMatrix(benchCandidates, benchDims)
	dots := make([]float32, benchCandidates)
	distances := make([]float32, benchCandidates)

	kernel, err := newMetalDotKernel(benchDims, benchCandidates)
	if err != nil {
		b.Skipf("Metal unavailable: %v", err)
	}
	defer kernel.Close()

	b.ReportMetric(benchCandidates, "candidates/op")
	b.ReportMetric(benchDims, "dims")

	b.Run("metal_copy_candidates_each_query", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.Dot(query, candidateVectors, dots); err != nil {
				b.Fatalf("run Metal dot: %v", err)
			}
			scaleDotRow32(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDistanceBuf = distances
	})

	b.Run("metal_no_copy_go_slices", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.DotNoCopy(query, candidateVectors, dots); err != nil {
				b.Fatalf("run no-copy Metal dot: %v", err)
			}
			scaleDotRow32(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDistanceBuf = distances
	})

	b.Run("metal_reuse_candidate_buffer", func(b *testing.B) {
		if err := kernel.LoadCandidates(candidateVectors); err != nil {
			b.Fatalf("load Metal candidates: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.DotLoaded(query, dots); err != nil {
				b.Fatalf("run Metal dot: %v", err)
			}
			scaleDotRow32(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDistanceBuf = distances
	})

	b.Run("metal_reuse_candidate_buffer_no_copy_output", func(b *testing.B) {
		if err := kernel.LoadCandidates(candidateVectors); err != nil {
			b.Fatalf("load Metal candidates: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.DotLoadedNoCopyOutput(query, dots); err != nil {
				b.Fatalf("run Metal dot with no-copy output: %v", err)
			}
			scaleDotRow32(dots, queryInvNorm, candidateInvNorms, distances)
		}
		sinkDistanceBuf = distances
	})
}

func BenchmarkMetalDotRawBatchSizes(b *testing.B) {
	query, _ := benchVector(17, benchDims)
	for _, candidateCount := range []int{128, 1024, 8192, 65536} {
		candidateVectors, _ := benchCandidateMatrix(candidateCount, benchDims)

		b.Run(fmt.Sprintf("candidates_%d/axiomhq_dot_product_f32_loop", candidateCount), func(b *testing.B) {
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < candidateCount; j++ {
					start := j * benchDims
					dots[j] = axiomsimd.DotProductFloat32(query, candidateVectors[start:start+benchDims])
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(fmt.Sprintf("candidates_%d/metal_copy_candidates_each_query", candidateCount), func(b *testing.B) {
			kernel, err := newMetalDotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.Dot(query, candidateVectors, dots); err != nil {
					b.Fatalf("run Metal dot: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(fmt.Sprintf("candidates_%d/metal_no_copy_go_slices", candidateCount), func(b *testing.B) {
			kernel, err := newMetalDotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.DotNoCopy(query, candidateVectors, dots); err != nil {
					b.Fatalf("run no-copy Metal dot: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(fmt.Sprintf("candidates_%d/metal_reuse_candidate_buffer", candidateCount), func(b *testing.B) {
			kernel, err := newMetalDotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			if err := kernel.LoadCandidates(candidateVectors); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.DotLoaded(query, dots); err != nil {
					b.Fatalf("run Metal dot: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(fmt.Sprintf("candidates_%d/metal_reuse_candidate_buffer_no_copy_output", candidateCount), func(b *testing.B) {
			kernel, err := newMetalDotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			if err := kernel.LoadCandidates(candidateVectors); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.DotLoadedNoCopyOutput(query, dots); err != nil {
					b.Fatalf("run Metal dot with no-copy output: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})
	}
}

func BenchmarkMetalDotQueryBatchSizes(b *testing.B) {
	cases := []struct {
		dims           int
		queryCount     int
		candidateCount int
	}{
		{dims: 64, queryCount: 1, candidateCount: 8192},
		{dims: 64, queryCount: 8, candidateCount: 8192},
		{dims: 64, queryCount: 32, candidateCount: 8192},
		{dims: 64, queryCount: 128, candidateCount: 8192},
		{dims: 64, queryCount: 32, candidateCount: 65536},
		{dims: 768, queryCount: 8, candidateCount: 8192},
		{dims: 768, queryCount: 32, candidateCount: 8192},
		{dims: 768, queryCount: 128, candidateCount: 8192},
	}

	for _, tc := range cases {
		queries, queryInvNorms := benchQueryMatrix(tc.queryCount, tc.dims)
		candidates, candidateInvNorms := benchCandidateMatrix(tc.candidateCount, tc.dims)
		dots := make([]float32, tc.queryCount*tc.candidateCount)
		name := fmt.Sprintf("dims_%d_queries_%d_candidates_%d", tc.dims, tc.queryCount, tc.candidateCount)

		b.Run(name+"/axiomhq_dot_product_f32_loop", func(b *testing.B) {
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runAxiomDotBatch(queries, candidates, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/axiomhq_cosine_f32_loop", func(b *testing.B) {
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runAxiomCosineBatch(queries, queryInvNorms, candidates, candidateInvNorms, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_reuse_candidate_buffer_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchLoaded(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_async_double_buffer_reuse_candidate_buffer_copy_query_output", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			if err := kernel.QueryBatchLoadedAsyncDoubleBuffered(queries, tc.queryCount, b.N, dots); err != nil {
				b.Fatalf("run async double-buffered Metal query batch: %v", err)
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_tiled_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchTiledLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run tiled no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_simd_reduce_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchSIMDLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run SIMD no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_fixed_dim_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_fused_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
				b.Fatalf("load Metal candidate inverse norms: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchCosineLoadedNoCopy(queries, queryInvNorms, tc.queryCount, dots); err != nil {
					b.Fatalf("run fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_fixed_dim_fused_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
				b.Fatalf("load Metal candidate inverse norms: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, tc.queryCount, dots); err != nil {
					b.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_mps_matmul_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchMPSLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_no_copy_go_slices", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchNoCopy(queries, candidates, tc.queryCount, dots); err != nil {
					b.Fatalf("run no-copy Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})
	}
}

func BenchmarkMetalDotQueryBatchDimensionSizes(b *testing.B) {
	cases := []struct {
		dims           int
		queryCount     int
		candidateCount int
	}{
		{dims: 64, queryCount: 8, candidateCount: 8192},
		{dims: 128, queryCount: 8, candidateCount: 8192},
		{dims: 256, queryCount: 8, candidateCount: 8192},
		{dims: 768, queryCount: 8, candidateCount: 8192},
		{dims: 1536, queryCount: 8, candidateCount: 8192},
	}

	for _, tc := range cases {
		queries, queryInvNorms := benchQueryMatrix(tc.queryCount, tc.dims)
		candidates, candidateInvNorms := benchCandidateMatrix(tc.candidateCount, tc.dims)
		dots := make([]float32, tc.queryCount*tc.candidateCount)
		name := fmt.Sprintf("dims_%d_queries_%d_candidates_%d", tc.dims, tc.queryCount, tc.candidateCount)

		b.Run(name+"/axiomhq_dot_product_f32_loop", func(b *testing.B) {
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runAxiomDotBatch(queries, candidates, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/axiomhq_cosine_f32_parallel", func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(dots))
			b.ReportMetric(float64(workers), "cpu_workers")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runAxiomCosineBatchParallel(queries, queryInvNorms, candidates, candidateInvNorms, tc.queryCount, tc.candidateCount, tc.dims, dots, workers)
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_tiled_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchTiledLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run tiled no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_simd_reduce_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchSIMDLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run SIMD no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_fixed_dim_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_fixed_dim_fused_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
				b.Fatalf("load Metal candidate inverse norms: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, tc.queryCount, dots); err != nil {
					b.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_mps_matmul_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchMPSLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})
	}
}

func BenchmarkMetalDotQueryBatchTiledVariantTuning(b *testing.B) {
	cases := []struct {
		dims           int
		queryCount     int
		candidateCount int
	}{
		{dims: 768, queryCount: 8, candidateCount: 8192},
		{dims: 1536, queryCount: 8, candidateCount: 8192},
	}

	for _, tc := range cases {
		queries, _ := benchQueryMatrix(tc.queryCount, tc.dims)
		candidates, _ := benchCandidateMatrix(tc.candidateCount, tc.dims)
		dots := make([]float32, tc.queryCount*tc.candidateCount)
		name := fmt.Sprintf("dims_%d_queries_%d_candidates_%d", tc.dims, tc.queryCount, tc.candidateCount)

		b.Run(name+"/metal_fixed_dim_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_mps_matmul_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchMPSLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_tiled_q8_c16_d64_current", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchTiledLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run tiled no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		b.Run(name+"/metal_simd_reduce_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
			kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("Metal unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load Metal candidates: %v", err)
			}
			reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchSIMDLoadedNoCopy(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run SIMD no-copy loaded-candidate Metal query batch: %v", err)
				}
			}
			sinkDistanceBuf = dots
		})

		for variant, variantName := range metalDotTiledVariantNames {
			b.Run(name+"/metal_tiled_variant_"+variantName, func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchTiledVariantLoadedNoCopy(queries, tc.queryCount, variant, dots); err != nil {
						b.Fatalf("run tiled variant %s no-copy loaded-candidate Metal query batch: %v", variantName, err)
					}
				}
				sinkDistanceBuf = dots
			})
		}
	}
}

func BenchmarkMetalDotQueryBatchRealisticIndexShapes(b *testing.B) {
	runLarge := os.Getenv("METAL_DOT_LARGE") == "1"
	cases := []struct {
		dims           int
		queryCount     int
		candidateCount int
		large          bool
	}{
		{dims: 64, queryCount: 32, candidateCount: 8192},
		{dims: 64, queryCount: 128, candidateCount: 8192},
		{dims: 64, queryCount: 512, candidateCount: 8192},
		{dims: 64, queryCount: 32, candidateCount: 65536},
		{dims: 64, queryCount: 128, candidateCount: 65536},
		{dims: 64, queryCount: 512, candidateCount: 65536},
		{dims: 64, queryCount: 32, candidateCount: 1_000_000, large: true},
		{dims: 64, queryCount: 128, candidateCount: 1_000_000, large: true},
		{dims: 64, queryCount: 512, candidateCount: 1_000_000, large: true},
		{dims: 768, queryCount: 32, candidateCount: 8192},
		{dims: 768, queryCount: 128, candidateCount: 8192},
		{dims: 768, queryCount: 512, candidateCount: 8192},
		{dims: 768, queryCount: 32, candidateCount: 65536},
		{dims: 768, queryCount: 128, candidateCount: 65536},
		{dims: 768, queryCount: 512, candidateCount: 65536},
		{dims: 768, queryCount: 32, candidateCount: 1_000_000, large: true},
		{dims: 768, queryCount: 128, candidateCount: 1_000_000, large: true},
		{dims: 768, queryCount: 512, candidateCount: 1_000_000, large: true},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("dims_%d_queries_%d_candidates_%d", tc.dims, tc.queryCount, tc.candidateCount)
		b.Run(name, func(b *testing.B) {
			if tc.large && !runLarge {
				b.Skip("set METAL_DOT_LARGE=1 to run 1M-candidate dense-output shapes")
			}

			queries, queryInvNorms := benchQueryMatrix(tc.queryCount, tc.dims)
			candidates, candidateInvNorms := benchCandidateMatrix(tc.candidateCount, tc.dims)
			outputs := make([]float32, tc.queryCount*tc.candidateCount)

			b.Run("axiomhq_cosine_f32_loop", func(b *testing.B) {
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					runAxiomCosineBatch(queries, queryInvNorms, candidates, candidateInvNorms, tc.queryCount, tc.candidateCount, tc.dims, outputs)
				}
				sinkDistanceBuf = outputs
			})

			b.Run("axiomhq_cosine_f32_parallel", func(b *testing.B) {
				workers := runtime.GOMAXPROCS(0)
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(outputs))
				b.ReportMetric(float64(workers), "cpu_workers")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					runAxiomCosineBatchParallel(queries, queryInvNorms, candidates, candidateInvNorms, tc.queryCount, tc.candidateCount, tc.dims, outputs, workers)
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_copy_candidates_each_batch_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatch(queries, candidates, tc.queryCount, outputs); err != nil {
						b.Fatalf("run staged Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_reuse_candidate_buffer_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchLoaded(queries, tc.queryCount, outputs); err != nil {
						b.Fatalf("run Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchLoadedNoCopy(queries, tc.queryCount, outputs); err != nil {
						b.Fatalf("run no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_fixed_dim_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchFixedDimLoadedNoCopy(queries, tc.queryCount, outputs); err != nil {
						b.Fatalf("run fixed-dimension no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			for _, depth := range []int{1, 2, 3} {
				b.Run(fmt.Sprintf("metal_async_depth_%d_distinct_batches_reuse_candidate_buffer_copy_query_output", depth), func(b *testing.B) {
					const queryRingBatches = 4
					queryRing, _ := benchQueryMatrix(tc.queryCount*queryRingBatches, tc.dims)
					kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
					if err != nil {
						b.Skipf("Metal unavailable: %v", err)
					}
					defer kernel.Close()
					if err := kernel.LoadCandidates(candidates); err != nil {
						b.Fatalf("load Metal candidates: %v", err)
					}
					reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
					b.ReportMetric(float64(depth), "async_depth")
					b.ReportMetric(float64(queryRingBatches), "query_batches")
					b.ReportAllocs()
					b.ResetTimer()
					if err := kernel.QueryBatchLoadedAsyncBuffered(queryRing, tc.queryCount, queryRingBatches, depth, b.N, outputs); err != nil {
						b.Fatalf("run async depth-%d Metal query batch: %v", depth, err)
					}
					sinkDistanceBuf = outputs
				})
			}

			b.Run("metal_fused_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
					b.Fatalf("load Metal candidate inverse norms: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchCosineLoadedNoCopy(queries, queryInvNorms, tc.queryCount, outputs); err != nil {
						b.Fatalf("run fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_fixed_dim_fused_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
					b.Fatalf("load Metal candidate inverse norms: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "distances/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms, tc.queryCount, outputs); err != nil {
						b.Fatalf("run fixed-dimension fused-cosine no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_mps_matmul_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, tc.queryCount*tc.candidateCount, "dots/op", len(candidates), len(outputs))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := kernel.QueryBatchMPSLoadedNoCopy(queries, tc.queryCount, outputs); err != nil {
						b.Fatalf("run MPS no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = outputs
			})

			b.Run("metal_block_topk_cosine_reuse_candidate_buffer_no_copy_io", func(b *testing.B) {
				const topK = 10
				kernel, err := newMetalDotKernel(tc.dims, tc.candidateCount)
				if err != nil {
					b.Skipf("Metal unavailable: %v", err)
				}
				defer kernel.Close()
				if err := kernel.LoadCandidates(candidates); err != nil {
					b.Fatalf("load Metal candidates: %v", err)
				}
				if err := kernel.LoadCandidateInvNorms(candidateInvNorms); err != nil {
					b.Fatalf("load Metal candidate inverse norms: %v", err)
				}
				blockCount := kernel.topKBlockCount()
				topKScores := make([]float32, tc.queryCount*blockCount*topK)
				topKIndices := make([]uint32, len(topKScores))
				reportQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(topKScores), "block_topk_results/op", len(candidates), len(topKScores)+len(topKIndices))
				b.ReportMetric(float64(topK), "top_k")
				b.ReportMetric(float64(blockCount), "candidate_blocks/query")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := kernel.QueryBatchCosineBlockTopKLoadedNoCopy(queries, queryInvNorms, tc.queryCount, topK, topKScores, topKIndices); err != nil {
						b.Fatalf("run block top-k no-copy loaded-candidate Metal query batch: %v", err)
					}
				}
				sinkDistanceBuf = topKScores
			})
		})
	}
}

func scaleDotRow32(dots []float32, queryInvNorm float32, candidateInvNorms []float32, distances []float32) {
	for j, candidateInvNorm := range candidateInvNorms {
		distances[j] = 1 - dots[j]*queryInvNorm*candidateInvNorm
	}
}

func pureGoDotF32Accum(left, right []float32) float32 {
	var dot float32
	for i, v := range left {
		dot += v * right[i]
	}
	return dot
}
