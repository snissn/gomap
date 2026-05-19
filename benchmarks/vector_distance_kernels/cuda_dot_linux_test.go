//go:build linux && cgo && cuda
// +build linux,cgo,cuda

package vectordistancekernels

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	axiomsimd "github.com/axiomhq/simd-go"
	simdf32 "github.com/tphakala/simd/f32"
	"github.com/viterin/vek/vek32"
	"gonum.org/v1/gonum/blas/blas32"
)

func TestCUDADotKernelQueryBatch(t *testing.T) {
	deviceName, err := cudaDotDeviceName()
	if err != nil {
		t.Skipf("CUDA unavailable: %v", err)
	}
	t.Logf("CUDA device: %s", deviceName)

	const queryCount = 7
	kernel, err := newCUDADotKernel(benchDims, benchCandidates)
	if err != nil {
		t.Skipf("CUDA unavailable: %v", err)
	}
	defer kernel.Close()

	queries, _ := benchQueryMatrix(queryCount, benchDims)
	candidates, _ := benchCandidateMatrix(benchCandidates, benchDims)
	dots := make([]float32, queryCount*benchCandidates)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load CUDA candidates: %v", err)
	}
	if err := kernel.QueryBatchLoaded(queries, queryCount, dots); err != nil {
		t.Fatalf("run CUDA query batch: %v", err)
	}
	assertCUDAQueryBatchDotsForDims(t, queries, candidates, queryCount, benchDims, dots, 1e-3)

	clear(dots)
	if err := kernel.QueryBatch(queries, candidates, queryCount, dots); err != nil {
		t.Fatalf("run staged CUDA query batch: %v", err)
	}
	assertCUDAQueryBatchDotsForDims(t, queries, candidates, queryCount, benchDims, dots, 1e-3)
}

func TestCUDADotKernelQueryBatch768(t *testing.T) {
	const (
		dims           = 768
		queryCount     = 4
		candidateCount = 128
	)
	kernel, err := newCUDADotKernel(dims, candidateCount)
	if err != nil {
		t.Skipf("CUDA unavailable: %v", err)
	}
	defer kernel.Close()

	queries, _ := benchQueryMatrix(queryCount, dims)
	candidates, _ := benchCandidateMatrix(candidateCount, dims)
	dots := make([]float32, queryCount*candidateCount)

	if err := kernel.LoadCandidates(candidates); err != nil {
		t.Fatalf("load CUDA candidates: %v", err)
	}
	if err := kernel.QueryBatchLoaded(queries, queryCount, dots); err != nil {
		t.Fatalf("run CUDA query batch: %v", err)
	}
	assertCUDAQueryBatchDotsForDims(t, queries, candidates, queryCount, dims, dots, 2e-3)
}

func BenchmarkCUDADotCandidateBatch128(b *testing.B) {
	query, _ := benchVector(17, benchDims)
	candidateVectors, _ := benchCandidateMatrix(benchCandidates, benchDims)
	dots := make([]float32, benchCandidates)

	kernel, err := newCUDADotKernel(benchDims, benchCandidates)
	if err != nil {
		b.Skipf("CUDA unavailable: %v", err)
	}
	defer kernel.Close()

	b.ReportMetric(benchCandidates, "candidates/op")
	b.ReportMetric(benchDims, "dims")

	b.Run("cuda_cublas_sgemm_copy_candidates_each_query", func(b *testing.B) {
		if err := kernel.QueryBatch(query, candidateVectors, 1, dots); err != nil {
			b.Fatalf("warm CUDA dot: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.QueryBatch(query, candidateVectors, 1, dots); err != nil {
				b.Fatalf("run CUDA dot: %v", err)
			}
		}
		finishCUDADotBenchmark(b, dots, 1, benchCandidates, benchDims)
	})

	b.Run("cuda_cublas_sgemm_reuse_candidate_buffer", func(b *testing.B) {
		if err := kernel.LoadCandidates(candidateVectors); err != nil {
			b.Fatalf("load CUDA candidates: %v", err)
		}
		if err := kernel.QueryBatchLoaded(query, 1, dots); err != nil {
			b.Fatalf("warm CUDA dot: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := kernel.QueryBatchLoaded(query, 1, dots); err != nil {
				b.Fatalf("run CUDA dot: %v", err)
			}
		}
		finishCUDADotBenchmark(b, dots, 1, benchCandidates, benchDims)
	})
}

func BenchmarkCUDADotRawBatchSizes(b *testing.B) {
	query, _ := benchVector(17, benchDims)
	for _, candidateCount := range []int{128, 1024, 8192, 65536} {
		candidateVectors, _ := benchCandidateMatrix(candidateCount, benchDims)
		candidateRows := candidateRows(candidateVectors, candidateCount, benchDims)

		b.Run(fmt.Sprintf("candidates_%d/cpu_axiomhq_dot_product_f32_loop", candidateCount), func(b *testing.B) {
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
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cpu_gonum_blas32_dot_loop", candidateCount), func(b *testing.B) {
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDAGONUMDotBatch(query, candidateVectors, 1, candidateCount, benchDims, dots)
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cpu_vek32_dot_loop", candidateCount), func(b *testing.B) {
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDAVek32DotBatch(query, candidateVectors, 1, candidateCount, benchDims, dots)
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cpu_tphakala_simd_f32_dot_batch", candidateCount), func(b *testing.B) {
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				simdf32.DotProductBatch(dots, candidateRows, query)
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cpu_tphakala_simd_f32_dot_batch_parallel_worker_pool", candidateCount), func(b *testing.B) {
			dots := make([]float32, candidateCount)
			workers := runtime.GOMAXPROCS(0)
			pool := newCUDATphakalaCandidateBatchWorkerPool(query, candidateRows, dots, workers)
			defer pool.Close()
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			b.ReportMetric(float64(pool.workers), "cpu_workers")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.Run()
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cuda_cublas_sgemm_copy_candidates_each_query", candidateCount), func(b *testing.B) {
			kernel, err := newCUDADotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("CUDA unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			if err := kernel.QueryBatch(query, candidateVectors, 1, dots); err != nil {
				b.Fatalf("warm CUDA dot: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatch(query, candidateVectors, 1, dots); err != nil {
					b.Fatalf("run CUDA dot: %v", err)
				}
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})

		b.Run(fmt.Sprintf("candidates_%d/cuda_cublas_sgemm_reuse_candidate_buffer", candidateCount), func(b *testing.B) {
			kernel, err := newCUDADotKernel(benchDims, candidateCount)
			if err != nil {
				b.Skipf("CUDA unavailable: %v", err)
			}
			defer kernel.Close()
			dots := make([]float32, candidateCount)
			if err := kernel.LoadCandidates(candidateVectors); err != nil {
				b.Fatalf("load CUDA candidates: %v", err)
			}
			b.ReportMetric(float64(candidateCount), "candidates/op")
			b.ReportMetric(benchDims, "dims")
			if err := kernel.QueryBatchLoaded(query, 1, dots); err != nil {
				b.Fatalf("warm CUDA dot: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchLoaded(query, 1, dots); err != nil {
					b.Fatalf("run CUDA dot: %v", err)
				}
			}
			finishCUDADotBenchmark(b, dots, 1, candidateCount, benchDims)
		})
	}
}

func BenchmarkCUDADotQueryBatchSizes(b *testing.B) {
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
		queries, _ := benchQueryMatrix(tc.queryCount, tc.dims)
		candidates, _ := benchCandidateMatrix(tc.candidateCount, tc.dims)
		candidateRows := candidateRows(candidates, tc.candidateCount, tc.dims)
		dots := make([]float32, tc.queryCount*tc.candidateCount)
		name := fmt.Sprintf("dims_%d_queries_%d_candidates_%d", tc.dims, tc.queryCount, tc.candidateCount)

		b.Run(name+"/cpu_axiomhq_dot_product_f32_loop", func(b *testing.B) {
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDAAxiomDotBatch(queries, candidates, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cpu_gonum_blas32_dot_loop", func(b *testing.B) {
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDAGONUMDotBatch(queries, candidates, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cpu_vek32_dot_loop", func(b *testing.B) {
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDAVek32DotBatch(queries, candidates, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cpu_tphakala_simd_f32_dot_batch_by_query", func(b *testing.B) {
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runCUDATphakalaDotBatchByQuery(queries, candidateRows, tc.queryCount, tc.candidateCount, tc.dims, dots)
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cpu_tphakala_simd_f32_dot_batch_parallel_worker_pool", func(b *testing.B) {
			workers := runtime.GOMAXPROCS(0)
			pool := newCUDATphakalaDotBatchWorkerPool(queries, candidateRows, tc.queryCount, tc.candidateCount, tc.dims, dots, workers)
			defer pool.Close()
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			b.ReportMetric(float64(pool.workers), "cpu_workers")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pool.Run()
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cuda_cublas_sgemm_copy_candidates_each_batch", func(b *testing.B) {
			kernel, err := newCUDADotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("CUDA unavailable: %v", err)
			}
			defer kernel.Close()
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			if err := kernel.QueryBatch(queries, candidates, tc.queryCount, dots); err != nil {
				b.Fatalf("warm CUDA query batch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatch(queries, candidates, tc.queryCount, dots); err != nil {
					b.Fatalf("run CUDA query batch: %v", err)
				}
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})

		b.Run(name+"/cuda_cublas_sgemm_reuse_candidate_buffer", func(b *testing.B) {
			kernel, err := newCUDADotKernel(tc.dims, tc.candidateCount)
			if err != nil {
				b.Skipf("CUDA unavailable: %v", err)
			}
			defer kernel.Close()
			if err := kernel.LoadCandidates(candidates); err != nil {
				b.Fatalf("load CUDA candidates: %v", err)
			}
			reportCUDADotQueryBatchMetrics(b, tc.queryCount, tc.candidateCount, tc.dims, len(dots), "dots/op", len(candidates), len(dots))
			if err := kernel.QueryBatchLoaded(queries, tc.queryCount, dots); err != nil {
				b.Fatalf("warm CUDA query batch: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := kernel.QueryBatchLoaded(queries, tc.queryCount, dots); err != nil {
					b.Fatalf("run CUDA query batch: %v", err)
				}
			}
			finishCUDADotBenchmark(b, dots, tc.queryCount, tc.candidateCount, tc.dims)
		})
	}
}

func assertCUDAQueryBatchDotsForDims(t *testing.T, queries, candidates []float32, queryCount, dims int, dots []float32, tolerance float64) {
	t.Helper()
	candidateCount := len(candidates) / dims
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		for j := 0; j < candidateCount; j++ {
			got := dots[q*candidateCount+j]
			want := pureGoDotF32AccumCUDA(query, candidateAt(candidates, j, dims))
			if diff := math.Abs(float64(got - want)); diff > tolerance {
				t.Fatalf("dot[%d,%d]=%g want %g diff %g", q, j, got, want, diff)
			}
		}
	}
}

func runCUDAAxiomDotBatch(queries, candidates []float32, queryCount, candidateCount, dims int, dots []float32) {
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := dots[q*candidateCount : (q+1)*candidateCount]
		for j := 0; j < candidateCount; j++ {
			start := j * dims
			row[j] = axiomsimd.DotProductFloat32(query, candidates[start:start+dims])
		}
	}
}

func runCUDAGONUMDotBatch(queries, candidates []float32, queryCount, candidateCount, dims int, dots []float32) {
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := dots[q*candidateCount : (q+1)*candidateCount]
		for j := 0; j < candidateCount; j++ {
			start := j * dims
			row[j] = blas32.Dot(
				blas32.Vector{N: dims, Inc: 1, Data: query},
				blas32.Vector{N: dims, Inc: 1, Data: candidates[start : start+dims]},
			)
		}
	}
}

func runCUDAVek32DotBatch(queries, candidates []float32, queryCount, candidateCount, dims int, dots []float32) {
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := dots[q*candidateCount : (q+1)*candidateCount]
		for j := 0; j < candidateCount; j++ {
			start := j * dims
			row[j] = vek32.Dot(query, candidates[start:start+dims])
		}
	}
}

func runCUDATphakalaDotBatchByQuery(queries []float32, candidates [][]float32, queryCount, candidateCount, dims int, dots []float32) {
	for q := 0; q < queryCount; q++ {
		query := queries[q*dims : (q+1)*dims]
		row := dots[q*candidateCount : (q+1)*candidateCount]
		simdf32.DotProductBatch(row, candidates, query)
	}
}

type cudaTphakalaCandidateBatchWorkerPool struct {
	runChans []chan struct{}
	stop     chan struct{}
	done     chan struct{}
	workers  int
}

func newCUDATphakalaCandidateBatchWorkerPool(query []float32, candidates [][]float32, dots []float32, workers int) *cudaTphakalaCandidateBatchWorkerPool {
	if workers > len(candidates) {
		workers = len(candidates)
	}
	if workers < 1 {
		workers = 1
	}
	pool := &cudaTphakalaCandidateBatchWorkerPool{
		runChans: make([]chan struct{}, 0, workers),
		stop:     make(chan struct{}),
		done:     make(chan struct{}, workers),
		workers:  workers,
	}
	rowsPerWorker := (len(candidates) + workers - 1) / workers
	for worker := 0; worker < workers; worker++ {
		start := worker * rowsPerWorker
		end := start + rowsPerWorker
		if end > len(candidates) {
			end = len(candidates)
		}
		if start >= end {
			break
		}
		run := make(chan struct{})
		pool.runChans = append(pool.runChans, run)
		go func(run <-chan struct{}, start, end int) {
			workerCandidates := candidates[start:end]
			workerDots := dots[start:end]
			for {
				select {
				case <-run:
					simdf32.DotProductBatch(workerDots, workerCandidates, query)
					pool.done <- struct{}{}
				case <-pool.stop:
					return
				}
			}
		}(run, start, end)
	}
	pool.workers = len(pool.runChans)
	return pool
}

func (p *cudaTphakalaCandidateBatchWorkerPool) Run() {
	for _, run := range p.runChans {
		run <- struct{}{}
	}
	for range p.runChans {
		<-p.done
	}
}

func (p *cudaTphakalaCandidateBatchWorkerPool) Close() {
	close(p.stop)
}

type cudaTphakalaDotBatchWorkerPool struct {
	runChans []chan struct{}
	stop     chan struct{}
	done     chan struct{}
	workers  int
}

func newCUDATphakalaDotBatchWorkerPool(queries []float32, candidates [][]float32, queryCount, candidateCount, dims int, dots []float32, workers int) *cudaTphakalaDotBatchWorkerPool {
	if workers > queryCount {
		workers = queryCount
	}
	if workers < 1 {
		workers = 1
	}
	pool := &cudaTphakalaDotBatchWorkerPool{
		runChans: make([]chan struct{}, 0, workers),
		stop:     make(chan struct{}),
		done:     make(chan struct{}, workers),
		workers:  workers,
	}
	rowsPerWorker := (queryCount + workers - 1) / workers
	for worker := 0; worker < workers; worker++ {
		startQuery := worker * rowsPerWorker
		endQuery := startQuery + rowsPerWorker
		if endQuery > queryCount {
			endQuery = queryCount
		}
		if startQuery >= endQuery {
			break
		}
		run := make(chan struct{})
		pool.runChans = append(pool.runChans, run)
		go func(run <-chan struct{}, startQuery, endQuery int) {
			for {
				select {
				case <-run:
					for q := startQuery; q < endQuery; q++ {
						query := queries[q*dims : (q+1)*dims]
						row := dots[q*candidateCount : (q+1)*candidateCount]
						simdf32.DotProductBatch(row, candidates, query)
					}
					pool.done <- struct{}{}
				case <-pool.stop:
					return
				}
			}
		}(run, startQuery, endQuery)
	}
	pool.workers = len(pool.runChans)
	return pool
}

func (p *cudaTphakalaDotBatchWorkerPool) Run() {
	for _, run := range p.runChans {
		run <- struct{}{}
	}
	for range p.runChans {
		<-p.done
	}
}

func (p *cudaTphakalaDotBatchWorkerPool) Close() {
	close(p.stop)
}

func finishCUDADotBenchmark(b *testing.B, dots []float32, queryCount, candidateCount, dims int) {
	b.StopTimer()
	reportCUDADotThroughputMetrics(b, b.Elapsed().Seconds(), b.N, queryCount*candidateCount, dims)
	sinkDistanceBuf = dots
}

func reportCUDADotQueryBatchMetrics(b *testing.B, queryCount, candidateCount, dims, resultCount int, resultMetric string, candidateValues, outputValues int) {
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

func reportCUDADotThroughputMetrics(b *testing.B, elapsedSeconds float64, iterations, dotsPerOp, dims int) {
	if elapsedSeconds <= 0 || iterations <= 0 || dotsPerOp <= 0 || dims <= 0 {
		return
	}
	dotsPerSecond := float64(iterations) * float64(dotsPerOp) / elapsedSeconds
	b.ReportMetric(dotsPerSecond, "dots/s")
	b.ReportMetric(dotsPerSecond*float64(dims*2)/1e9, "dot_GFLOP/s")
	b.ReportMetric(dotsPerSecond*float64(dims*4)/(1024*1024*1024), "logical_GiB/s")
}

func pureGoDotF32AccumCUDA(left, right []float32) float32 {
	var dot float32
	for i, v := range left {
		dot += v * right[i]
	}
	return dot
}
