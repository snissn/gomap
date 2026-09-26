//go:build cuda

package gpudottournament

import (
	"fmt"
	"testing"
	"unsafe"
)

func BenchmarkDotTournamentCUDA(b *testing.B) {
	if err := requireCUDA(); err != nil {
		b.Skip(err)
	}
	for _, rows := range []int{128, 1024, 8192, 65536} {
		b.Run(fmt.Sprintf("cuda_cublas_sgemv_device_resident_rows_%d", rows), func(b *testing.B) {
			benchmarkCUDADot(b, rows, cudaCandidateSGEMVResident)
		})
		b.Run(fmt.Sprintf("cuda_cublas_sgemv_upload_query_rows_%d", rows), func(b *testing.B) {
			benchmarkCUDADot(b, rows, cudaCandidateSGEMVUploadQuery)
		})
		b.Run(fmt.Sprintf("cuda_cublas_sgemm_device_resident_rows_%d", rows), func(b *testing.B) {
			benchmarkCUDADot(b, rows, cudaCandidateSGEMMResident)
		})
	}
}

type cudaCandidate int

const (
	cudaCandidateSGEMVResident cudaCandidate = iota
	cudaCandidateSGEMVUploadQuery
	cudaCandidateSGEMMResident
)

func benchmarkCUDADot(b *testing.B, rows int, candidate cudaCandidate) {
	query := deterministicVector(17, benchDim)
	matrix := deterministicMatrix(rows, benchDim)
	out := make([]float32, rows)
	ctx := newCUDAContext(b, matrix, query, out)
	defer ctx.close(b)
	b.ReportAllocs()
	b.ReportMetric(float64(rows), "dots/op")
	b.SetBytes(int64(rows * benchDim * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch candidate {
		case cudaCandidateSGEMVResident:
			ctx.sgemv(b)
		case cudaCandidateSGEMVUploadQuery:
			ctx.uploadQuery(b, query)
			ctx.sgemv(b)
			ctx.downloadOut(b, out)
		case cudaCandidateSGEMMResident:
			ctx.sgemm(b)
		}
	}
	ctx.sync(b)
	sinkDots = out
}

type cudaContext struct {
	handle  cudaHandle
	dMatrix unsafe.Pointer
	dQuery  unsafe.Pointer
	dOut    unsafe.Pointer
	rows    int
}

func newCUDAContext(tb testing.TB, matrix, query, out []float32) *cudaContext {
	tb.Helper()
	ctx := &cudaContext{rows: len(out)}
	var err error
	ctx.handle, err = cudaCreate()
	if err != nil {
		tb.Fatal(err)
	}
	ctx.dMatrix = mustCUDAMalloc(tb, len(matrix)*4)
	ctx.dQuery = mustCUDAMalloc(tb, len(query)*4)
	ctx.dOut = mustCUDAMalloc(tb, len(out)*4)
	if err := cudaH2DRaw(ctx.dMatrix, matrix); err != nil {
		tb.Fatal(err)
	}
	if err := cudaH2DRaw(ctx.dQuery, query); err != nil {
		tb.Fatal(err)
	}
	return ctx
}

func (c *cudaContext) close(tb testing.TB) {
	tb.Helper()
	if c.dMatrix != nil {
		if err := cudaFreeRaw(c.dMatrix); err != nil {
			tb.Fatal(err)
		}
	}
	if c.dQuery != nil {
		if err := cudaFreeRaw(c.dQuery); err != nil {
			tb.Fatal(err)
		}
	}
	if c.dOut != nil {
		if err := cudaFreeRaw(c.dOut); err != nil {
			tb.Fatal(err)
		}
	}
	if c.handle != nil {
		if err := cudaDestroy(c.handle); err != nil {
			tb.Fatal(err)
		}
	}
}

func (c *cudaContext) sgemv(tb testing.TB) {
	tb.Helper()
	if err := cudaSgemvT(c.handle, benchDim, c.rows, c.dMatrix, c.dQuery, c.dOut); err != nil {
		tb.Fatal(err)
	}
}
func (c *cudaContext) sgemm(tb testing.TB) {
	tb.Helper()
	if err := cudaSgemmT(c.handle, benchDim, c.rows, c.dMatrix, c.dQuery, c.dOut); err != nil {
		tb.Fatal(err)
	}
}
func (c *cudaContext) uploadQuery(tb testing.TB, query []float32) {
	tb.Helper()
	if err := cudaH2DRaw(c.dQuery, query); err != nil {
		tb.Fatal(err)
	}
}
func (c *cudaContext) downloadOut(tb testing.TB, out []float32) {
	tb.Helper()
	if err := cudaD2HRaw(out, c.dOut); err != nil {
		tb.Fatal(err)
	}
}
func (c *cudaContext) sync(tb testing.TB) {
	tb.Helper()
	if err := cudaSyncRaw(); err != nil {
		tb.Fatal(err)
	}
}

func requireCUDA() error {
	n, err := cudaDeviceCount()
	if err != nil {
		return err
	}
	if n <= 0 {
		return fmt.Errorf("no CUDA devices")
	}
	return nil
}

func mustCUDAMalloc(tb testing.TB, bytes int) unsafe.Pointer {
	tb.Helper()
	p, err := cudaMallocRaw(bytes)
	if err != nil {
		tb.Fatal(err)
	}
	return p
}
