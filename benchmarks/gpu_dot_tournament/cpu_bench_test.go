package gpudottournament

import (
	"fmt"
	"testing"

	simdf32 "github.com/tphakala/simd/f32"
)

const (
	benchDim = 768
)

var sinkDots []float32

func BenchmarkDotTournamentCPU(b *testing.B) {
	for _, rows := range []int{128, 1024, 8192, 65536} {
		b.Run(fmt.Sprintf("cpu_tphakala_loop_rows_%d", rows), func(b *testing.B) {
			query := deterministicVector(17, benchDim)
			matrix := deterministicMatrix(rows, benchDim)
			out := make([]float32, rows)
			b.ReportAllocs()
			b.ReportMetric(float64(rows), "dots/op")
			b.SetBytes(int64(rows * benchDim * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for row := 0; row < rows; row++ {
					base := row * benchDim
					out[row] = simdf32.DotProduct(query, matrix[base:base+benchDim])
				}
			}
			sinkDots = out
		})
	}
}

func deterministicVector(seed, n int) []float32 {
	out := make([]float32, n)
	x := uint32(seed)*747796405 + 2891336453
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = float32(int32(x>>9)%2000-1000) / 1000
	}
	return out
}

func deterministicMatrix(rows, dim int) []float32 {
	out := make([]float32, rows*dim)
	x := uint32(rows*dim)*747796405 + 2891336453
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = float32(int32(x>>9)%2000-1000) / 1000
	}
	return out
}
