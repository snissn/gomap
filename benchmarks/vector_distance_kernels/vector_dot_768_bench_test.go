package vectordistancekernels

import (
	"testing"

	axiomsimd "github.com/axiomhq/simd-go"
	dahvrisimd "github.com/ic-timon/da-hvri/simd"
	simdf32 "github.com/tphakala/simd/f32"
	"github.com/viterin/vek/vek32"
	"gonum.org/v1/gonum/blas/blas32"
)

func BenchmarkDotProduct768(b *testing.B) {
	const dims = 768
	query, _ := benchVector(17, dims)
	candidate, _ := benchVector(23, dims)
	b.Run("axiomhq", func(b *testing.B) {
		var s float32
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += axiomsimd.DotProductFloat32(query, candidate)
		}
		sinkDistance32 = s
	})
	b.Run("vek32", func(b *testing.B) {
		var s float32
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += vek32.Dot(query, candidate)
		}
		sinkDistance32 = s
	})
	b.Run("tphakala", func(b *testing.B) {
		var s float32
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += simdf32.DotProduct(query, candidate)
		}
		sinkDistance32 = s
	})
	b.Run("tphakala_unsafe", func(b *testing.B) {
		var s float32
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += simdf32.DotProductUnsafe(query, candidate)
		}
		sinkDistance32 = s
	})
	b.Run("da_hvri", func(b *testing.B) {
		var s float64
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += dahvrisimd.DotProduct(query, candidate)
		}
		sinkDistance64 = s
	})
	b.Run("gonum_blas32", func(b *testing.B) {
		var s float32
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s += blas32.Dot(blas32.Vector{N: dims, Inc: 1, Data: query}, blas32.Vector{N: dims, Inc: 1, Data: candidate})
		}
		sinkDistance32 = s
	})
}

func BenchmarkDotProductBatch768x128(b *testing.B) {
	const dims = 768
	const candidatesN = 128
	query, _ := benchVector(17, dims)
	flat, _ := benchCandidateMatrix(candidatesN, dims)
	rows := candidateRows(flat, candidatesN, dims)
	dots := make([]float32, candidatesN)
	b.Run("tphakala_batch", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			simdf32.DotProductBatch(dots, rows, query)
		}
		sinkDistanceBuf = dots
	})
	b.Run("tphakala_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < candidatesN; j++ {
				dots[j] = simdf32.DotProduct(query, rows[j])
			}
		}
		sinkDistanceBuf = dots
	})
	b.Run("vek32_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < candidatesN; j++ {
				dots[j] = vek32.Dot(query, rows[j])
			}
		}
		sinkDistanceBuf = dots
	})
	b.Run("axiomhq_loop", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < candidatesN; j++ {
				dots[j] = axiomsimd.DotProductFloat32(query, rows[j])
			}
		}
		sinkDistanceBuf = dots
	})
}
