package typedcolumn

import (
	"fmt"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedColumnDenseFloat32DotDirectViewParity1790(t *testing.T) {
	for _, dims := range []int{1, 2, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129} {
		t.Run(fmt.Sprintf("dims_%d", dims), func(t *testing.T) {
			const rows = 8
			view, release := mustDenseDotFloat32View1790(t, rows, dims)
			defer release()
			for i := 0; i < rows; i++ {
				left := view[i*dims : (i+1)*dims]
				for j := 0; j < rows; j++ {
					right := view[j*dims : (j+1)*dims]
					got := DenseFloat32Dot(left, right)
					want := DenseFloat32DotScalar(left, right)
					if !dotFloat32Close1790(got, want) {
						t.Fatalf("row %d dot row %d = %v want scalar %v implementation=%s", i, j, got, want, DenseFloat32DotImplementation())
					}
				}
			}
		})
	}
}

func TestTypedColumnDenseFloat32DotZeroAllocs1790(t *testing.T) {
	const rows = 1024
	const dims = 31
	view, release := mustDenseDotFloat32View1790(t, rows, dims)
	defer release()
	query := view[0:dims]
	document := view[(rows-1)*dims : rows*dims]
	var sink float32
	allocsOptimized := testing.AllocsPerRun(1000, func() {
		sink += DenseFloat32Dot(query, document)
	})
	if allocsOptimized != 0 {
		t.Fatalf("DenseFloat32Dot allocs/run=%v want 0", allocsOptimized)
	}
	allocsScalar := testing.AllocsPerRun(1000, func() {
		sink += DenseFloat32DotScalar(query, document)
	})
	if allocsScalar != 0 {
		t.Fatalf("DenseFloat32DotScalar allocs/run=%v want 0", allocsScalar)
	}
	if sink == 0 {
		t.Fatalf("sink=0")
	}
}

func BenchmarkTypedColumnDenseFloat32Dot1790(b *testing.B) {
	const rows = 4096
	const dims = 128
	view, release := mustDenseDotFloat32View1790(b, rows, dims)
	defer release()
	query := view[0:dims]
	run := func(b *testing.B, implementation string, dot func([]float32, []float32) float32) {
		// Setup above parses the typed-column image, acquires the dense column-data
		// section, and validates the direct []float32 view. ResetTimer below keeps
		// the benchmark focused on the dot-product hot loop only.
		b.ReportAllocs()
		b.ResetTimer()
		var sink float32
		for i := 0; i < b.N; i++ {
			for row := 0; row < rows; row++ {
				document := view[row*dims : (row+1)*dims]
				sink += dot(query, document)
			}
		}
		b.StopTimer()
		if sink == 0 {
			b.Fatalf("sink=0")
		}
		elapsed := b.Elapsed().Seconds()
		b.ReportMetric(float64(b.N)/elapsed, "ops/s")
		b.ReportMetric(float64(b.N*rows)/elapsed, "rows/s")
		b.ReportMetric(float64(b.N*rows*dims)/elapsed, "elements/s")
		_ = implementation // implementation is encoded in the sub-benchmark name.
	}
	b.Run("optimized_"+DenseFloat32DotImplementation(), func(b *testing.B) {
		run(b, DenseFloat32DotImplementation(), DenseFloat32Dot)
	})
	b.Run("scalar_portable", func(b *testing.B) {
		run(b, "scalar", DenseFloat32DotScalar)
	})
}

func dotFloat32Close1790(got, want float32) bool {
	if math.IsNaN(float64(got)) || math.IsNaN(float64(want)) {
		return math.IsNaN(float64(got)) && math.IsNaN(float64(want))
	}
	if math.IsInf(float64(got), 0) || math.IsInf(float64(want), 0) {
		return got == want
	}
	delta := math.Abs(float64(got - want))
	scale := math.Max(1, math.Abs(float64(want)))
	return delta <= 1e-5*scale
}

func mustDenseDotFloat32View1790(t testing.TB, rows int, dims int) ([]float32, func()) {
	t.Helper()
	part := mustDenseVectorPart1756(t, rows, dims)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	section := mustColumnDataSection1756(t, parsed, "embedding")
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(t, mgr, parsed, section, parsed.SectionBytesMust1756(t, section))
	// Float32View is the validation boundary for mapped dense bytes: it verifies
	// length, alignment, and little-endian eligibility before the dot kernels see
	// typed slices.
	view, err := mgr.Float32View(h)
	if err != nil {
		h.Release()
		t.Fatalf("Float32View setup: %v", err)
	}
	if len(view) != rows*dims {
		h.Release()
		t.Fatalf("view elements=%d want %d", len(view), rows*dims)
	}
	return view, func() {
		if err := h.Release(); err != nil {
			t.Fatalf("release dense dot handle: %v", err)
		}
	}
}
