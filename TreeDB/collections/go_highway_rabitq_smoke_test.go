package collections_test

import (
	"math"
	"math/bits"
	"testing"

	"github.com/ajroetker/go-highway/hwy/contrib/rabitq"
)

func TestGoHighwayRaBitQSmoke(t *testing.T) {
	const dims = 64
	const count = 2

	width := rabitq.CodeWidth(dims)
	if width != 1 {
		t.Fatalf("CodeWidth(%d) = %d, want 1", dims, width)
	}

	sqrtDimsInv := float32(1.0 / math.Sqrt(float64(dims)))
	unitVectors := make([]float32, count*dims)
	for dim := 0; dim < dims; dim++ {
		unitVectors[dim] = sqrtDimsInv
		unitVectors[dims+dim] = -sqrtDimsInv
	}

	codes := make([]uint64, count*width)
	dotProducts := make([]float32, count)
	codeCounts := make([]uint32, count)
	rabitq.QuantizeVectors(unitVectors, codes, dotProducts, codeCounts, sqrtDimsInv, count, dims, width)

	if codes[0] != ^uint64(0) {
		t.Fatalf("positive-vector code = %#016x, want all bits set", codes[0])
	}
	if codes[1] != 0 {
		t.Fatalf("negative-vector code = %#016x, want zero", codes[1])
	}
	if codeCounts[0] != dims || codeCounts[1] != 0 {
		t.Fatalf("codeCounts = %v, want [%d 0]", codeCounts, dims)
	}
	for i, got := range dotProducts {
		if math.Abs(float64(got-1)) > 1e-6 {
			t.Fatalf("dotProducts[%d] = %g, want 1", i, got)
		}
	}

	code := []uint64{0xF0F0_F0F0_F0F0_F0F0}
	q1 := []uint64{0xFFFF_0000_FFFF_0000}
	q2 := []uint64{0x0F0F_0F0F_0F0F_0F0F}
	q3 := []uint64{0x3333_3333_3333_3333}
	q4 := []uint64{0xAAAA_AAAA_AAAA_AAAA}
	got := rabitq.BitProduct(code, q1, q2, q3, q4)
	want := uint32(bits.OnesCount64(code[0]&q1[0]) +
		2*bits.OnesCount64(code[0]&q2[0]) +
		4*bits.OnesCount64(code[0]&q3[0]) +
		8*bits.OnesCount64(code[0]&q4[0]))
	if got != want {
		t.Fatalf("BitProduct() = %d, want %d", got, want)
	}
}
