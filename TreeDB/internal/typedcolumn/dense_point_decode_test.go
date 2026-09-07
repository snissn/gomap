package typedcolumn

import (
	"fmt"
	"testing"
)

func TestDecodeRawFloat32VectorPointAllocation(t *testing.T) {
	raw := make([]byte, 8*4)
	dst := make([]float32, 8)
	for _, owned := range []bool{false, true} {
		allocs := testing.AllocsPerRun(100, func() {
			buffer := dst[:0]
			if owned {
				buffer = nil
			}
			out, err := DecodeRawFloat32VectorPayload(buffer, raw, 1, 8)
			if err != nil || len(out) != 8 {
				panic("point decode failed")
			}
		})
		want := float64(0)
		if owned {
			want = 1
		}
		if allocs != want {
			t.Fatalf("owned=%v allocations=%g want=%g", owned, allocs, want)
		}
	}
}

func TestDensePayloadValidationPreservesOverflowText(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		rows, dims, width int
		want              string
	}{
		{2, maxInt, 4, fmt.Sprintf("float32_vector elements overflow 2*%d", maxInt)},
		{1, maxInt, 4, fmt.Sprintf("float32_vector raw bytes overflow %d*4", maxInt)},
		{1, 8, 4, "typedcolumn: float32_vector raw bytes=0 want=32"},
		{0, 8, 4, "typedcolumn: invalid float32_vector rows 0"},
	} {
		err := validateDensePayloadBytes(0, tc.rows, tc.dims, tc.width, "float32_vector")
		if err == nil || err.Error() != tc.want {
			t.Fatalf("rows=%d dims=%d: err=%v want=%q", tc.rows, tc.dims, err, tc.want)
		}
	}
}
