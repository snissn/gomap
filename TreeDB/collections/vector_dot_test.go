package collections

import (
	"math"
	"testing"
)

func TestVectorDotProductFloat32SameLenEmptyV1(t *testing.T) {
	got := vectorDotProductFloat32SameLen(nil, nil)
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32SameLen(nil, nil)=%v want 0", got)
	}
	got = vectorDotProductFloat32SameLen([]float32{}, []float32{})
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32SameLen(empty, empty)=%v want 0", got)
	}
}

func TestVectorDotProductFloat32SameLenSingleElementV1(t *testing.T) {
	got := vectorDotProductFloat32SameLen([]float32{3}, []float32{4})
	if got != 12 {
		t.Fatalf("vectorDotProductFloat32SameLen([3],[4])=%v want 12", got)
	}
}

func TestVectorDotProductFloat32SameLenBasicV1(t *testing.T) {
	// [1,2,3] · [4,5,6] = 4 + 10 + 18 = 32
	left := []float32{1, 2, 3}
	right := []float32{4, 5, 6}
	got := vectorDotProductFloat32SameLen(left, right)
	if math.Abs(float64(got)-32) > 1e-4 {
		t.Fatalf("vectorDotProductFloat32SameLen([1,2,3],[4,5,6])=%v want 32", got)
	}
}

func TestVectorDotProductFloat32SameLenNegativeValuesV1(t *testing.T) {
	// [-1,-2] · [3,4] = -3 + -8 = -11
	left := []float32{-1, -2}
	right := []float32{3, 4}
	got := vectorDotProductFloat32SameLen(left, right)
	if math.Abs(float64(got)-(-11)) > 1e-4 {
		t.Fatalf("vectorDotProductFloat32SameLen([-1,-2],[3,4])=%v want -11", got)
	}
}

func TestVectorDotProductFloat32SameLenZeroVectorV1(t *testing.T) {
	left := []float32{0, 0, 0}
	right := []float32{1, 2, 3}
	got := vectorDotProductFloat32SameLen(left, right)
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32SameLen(zeros, [1,2,3])=%v want 0", got)
	}
}

func TestVectorDotProductFloat32SameLenUnitVectorV1(t *testing.T) {
	// [1,0,0] · [0,1,0] = 0 (orthogonal)
	left := []float32{1, 0, 0}
	right := []float32{0, 1, 0}
	got := vectorDotProductFloat32SameLen(left, right)
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32SameLen([1,0,0],[0,1,0])=%v want 0", got)
	}
}

func TestVectorDotProductFloat32SameLenIdentityV1(t *testing.T) {
	// [1,0] · [1,0] = 1 (same unit vector)
	left := []float32{1, 0}
	right := []float32{1, 0}
	got := vectorDotProductFloat32SameLen(left, right)
	if got != 1 {
		t.Fatalf("vectorDotProductFloat32SameLen([1,0],[1,0])=%v want 1", got)
	}
}

func TestVectorDotProductFloat32EmptyInputsV1(t *testing.T) {
	got := vectorDotProductFloat32(nil, nil)
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32(nil, nil)=%v want 0", got)
	}
	got = vectorDotProductFloat32([]float32{}, []float32{})
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32(empty, empty)=%v want 0", got)
	}
}

func TestVectorDotProductFloat32UnequalLengthsUsesMinV1(t *testing.T) {
	// When left is shorter, only left[:n] · right[:n] should be computed.
	// [1,2] · [3,4,999] should compute 1*3 + 2*4 = 11 (ignoring 999)
	left := []float32{1, 2}
	right := []float32{3, 4, 999}
	got := vectorDotProductFloat32(left, right)
	if math.Abs(float64(got)-11) > 1e-4 {
		t.Fatalf("vectorDotProductFloat32([1,2],[3,4,999])=%v want 11 (min-len=2)", got)
	}
}

func TestVectorDotProductFloat32UnequalLengthsRightShorterV1(t *testing.T) {
	// When right is shorter, only left[:n] · right[:n] should be computed.
	// [3,4,999] · [1,2] should compute 3*1 + 4*2 = 11 (ignoring 999)
	left := []float32{3, 4, 999}
	right := []float32{1, 2}
	got := vectorDotProductFloat32(left, right)
	if math.Abs(float64(got)-11) > 1e-4 {
		t.Fatalf("vectorDotProductFloat32([3,4,999],[1,2])=%v want 11 (min-len=2)", got)
	}
}

func TestVectorDotProductFloat32EmptyOneNilV1(t *testing.T) {
	// One nil and one non-empty: min-len is 0, result is 0.
	got := vectorDotProductFloat32(nil, []float32{1, 2, 3})
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32(nil, [1,2,3])=%v want 0", got)
	}
	got = vectorDotProductFloat32([]float32{1, 2, 3}, nil)
	if got != 0 {
		t.Fatalf("vectorDotProductFloat32([1,2,3], nil)=%v want 0", got)
	}
}

func TestVectorDotProductFloat32SameLenLargerVectorV1(t *testing.T) {
	// Test with a larger vector to exercise the SIMD path more thoroughly.
	n := 16
	left := make([]float32, n)
	right := make([]float32, n)
	var expected float32
	for i := 0; i < n; i++ {
		left[i] = float32(i + 1)
		right[i] = float32(i + 1)
		expected += float32((i + 1) * (i + 1))
	}
	got := vectorDotProductFloat32SameLen(left, right)
	if math.Abs(float64(got)-float64(expected)) > 1e-2 {
		t.Fatalf("vectorDotProductFloat32SameLen(1..16, 1..16)=%v want %v", got, expected)
	}
}