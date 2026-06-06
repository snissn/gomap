package vectorops

import (
	"fmt"
	"math"
	"testing"
)

func TestDotFloat32ParityWithScalar1790(t *testing.T) {
	cases := []struct {
		name  string
		left  []float32
		right []float32
	}{
		{name: "nil"},
		{name: "empty", left: []float32{}, right: []float32{}},
		{name: "left empty", left: []float32{}, right: []float32{1, 2, 3}},
		{name: "right empty", left: []float32{1, 2, 3}, right: []float32{}},
		{name: "left longer", left: []float32{1, -2, 3, 99}, right: []float32{4, 5, -6}},
		{name: "right longer", left: []float32{1, -2, 3}, right: []float32{4, 5, -6, 99}},
		{name: "negative values", left: []float32{-1, -2, 3, 4, -5}, right: []float32{6, -7, -8, 9, -10}},
	}
	for _, dims := range []int{1, 2, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129} {
		cases = append(cases, struct {
			name  string
			left  []float32
			right []float32
		}{name: fmt.Sprintf("dims_%d", dims), left: dotTestVector1790(dims, 3), right: dotTestVector1790(dims, 5)})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DotFloat32(tc.left, tc.right)
			want := DotFloat32Scalar(tc.left, tc.right)
			if got != want {
				t.Fatalf("DotFloat32=%v want scalar=%v implementation=%s", got, want, DotFloat32Implementation())
			}
		})
	}
}

func TestDotFloat32SpecialValues1790(t *testing.T) {
	cases := []struct {
		name    string
		left    []float32
		right   []float32
		wantNaN bool
		wantInf int
	}{
		{name: "nan", left: []float32{1, float32(math.NaN()), 3}, right: []float32{4, 5, 6}, wantNaN: true},
		{name: "positive infinity", left: []float32{1, float32(math.Inf(1))}, right: []float32{2, 3}, wantInf: 1},
		{name: "negative infinity", left: []float32{1, float32(math.Inf(-1))}, right: []float32{2, 3}, wantInf: -1},
		{name: "opposing infinities", left: []float32{float32(math.Inf(1)), float32(math.Inf(-1))}, right: []float32{1, 1}, wantNaN: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DotFloat32(tc.left, tc.right)
			want := DotFloat32Scalar(tc.left, tc.right)
			if tc.wantNaN {
				if !math.IsNaN(float64(got)) || !math.IsNaN(float64(want)) {
					t.Fatalf("got=%v want scalar NaN=%v implementation=%s", got, want, DotFloat32Implementation())
				}
				return
			}
			if tc.wantInf != 0 {
				if !math.IsInf(float64(got), tc.wantInf) || !math.IsInf(float64(want), tc.wantInf) {
					t.Fatalf("got=%v want scalar Inf(%d)=%v implementation=%s", got, tc.wantInf, want, DotFloat32Implementation())
				}
				return
			}
			if got != want {
				t.Fatalf("got=%v want scalar=%v implementation=%s", got, want, DotFloat32Implementation())
			}
		})
	}
}

func TestDotFloat32OptimizedAvailabilityMatchesImplementation(t *testing.T) {
	optimized := DotFloat32OptimizedAvailable()
	switch DotFloat32Implementation() {
	case "scalar":
		if optimized {
			t.Fatalf("DotFloat32OptimizedAvailable=true for scalar implementation")
		}
		if DotFloat32OptimizedEligible(1024) {
			t.Fatalf("DotFloat32OptimizedEligible=true for scalar implementation")
		}
	default:
		if !optimized {
			t.Fatalf("DotFloat32OptimizedAvailable=false for optimized implementation %q", DotFloat32Implementation())
		}
		if DotFloat32OptimizedEligible(0) || DotFloat32OptimizedEligible(1) {
			t.Fatalf("DotFloat32OptimizedEligible accepted tiny dot for implementation %q", DotFloat32Implementation())
		}
		if !DotFloat32OptimizedEligible(1024) {
			t.Fatalf("DotFloat32OptimizedEligible rejected large dot for implementation %q", DotFloat32Implementation())
		}
	}
}

func TestDotFloat32ZeroAllocs1790(t *testing.T) {
	left := dotTestVector1790(257, 3)
	right := dotTestVector1790(257, 5)
	var sink float32
	allocsOptimized := testing.AllocsPerRun(1000, func() {
		sink += DotFloat32(left, right)
	})
	if allocsOptimized != 0 {
		t.Fatalf("DotFloat32 allocs/run=%v want 0", allocsOptimized)
	}
	allocsScalar := testing.AllocsPerRun(1000, func() {
		sink += DotFloat32Scalar(left, right)
	})
	if allocsScalar != 0 {
		t.Fatalf("DotFloat32Scalar allocs/run=%v want 0", allocsScalar)
	}
	if sink == 0 {
		t.Fatalf("sink=0")
	}
}

func dotTestVector1790(n int, salt int) []float32 {
	values := make([]float32, n)
	for i := range values {
		values[i] = float32(((i + salt) % 11) - 5)
	}
	return values
}
