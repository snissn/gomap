//go:build amd64 && !purego

package vectorops

import (
	"math"
	"math/rand"
	"testing"

	simdf32 "github.com/tphakala/simd/f32"
)

func TestDotFloat32AMD64DependencyParity(t *testing.T) {
	rng := rand.New(rand.NewSource(4619))
	for n := 0; n <= 33; n++ {
		for sample := 0; sample < 1000; sample++ {
			left, right := make([]float32, n+1), make([]float32, n)
			for i := range right {
				left[i] = math.Float32frombits(rng.Uint32())
				right[i] = math.Float32frombits(rng.Uint32())
			}
			for _, swapped := range []bool{false, true} {
				if swapped {
					left, right = right, left
				}
				got := DotFloat32(left, right)
				want := simdf32.DotProduct(left[:n], right[:n])
				if math.Float32bits(got) != math.Float32bits(want) {
					t.Fatalf("n=%d sample=%d swapped=%v: got bits=%08x want=%08x", n, sample, swapped, math.Float32bits(got), math.Float32bits(want))
				}
			}
		}
	}
}
