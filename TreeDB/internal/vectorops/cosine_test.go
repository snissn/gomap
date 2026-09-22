package vectorops

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

func testCosineDistanceFloat32Normalized(t *testing.T, kernel func([]float32, []float32, float64, float64) float32) {
	random := rand.New(rand.NewPCG(4619, 1))
	for _, dims := range []int{0, 1, 2, 3, 7, 8, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 71, 127, 128, 129, 768} {
		for _, scale := range []float32{1, 1e20, 1e-20, math.SmallestNonzeroFloat32, math.MaxFloat32 / 2} {
			t.Run(fmt.Sprintf("dims=%d/scale=%g", dims, scale), func(t *testing.T) {
				left, right := make([]float32, dims), make([]float32, dims)
				for i := range left {
					left[i] = (random.Float32()*2 - 1) * scale
					right[i] = (random.Float32()*2 - 1) * scale
				}
				if dims > 0 {
					left[0], right[0] = scale, -scale
				}
				invNorm := func(vector []float32) float64 {
					var norm float64
					for _, v := range vector {
						norm += float64(v) * float64(v)
					}
					if norm == 0 {
						return 1
					}
					return 1 / math.Sqrt(norm)
				}
				for _, shape := range []string{"mixed", "close", "identical"} {
					if shape != "mixed" {
						copy(right, left)
						if shape == "close" && dims > 0 {
							right[dims-1] = math.Nextafter32(right[dims-1], float32(math.Inf(1)))
						}
					}
					li, ri := invNorm(left), invNorm(right)
					want := cosineDistanceFloat32NormalizedScalar(left, right, li, ri)
					got := kernel(left, right, li, ri)
					backward := kernel(right, left, ri, li)
					if shape == "identical" && (got != 0 || want != 0) {
						t.Fatalf("identical vector distance must be zero: got=%g scalar=%g", got, want)
					}
					if math.IsNaN(float64(got)) || math.IsInf(float64(got), 0) || got != backward || math.Abs(float64(got)-float64(want)) > float64(want)*1e-6 {
						t.Fatalf("%s got=%g reversed=%g scalar=%g", shape, got, backward, want)
					}
				}
			})
		}
	}
}

func TestCosineDistanceFloat32Normalized(t *testing.T) {
	testCosineDistanceFloat32Normalized(t, CosineDistanceFloat32Normalized)
	t.Run("dimension_mismatch", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("dimension mismatch did not panic")
			}
		}()
		CosineDistanceFloat32Normalized(make([]float32, 64), nil, 1, 1)
	})
}
