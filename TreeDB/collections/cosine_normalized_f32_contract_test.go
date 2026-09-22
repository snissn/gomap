package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
)

func normalizeCosineNormalizedF32V1Reference(input []float32, dimensions int) ([]float32, error) {
	if dimensions <= 0 || len(input) != dimensions {
		return nil, fmt.Errorf("dimensions=%d want=%d", len(input), dimensions)
	}
	var squaredNorm float64
	for dimension, value := range input {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, fmt.Errorf("component %d is non-finite", dimension)
		}
		squaredNorm += float64(value) * float64(value)
	}
	if squaredNorm == 0 || math.IsNaN(squaredNorm) || math.IsInf(squaredNorm, 0) {
		return nil, fmt.Errorf("invalid norm")
	}

	canonical := make([]float32, len(input))
	invNorm := 1 / math.Sqrt(squaredNorm)
	for dimension, value := range input {
		canonical[dimension] = float32(float64(value) * invNorm)
		if math.IsNaN(float64(canonical[dimension])) || math.IsInf(float64(canonical[dimension]), 0) {
			return nil, fmt.Errorf("normalized component %d is non-finite", dimension)
		}
	}
	return canonical, nil
}

func cosineNormalizedF32V1ReferenceScore(left, right []float32) float32 {
	// This deterministic scalar reduction is the conformance oracle. Packed
	// implementations may reduce in a different order within the documented
	// score tolerance, but may not change result ordering.
	var dot float64
	for dimension := range left {
		dot += float64(left[dimension]) * float64(right[dimension])
	}
	score := float32(dot)
	if score > 1 {
		return 1
	}
	if score < -1 {
		return -1
	}
	return score
}

// Shared with the Python campaign oracle; these fixed bit patterns include
// magnitudes whose norm overflows or underflows a float32 accumulator.
func TestCosineNormalizedF32V1PythonGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/cosine_normalized_f32_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Input     []uint32 `json:"input_bits"`
		Canonical []uint32 `json:"canonical_bits"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatalf("golden rows=%d want 6", len(rows))
	}
	for _, row := range rows {
		input, want := make([]float32, len(row.Input)), make([]float32, len(row.Canonical))
		for i, bits := range row.Input {
			input[i] = math.Float32frombits(bits)
		}
		for i, bits := range row.Canonical {
			want[i] = math.Float32frombits(bits)
		}
		got, err := normalizeCosineNormalizedF32V1(input, len(input))
		if err != nil {
			t.Fatal(err)
		}
		requireFloat32BitsEqual(t, got, want)
	}
}

func TestCosineNormalizedF32V1ReferenceContract(t *testing.T) {
	t.Run("admission arithmetic and ownership", func(t *testing.T) {
		input := []float32{3, 4}
		canonical, err := normalizeCosineNormalizedF32V1Reference(input, 2)
		if err != nil {
			t.Fatal(err)
		}
		if input[0] != 3 || input[1] != 4 {
			t.Fatalf("admission mutated caller memory: %v", input)
		}
		input[0] = 30
		if canonical[0] == input[0] {
			t.Fatalf("canonical vector aliases caller memory: canonical=%v input=%v", canonical, input)
		}
		want := []float32{
			float32(float64(3) / math.Sqrt(25)),
			float32(float64(4) / math.Sqrt(25)),
		}
		for dimension := range want {
			if math.Float32bits(canonical[dimension]) != math.Float32bits(want[dimension]) {
				t.Fatalf("component %d bits=%08x want=%08x", dimension, math.Float32bits(canonical[dimension]), math.Float32bits(want[dimension]))
			}
		}

		scaled, err := normalizeCosineNormalizedF32V1Reference([]float32{6, 8}, 2)
		if err != nil {
			t.Fatal(err)
		}
		for dimension := range canonical {
			if math.Float32bits(scaled[dimension]) != math.Float32bits(canonical[dimension]) {
				t.Fatalf("original magnitude leaked at component %d: %v vs %v", dimension, scaled, canonical)
			}
		}
	})

	t.Run("invalid vectors", func(t *testing.T) {
		for name, input := range map[string][]float32{
			"empty":        {},
			"wrong_dims":   {1},
			"zero":         {0, 0},
			"nan":          {1, float32(math.NaN())},
			"positive_inf": {1, float32(math.Inf(1))},
			"negative_inf": {1, float32(math.Inf(-1))},
		} {
			t.Run(name, func(t *testing.T) {
				if _, err := normalizeCosineNormalizedF32V1Reference(input, 2); err == nil {
					t.Fatalf("accepted invalid vector %v", input)
				}
			})
		}
	})

	t.Run("clamp and deterministic ordering", func(t *testing.T) {
		if got := cosineNormalizedF32V1ReferenceScore([]float32{1.0001}, []float32{1}); got != 1 {
			t.Fatalf("positive clamp=%v want=1", got)
		}
		if got := cosineNormalizedF32V1ReferenceScore([]float32{-1.0001}, []float32{1}); got != -1 {
			t.Fatalf("negative clamp=%v want=-1", got)
		}

		type result struct {
			id    []byte
			score float32
		}
		low := float32(0.5)
		high := math.Nextafter32(low, 1)
		results := []result{
			{id: []byte("b"), score: low},
			{id: []byte("z"), score: high},
			{id: []byte("a"), score: low},
		}
		slices.SortFunc(results, func(left, right result) int {
			if left.score > right.score {
				return -1
			}
			if left.score < right.score {
				return 1
			}
			return bytes.Compare(left.id, right.id)
		})
		if got := string(results[0].id) + string(results[1].id) + string(results[2].id); got != "zab" {
			t.Fatalf("near-tie/exact-tie order=%q want=zab", got)
		}
	})
}
