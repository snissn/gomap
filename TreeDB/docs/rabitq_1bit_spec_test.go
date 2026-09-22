package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_RaBitQ1BitV1Spec2449(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "rabitq-1bit-v1.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rabitq spec: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"`rabitq_1bit`",
		"Codec version | `1`",
		"`signed_permutation_fwht_padded_v1`",
		"`0x7261626974710001`",
		"`quantizedasset.RolePackedCodes` (`packed_codes`) backed by typed-column `packed_bit_vector` / `raw_packed_bit_vector` rows",
		"`CodeDimensions = next_power_of_two(VectorDimensions)`",
		"`CodeWidthBits = 1`",
		"Logical bit `i` is stored LSB-first at byte `i/8`, bit `i%8`",
		"`code_count`: number of set logical code bits (`uint32`)",
		"`quantized_dot_product_inv`: `1 / sum(abs(rotated_data[i]))` (`float32`)",
		"score = weighted_sign_dot / (quantized_dot_product_inv * CodeDimensions)",
		"`Plan.Encode` emits `EncodedVector{Code, CodeCount, QuantizedDotProductInv}`",
		"Durable TreeDB asset shape (#2450)",
		"Search/query modes (#2451/#2452)",
		"No go-highway accelerated RaBitQ backend landed",
		"Production TreeDB search evidence must cite the #2451 lower-level buffered rows",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("rabitq spec missing %q", want)
		}
	}
}

func TestDocs_RaBitQ1BitV1LinkedOwners2449(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	checks := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"rabitq-1bit-v1.md",
			"rabitq-closeout-2454.md",
			"`rabitq_1bit` v1 codec identity",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-vector-index.md"): {
			"rabitq-1bit-v1.md",
			"rabitq-closeout-2454.md",
			"`rabitq_1bit` v1 score plane",
			"SIMD/popcount integration",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-asset-schema.md"): {
			"rabitq-1bit-v1.md",
			"RolePackedCodes",
			"CodeWidthBits=1",
		},
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"): {
			"`rabitq_1bit` v1 score planes",
			"`quantized/<name>/packed_codes`",
			"`packed_bit_vector` over `raw_packed_bit_vector`",
		},
		filepath.Join(treeRoot, "docs", "spec", "rabitq-closeout-2454.md"): {
			"RaBitQ pure-Go",
			"go-highway acceleration",
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		},
	}
	for path, wants := range checks {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing rabitq spec text %q", path, want)
			}
		}
	}
}
