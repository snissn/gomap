package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_BRQ1BitV1Spec2480(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "brq-1bit-v1.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read brq spec: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"`brq_1bit` version `1`",
		"lower-level runtime prototype for #2481",
		"MUST NOT reinterpret or mutate `rabitq_1bit` v1",
		"License survey and implementation boundary",
		"Default seed | `0x6272713162697401`",
		"Query weight width | `QueryWeightBits=4` unsigned runtime-only bit planes",
		"Score label | `brq_1bit_estimated_cosine_q4`",
		"codec=brq_1bit",
		"query_weight_quantizer=max_abs_uint4_round_half_up",
		"Asset id | `quantized/<name>/brq_1bit/packed_codes`",
		"`packed_codes` | `packed_codes` | `packed_bit_vector` / `raw_packed_bit_vector`",
		"logical bit `i` is byte `i/8`, bit `i%8` (LSB0 within each byte)",
		"little-endian `uint64` word views map logical bit `i` to word `i/64`, bit `i%64`",
		"score = signed_weight_int * query_weight_scale /",
		"`quantized_only` returns this approximate score and must label it",
		"Failures return `ErrVectorIndexSearchUnavailable`",
		"canonical config bytes and `Config.Hash64` golden",
		"Recall, storage, and performance gates",
		"`quantized_score_codec_brq_1bit/search=1`",
		"#2507 added lower-level BRQ quantized asset/search runtime",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("brq spec missing %q", want)
		}
	}
}

func TestDocs_BRQ1BitV1LinkedOwners2480(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	checks := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"brq-1bit-v1.md",
			"`brq_1bit` v1",
			"query `uint4` bit-product score semantics",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-vector-index.md"): {
			"brq-1bit-v1.md",
			"`brq_1bit` v1 prototype",
			"explicit query `uint4` score label",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-asset-schema.md"): {
			"brq-1bit-v1.md",
			"`brq_1bit` v1 contract/prototype",
			"runtime query `uint4` bit-product score semantics",
		},
		filepath.Join(treeRoot, "docs", "spec", "rabitq-1bit-v1.md"): {
			"brq-1bit-v1.md",
			"new codec identity/version",
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
				t.Fatalf("%s missing brq spec text %q", path, want)
			}
		}
	}
}
