package embedding

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
)

// parityFixture pins reference-embedder output bit-for-bit. VectorBitsHex
// entries are math.Float32bits values in %08x hex, so any drift in the
// hashing pipeline — tokenizer, hash constants, accumulation, normalization —
// fails the parity test instead of silently re-baselining.
type parityFixture struct {
	Provider      string     `json:"provider"`
	Dimensions    int        `json:"dimensions"`
	Texts         []string   `json:"texts"`
	VectorBitsHex [][]string `json:"vector_bits_hex"`
}

const parityFixturePath = "testdata/hashing_parity.json"

func loadParityFixture(path string) (*parityFixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f parityFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &f, nil
}

func float32BitsHex(v float32) string {
	return fmt.Sprintf("%08x", math.Float32bits(v))
}

// TestRegenerateParityFixture rewrites the committed parity fixture from the
// current implementation. It is skipped unless explicitly requested:
//
//	EMBEDDING_REGENERATE_FIXTURE=1 go test ./TreeDB/collections/embedding/ -run TestRegenerateParityFixture
//
// Regeneration is a deliberate, reviewed act: any diff in the committed
// fixture is a determinism-contract change and must be justified in review.
func TestRegenerateParityFixture(t *testing.T) {
	if os.Getenv("EMBEDDING_REGENERATE_FIXTURE") == "" {
		t.Skip("set EMBEDDING_REGENERATE_FIXTURE=1 to rewrite testdata/hashing_parity.json")
	}
	texts := parityTexts()
	got, err := embedAll(stringSliceToByteSlices(texts))
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	fixture := parityFixture{
		Provider:   ProviderHashing,
		Dimensions: 16,
		Texts:      texts,
	}
	for _, vec := range got {
		hexes := make([]string, len(vec))
		for j, v := range vec {
			hexes[j] = float32BitsHex(v)
		}
		fixture.VectorBitsHex = append(fixture.VectorBitsHex, hexes)
	}
	raw, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(parityFixturePath, append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	t.Logf("rewrote %s", parityFixturePath)
}

func parityTexts() []string {
	return []string{
		"",
		"   ",
		"alpha beta gamma",
		"the quick brown fox jumps over the lazy dog",
		"repeat repeat repeat repeat",
		"unicode: café naïve 犬 ねこ",
		"TreeDB stores documents; embeddings stay deterministic.",
		"MixedCase tokens KEEP their case, punctuation stays attached.",
	}
}

func stringSliceToByteSlices(in []string) [][]byte {
	out := make([][]byte, len(in))
	for i, s := range in {
		out[i] = []byte(s)
	}
	return out
}
