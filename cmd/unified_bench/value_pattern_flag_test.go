package main

import (
	"flag"
	"testing"
)

func TestDatasetValPatternFlagRemoved(t *testing.T) {
	if f := flag.Lookup("dataset-val-pattern"); f != nil {
		t.Fatalf("expected -dataset-val-pattern to be removed, found flag with default %q", f.DefValue)
	}
}

func TestNormalizeWriteValuePattern_Strict(t *testing.T) {
	for _, in := range []string{
		"random",
		"zero",
		"repeat",
		"repeat_tail64",
		"half_repeat_half_random",
		"ultra_compressible_repeat",
		"highly_compressible_notail",
		"medium_compressible_sparse",
		"celestia_height_prefix_fill",
	} {
		if _, err := normalizeWriteValuePattern(in); err != nil {
			t.Fatalf("normalizeWriteValuePattern(%q): %v", in, err)
		}
	}

	for _, in := range []string{
		"zeros",
		"rand",
		"incompressible",
		"highly_compressible",
		"highly_compressible_tail64",
		"ultra_compressible",
		"medium_compressible",
	} {
		if _, err := normalizeWriteValuePattern(in); err == nil {
			t.Fatalf("normalizeWriteValuePattern(%q): expected error, got nil", in)
		}
	}
}
