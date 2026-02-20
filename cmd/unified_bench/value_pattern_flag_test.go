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

func TestNormalizeWriteValuePattern_AcceptsDatasetLegacyPatternNames(t *testing.T) {
	// This test intentionally checks alias acceptance only. It does not assert
	// legacy byte-for-byte generation behavior; dataset writes now share the
	// unified makeValuePool semantics used by other write tests.
	for _, in := range []string{"random", "zero", "repeat", "repeat_tail64", "half_repeat_half_random"} {
		if _, err := normalizeWriteValuePattern(in); err != nil {
			t.Fatalf("normalizeWriteValuePattern(%q): %v", in, err)
		}
	}
}
