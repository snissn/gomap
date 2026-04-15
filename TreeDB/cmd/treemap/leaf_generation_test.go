package main

import (
	"strings"
	"testing"
)

func TestParseUint64CSV(t *testing.T) {
	got, err := parseUint64CSV("3, 1, 3, 2, 0")
	if err != nil {
		t.Fatalf("parseUint64CSV: %v", err)
	}
	want := []uint64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d]=%d, want %d (%v)", i, got[i], want[i], got)
		}
	}
}

func TestFormatUint32List(t *testing.T) {
	if got, want := formatUint32List(nil), "-"; got != want {
		t.Fatalf("formatUint32List(nil)=%q, want %q", got, want)
	}
	if got, want := formatUint32List([]uint32{7, 11, 42}), "7,11,42"; got != want {
		t.Fatalf("formatUint32List=%q, want %q", got, want)
	}
}

func TestUsageTextMentionsLeafGenerationGC(t *testing.T) {
	if got := usageText; !strings.Contains(got, "leafgen-gc") {
		t.Fatalf("usageText missing leafgen-gc command: %q", got)
	}
}
