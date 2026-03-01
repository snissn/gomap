package treedb

import (
	"testing"
)

func TestNormalizePublicOuterLeafMode_V1Preserved(t *testing.T) {
	if got := normalizePublicOuterLeafMode(IndexOuterLeafModeV1); got != IndexOuterLeafModeV1 {
		t.Fatalf("normalizePublicOuterLeafMode=%q want %q", got, IndexOuterLeafModeV1)
	}
}

func TestNormalizePublicOuterLeafMode_CanonicalizesKnownModeCasing(t *testing.T) {
	const in = "V1"
	if got := normalizePublicOuterLeafMode(in); got != IndexOuterLeafModeV1 {
		t.Fatalf("normalizePublicOuterLeafMode=%q want %q", got, IndexOuterLeafModeV1)
	}
}

func TestNormalizePublicOuterLeafMode_LeavesLegacyModeRaw(t *testing.T) {
	if got := normalizePublicOuterLeafMode(" v1_leaflog "); got != "v1_leaflog" {
		t.Fatalf("normalizePublicOuterLeafMode(%q)=%q want %q", " v1_leaflog ", got, "v1_leaflog")
	}
}

func TestOpen_V1Mode_StatsModePreserved(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1,
	})
	if err != nil {
		t.Fatalf("open %q: %v", IndexOuterLeafModeV1, err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.index.outer_leaf_mode"]; got != IndexOuterLeafModeV1 {
		t.Fatalf("treedb.index.outer_leaf_mode=%q want %q", got, IndexOuterLeafModeV1)
	}
}

func TestOpen_V1MixedCase_StatsModeCanonicalized(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: " v1 ",
	})
	if err != nil {
		t.Fatalf("open mixed-case v1: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.index.outer_leaf_mode"]; got != IndexOuterLeafModeV1 {
		t.Fatalf("treedb.index.outer_leaf_mode=%q want %q", got, IndexOuterLeafModeV1)
	}
}

func TestOpen_RejectsLegacyMode(t *testing.T) {
	_, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: "v1_leaflog_route",
	})
	if err == nil {
		t.Fatalf("expected unsupported outer-leaf mode error")
	}
}
