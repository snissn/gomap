package treedb

import (
	"testing"
)

func TestNormalizePublicOuterLeafMode_V1LeafLogLegacyPreserved(t *testing.T) {
	if got := normalizePublicOuterLeafMode(IndexOuterLeafModeV1LeafLogLegacy); got != IndexOuterLeafModeV1LeafLogLegacy {
		t.Fatalf("normalizePublicOuterLeafMode=%q want %q", got, IndexOuterLeafModeV1LeafLogLegacy)
	}
}

func TestNormalizePublicOuterLeafMode_CanonicalizesKnownModeCasing(t *testing.T) {
	const in = "V2_FENCEPTR"
	if got := normalizePublicOuterLeafMode(in); got != IndexOuterLeafModeV2FencePtr {
		t.Fatalf("normalizePublicOuterLeafMode=%q want %q", got, IndexOuterLeafModeV2FencePtr)
	}
}

func TestOpen_V1LeafLogLegacy_StatsModePreserved(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: IndexOuterLeafModeV1LeafLogLegacy,
	})
	if err != nil {
		t.Fatalf("open %q: %v", IndexOuterLeafModeV1LeafLogLegacy, err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.index.outer_leaf_mode"]; got != IndexOuterLeafModeV1LeafLogLegacy {
		t.Fatalf("treedb.index.outer_leaf_mode=%q want %q", got, IndexOuterLeafModeV1LeafLogLegacy)
	}
}

func TestOpen_V2FencePtrMixedCase_AllowsSimpleInlineWALFenceMode(t *testing.T) {
	db, err := Open(Options{
		Dir:                t.TempDir(),
		IndexOuterLeafMode: "V2_FENCEPTR",
		ValueLog: ValueLogOptions{
			WALFenceMode: ValueLogWALFenceModeSimpleInline,
		},
	})
	if err != nil {
		t.Fatalf("open mixed-case v2_fenceptr simple_inline: %v", err)
	}
	defer db.Close()
}
