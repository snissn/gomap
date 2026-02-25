package treedb

import "testing"

func TestOpen_DefaultIndexOuterLeafModeUsesV1LeafLogRoute(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.index.outer_leaf_mode"]; got != IndexOuterLeafModeV1LeafLogRoute {
		t.Fatalf("expected default TreeDB open to use v1_leaflog_route, got %q", got)
	}
}
