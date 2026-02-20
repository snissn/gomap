package treedb

import "testing"

func TestOpen_DefaultIndexOuterLeafModeUsesV2FencePtr(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.v2_fenceptr.wal_fence_mode"]; got != "simple_inline" {
		t.Fatalf("expected default TreeDB open to use v2_fenceptr WAL-on defaults (simple_inline), got %q", got)
	}
}
