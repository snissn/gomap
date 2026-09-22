package treedb

import (
	"path/filepath"
	"testing"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// Regression test: the dict side-store (dictdb/) is opened via the backend DB
// (no caching layer) and therefore must not inherit
// IndexOuterLeavesInValueLog=true from the main DB/profile. That mode requires
// a leaf-page log wired by the cached layer; without it, any write fails with:
// "zipper: outer leaves in value log enabled without leaf page log".
func TestDictDB_DoesNotInheritOuterLeavesInValueLog(t *testing.T) {
	dir := t.TempDir()

	opts := OptionsFor(ProfileFast, dir)
	EnableValueLogDictCompression(&opts)
	opts.ValueLog.Compression = ValueLogCompressionDict

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.dictdb == nil {
		t.Fatalf("expected dictdb to be open")
	}

	// This would fail (zipper error) if dictdb inherited IndexOuterLeavesInValueLog=true.
	if err := db.dictdb.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("dictdb SetSync: %v", err)
	}

	cfg, ok, err := treedbdb.LoadFormatConfig(db.dictdb.Dir())
	if err != nil {
		t.Fatalf("LoadFormatConfig(dictdb): %v", err)
	}
	if !ok {
		t.Fatalf("expected dictdb format.json to exist at %s", filepath.Join(db.dictdb.Dir(), "format.json"))
	}
	if cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("dictdb unexpectedly has IndexOuterLeavesInValueLog=true (should be forced off for side stores)")
	}
}
