package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCollectionNamedRootCatalogSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mgr := collections.NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	reopenMgr := collections.NewCollectionManager(reopen)
	col, err := reopenMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	reopenMeta := col.Meta()
	if reopenMeta.PrimaryRoot == "" {
		t.Fatalf("expected primary root after reopen")
	}
	if len(reopenMeta.Indexes) != 1 || reopenMeta.Indexes[0].RootName == "" {
		t.Fatalf("expected secondary root after reopen, got %+v", reopenMeta.Indexes)
	}

	rootKey, err := collections.SystemCollectionRootKey(reopenMeta.PrimaryRoot)
	if err != nil {
		t.Fatalf("primary root key: %v", err)
	}
	raw, err := reopen.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("get primary root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected primary root descriptor after reopen")
	}
}
