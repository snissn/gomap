package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogRewriteOffline_PreservesNamedPrimaryRootFormatWithoutGlobalOuterLeafMode(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: false,
		LeafPrefixCompression:      true,
		IndexPackedValuePtr:        true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	manager := collections.NewCollectionManager(d)
	meta, err := manager.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	coll, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"ada@example.com"}`)
	if _, err := coll.Insert(docID, doc); err != nil {
		_ = d.Close()
		t.Fatalf("insert: %v", err)
	}

	rootDesc := loadNamedRootDescriptorForTest(t, d, meta.PrimaryRoot)
	if _, ok := page.DecodeLeafRef(rootDesc.RootPageID); !ok {
		_ = d.Close()
		t.Fatalf("expected named primary root %d to use leafref format", rootDesc.RootPageID)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if _, err := ValueLogRewriteOffline(Options{Dir: dir}); err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}

	reopen, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	reopenDesc := loadNamedRootDescriptorForTest(t, reopen, meta.PrimaryRoot)
	if _, ok := page.DecodeLeafRef(reopenDesc.RootPageID); !ok {
		t.Fatalf("expected rewritten named primary root %d to remain leafref", reopenDesc.RootPageID)
	}

	got, err := reopen.GetAtRoot(reopenDesc.RootPageID, docID)
	if err != nil {
		t.Fatalf("GetAtRoot after rewrite: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("rewritten doc mismatch: got=%q want=%q", got, doc)
	}
}
