package treedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCachedCollectionsCheckpointFailure_NamedRootsRemainBuffered(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	primaryRootName, err := collections.CollectionPrimaryRootName(meta.Name)
	if err != nil {
		t.Fatalf("primary root name: %v", err)
	}
	stateRootName, err := collections.CollectionIndexStateRootName(meta.Name)
	if err != nil {
		t.Fatalf("state root name: %v", err)
	}
	indexRootName, err := collections.CollectionIndexRootName(meta.Name, "email_idx")
	if err != nil {
		t.Fatalf("index root name: %v", err)
	}
	primaryBefore := loadBackendRootDescriptor(t, d, primaryRootName)
	stateBefore := loadBackendRootDescriptor(t, d, stateRootName)
	indexBefore := loadBackendRootDescriptor(t, d, indexRootName)

	restore := setNamedRootPublishTestHook(func(stage string) error {
		if stage == "before_publish" {
			return errors.New("named root publish failed")
		}
		return nil
	})
	defer restore()

	if err := d.Checkpoint(); err == nil {
		t.Fatalf("expected checkpoint failure")
	}

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("cached get after failed checkpoint: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"email":"ada@example.com"}`)) {
		t.Fatalf("cached doc after failed checkpoint = %q", got)
	}
	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("cached find by index after failed checkpoint: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("cached ids after failed checkpoint = %#v", ids)
	}

	assertBackendDocMissingAtRoot(t, d, primaryBefore.RootPageID, []byte("u1"))
	assertBackendDocMissingAtRoot(t, d, stateBefore.RootPageID, []byte("u1"))
	if got := countBackendRootEntries(t, d, indexBefore.RootPageID); got != 0 {
		t.Fatalf("expected backend secondary root unchanged after failed checkpoint, got %d entries", got)
	}

	restore()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint retry: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close after successful retry: %v", err)
	}
	d = nil

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen cached: %v", err)
	}
	defer reopen.Close()

	reopenMgr := NewCollectionManager(reopen)
	reopenCol, err := reopenMgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	reopenDoc, err := reopenCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(reopenDoc, []byte(`{"email":"ada@example.com"}`)) {
		t.Fatalf("doc after reopen = %q", reopenDoc)
	}
	reopenIDs, err := reopenCol.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index after reopen: %v", err)
	}
	if len(reopenIDs) != 1 || !bytes.Equal(reopenIDs[0], []byte("u1")) {
		t.Fatalf("ids after reopen = %#v", reopenIDs)
	}
}

func TestCachedCollectionsCloseFailure_DoesNotDiscardBufferedNamedRoots(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "users"})
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	if _, err := mgr.CreateIndex(meta.Name, collections.IndexDefinition{Name: "email_idx", Field: "email", Unique: true}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint schema: %v", err)
	}

	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.Insert([]byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	restore := setNamedRootPublishTestHook(func(stage string) error {
		if stage == "before_publish" {
			return errors.New("named root close publish failed")
		}
		return nil
	})
	if err := d.Close(); err == nil {
		restore()
		t.Fatalf("expected close failure")
	}
	restore()

	got, err := col.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("cached get after failed close: %v", err)
	}
	if !bytes.Equal(got, []byte(`{"email":"ada@example.com"}`)) {
		t.Fatalf("cached doc after failed close = %q", got)
	}
	ids, err := col.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("cached find by index after failed close: %v", err)
	}
	if len(ids) != 1 || !bytes.Equal(ids[0], []byte("u1")) {
		t.Fatalf("cached ids after failed close = %#v", ids)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close retry: %v", err)
	}
	d = nil

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen cached: %v", err)
	}
	defer reopen.Close()

	reopenMgr := NewCollectionManager(reopen)
	reopenCol, err := reopenMgr.OpenCollection(meta.Name)
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	reopenDoc, err := reopenCol.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(reopenDoc, []byte(`{"email":"ada@example.com"}`)) {
		t.Fatalf("doc after reopen = %q", reopenDoc)
	}
	reopenIDs, err := reopenCol.FindByIndex("email_idx", "ada@example.com")
	if err != nil {
		t.Fatalf("find by index after reopen: %v", err)
	}
	if len(reopenIDs) != 1 || !bytes.Equal(reopenIDs[0], []byte("u1")) {
		t.Fatalf("ids after reopen = %#v", reopenIDs)
	}
}
