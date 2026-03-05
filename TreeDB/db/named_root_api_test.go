package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
)

func TestGetAtRoot_ZeroRootBehavesEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	got, err := d.GetAtRoot(0, []byte("missing"))
	if err != nil {
		t.Fatalf("GetAtRoot zero root: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil from zero root, got %q", got)
	}

	it, err := d.IteratorAtRoot(0, nil, nil)
	if err != nil {
		t.Fatalf("IteratorAtRoot zero root: %v", err)
	}
	defer it.Close()
	if it.Valid() {
		t.Fatalf("expected zero-root iterator to be empty")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("zero-root iterator error: %v", err)
	}
}

func TestMutateRoot_PublishesDedicatedRootAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	desc := collections.CollectionRootDescriptor{
		Name:       "col.users.primary",
		Collection: "users",
		Kind:       collections.CollectionRootKindPrimary,
		Format: collections.CollectionRootFormat{
			OuterLeavesInValueLog: true,
			LeafPrefixCompression: true,
			AllowValues:           true,
		},
	}
	rootKey, err := collections.SystemCollectionRootKey(desc.Name)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"ada@example.com"}`)
	rootID, err := d.MutateRoot(0, false, func(b batch.Interface) error {
		return b.Set(docID, doc)
	}, func(sys batch.Interface, newRootID uint64) error {
		desc.RootPageID = newRootID
		encoded, err := desc.Encode()
		if err != nil {
			return err
		}
		return sys.Set(rootKey, encoded)
	})
	if err != nil {
		t.Fatalf("MutateRoot: %v", err)
	}
	if rootID == 0 {
		t.Fatalf("expected non-zero dedicated root id")
	}

	got, err := d.GetAtRoot(rootID, docID)
	if err != nil {
		t.Fatalf("GetAtRoot before reopen: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("root value mismatch before reopen: got=%q want=%q", got, doc)
	}

	shared, err := d.Get(docID)
	if err != nil {
		t.Fatalf("Get shared root: %v", err)
	}
	if shared != nil {
		t.Fatalf("expected shared user root to remain untouched, got %q", shared)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	raw, err := reopen.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("GetSystem root descriptor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected persisted root descriptor")
	}
	var reopenDesc collections.CollectionRootDescriptor
	if err := reopenDesc.Decode(raw); err != nil {
		t.Fatalf("decode root descriptor: %v", err)
	}
	if reopenDesc.RootPageID == 0 {
		t.Fatalf("expected non-zero root descriptor page id after reopen")
	}

	reopenGot, err := reopen.GetAtRoot(reopenDesc.RootPageID, docID)
	if err != nil {
		t.Fatalf("GetAtRoot after reopen: %v", err)
	}
	if !bytes.Equal(reopenGot, doc) {
		t.Fatalf("root value mismatch after reopen: got=%q want=%q", reopenGot, doc)
	}
}

func TestMutateRoots_PublishesMultipleDedicatedRootsAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	primaryDesc := collections.CollectionRootDescriptor{
		Name:       "col.users.primary",
		Collection: "users",
		Kind:       collections.CollectionRootKindPrimary,
		Format: collections.CollectionRootFormat{
			OuterLeavesInValueLog: true,
			LeafPrefixCompression: true,
			AllowValues:           true,
		},
	}
	indexDesc := collections.CollectionRootDescriptor{
		Name:       "col.users.email_idx",
		Collection: "users",
		IndexName:  "email_idx",
		Kind:       collections.CollectionRootKindSecondaryIndex,
		Format: collections.CollectionRootFormat{
			LeafPrefixCompression: true,
		},
	}
	primaryRootKey, err := collections.SystemCollectionRootKey(primaryDesc.Name)
	if err != nil {
		t.Fatalf("primary root key: %v", err)
	}
	indexRootKey, err := collections.SystemCollectionRootKey(indexDesc.Name)
	if err != nil {
		t.Fatalf("index root key: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"ada@example.com"}`)
	indexKey := []byte("col:i:users:email_idx:\x00\x11s:ada@example.comu1")
	rootIDs, err := d.MutateRoots(false, []RootMutation{
		{RootID: 0, Mutate: func(b batch.Interface) error { return b.Set(docID, doc) }},
		{RootID: 0, Mutate: func(b batch.Interface) error { return b.Set(indexKey, nil) }},
	}, func(sys batch.Interface, newRootIDs []uint64) error {
		if len(newRootIDs) != 2 {
			t.Fatalf("expected 2 root ids, got %d", len(newRootIDs))
		}
		primaryDesc.RootPageID = newRootIDs[0]
		indexDesc.RootPageID = newRootIDs[1]
		primaryEncoded, err := primaryDesc.Encode()
		if err != nil {
			return err
		}
		indexEncoded, err := indexDesc.Encode()
		if err != nil {
			return err
		}
		if err := sys.Set(primaryRootKey, primaryEncoded); err != nil {
			return err
		}
		return sys.Set(indexRootKey, indexEncoded)
	})
	if err != nil {
		t.Fatalf("MutateRoots: %v", err)
	}
	if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("expected two non-zero root ids, got %#v", rootIDs)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopen, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	gotDoc, err := reopen.GetAtRoot(rootIDs[0], docID)
	if err != nil {
		t.Fatalf("GetAtRoot primary after reopen: %v", err)
	}
	if !bytes.Equal(gotDoc, doc) {
		t.Fatalf("primary doc mismatch after reopen: got=%q want=%q", gotDoc, doc)
	}
	gotIndex, err := reopen.GetAtRoot(rootIDs[1], indexKey)
	if err != nil {
		t.Fatalf("GetAtRoot secondary after reopen: %v", err)
	}
	if len(gotIndex) != 0 {
		t.Fatalf("expected empty secondary value payload, got %q", gotIndex)
	}
}
