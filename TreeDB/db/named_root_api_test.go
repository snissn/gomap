package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/rootfmt"
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
	appended, err := d.GetAtRootAppend(0, []byte("missing"), []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAtRootAppend zero root: %v", err)
	}
	if string(appended) != "prefix:" {
		t.Fatalf("expected GetAtRootAppend zero root to preserve dst, got %q", appended)
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

func TestHasAtRoot_ZeroRootBehavesEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	has, err := d.HasAtRoot(0, []byte("missing"))
	if err != nil {
		t.Fatalf("HasAtRoot zero root: %v", err)
	}
	if has {
		t.Fatalf("expected HasAtRoot zero root to be false")
	}
}

func TestHasPrefixAtRoot_ZeroRootBehavesEmpty(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	has, err := d.HasPrefixAtRoot(0, []byte("missing"))
	if err != nil {
		t.Fatalf("HasPrefixAtRoot zero root: %v", err)
	}
	if has {
		t.Fatalf("expected HasPrefixAtRoot zero root to be false")
	}
}

func TestHasPrefixAtRoot_FindsMatchingEntries(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	rootID, err := d.MutateRoot(0, false, func(b batch.Interface) error {
		if err := b.Set([]byte("email:a@example.com:u1"), nil); err != nil {
			return err
		}
		return b.Set([]byte("city:hnl:u1"), nil)
	}, nil)
	if err != nil {
		t.Fatalf("MutateRoot: %v", err)
	}

	has, err := d.HasPrefixAtRoot(rootID, []byte("email:a@example.com:"))
	if err != nil {
		t.Fatalf("HasPrefixAtRoot match: %v", err)
	}
	if !has {
		t.Fatalf("expected matching prefix to be present")
	}
	has, err = d.HasPrefixAtRoot(rootID, []byte("email:missing:"))
	if err != nil {
		t.Fatalf("HasPrefixAtRoot missing: %v", err)
	}
	if has {
		t.Fatalf("expected missing prefix to be absent")
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
	appended, err := d.GetAtRootAppend(rootID, docID, []byte("prefix:"))
	if err != nil {
		t.Fatalf("GetAtRootAppend before reopen: %v", err)
	}
	if !bytes.Equal(appended, append([]byte("prefix:"), doc...)) {
		t.Fatalf("root value mismatch from append before reopen: got=%q want=%q", appended, append([]byte("prefix:"), doc...))
	}
	has, err := d.HasAtRoot(rootID, docID)
	if err != nil {
		t.Fatalf("HasAtRoot before reopen: %v", err)
	}
	if !has {
		t.Fatalf("expected HasAtRoot before reopen to report existing document")
	}
	has, err = d.HasAtRoot(rootID, []byte("missing"))
	if err != nil {
		t.Fatalf("HasAtRoot missing before reopen: %v", err)
	}
	if has {
		t.Fatalf("expected HasAtRoot before reopen to report missing document absent")
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

func TestMutateRootsWithFormatOps_MatchesCallbackForm(t *testing.T) {
	dir := t.TempDir()

	run := func(t *testing.T, useOps bool) {
		t.Helper()
		subdir := filepath.Join(dir, t.Name())
		if err := os.MkdirAll(subdir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", subdir, err)
		}
		d, err := Open(Options{Dir: subdir})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer d.Close()

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

		var (
			rootIDs []uint64
			errRun  error
		)
		encodeDesc := func(desc collections.CollectionRootDescriptor, rootID uint64) []byte {
			desc.RootPageID = rootID
			encoded, err := desc.Encode()
			if err != nil {
				t.Fatalf("encode descriptor %s: %v", desc.Name, err)
			}
			return encoded
		}

		if useOps {
			rootIDs, errRun = d.MutateRootsWithFormatOps(false,
				[]uint64{0, 0},
				[]*rootfmt.Format{
					{
						OuterLeavesInValueLog: true,
						LeafPrefixCompression: true,
						AllowValues:           true,
					},
					{
						LeafPrefixCompression: true,
					},
				},
				[][]batch.Entry{
					{{Type: batch.OpPut, Key: docID, Value: doc}},
					{{Type: batch.OpPut, Key: indexKey, Value: []byte{}}},
				},
				func(newRootIDs []uint64) ([]batch.Entry, error) {
					return []batch.Entry{
						{Type: batch.OpPut, Key: primaryRootKey, Value: encodeDesc(primaryDesc, newRootIDs[0])},
						{Type: batch.OpPut, Key: indexRootKey, Value: encodeDesc(indexDesc, newRootIDs[1])},
					}, nil
				},
			)
		} else {
			rootIDs, errRun = d.MutateRootsWithFormats(false,
				[]uint64{0, 0},
				[]*rootfmt.Format{
					{
						OuterLeavesInValueLog: true,
						LeafPrefixCompression: true,
						AllowValues:           true,
					},
					{
						LeafPrefixCompression: true,
					},
				},
				[]func(batch.Interface) error{
					func(b batch.Interface) error { return b.Set(docID, doc) },
					func(b batch.Interface) error { return b.Set(indexKey, []byte{}) },
				},
				func(sys batch.Interface, newRootIDs []uint64) error {
					if len(newRootIDs) != 2 {
						t.Fatalf("expected 2 root ids, got %d", len(newRootIDs))
					}
					if err := sys.Set(primaryRootKey, encodeDesc(primaryDesc, newRootIDs[0])); err != nil {
						return err
					}
					return sys.Set(indexRootKey, encodeDesc(indexDesc, newRootIDs[1]))
				},
			)
		}
		if errRun != nil {
			t.Fatalf("mutate roots: %v", errRun)
		}
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			t.Fatalf("unexpected root ids: %#v", rootIDs)
		}

		gotDoc, err := d.GetAtRoot(rootIDs[0], docID)
		if err != nil {
			t.Fatalf("get doc: %v", err)
		}
		if !bytes.Equal(gotDoc, doc) {
			t.Fatalf("doc mismatch: got=%q want=%q", gotDoc, doc)
		}
		hasIndex, err := d.HasAtRoot(rootIDs[1], indexKey)
		if err != nil {
			t.Fatalf("has index key: %v", err)
		}
		if !hasIndex {
			t.Fatalf("expected index key")
		}
	}

	t.Run("callbacks", func(t *testing.T) { run(t, false) })
	t.Run("ops", func(t *testing.T) { run(t, true) })
}

func TestMutateRootsWithFormatIterators_MatchesCallbackForm(t *testing.T) {
	dir := t.TempDir()

	run := func(t *testing.T, useIters bool) {
		t.Helper()
		subdir := filepath.Join(dir, t.Name())
		if err := os.MkdirAll(subdir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", subdir, err)
		}
		d, err := Open(Options{Dir: subdir})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer d.Close()

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

		var (
			rootIDs []uint64
			errRun  error
		)
		encodeDesc := func(desc collections.CollectionRootDescriptor, rootID uint64) []byte {
			desc.RootPageID = rootID
			encoded, err := desc.Encode()
			if err != nil {
				t.Fatalf("encode descriptor %s: %v", desc.Name, err)
			}
			return encoded
		}

		if useIters {
			primaryMem, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
			if err != nil {
				t.Fatalf("primary memtable: %v", err)
			}
			indexMem, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
			if err != nil {
				t.Fatalf("index memtable: %v", err)
			}
			primaryMem.SetSteal(append([]byte(nil), docID...), append([]byte(nil), doc...))
			indexMem.SetSteal(append([]byte(nil), indexKey...), nil)

			rootIDs, errRun = d.MutateRootsWithFormatIterators(false,
				[]uint64{0, 0},
				[]*rootfmt.Format{
					{
						OuterLeavesInValueLog: true,
						LeafPrefixCompression: true,
						AllowValues:           true,
					},
					{
						LeafPrefixCompression: true,
					},
				},
				[]iterator.UnsafeIterator{
					primaryMem.NewIterator(nil, nil),
					indexMem.NewIterator(nil, nil),
				},
				func(newRootIDs []uint64) ([]batch.Entry, error) {
					return []batch.Entry{
						{Type: batch.OpPut, Key: primaryRootKey, Value: encodeDesc(primaryDesc, newRootIDs[0])},
						{Type: batch.OpPut, Key: indexRootKey, Value: encodeDesc(indexDesc, newRootIDs[1])},
					}, nil
				},
			)
		} else {
			rootIDs, errRun = d.MutateRootsWithFormats(false,
				[]uint64{0, 0},
				[]*rootfmt.Format{
					{
						OuterLeavesInValueLog: true,
						LeafPrefixCompression: true,
						AllowValues:           true,
					},
					{
						LeafPrefixCompression: true,
					},
				},
				[]func(batch.Interface) error{
					func(b batch.Interface) error { return b.Set(docID, doc) },
					func(b batch.Interface) error { return b.Set(indexKey, []byte{}) },
				},
				func(sys batch.Interface, newRootIDs []uint64) error {
					if len(newRootIDs) != 2 {
						t.Fatalf("expected 2 root ids, got %d", len(newRootIDs))
					}
					if err := sys.Set(primaryRootKey, encodeDesc(primaryDesc, newRootIDs[0])); err != nil {
						return err
					}
					return sys.Set(indexRootKey, encodeDesc(indexDesc, newRootIDs[1]))
				},
			)
		}
		if errRun != nil {
			t.Fatalf("mutate roots: %v", errRun)
		}
		if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
			t.Fatalf("unexpected root ids: %#v", rootIDs)
		}

		gotDoc, err := d.GetAtRoot(rootIDs[0], docID)
		if err != nil {
			t.Fatalf("get doc: %v", err)
		}
		if !bytes.Equal(gotDoc, doc) {
			t.Fatalf("doc mismatch: got=%q want=%q", gotDoc, doc)
		}
		hasIndex, err := d.HasAtRoot(rootIDs[1], indexKey)
		if err != nil {
			t.Fatalf("has index key: %v", err)
		}
		if !hasIndex {
			t.Fatalf("expected index key")
		}
	}

	t.Run("callbacks", func(t *testing.T) { run(t, false) })
	t.Run("iterators", func(t *testing.T) { run(t, true) })
}

func TestMutateRootsWithFormatIterators_EmptyRootsUseBulkBuilder(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	primaryMem, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
	if err != nil {
		t.Fatalf("primary memtable: %v", err)
	}
	indexMem, err := memtable.NewWithCapacityMode(0, memtable.ModeBTree)
	if err != nil {
		t.Fatalf("index memtable: %v", err)
	}

	docID := []byte("u1")
	doc := []byte(`{"email":"ada@example.com"}`)
	indexKey := []byte("col:i:users:email_idx:\x00\x11s:ada@example.comu1")
	primaryMem.SetSteal(append([]byte(nil), docID...), append([]byte(nil), doc...))
	indexMem.SetSteal(append([]byte(nil), indexKey...), nil)

	builds := 0
	restore := setRootIteratorBulkBuildTestHook(func(rootIndex int) {
		builds++
	})
	defer restore()

	rootIDs, err := d.MutateRootsWithFormatIterators(false,
		[]uint64{0, 0},
		[]*rootfmt.Format{
			{
				OuterLeavesInValueLog: true,
				LeafPrefixCompression: true,
				AllowValues:           true,
			},
			{
				LeafPrefixCompression: true,
			},
		},
		[]iterator.UnsafeIterator{
			primaryMem.NewIterator(nil, nil),
			indexMem.NewIterator(nil, nil),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("MutateRootsWithFormatIterators: %v", err)
	}
	if len(rootIDs) != 2 || rootIDs[0] == 0 || rootIDs[1] == 0 {
		t.Fatalf("unexpected root ids: %#v", rootIDs)
	}
	if builds != 2 {
		t.Fatalf("bulk builds=%d want 2", builds)
	}
}
