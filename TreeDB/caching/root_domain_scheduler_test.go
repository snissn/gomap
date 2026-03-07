package caching

import (
	"bytes"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPendingRootDomainUnits_ReportSystemAndNamed(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	if err := cache.ApplySystemOverlayEntriesOwned([]batch.Entry{{
		Type:  batch.OpPut,
		Key:   []byte("sys:users"),
		Value: []byte("v1"),
	}}); err != nil {
		t.Fatalf("apply system entries: %v", err)
	}

	if err := bufferNamedRootDocument(cache, "users", []byte("u1"), []byte(`{"email":"ada@example.com"}`)); err != nil {
		t.Fatalf("buffer named root: %v", err)
	}

	units := cache.pendingRootDomainUnits()
	if len(units) != 2 {
		t.Fatalf("pending root domain units=%d want 2", len(units))
	}

	kinds := make([]rootDomainFlushKind, 0, len(units))
	for _, unit := range units {
		kinds = append(kinds, unit.kind)
	}
	if !slices.Contains(kinds, rootDomainFlushKindNamedRoots) {
		t.Fatalf("pending root domain units=%v want named roots", kinds)
	}
	if !slices.Contains(kinds, rootDomainFlushKindSystem) {
		t.Fatalf("pending root domain units=%v want system root", kinds)
	}
}

func TestFlushPendingRootDomainUnitsLocked_PublishesSystemAndNamed(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cache, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	if err := cache.ApplySystemOverlayEntriesOwned([]batch.Entry{{
		Type:  batch.OpPut,
		Key:   []byte("sys:users"),
		Value: []byte("v1"),
	}}); err != nil {
		t.Fatalf("apply system entries: %v", err)
	}

	rootName, virtualRootID, docKey, docValue, err := bufferNamedRootDocumentForTest(cache, "users", []byte("u1"), []byte(`{"email":"ada@example.com"}`))
	if err != nil {
		t.Fatalf("buffer named root: %v", err)
	}

	bridge, err := cache.directBridge()
	if err != nil {
		t.Fatalf("direct bridge: %v", err)
	}
	if err := cache.flushPendingRootDomainUnitsLocked(bridge, true); err != nil {
		t.Fatalf("flush root domains: %v", err)
	}
	if cache.PendingNamedRoots() {
		t.Fatalf("expected named roots drained")
	}
	if cache.PendingSystemOverlay() {
		t.Fatalf("expected system root drained")
	}

	gotSystem, err := backend.GetSystem([]byte("sys:users"))
	if err != nil {
		t.Fatalf("backend GetSystem: %v", err)
	}
	if !bytes.Equal(gotSystem, []byte("v1")) {
		t.Fatalf("backend GetSystem=%q want %q", gotSystem, []byte("v1"))
	}

	rootID := cache.ResolvedNamedRootID(virtualRootID)
	gotDoc, err := backend.GetAtRoot(rootID, docKey)
	if err != nil {
		t.Fatalf("backend GetAtRoot: %v", err)
	}
	if !bytes.Equal(gotDoc, docValue) {
		t.Fatalf("backend value=%q want %q", gotDoc, docValue)
	}

	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	gotDesc, err := backend.GetSystem(rootKey)
	if err != nil {
		t.Fatalf("backend GetSystem descriptor: %v", err)
	}
	if len(gotDesc) == 0 {
		t.Fatalf("expected published root descriptor")
	}
}

func bufferNamedRootDocument(cache *DB, collection string, docKey, docValue []byte) error {
	_, _, _, _, err := bufferNamedRootDocumentForTest(cache, collection, docKey, docValue)
	return err
}

func bufferNamedRootDocumentForTest(cache *DB, collection string, docKey, docValue []byte) (rootName string, virtualRootID uint64, key []byte, value []byte, err error) {
	rootName, err = collections.CollectionPrimaryRootName(collection)
	if err != nil {
		return "", 0, nil, nil, err
	}
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		return "", 0, nil, nil, err
	}
	virtualRootIDs, err := cache.BufferNamedRootMutationsOps(
		false,
		[]uint64{0},
		nil,
		[][]batch.Entry{{{
			Type:  batch.OpPut,
			Key:   append([]byte(nil), docKey...),
			Value: append([]byte(nil), docValue...),
		}}},
		func(newRootIDs []uint64) ([]batch.Entry, error) {
			desc := collections.CollectionRootDescriptor{
				Name:       rootName,
				Collection: collection,
				Kind:       collections.CollectionRootKindPrimary,
				RootPageID: newRootIDs[0],
				Format: collections.CollectionRootFormat{
					OuterLeavesInValueLog: true,
					LeafPrefixCompression: true,
					AllowValues:           true,
				},
			}
			encoded, err := desc.Encode()
			if err != nil {
				return nil, err
			}
			return []batch.Entry{{Type: batch.OpPut, Key: rootKey, Value: encoded}}, nil
		},
	)
	if err != nil {
		return "", 0, nil, nil, err
	}
	if len(virtualRootIDs) != 1 {
		return "", 0, nil, nil, err
	}
	return rootName, virtualRootIDs[0], append([]byte(nil), docKey...), append([]byte(nil), docValue...), nil
}
