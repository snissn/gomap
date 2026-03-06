package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestFlushSome_FlushesNamedRootsWithoutQueuedMemtables(t *testing.T) {
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

	rootName, err := collections.CollectionPrimaryRootName("users")
	if err != nil {
		t.Fatalf("root name: %v", err)
	}
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}
	docKey := []byte("u1")
	docValue := []byte(`{"email":"ada@example.com"}`)

	virtualRootIDs, err := cache.BufferNamedRootMutationsOps(
		false,
		[]uint64{0},
		nil,
		[][]batch.Entry{{{Type: batch.OpPut, Key: append([]byte(nil), docKey...), Value: append([]byte(nil), docValue...)}}},
		func(newRootIDs []uint64) ([]batch.Entry, error) {
			desc := collections.CollectionRootDescriptor{
				Name:       rootName,
				Collection: "users",
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
		t.Fatalf("buffer named root mutations: %v", err)
	}
	if len(virtualRootIDs) != 1 {
		t.Fatalf("virtual root ids len=%d want 1", len(virtualRootIDs))
	}
	if !cache.PendingNamedRoots() {
		t.Fatalf("expected pending named roots before flush")
	}

	flushed := cache.flushSome(false, 1, 0)
	if flushed != 1 {
		t.Fatalf("flushSome flushed=%d want 1", flushed)
	}
	if cache.PendingNamedRoots() {
		t.Fatalf("expected named roots drained by flushSome")
	}

	resolvedRootID := cache.ResolvedNamedRootID(virtualRootIDs[0])
	got, err := backend.GetAtRoot(resolvedRootID, docKey)
	if err != nil {
		t.Fatalf("backend GetAtRoot: %v", err)
	}
	if !bytes.Equal(got, docValue) {
		t.Fatalf("backend value=%q want %q", got, docValue)
	}
}
