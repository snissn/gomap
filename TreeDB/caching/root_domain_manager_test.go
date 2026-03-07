package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestRootDomainManager_TracksSystemAndNamedState(t *testing.T) {
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

	state := cache.DebugRootDomainManagerState()
	if !state.HasSystemMutable {
		t.Fatalf("expected mutable system root-domain state")
	}
	if state.PendingNamedRoots != 1 {
		t.Fatalf("pending named roots=%d want 1", state.PendingNamedRoots)
	}
	if state.PublishedNamedRoots != 0 {
		t.Fatalf("published named roots=%d want 0", state.PublishedNamedRoots)
	}
	if state.BufferedNamedRootBytes <= 0 {
		t.Fatalf("buffered named root bytes=%d want > 0", state.BufferedNamedRootBytes)
	}
}

func TestRootDomainManager_FlushKeepsPublishedNamedMapping(t *testing.T) {
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

	_, virtualRootID, _, _, err := bufferNamedRootDocumentForTest(cache, "users", []byte("u1"), []byte(`{"email":"ada@example.com"}`))
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

	state := cache.DebugRootDomainManagerState()
	if state.PendingNamedRoots != 0 {
		t.Fatalf("pending named roots=%d want 0", state.PendingNamedRoots)
	}
	if state.PublishedNamedRoots != 1 {
		t.Fatalf("published named roots=%d want 1", state.PublishedNamedRoots)
	}
	if state.BufferedNamedRootBytes != 0 {
		t.Fatalf("buffered named root bytes=%d want 0", state.BufferedNamedRootBytes)
	}
	if resolved := cache.ResolvedNamedRootID(virtualRootID); resolved == virtualRootID {
		t.Fatalf("expected published mapping for virtual root %d", virtualRootID)
	}
}
