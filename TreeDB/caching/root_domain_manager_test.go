package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
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

func TestRootDomainManager_TracksNamedRootByKeyAcrossMutationKinds(t *testing.T) {
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

	rootName, virtualRootID, _, _, err := bufferNamedRootDocumentForTest(cache, "users", []byte("u1"), []byte(`{"email":"ada@example.com"}`))
	if err != nil {
		t.Fatalf("buffer named root: %v", err)
	}
	rootKey, err := collections.SystemCollectionRootKey(rootName)
	if err != nil {
		t.Fatalf("root key: %v", err)
	}

	stateByKey, ok := cache.DebugNamedRootStateByKey(rootKey)
	if !ok {
		t.Fatalf("expected pending named root state by key")
	}
	if !stateByKey.HasDomain {
		t.Fatalf("expected pending named root state to keep a domain")
	}
	if stateByKey.VirtualRootID != virtualRootID {
		t.Fatalf("pending named root virtual id=%d want %d", stateByKey.VirtualRootID, virtualRootID)
	}

	iterIDs, err := cache.BufferNamedRootMutationsIterators(false, []uint64{virtualRootID}, nil, []iterator.UnsafeIterator{
		&batchEntryIterator{entries: []batch.Entry{{
			Type:  batch.OpPut,
			Key:   []byte("u2"),
			Value: []byte(`{"email":"grace@example.com"}`),
		}}},
	}, nil)
	if err != nil {
		t.Fatalf("buffer iterator mutation: %v", err)
	}
	if len(iterIDs) != 1 || iterIDs[0] != virtualRootID {
		t.Fatalf("iterator mutation root ids=%v want [%d]", iterIDs, virtualRootID)
	}

	table, err := newRootDomainTable()
	if err != nil {
		t.Fatalf("new root-domain table: %v", err)
	}
	table.SetEntrySteal([]byte("u3"), []byte(`{"email":"lin@example.com"}`), page.ValuePtr{}, 0)
	tableIDs, err := cache.BufferNamedRootMutationsTables(false, []uint64{virtualRootID}, nil, []memtable.Table{table}, nil)
	if err != nil {
		t.Fatalf("buffer table mutation: %v", err)
	}
	if len(tableIDs) != 1 || tableIDs[0] != virtualRootID {
		t.Fatalf("table mutation root ids=%v want [%d]", tableIDs, virtualRootID)
	}

	stateByKey, ok = cache.DebugNamedRootStateByKey(rootKey)
	if !ok {
		t.Fatalf("expected pending named root state by key after iterator/table mutations")
	}
	if stateByKey.VirtualRootID != virtualRootID {
		t.Fatalf("pending named root virtual id after iterator/table=%d want %d", stateByKey.VirtualRootID, virtualRootID)
	}
	managerState := cache.DebugRootDomainManagerState()
	if managerState.PendingNamedRoots != 1 {
		t.Fatalf("pending named roots=%d want 1", managerState.PendingNamedRoots)
	}
}
