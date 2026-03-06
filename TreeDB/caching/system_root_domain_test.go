package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSystemRootDomain_BufferedWritesUseMemtableState(t *testing.T) {
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

	state := cache.DebugSystemRootState()
	if !state.HasMutable {
		t.Fatalf("expected buffered system writes to live in mutable root-domain state")
	}
	if state.QueueLen != 0 {
		t.Fatalf("queue len=%d want 0", state.QueueLen)
	}
	if state.LegacyEntryCount != 0 {
		t.Fatalf("legacy entry count=%d want 0", state.LegacyEntryCount)
	}

	got, err := cache.GetSystem([]byte("sys:users"))
	if err != nil {
		t.Fatalf("GetSystem: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("GetSystem=%q want %q", got, []byte("v1"))
	}
}

func TestSystemRootDomain_IteratorMergesBufferedStateAndBackend(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	if err := backend.SetSystem([]byte("sys:a"), []byte("persisted-a")); err != nil {
		t.Fatalf("backend SetSystem sys:a: %v", err)
	}
	if err := backend.SetSystem([]byte("sys:b"), []byte("persisted-b")); err != nil {
		t.Fatalf("backend SetSystem sys:b: %v", err)
	}

	cache, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	if err := cache.ApplySystemOverlayEntriesOwned([]batch.Entry{
		{Type: batch.OpDelete, Key: []byte("sys:a")},
		{Type: batch.OpPut, Key: []byte("sys:c"), Value: []byte("buffered-c")},
	}); err != nil {
		t.Fatalf("apply system entries: %v", err)
	}

	it, err := cache.SystemIterator(nil, nil)
	if err != nil {
		t.Fatalf("SystemIterator: %v", err)
	}
	defer it.Close()

	var keys [][]byte
	var values [][]byte
	for it.Valid() {
		keys = append(keys, it.KeyCopy(nil))
		values = append(values, it.ValueCopy(nil))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	wantKeys := [][]byte{[]byte("sys:b"), []byte("sys:c")}
	wantValues := [][]byte{[]byte("persisted-b"), []byte("buffered-c")}
	if len(keys) != len(wantKeys) {
		t.Fatalf("keys len=%d want %d", len(keys), len(wantKeys))
	}
	for i := range wantKeys {
		if !bytes.Equal(keys[i], wantKeys[i]) {
			t.Fatalf("key[%d]=%q want %q", i, keys[i], wantKeys[i])
		}
		if !bytes.Equal(values[i], wantValues[i]) {
			t.Fatalf("value[%d]=%q want %q", i, values[i], wantValues[i])
		}
	}
}

func TestSystemRootDomain_FlushPublishesAndClearsMutableState(t *testing.T) {
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

	before := cache.SystemRootVersion()
	if err := cache.flushSystemOverlay(true); err != nil {
		t.Fatalf("flushSystemOverlay: %v", err)
	}

	state := cache.DebugSystemRootState()
	if state.HasMutable {
		t.Fatalf("expected mutable system root-domain state to be cleared after flush")
	}
	if state.QueueLen != 0 {
		t.Fatalf("queue len=%d want 0", state.QueueLen)
	}
	if state.LegacyEntryCount != 0 {
		t.Fatalf("legacy entry count=%d want 0", state.LegacyEntryCount)
	}
	if cache.PendingSystemOverlay() {
		t.Fatalf("expected no pending system root-domain state after flush")
	}

	after := cache.SystemRootVersion()
	if before == after {
		t.Fatalf("expected SystemRootVersion to change across flush")
	}

	got, err := backend.GetSystem([]byte("sys:users"))
	if err != nil {
		t.Fatalf("backend GetSystem: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("backend GetSystem=%q want %q", got, []byte("v1"))
	}
}
