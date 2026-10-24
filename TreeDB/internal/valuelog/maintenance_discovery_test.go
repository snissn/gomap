package valuelog_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/caching"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// A manager can discover an unreachable file after another manager has already
// admitted its deletion. Exercise the actual COW maintenance API while the
// original physical file still exists; a pathname-not-found retry cannot hide
// the conflict. The segment is created after the cache acquired its initial cut.
func TestCOWBackendMaintenanceDiscoversAdmittedDeletion(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := caching.Open(dir, backend, caching.Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered",
		FlushThreshold: 1 << 30, IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Error(err)
		}
	})
	key, want := []byte("reachable"), bytes.Repeat([]byte("persistent value"), 64)
	batch := cache.NewBatch()
	if err := batch.Set(key, want); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	fileID, err := valuelog.EncodeFileID(1, 999)
	if err != nil {
		t.Fatal(err)
	}
	path := valuelog.SegmentPath(backenddb.ValueLogDirPath(dir), fileID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(0, nil, 1, []byte("unreachable maintenance input")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	registry := backend.ValueLogIdentityPinRegistry()
	deleter, err := valuelog.NewManagerWithStableResourcePinRegistry(filepath.Dir(path), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := deleter.Close(); err != nil {
			t.Error(err)
		}
	})
	// Existing COW/metadata pins must still exclude deletion across managers.
	set := deleter.CurrentSetNoRefresh()
	var protected uint32
	for id, file := range set.Files {
		if physical, ok := file.RegisteredStableIdentity(); ok && id != fileID && registry.PinCount(physical) != 0 {
			protected = id
			break
		}
	}
	if err := deleter.Release(set); err != nil {
		t.Fatal(err)
	}
	if protected == 0 {
		t.Fatal("fixture has no physically pinned reachable value-log file")
	}
	if removed, err := deleter.RemoveSegmentIfUnpinned(protected); removed || err != nil {
		t.Fatalf("another manager removed a reachable pinned file: removed=%v err=%v", removed, err)
	}
	original, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	admitted, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	valuelog.PauseDirectDeletionForTest(deleter, admitted, resume)
	done := make(chan error, 1)
	go func() { _, err := deleter.RemoveSegmentIfUnpinned(fileID); done <- err }()
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("deletion did not join")
			}
		}
	})
	select {
	case <-admitted:
	case err := <-done:
		joined = true
		t.Fatalf("deletion never admitted: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("deletion never admitted")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("original missing before discovery: %v", err)
	}
	if pin, err := registry.Pin(identity); !errors.Is(err, rootpublication.ErrResourceDeletionInProgress) {
		if pin != nil {
			pin.Release()
		}
		t.Fatalf("admitted deletion permitted a new pin: %v", err)
	}
	called := false
	if err := cache.RunBackendMaintenance(func() error { called = true; return nil }); err != nil {
		t.Fatalf("real maintenance failed discovering admitted physical deletion: %v", err)
	}
	if !called {
		t.Fatal("maintenance did not run its callback")
	}
	if got, err := cache.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reachable value changed: len=%d err=%v", len(got), err)
	}
	release()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deletion did not finish")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreachable original was not reclaimed: %v", err)
	}
	if err := cache.RunBackendMaintenance(nil); err != nil {
		t.Fatal(err)
	}
	if got, err := cache.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reachable value changed after deletion: len=%d err=%v", len(got), err)
	}
}
