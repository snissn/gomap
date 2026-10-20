package caching

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Retained pruning must not confuse an original Manager's pinned zombie with
// an orphan. Its second Manager cannot see the first Manager's Set references.
func TestRetainedPrunePreservesBackendTrackedZombieUntilLastSnapshot(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("backend Close: %v", err)
		}
	})
	database, err := Open(dir, backend, Options{
		DisableWAL: true, RelaxedSync: true, AllowUnsafe: true,
		FlushThreshold: 256 << 20, MemtableShards: 1, JournalLanes: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("cache Close: %v", err)
		}
	})

	// The original Set owns a valid pointer even though neither current index
	// nor either recovery root references this segment. No stable token is
	// fabricated: the regression is the ordinary snapshot's Manager/File pin.
	fileID, err := valuelog.EncodeFileID(0, 100)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(database.valueLogDir, valueLogName(0, 100))
	writer, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("held original persistent pointer|"), 64)
	ptr, appendErr := writer.Append(0, nil, 1, value)
	if err := errors.Join(appendErr, writer.Close()); err != nil {
		t.Fatal(err)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	if err := database.valueLogReader.RegisterSegment(path, fileID); err != nil {
		t.Fatal(err)
	}
	held, last := backend.AcquireSnapshot(), backend.AcquireSnapshot()
	t.Cleanup(func() {
		if err := errors.Join(held.Close(), last.Close()); err != nil {
			t.Errorf("snapshot Close: %v", err)
		}
	})
	if held == nil || last == nil {
		t.Fatal("missing original snapshots")
	}
	original, identity, err := held.PinnedValueLogFile(fileID)
	if err != nil {
		t.Fatal(err)
	}
	originalInfo, err := original.Stat()
	if err != nil {
		t.Fatal(err)
	}
	checkHeld := func(snapshot *backenddb.Snapshot) {
		t.Helper()
		file, currentIdentity, err := snapshot.PinnedValueLogFile(fileID)
		if err != nil || currentIdentity != identity {
			t.Fatalf("original pointer identity changed: identity=%v err=%v", currentIdentity, err)
		}
		got, err := valuelog.ReadAt(file, ptr, true)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("held pointer read: bytes=%d err=%v", len(got), err)
		}
		info, err := os.Stat(path)
		if err != nil || !os.SameFile(originalInfo, info) {
			t.Fatalf("held pointer lost original linked file: %v", err)
		}
	}
	checkHeld(held)
	if err := backend.MarkValueLogZombie(fileID); err != nil {
		t.Fatalf("first retirement: %v", err)
	}
	checkHeld(held)

	seedRetainedPrunePressure(database, path, int64(len(value)))
	for attempt := 0; attempt < 2; attempt++ {
		database.markValueLogRetain(path)
		// Exercise the exact cached orphan-fallback dispatch after backend
		// retirement, without depending on a background scheduler interleaving.
		stats, ran := database.runRetainedValueLogPruneInline(true, map[uint32]struct{}{fileID: {}})
		if !ran || stats.ScanError || stats.ObservedSourceZombieMarkedSegments != 1 || stats.ObservedSourceRemovedSegments != 0 {
			t.Fatalf("repeat %d treated tracked zombie as orphan: ran=%t stats=%+v", attempt, ran, stats)
		}
		checkHeld(held)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	checkHeld(last)
	if err := backend.MarkValueLogZombie(fileID); err != nil {
		t.Fatalf("retirement with only last snapshot: %v", err)
	}
	if err := last.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unreachable original file survived last snapshot: %v", err)
	}
	if err := backend.MarkValueLogZombie(fileID); !errors.Is(err, valuelog.ErrFileNotFound) {
		t.Fatalf("genuinely absent original file error=%v, want ErrFileNotFound", err)
	}
	stats, ran := database.runRetainedValueLogPruneInline(true, map[uint32]struct{}{fileID: {}})
	if !ran || stats.ScanError || database.valueLogRetained(path) {
		t.Fatalf("completed retirement left cached retention: ran=%t stats=%+v", ran, stats)
	}
}
