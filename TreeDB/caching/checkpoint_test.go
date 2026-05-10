package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func countCommitLogFiles(entries []os.DirEntry) int {
	n := 0
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if strings.HasPrefix(ent.Name(), "commit-") {
			n++
		}
	}
	return n
}

func TestCachingDB_Checkpoint_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 2 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rotateWAL := func() {
		t.Helper()
		db.writeMu.Lock()
		db.mu.Lock()
		var err error
		for i := range db.lanes {
			err = db.rotateWALLocked(&db.lanes[i])
			if err != nil {
				break
			}
		}
		db.mu.Unlock()
		db.writeMu.Unlock()
		if err != nil {
			t.Fatalf("rotateWAL: %v", err)
		}
	}

	// Create multiple WAL segments by rotating explicitly between writes.
	val := bytes.Repeat([]byte("v"), 1<<20) // 1MiB
	if err := db.Set([]byte("k000"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotateWAL()
	if err := db.Set([]byte("k001"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	before, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}

	walFilesBefore := countCommitLogFiles(before)
	if walFilesBefore < 2 {
		t.Fatalf("expected multiple WAL segments before checkpoint, got %d", walFilesBefore)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	after, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal) after: %v", err)
	}

	walFilesAfter := countCommitLogFiles(after)
	if walFilesAfter != len(db.lanes) {
		t.Fatalf("expected exactly %d WAL segments after checkpoint, got %d", len(db.lanes), walFilesAfter)
	}
}

func TestCachingDB_AutoCheckpoint_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(5*time.Millisecond, 1<<20 /* 1MiB */, 0)

	val := bytes.Repeat([]byte("v"), 512<<10) // 512KiB
	for i := 0; i < 40; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	walDir := filepath.Join(dir, "wal")
	deadline := time.Now().Add(2 * time.Second)
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for auto checkpoint to trim WAL (files=%d)", walFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCachingDB_AutoCheckpoint_IdleTrigger_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 2 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 0, 100*time.Millisecond)

	rotateWAL := func() {
		t.Helper()
		db.writeMu.Lock()
		db.mu.Lock()
		var err error
		for i := range db.lanes {
			err = db.rotateWALLocked(&db.lanes[i])
			if err != nil {
				break
			}
		}
		db.mu.Unlock()
		db.writeMu.Unlock()
		if err != nil {
			t.Fatalf("rotateWAL: %v", err)
		}
	}

	// Create multiple WAL segments by rotating explicitly between writes.
	val := bytes.Repeat([]byte("v"), 1<<20) // 1MiB
	if err := db.Set([]byte("k000"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	rotateWAL()
	if err := db.Set([]byte("k001"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	before, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFilesBefore := countCommitLogFiles(before)
	if walFilesBefore < 2 {
		t.Fatalf("expected multiple WAL segments before idle checkpoint, got %d", walFilesBefore)
	}

	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles == len(db.lanes) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for idle auto checkpoint to trim WAL (files=%d)", walFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The WAL directory can reflect a completed trim slightly before the
	// auto-checkpoint goroutine updates its counters. Poll stats to avoid a
	// timing-dependent flake.
	deadline = time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		stats := db.Stats()
		if stats == nil {
			t.Fatalf("Stats() returned nil")
		}
		n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			t.Fatalf("parse auto checkpoint count: %v", err)
		}
		reason := stats["treedb.cache.auto_checkpoint.last_reason"]
		if n > 0 && reason == "idle" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for idle auto checkpoint stats (count=%d reason=%q)", n, reason)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func withRaceTimeout(d time.Duration) time.Duration {
	if testRaceEnabled {
		return d * 5
	}
	return d
}

func TestCachingDB_AutoCheckpoint_IdleTrigger_SkipsTinyWrites(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 0, 50*time.Millisecond)

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected no auto checkpoint for tiny write burst, got %d", n)
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 1<<20 /* 1MiB */, 0)

	// Force WAL rotation by exceeding the ~10MiB WAL reuse threshold.
	val := bytes.Repeat([]byte("v"), 11<<20) // 11MiB
	if err := db.Set([]byte("k"), val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got := db.effectiveWALBytes(); got < 1<<20 {
		t.Fatalf("expected WAL bytes >= 1MiB, got %d", got)
	}
	db.autoCheckpointMaxWALBytes.Store(1 << 20)
	db.autoCheckpointSizeArmed.Store(true)
	db.maybeAutoCheckpoint(1<<20, autoCheckpointModeSize)

	walDir := filepath.Join(dir, "wal")
	deadline := time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		stats := db.Stats()
		if stats == nil {
			t.Fatalf("Stats() returned nil")
		}
		n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
		if err != nil {
			t.Fatalf("parse auto checkpoint count: %v", err)
		}
		if n > 0 && stats["treedb.cache.auto_checkpoint.last_reason"] == "size" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for size auto checkpoint to run (count=%d reason=%q)", n, stats["treedb.cache.auto_checkpoint.last_reason"])
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadline = time.Now().Add(withRaceTimeout(2 * time.Second))
	for {
		ents, err := os.ReadDir(walDir)
		if err != nil {
			t.Fatalf("ReadDir(wal): %v", err)
		}
		walFiles := countCommitLogFiles(ents)
		if walFiles < len(db.lanes) {
			t.Fatalf("timed out waiting for size checkpoint WAL trim (files=%d)", walFiles)
		}
		if walFiles <= len(db.lanes)+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for size checkpoint to trim WAL (files=%d)", walFiles)
		}
		time.Sleep(10 * time.Millisecond)
	}

	ents, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFiles := countCommitLogFiles(ents)
	if walFiles < len(db.lanes) || walFiles > len(db.lanes)+1 {
		t.Fatalf("expected %d..%d WAL segments after size checkpoint, got %d", len(db.lanes), len(db.lanes)+1, walFiles)
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_SeedsExistingWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	preexisting := []string{
		filepath.Join(walDir, "commit-l0-000010.log"),
	}
	for _, path := range preexisting {
		if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 2<<20), 0o600); err != nil {
			t.Fatalf("WriteFile(preexisting WAL): %v", err)
		}
	}

	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 16 << 20,
		JournalLanes:             1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	db.autoCheckpointSizeArmed.Store(true)
	db.maybeAutoCheckpoint(1<<20, autoCheckpointModeSize)

	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n == 0 {
		t.Fatalf("expected size auto checkpoint to run")
	}
	if reason := stats["treedb.cache.auto_checkpoint.last_reason"]; reason != "size" {
		t.Fatalf("expected last reason size, got %q", reason)
	}

	ents, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	walFiles := countCommitLogFiles(ents)
	if walFiles < len(db.lanes) || walFiles > len(db.lanes)+1 {
		t.Fatalf("expected %d..%d WAL segments after size checkpoint, got %d", len(db.lanes), len(db.lanes)+1, walFiles)
	}
	for _, ent := range ents {
		if ent.Name() == "commit-l0-000010.log" {
			t.Fatalf("expected seeded WAL segment to be trimmed, still present: %s", ent.Name())
		}
	}
}

func TestCachingDB_AutoCheckpoint_SizeTrigger_DoesNotThrashWithRetainedValueLog(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.testSkipVlogCheckpointKick = true

	db.StartAutoCheckpoint(0, 1<<20 /* 1MiB */, 0)

	// Seed a retained value-log segment by writing a large value. In value-log
	// mode this segment cannot be deleted by checkpoint, so reclaimable WAL bytes
	// remain below maxWALBytes indefinitely.
	val := bytes.Repeat([]byte("v"), 2<<20) // 2MiB
	if err := db.Set([]byte("seed"), val); err != nil {
		t.Fatalf("Set(seed): %v", err)
	}

	var initialCount uint64
	time.Sleep(withRaceTimeout(200 * time.Millisecond))
	stats := db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err := strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected size auto checkpoint to remain idle with retained value-log (count=%d)", n)
	}
	initialCount = n

	// Continue writing while effectiveWALBytes remains above maxWALBytes. The
	// size-triggered checkpoint should remain disarmed and not repeatedly run.
	val = bytes.Repeat([]byte("x"), 512<<10) // 512KiB
	for i := 0; i < 8; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	stats = db.Stats()
	if stats == nil {
		t.Fatalf("Stats() returned nil")
	}
	n, err = strconv.ParseUint(stats["treedb.cache.auto_checkpoint.count"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto checkpoint count: %v", err)
	}
	if n != initialCount {
		t.Fatalf("expected size-triggered checkpoint to run once (count=%d), got %d", initialCount, n)
	}
}
