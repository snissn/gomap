package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCommandWALCleanupDeferredClosedScan(t *testing.T) {
	for _, tc := range []struct {
		name        string
		closed      int
		rotate      bool
		checkpoint  bool
		cleanup     func(*DB) error
		wantOverlap bool
	}{
		{"same_active", 2, false, false, cleanupDeferredCommandWALPrefixForTest, true},
		{"rotate_active", 2, true, false, cleanupDeferredCommandWALPrefixForTest, true},
		{"single_predecessor", 1, false, false, cleanupDeferredCommandWALPrefixForTest, false},
		{"explicit_cleanup", 2, false, false, func(database *DB) error {
			return database.CleanupCommandWALCoveredSegments(false)
		}, false},
		{"checkpoint", 2, false, true, (*DB).Checkpoint, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCommandWALCleanupClosedScanWrite(t, t.TempDir(), "fixed", tc.closed, tc.rotate, tc.checkpoint, tc.cleanup, tc.wantOverlap)
		})
	}
}

func TestCommandWALCleanupClosedScanKeepsStreamExclusion(t *testing.T) {
	t.Run("stream", func(t *testing.T) {
		dir := t.TempDir()
		if runtime.GOOS == "linux" {
			var err error
			dir, err = os.MkdirTemp("/dev/shm", "treedb-wal-stream-cleanup-")
			if err != nil {
				t.Skipf("memory filesystem unavailable: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
		}
		testCommandWALCleanupClosedScanWrite(t, dir, "stream", 2, false, false, cleanupDeferredCommandWALPrefixForTest, false)
	})
	t.Run("fixed_with_stream_predecessor", func(t *testing.T) {
		dir := t.TempDir()
		writeCommandWALFrame(t, dir, 1, 1)
		emptyPath := writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
		database := zeroFrontierCleanupDBWithActiveForTest(t, dir, 2, true)
		streamPath := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
		activePath, _ := database.commandJournal.ActiveSegmentSnapshot()
		cleanup := func(database *DB) error {
			// This fixture retains a frame beyond the zero replay frontier.
			// Deferred cleanup may report an unavailable proof as pending.
			_, err := database.CleanupCommandWALCoveredPrefix()
			return err
		}
		scanned := testCommandWALCleanupScanOverlap(t, database, cleanup, streamPath, false, func() error {
			database.commandJournal.ActiveSegmentSnapshot()
			return nil
		})
		if !slices.Equal(scanned, []string{streamPath, emptyPath, activePath}) {
			t.Fatalf("mixed inventory scans=%q, want stream and fixed active", scanned)
		}
		if _, err := os.Stat(streamPath); err != nil {
			t.Fatalf("older recovery root needs stream predecessor: %v", err)
		}
	})
}

func cleanupDeferredCommandWALPrefixForTest(database *DB) error {
	complete, err := database.CleanupCommandWALCoveredPrefix()
	if err == nil && !complete {
		return errors.New("deferred command WAL cleanup did not service the debt")
	}
	return err
}

func testCommandWALCleanupClosedScanWrite(t *testing.T, dir, container string, closed int, rotate, checkpoint bool, cleanup func(*DB) error, wantOverlap bool) {
	t.Helper()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	wantValues := map[string]string{"suffix": "during"}
	var closedPaths []string
	for i := 0; i < closed; i++ {
		if actual := database.commandJournal.WriterBufferStats().Container; actual != container {
			t.Skipf("fixture requires %s WAL, got %s", container, actual)
		}
		key := fmt.Sprintf("covered-%d", i)
		if err := database.SetSync([]byte(key), []byte("before")); err != nil {
			t.Fatal(err)
		}
		wantValues[key] = "before"
		path, _ := database.commandWALActiveSegmentSnapshot()
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		closedPaths = append(closedPaths, path)
	}
	if err := database.rootPublication.coordinator.WaitThrough(context.Background(), database.State().CommitSeq); err != nil {
		t.Fatal(err)
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("fixture did not leave real deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	if checkpoint {
		key := "checkpoint-prefix"
		if err := database.SetSync([]byte(key), []byte("before")); err != nil {
			t.Fatal(err)
		}
		wantValues[key] = "before"
	}
	activePath, _ := database.commandWALActiveSegmentSnapshot()
	if slices.Contains(closedPaths, activePath) {
		t.Fatal("fixture did not rotate an actual WAL segment")
	}
	if actual := database.commandJournal.WriterBufferStats().Container; actual != container {
		t.Skipf("fixture requires %s WAL after rotation, got %s", container, actual)
	}
	scanned := testCommandWALCleanupScanOverlap(t, database, cleanup, closedPaths[0], wantOverlap, func() error {
		err := database.SetSync([]byte("suffix"), []byte("during"))
		if err == nil && rotate {
			err = database.RotateCommandWALActiveSegment(true)
		}
		return err
	})
	for _, path := range closedPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("covered file must be reclaimed after successful proof, stat error=%v", err)
		}
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active suffix must remain linked: %v", err)
	}
	finalActive, _ := database.commandWALActiveSegmentSnapshot()
	wantScans := append(closedPaths, activePath)
	if rotate || checkpoint {
		if finalActive == activePath {
			t.Fatal("rotation did not install a successor")
		}
		wantScans = append(wantScans, finalActive)
	}
	if !slices.Equal(scanned, wantScans) {
		t.Fatalf("physical scans=%q, want original prefix and current suffix %q", scanned, wantScans)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for key, expected := range wantValues {
		value, err := reopened.Get([]byte(key))
		if err != nil || string(value) != expected {
			t.Fatalf("reopened %s=%q err=%v", key, value, err)
		}
	}
}

func testCommandWALCleanupScanOverlap(t *testing.T, database *DB, cleanup func(*DB) error, closedPath string, wantOverlap bool, op func() error) []string {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	var scanned []string
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{beforeSegment: func(path string) {
		scanned = append(scanned, path)
		if path == closedPath && len(scanned) == 1 {
			close(reached)
			<-release
		}
	}}
	defer func() { database.testCommandWALCleanupHooks = nil }()
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- cleanup(database) }()
	select {
	case <-reached:
	case err := <-cleanupDone:
		close(release)
		t.Fatalf("cleanup returned before validating the closed file: %v", err)
	case <-time.After(5 * time.Second):
		close(release)
		<-cleanupDone
		t.Fatal("cleanup did not enter the closed scan")
	}
	started, performed := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		performed <- op()
	}()
	<-started
	wait := 2 * time.Second
	if !wantOverlap {
		wait = 100 * time.Millisecond
	}
	var opErr error
	var overlapped bool
	select {
	case opErr = <-performed:
		overlapped = true
	case <-time.After(wait):
	}
	close(release)
	if !overlapped {
		opErr = <-performed
	}
	if cleanupErr := <-cleanupDone; opErr != nil || cleanupErr != nil || overlapped != wantOverlap {
		t.Fatalf("operation overlapped closed WAL scan=%t want %t, operation=%v cleanup=%v", overlapped, wantOverlap, opErr, cleanupErr)
	}
	return scanned
}

func TestCommandWALCleanupOffLockCorruptionIsNotUnavailable(t *testing.T) {
	dir := t.TempDir()
	// The legitimate replay origin is 1. Leave LSN 2 missing and corrupt a
	// later sealed zero reserve after the live journal has validated it.
	writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
	writeCommandWALFrame(t, dir, 1, 1)
	empty := writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
	database := zeroFrontierCleanupDBWithActiveForTest(t, dir, 2, true)
	file, err := os.OpenFile(empty, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte{1}, 8191)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}

	for _, cleanup := range []func() error{
		func() error { return database.CleanupCommandWALCoveredSegments(false) },
		func() error { return database.CleanupCommandWALCoveredSegmentsAtCheckpoint(false) },
		func() error { _, err := database.CleanupCommandWALCoveredPrefix(); return err },
	} {
		if err := cleanup(); !errors.Is(err, commitlog.ErrCorrupt) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
			t.Fatalf("cleanup error=%v, want hard corruption refusal", err)
		}
		if _, err := os.Stat(empty); err != nil {
			t.Fatalf("corrupt segment must remain linked: %v", err)
		}
		if database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupNamespaceDirty.Load() {
			t.Fatal("refused cleanup changed deletion accounting")
		}
	}
}
