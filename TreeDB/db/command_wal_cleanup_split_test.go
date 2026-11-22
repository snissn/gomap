package db

import (
	"bytes"
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

func TestCommandWALCleanupDeferredStreamScan(t *testing.T) {
	for _, tc := range []struct {
		name        string
		closed      int
		rotate      bool
		wantOverlap bool
	}{
		{"same_active", 2, false, true},
		{"rotate_active", 2, true, true},
		{"single_predecessor", 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if runtime.GOOS == "linux" {
				var err error
				dir, err = os.MkdirTemp("/dev/shm", "treedb-wal-stream-cleanup-")
				if err != nil {
					t.Skipf("memory filesystem unavailable: %v", err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(dir) })
			}
			testCommandWALCleanupClosedScanWrite(t, dir, "stream", tc.closed, tc.rotate, false, cleanupDeferredCommandWALPrefixForTest, tc.wantOverlap)
		})
	}
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

func TestCommandWALCleanupDeferredClosedStreamRejectsLateDamage(t *testing.T) {
	opts := Options{
		Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true,
		ResolvedProfile: ProfileCommandWALRelaxed, Durability: DurabilityWALOnRelaxed,
		MemtableMode: "cow_btree", FreshRelaxedStandaloneWALStream: true,
	}
	opts.ValueLog.PointerThreshold = 4096
	database, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if actual := database.commandJournal.WriterBufferStats().Container; actual != "stream" {
		t.Skipf("fixture requires a stream WAL, got %s", actual)
	}
	value := bytes.Repeat([]byte("original"), 146)
	var paths [2]string
	var keys [][]byte
	for segment, count := range []int{1, 130} {
		paths[segment], _ = database.commandWALActiveSegmentSnapshot()
		for i := 0; i < count; i++ {
			key := []byte(fmt.Sprintf("covered-%d-%d", segment, i))
			keys = append(keys, key)
			write := database.Set
			if i == count-1 {
				write = database.SetSync
			}
			if err := write(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatal(err)
	}
	if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
		t.Fatalf("fixture did not leave real deferred cleanup debt: pending=%t err=%v", pending, err)
	}
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil || proof.rootCount != 2 || proof.cleanupThrough != database.State().AppliedCommandLSN {
		t.Fatalf("both roots must cover the closed predecessors: roots=%d frontier=%d err=%v", proof.rootCount, proof.cleanupThrough, err)
	}
	var originalInfo [2]os.FileInfo
	for i, path := range paths {
		originalInfo[i], err = os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	original, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(original) <= 128<<10 {
		t.Fatalf("damaged stream must cross two read windows, got %d bytes", len(original))
	}
	last := int64(len(original) - 1)
	replaceLastByte := func(value byte) error {
		file, err := os.OpenFile(paths[1], os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, err = file.WriteAt([]byte{value}, last)
		return errors.Join(err, file.Sync(), file.Close())
	}
	truncateLastByte := func() error {
		file, err := os.OpenFile(paths[1], os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		return errors.Join(file.Truncate(last), file.Sync(), file.Close())
	}
	removed := database.commandWALCleanupRemoved.Load()
	for _, tc := range []struct {
		name   string
		damage func() error
		want   error
	}{
		{"same_inode_crc", func() error { return replaceLastByte(original[last] ^ 1) }, commitlog.ErrCorrupt},
		{"short_closed_tail", truncateLastByte, commitlog.ErrCommandWALTerminalTail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if err := replaceLastByte(original[last]); err != nil {
					t.Fatal(err)
				}
			}()
			if err := tc.damage(); err != nil {
				t.Fatal(err)
			}
			beforeScanned := database.commandWALCleanupScanBytes.Load()
			complete, err := database.CleanupCommandWALCoveredPrefix()
			if complete || !errors.Is(err, tc.want) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
				t.Fatalf("deferred cleanup complete=%t error=%v, want hard %v", complete, err, tc.want)
			}
			if database.commandWALCleanupRemoved.Load() != removed || database.commandWALCleanupNamespaceDirty.Load() {
				t.Fatal("refused cleanup changed deletion accounting")
			}
			for i, path := range paths {
				info, err := os.Stat(path)
				if err != nil || !os.SameFile(info, originalInfo[i]) {
					t.Fatalf("original predecessor %d must remain linked: %v", i, err)
				}
			}
			delta := database.commandWALCleanupScanBytes.Load() - beforeScanned
			if delta < 128<<10 {
				t.Fatalf("cleanup did not validate to the damaged frame: scanned=%d", delta)
			}
			t.Logf("validated %d bytes before %v; both original predecessors retained", delta, err)
		})
	}
	t.Run("changed_after_detached_read", func(t *testing.T) {
		file, err := os.OpenFile(paths[0], os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := errors.Join(file.Truncate(originalInfo[0].Size()), file.Close()); err != nil {
				t.Fatal(err)
			}
		}()
		var changed bool
		database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{beforeSegment: func(path string) {
			if path != paths[1] {
				return
			}
			if _, err := file.WriteAt([]byte{0}, originalInfo[0].Size()); err != nil {
				t.Fatal(err)
			}
			changed = true
		}}
		defer func() { database.testCommandWALCleanupHooks = nil }()
		complete, err := database.CleanupCommandWALCoveredPrefix()
		if !changed || complete || !errors.Is(err, ErrRecoveryRequired) || errors.Is(err, errDurableWALCleanupProofUnavailable) {
			t.Fatalf("changed=%t complete=%t error=%v, want hard refusal of changed stream", changed, complete, err)
		}
		if database.commandWALCleanupRemoved.Load() != removed || database.commandWALCleanupNamespaceDirty.Load() {
			t.Fatal("refused cleanup changed deletion accounting")
		}
	})
	if t.Failed() {
		return
	}
	if err := cleanupDeferredCommandWALPrefixForTest(database); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("restored predecessor was not retired: %v", err)
		}
	}
	if delta := database.commandWALCleanupRemoved.Load() - removed; delta != uint64(len(paths)) {
		t.Fatalf("removed=%d, want %d predecessors", delta, len(paths))
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened, err := OpenForNativeSemanticOwner(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range keys {
		got, err := reopened.Get(key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("reopened %q: got %d bytes, err=%v", key, len(got), err)
		}
	}
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
	reached, release := make(chan bool, 1), make(chan struct{})
	var scanned []string
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{beforeSegment: func(path string) {
		scanned = append(scanned, path)
		if path == closedPath && len(scanned) == 1 {
			// No writer has started yet; sample append exclusion at the scan.
			reached <- database.commandJournal.AppendLockAvailableForTesting()
			<-release
		}
	}}
	wait := func(done <-chan error, operation string) error {
		t.Helper()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not complete after releasing the scan", operation)
			return nil
		}
	}
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- cleanup(database) }()
	select {
	case available := <-reached:
		if available != wantOverlap {
			t.Errorf("append lock available at closed WAL scan=%t, want %t", available, wantOverlap)
		}
	case err := <-cleanupDone:
		close(release)
		t.Fatalf("cleanup returned before validating the closed file: %v", err)
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatalf("cleanup did not enter the closed scan, eventual error %v", wait(cleanupDone, "cleanup"))
	}
	performed := make(chan error, 1)
	go func() { performed <- op() }()
	var opErr error
	completed := false
	if wantOverlap {
		select {
		case opErr = <-performed:
			completed = true
		case <-time.After(10 * time.Second):
			t.Error("independent writer did not finish while the closed WAL scan was paused")
		}
	}
	close(release)
	if !completed {
		opErr = wait(performed, "write")
	}
	cleanupErr := wait(cleanupDone, "cleanup")
	database.testCommandWALCleanupHooks = nil
	if opErr != nil || cleanupErr != nil {
		t.Fatalf("write error=%v, cleanup error=%v", opErr, cleanupErr)
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
