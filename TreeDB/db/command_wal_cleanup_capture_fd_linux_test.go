package db

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

func TestCommandWALCleanupLeavesRoomForFirstPrefixCapture(t *testing.T) {
	if !enterLimitedWALCleanupProcess(t) {
		return
	}
	for _, free := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("%d_free", free), func(t *testing.T) {
			testCommandWALCleanupFirstPrefixCapture(t, free, false)
		})
		t.Run(fmt.Sprintf("%d_free_both_covered", free), func(t *testing.T) {
			testCommandWALCleanupFirstPrefixCapture(t, free, true)
		})
	}
}

func testCommandWALCleanupFirstPrefixCapture(t *testing.T, free int, bothCovered bool) {
	opts := Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if database.commandJournal.WriterBufferStats().Container != "fixed" {
		t.Skip("requires fixed WAL")
	}
	var predecessors []string
	for _, key := range []string{"covered", "replay"} {
		if err := database.SetSync([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := database.rootPublication.coordinator.WaitThrough(t.Context(), database.State().CommitSeq); err != nil {
			t.Fatal(err)
		}
		path, _ := database.commandWALActiveSegmentSnapshot()
		predecessors = append(predecessors, path)
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		if !bothCovered && key == "covered" {
			if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
				t.Fatalf("prepare cleanup: pending=%v err=%v", pending, err)
			}
		}
	}

	if bothCovered {
		if err := database.RefreshCommandWALCheckpointFallback(); err != nil {
			t.Fatal(err)
		}
		if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
			t.Fatalf("prepare both predecessors: pending=%v err=%v", pending, err)
		}
	}

	var padding []*os.File
	release := func() {
		for _, f := range padding {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		}
		padding = nil
	}
	defer release()
	for {
		f, err := os.Open(os.DevNull)
		if errors.Is(err, syscall.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		padding = append(padding, f)
	}
	if len(padding) < free {
		t.Fatal("could not reserve the test descriptors")
	}
	for _, f := range padding[len(padding)-free:] {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	padding = padding[:len(padding)-free]

	// Start capture while staging owns the journal. On an admitted detached
	// scan force the capture to finish before validation and retirement.
	startCapture := make(chan struct{})
	captured := make(chan struct{})
	var released bool
	releaseCapture := func() {
		if !released {
			close(startCapture)
			released = true
		}
	}
	var captureErr error
	opened, scans := 0, 0
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{
		openPrefix: func(path string) (*os.File, error) {
			f, err := os.Open(path)
			if err == nil {
				opened++
				if opened == 2 {
					go func() {
						defer close(captured)
						<-startCapture
						target, _, err := database.CaptureCommandWALPrefix()
						if err == nil {
							_, err = target.Sync()
						}
						captureErr = err
					}()
				}
			}
			return f, err
		},
		beforeSegment: func(string) {
			scans++
			if scans == 1 {
				detached := database.commandJournal.AppendLockAvailableForTesting()
				if !detached {
					t.Error("the admitted first scan still owns the journal")
				}
				releaseCapture()
				if detached {
					<-captured
				}
			}
		},
		afterScan: func() { releaseCapture(); <-captured },
	}
	removed := database.commandWALCleanupRemoved.Load()
	complete, scanErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	releaseCapture()
	if opened != 2 {
		t.Fatalf("staged %d predecessors: %v", opened, scanErr)
	}
	<-captured
	if captureErr != nil {
		t.Fatalf("first capture: %v", captureErr)
	}
	wantPending := free < 4
	if scanErr != nil || complete == wantPending {
		t.Fatalf("cleanup: complete=%v err=%v", complete, scanErr)
	}
	if !wantPending && scans == 0 {
		t.Fatal("admitted cleanup did not scan any predecessor")
	}
	if wantPending {
		if scans != 0 || database.commandWALCleanupRemoved.Load() != removed {
			t.Fatalf("deferred cleanup scanned %d files or retired a predecessor", scans)
		}
		for _, path := range predecessors {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("pending cleanup lost %s: %v", path, err)
			}
		}
	}
	release()
	if wantPending {
		if complete, err := database.CleanupCommandWALCoveredPrefix(); err != nil || !complete {
			t.Fatalf("retry cleanup: complete=%v err=%v", complete, err)
		}
	}
	wantRemoved := uint64(1)
	if bothCovered {
		wantRemoved = 2
	}
	if n := database.commandWALCleanupRemoved.Load() - removed; n != wantRemoved {
		t.Fatalf("removed %d predecessors, want %d", n, wantRemoved)
	}
	if err := database.SetSync([]byte("after"), []byte("value")); err != nil {
		t.Fatal(err)
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
	for _, key := range []string{"covered", "replay", "after"} {
		if value, err := reopened.Get([]byte(key)); err != nil || string(value) != "value" {
			t.Fatalf("reopened %s: value=%q err=%v", key, value, err)
		}
	}
}
