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
	for _, free := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d_free", free), func(t *testing.T) {
			testCommandWALCleanupFirstPrefixCapture(t, free)
		})
	}
}

func testCommandWALCleanupFirstPrefixCapture(t *testing.T, free int) {
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
	for _, key := range []string{"covered", "replay"} {
		if err := database.SetSync([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := database.rootPublication.coordinator.WaitThrough(t.Context(), database.State().CommitSeq); err != nil {
			t.Fatal(err)
		}
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
		if key == "covered" {
			if pending, err := database.PrepareCommandWALCoveredPrefixCleanup(); err != nil || !pending {
				t.Fatalf("prepare cleanup: pending=%v err=%v", pending, err)
			}
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

	// Start a real capture at the first scan. If append exclusion has been
	// dropped, finish it before allowing the scanner to advance.
	var started bool
	captured := make(chan error, 1)
	finished := make(chan struct{})
	database.testCommandWALCleanupHooks = &commandWALCleanupTestHooks{beforeSegment: func(string) {
		if started {
			return
		}
		started = true
		detached := database.commandJournal.AppendLockAvailableForTesting()
		if detached != (free > 2) {
			t.Errorf("free=%d detached=%t", free, detached)
		}
		go func() {
			target, _, err := database.CaptureCommandWALPrefix()
			if err == nil {
				_, err = target.Sync()
			}
			captured <- err
			close(finished)
		}()
		if detached {
			<-finished
		}
	}}
	complete, scanErr := database.CleanupCommandWALCoveredPrefix()
	database.testCommandWALCleanupHooks = nil
	if !started {
		t.Fatalf("scanner did not start: %v", scanErr)
	}
	if err := <-captured; err != nil {
		t.Fatalf("first capture: %v", err)
	}
	release()
	if scanErr != nil || !complete {
		t.Fatalf("cleanup: complete=%v err=%v", complete, scanErr)
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
