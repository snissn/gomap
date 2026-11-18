package commitlog

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestCapturedWALSyncRetainsPhysicalFailureBeforeReport(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := journal.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	captured, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Release()

	originalIdentity, err := journal.writer.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int64
	journal.writer.syncFn = func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if !os.SameFile(originalIdentity, info) {
			t.Error("sync attempted on a different WAL inode")
		}
		if attempts.Add(1) == 1 {
			// Model a one-shot writeback error consumed by this open-file
			// description. Another fsync can return success after consumption.
			return syscall.EIO
		}
		return nil
	}
	first := make(chan error, 1)
	allowReport := make(chan struct{})
	reported := make(chan struct{})
	go func() {
		first <- journal.Sync()
		<-allowReport
		close(reported)
	}()
	defer func() {
		close(allowReport)
		<-reported
	}()
	if err := <-first; !errors.Is(err, syscall.EIO) {
		t.Fatalf("initial physical sync error=%v", err)
	}
	if err := captured.Sync(); !errors.Is(err, syscall.EIO) {
		t.Errorf("captured sync after physical failure, before its caller reports it=%v", err)
	}
	if err := journal.Sync(); !errors.Is(err, syscall.EIO) {
		t.Errorf("subsequent direct sync=%v", err)
	}
	if attempts.Load() != 1 {
		t.Errorf("performed %d physical attempts, want 1", attempts.Load())
	}
	captured.Release()
	if err := journal.Close(); !errors.Is(err, syscall.EIO) {
		t.Errorf("close after captured failure=%v", err)
	}
}

func TestCapturedWALSyncFailureSurvivesRotationAndDoesNotBlockAppend(t *testing.T) {
	var poisoned atomic.Bool
	journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{
		CaptureStableResources: true,
		OnFileSyncError:        func() { poisoned.Store(true) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := journal.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finishIO := make(chan struct{})
	journal.writer.syncFn = func(*os.File) error {
		close(entered)
		<-finishIO
		return syscall.EIO
	}
	worker, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Release()
	workerErr := make(chan error, 1)
	go func() { workerErr <- worker.Sync() }()
	released := false
	release := func() {
		if !released {
			close(finishIO)
			released = true
		}
	}
	joined := false
	join := func() error {
		release()
		if joined {
			return nil
		}
		joined = true
		return <-workerErr
	}
	defer join()
	select {
	case <-entered:
	case err := <-workerErr:
		joined = true
		t.Fatalf("capture did not enter physical sync: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("capture never reached the physical sync")
	}

	// A captured physical sync does not own append authority. Append and
	// rotate without syncing the old file while the syscall is outstanding.
	type rotationResult struct {
		lsn      uint64
		rotation *CommandJournalStableRotation
		err      error
	}
	installed := make(chan rotationResult, 1)
	go func() {
		lsn, err := journal.AppendCommand(fixedWALTestBarrier())
		var rotation *CommandJournalStableRotation
		if err == nil {
			rotation, err = journal.RotateActiveSegmentWithStableResources(false)
		}
		installed <- rotationResult{lsn: lsn, rotation: rotation, err: err}
	}()
	var rotation *CommandJournalStableRotation
	select {
	case result := <-installed:
		rotation = result.rotation
		if result.err != nil || result.lsn != 2 || rotation == nil {
			t.Fatalf("overlapping append LSN=%d rotation=%v err=%v", result.lsn, rotation != nil, result.err)
		}
	case <-time.After(5 * time.Second):
		_ = join()
		result := <-installed
		result.rotation.Release()
		t.Fatal("relaxed append and rotation blocked behind the captured sync")
	}
	defer rotation.Release()
	successor, err := journal.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Release()
	if poisoned.Load() {
		t.Fatal("capture or rotation reported failure before the syscall returned")
	}
	if err := join(); !errors.Is(err, syscall.EIO) {
		t.Errorf("original captured sync error=%v", err)
	}
	if !poisoned.Load() {
		t.Error("journal returned its physical error before marking the backend")
	}
	if err := successor.Sync(); !errors.Is(err, syscall.EIO) {
		t.Errorf("successor capture passed outstanding journal failure: %v", err)
	}
	if err := rotation.Closed.SyncThrough(); !errors.Is(err, syscall.EIO) {
		t.Errorf("closed stable token passed original file failure: %v", err)
	}
	if lsn, err := journal.AppendCommand(fixedWALTestBarrier()); lsn != 0 || !errors.Is(err, syscall.EIO) {
		t.Errorf("post-failure append LSN=%d err=%v", lsn, err)
	}
}
