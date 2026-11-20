package commitlog

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCheckpointSuccessorDirectoryPreservesPredecessorCloseFailure(t *testing.T) {
	dir := t.TempDir()
	var notifications atomic.Int32
	j, err := OpenCommandJournal(dir, CommandJournalOptions{
		PreferExt4Stream: true,
		OnFileSyncError:  func() { notifications.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if !IsExt4WALFilesystem(j.writer.f) {
		t.Skip("requires the ext4 stream selector")
	}
	if _, err := j.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	captured, err := j.CaptureSync(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Release()

	closeFailure := &os.PathError{Op: "close", Path: j.path, Err: syscall.EIO}
	dirFailure := &os.PathError{Op: "sync", Path: dir, Err: syscall.ENOSPC}
	j.writer.closeRotateFn = func(old *os.File) error {
		return errors.Join(old.Close(), closeFailure)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	var starts, completions atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Path == dir && e.StablePath != nil {
			switch e.Point {
			case durabilitycut.BeforeNewFileDirectorySync:
				if starts.Add(1) == 1 {
					close(entered)
					<-resume
				}
			case durabilitycut.AfterNewFileDirectorySync:
				completions.Add(1)
				return dirFailure
			}
		}
		return nil
	})
	defer restore()
	var workers sync.WaitGroup
	defer func() { release(); workers.Wait() }()
	rotated := make(chan struct{})
	var finish func() error
	var rotationErr error
	workers.Go(func() {
		finish, rotationErr = j.RotateAtAutomaticCOWCut()
		close(rotated)
	})
	select {
	case <-entered:
	case <-rotated:
		t.Fatalf("rotation returned before attempting the owed directory sync: %v", rotationErr)
	case <-time.After(5 * time.Second):
		t.Fatal("rotation never reached the owed directory sync")
	}
	if got := j.fileSync.err(); !errors.Is(got, closeFailure) || notifications.Load() != 1 {
		t.Errorf("while directory sync is held: first error=%v, notifications=%d", got, notifications.Load())
	}
	captureResult := make(chan error, 1)
	workers.Go(func() { captureResult <- captured.Sync() })
	select {
	case err := <-captureResult:
		if !errors.Is(err, closeFailure) {
			t.Errorf("predecessor capture after its writer closed with an error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("predecessor capture blocked on the successor directory")
	}
	release()
	workers.Wait()
	if finish != nil || !errors.Is(rotationErr, closeFailure) || !errors.Is(rotationErr, dirFailure) {
		t.Errorf("installed rotation returned finish=%t, err=%v", finish != nil, rotationErr)
	}
	if err := j.Sync(); !errors.Is(err, closeFailure) {
		t.Errorf("later journal sync lost the first error: %v", err)
	}
	captured.Release()
	for i := 0; i < 2; i++ {
		if err := j.Close(); !errors.Is(err, closeFailure) || !errors.Is(err, dirFailure) || !errors.Is(err, syscall.EIO) || !errors.Is(err, syscall.ENOSPC) {
			t.Errorf("checked Close %d: %v", i+1, err)
		}
	}
	if starts.Load() != 1 || completions.Load() != 1 || notifications.Load() != 1 {
		t.Fatalf("directory starts=%d, completions=%d, failure notifications=%d", starts.Load(), completions.Load(), notifications.Load())
	}
}
