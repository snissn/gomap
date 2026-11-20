package commitlog

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestJournalFileSyncSeesIndependentNamespaceFailure(t *testing.T) {
	dirError := &os.PathError{Op: "sync", Path: "WAL directory", Err: syscall.ENOSPC}
	dataError := &os.PathError{Op: "sync", Path: "captured WAL file", Err: syscall.EIO}
	for _, tc := range []struct {
		name    string
		dataErr error
	}{{"data-succeeded", nil}, {"both-failed", dataError}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			journal, err := OpenCommandJournal(t.TempDir(), CommandJournalOptions{OnFileSyncError: func() { calls.Add(1) }})
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			s := &journal.fileSync
			started := make(chan struct{})
			resume := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				result <- s.run(func() error {
					close(started)
					<-resume
					return tc.dataErr
				})
			}()
			<-started
			gotDir := s.fail(dirError)
			close(resume)
			gotData := <-result
			if !errors.Is(gotDir, dirError) || !errors.Is(gotData, dirError) || (tc.dataErr != nil && !errors.Is(gotData, tc.dataErr)) {
				t.Fatalf("directory=%v, concurrent file=%v", gotDir, gotData)
			}
			if calls.Load() != 1 || !errors.Is(s.err(), dirError) {
				t.Fatalf("notifications=%d sticky error=%v", calls.Load(), s.err())
			}
			if err := s.run(func() error { t.Fatal("physical I/O after fail-stop"); return nil }); !errors.Is(err, dirError) {
				t.Fatalf("next caller=%v", err)
			}
			for i := 0; i < 2; i++ {
				closed := journal.Close()
				if !errors.Is(closed, dirError) || !errors.Is(closed, syscall.ENOSPC) ||
					(tc.dataErr != nil && (!errors.Is(closed, tc.dataErr) || !errors.Is(closed, syscall.EIO))) {
					t.Fatalf("checked Close %d lost an outstanding file or namespace error: %v", i+1, closed)
				}
			}
		})
	}
}
