package commitlog

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestJournalFileSyncSeesIndependentNamespaceFailure(t *testing.T) {
	dirError := errors.New("directory sync failed")
	dataError := errors.New("data sync failed")
	for _, tc := range []struct {
		name    string
		dataErr error
	}{{"data-succeeded", nil}, {"both-failed", dataError}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			s := journalFileSync{onError: func() { calls.Add(1) }}
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
		})
	}
}
