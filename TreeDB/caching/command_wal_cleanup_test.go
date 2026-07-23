package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCachingDBCheckpointCleanupTreatsStaleProofAsRetryable(t *testing.T) {
	database := &DB{}
	calls := 0
	database.SetCommandWALCheckpointCleanupHook(func(bool) error {
		calls++
		return backenddb.ErrDurableWALCleanupProofStale
	})

	if err := database.cleanupCommandWALCheckpoint(true); err != nil {
		t.Fatalf("cleanupCommandWALCheckpoint error=%v, want stale proof retained for retry", err)
	}
	if calls != 1 {
		t.Fatalf("cleanup hook calls=%d, want 1", calls)
	}
}

func TestCachingDBCheckpointCleanupPropagatesUnexpectedError(t *testing.T) {
	want := errors.New("cleanup failed")
	database := &DB{}
	database.SetCommandWALCheckpointCleanupHook(func(bool) error { return want })

	if err := database.cleanupCommandWALCheckpoint(true); !errors.Is(err, want) {
		t.Fatalf("cleanupCommandWALCheckpoint error=%v, want %v", err, want)
	}
}
