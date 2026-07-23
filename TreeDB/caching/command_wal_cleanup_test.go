package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type checkpointBoundaryBackend struct {
	BackendDB
	err   error
	calls int
}

func (b *checkpointBoundaryBackend) Checkpoint() error {
	b.calls++
	return b.err
}

func TestBackendSyncBoundaryTreatsStaleCleanupProofAsRetryable(t *testing.T) {
	backend := &checkpointBoundaryBackend{
		err: errors.Join(
			backenddb.ErrDurableWALCleanupProofStale,
			errors.New("command WAL cleanup snapshot stale"),
		),
	}

	if err := backendSyncBoundary(backend); err != nil {
		t.Fatalf("backendSyncBoundary error=%v, want durable checkpoint success with cleanup retained", err)
	}
	if backend.calls != 1 {
		t.Fatalf("checkpoint calls=%d, want 1", backend.calls)
	}
}

func TestBackendSyncBoundaryPropagatesUnexpectedCheckpointError(t *testing.T) {
	want := errors.New("checkpoint failed")
	backend := &checkpointBoundaryBackend{err: want}

	if err := backendSyncBoundary(backend); !errors.Is(err, want) {
		t.Fatalf("backendSyncBoundary error=%v, want %v", err, want)
	}
}

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
