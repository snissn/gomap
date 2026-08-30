package caching

import (
	"errors"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type checkpointBoundaryBackend struct {
	BackendDB
	err               error
	calls             int
	maintenanceErr    error
	maintenanceCalls  int
	checkpointStarted chan struct{}
	checkpointRelease <-chan struct{}
}

func (b *checkpointBoundaryBackend) Checkpoint() error {
	b.calls++
	if b.checkpointStarted != nil {
		close(b.checkpointStarted)
	}
	if b.checkpointRelease != nil {
		<-b.checkpointRelease
	}
	return b.err
}

func (b *checkpointBoundaryBackend) MaintainCommandWALCoveredPrefix() error {
	b.maintenanceCalls++
	return b.maintenanceErr
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

func TestBackendAutomaticMaintenanceBoundaryDoesNotForceCheckpoint(t *testing.T) {
	release := make(chan struct{})
	backend := &checkpointBoundaryBackend{
		checkpointStarted: make(chan struct{}),
	}

	if err := backendAutomaticMaintenanceBoundary(backend); err != nil {
		t.Fatalf("backendAutomaticMaintenanceBoundary: %v", err)
	}
	if backend.maintenanceCalls != 1 {
		t.Fatalf("covered-prefix calls=%d, want 1", backend.maintenanceCalls)
	}
	if backend.calls != 0 {
		t.Fatalf("checkpoint calls=%d, want 0", backend.calls)
	}

	backend.checkpointRelease = release
	explicitDone := make(chan error, 1)
	go func() { explicitDone <- backendSyncBoundary(backend) }()
	select {
	case <-backend.checkpointStarted:
	case <-time.After(withRaceTimeout(time.Second)):
		t.Fatal("explicit checkpoint did not reach held visible frontier")
	}
	select {
	case err := <-explicitDone:
		t.Fatalf("explicit checkpoint returned early: %v", err)
	default:
	}
	close(release)
	if err := <-explicitDone; err != nil {
		t.Fatalf("explicit checkpoint: %v", err)
	}
}

func TestCachingDBBackendCheckpointBoundaryUsesCoveredPrefixOnlyForExternalCommandWAL(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		automatic            bool
		externalCommandWAL   bool
		wantCheckpoints      int
		wantCoveredPrefixOps int
	}{
		{name: "automatic legacy", automatic: true, wantCheckpoints: 1},
		{name: "automatic external command WAL", automatic: true, externalCommandWAL: true, wantCoveredPrefixOps: 1},
		{name: "explicit external command WAL", externalCommandWAL: true, wantCheckpoints: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := &checkpointBoundaryBackend{}
			db := &DB{backend: backend, externalCommandWAL: tc.externalCommandWAL}
			if err := db.backendCheckpointBoundary(tc.automatic); err != nil {
				t.Fatalf("backendCheckpointBoundary: %v", err)
			}
			if got := backend.calls; got != tc.wantCheckpoints {
				t.Fatalf("checkpoint calls=%d want %d", got, tc.wantCheckpoints)
			}
			if got := backend.maintenanceCalls; got != tc.wantCoveredPrefixOps {
				t.Fatalf("covered-prefix calls=%d want %d", got, tc.wantCoveredPrefixOps)
			}
		})
	}
}

func TestBackendAutomaticMaintenanceBoundaryRetainsStaleCleanupProof(t *testing.T) {
	backend := &checkpointBoundaryBackend{maintenanceErr: backenddb.ErrDurableWALCleanupProofStale}

	if err := backendAutomaticMaintenanceBoundary(backend); err != nil {
		t.Fatalf("backendAutomaticMaintenanceBoundary error=%v, want stale proof retained for retry", err)
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
