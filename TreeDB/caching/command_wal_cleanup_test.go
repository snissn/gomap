package caching

import (
	"errors"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type checkpointBoundaryBackend struct {
	BackendDB
	err                error
	calls              int
	maintenanceErr     error
	maintenanceCalls   int
	prefixCloseErr     error
	prefixClosePending bool
	prefixCloseCalls   int
	cleanupComplete    bool
	cleanupErr         error
	cleanupCalls       int
	cleanupStarted     chan struct{}
	cleanupRelease     <-chan struct{}
	cleanupStartedOnce sync.Once
	checkpointStarted  chan struct{}
	checkpointRelease  <-chan struct{}
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

func (b *checkpointBoundaryBackend) PrepareCommandWALCoveredPrefixCleanup() (bool, error) {
	b.prefixCloseCalls++
	return b.prefixClosePending, b.prefixCloseErr
}

func (b *checkpointBoundaryBackend) CleanupCommandWALCoveredPrefix() (bool, error) {
	b.cleanupCalls++
	if b.cleanupStarted != nil {
		b.cleanupStartedOnce.Do(func() { close(b.cleanupStarted) })
	}
	if b.cleanupRelease != nil {
		<-b.cleanupRelease
	}
	return b.cleanupComplete, b.cleanupErr
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
		name               string
		automatic          bool
		externalCommandWAL bool
		wantCheckpoints    int
		wantPrefixCloseOps int
	}{
		{name: "automatic legacy", automatic: true, wantCheckpoints: 1},
		{name: "automatic external command WAL", automatic: true, externalCommandWAL: true, wantPrefixCloseOps: 1},
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
			if got := backend.prefixCloseCalls; got != tc.wantPrefixCloseOps {
				t.Fatalf("covered-prefix close calls=%d want %d", got, tc.wantPrefixCloseOps)
			}
		})
	}
}

func TestAutoCheckpointCoveredPrefixCleanupRunsAfterWriterAdmissionRelease(t *testing.T) {
	releaseCleanup := make(chan struct{})
	backend := &checkpointBoundaryBackend{
		BackendDB:          NewMockBackend(),
		prefixClosePending: true,
		cleanupComplete:    true,
		cleanupStarted:     make(chan struct{}),
		cleanupRelease:     releaseCleanup,
	}
	database, err := Open(t.TempDir(), backend, Options{
		ExternalCommandWAL: true,
		FlushThreshold:     1 << 30,
		JournalLanes:       1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = database.Close() }()
	database.SetCommandWALCheckpointCutoverHook(func() {})
	database.SetAutoCheckpointWALBytesHook(func() int64 { return 1 })
	writePoint := func(key string) error {
		return database.SetAfterCommandWALAppend([]byte(key), []byte("value"), func() error { return nil })
	}
	if err := writePoint("pre-cut"); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	done := make(chan struct{})
	go func() {
		database.maybeAutoCheckpoint(0, autoCheckpointModeForce)
		close(done)
	}()
	select {
	case <-backend.cleanupStarted:
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("post-release cleanup did not start")
	}
	if database.checkpointing.Load() {
		t.Fatal("post-release cleanup still owns cache checkpoint admission")
	}

	writeDone := make(chan error, 1)
	go func() { writeDone <- writePoint("post-cut") }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("post-cut write: %v", err)
		}
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("post-cut write blocked behind cleanup")
	}
	// Automatic requests that arrive while cleanup is active coalesce into one
	// later debt bit; the completing pass must not clear it.
	database.requestCommandWALCleanup()
	database.requestCommandWALCleanup()
	close(releaseCleanup)
	select {
	case <-done:
	case <-time.After(withRaceTimeout(2 * time.Second)):
		t.Fatal("automatic checkpoint did not finish after cleanup release")
	}
	if total, critical := database.autoCheckpointLastDurNanos.Load(), int64(database.checkpointStageAutoCriticalSection.maxNs.Load()); total <= critical {
		t.Fatalf("automatic checkpoint duration=%d want whole worker pass greater than critical section=%d", total, critical)
	}
	if !database.commandWALCleanupPending.Load() {
		t.Fatal("cleanup completion dropped debt requested while the pass was active")
	}
	database.runPendingCommandWALCleanup()
	if got := backend.cleanupCalls; got != 2 {
		t.Fatalf("cleanup calls=%d want one active pass plus one coalesced later pass", got)
	}
	if database.commandWALCleanupPending.Load() {
		t.Fatal("cleanup debt remained after later pass")
	}
	if got := database.commandWALCleanupCoalesced.Load(); got != 1 {
		t.Fatalf("requests coalesced while cleanup active=%d want 1", got)
	}
}

func TestAutoCheckpointCoveredPrefixCleanupCoalescesAndRetries(t *testing.T) {
	backend := &checkpointBoundaryBackend{cleanupComplete: false}
	database := &DB{backend: backend}
	database.requestCommandWALCleanup()
	database.requestCommandWALCleanup()
	database.requestCommandWALCleanup()

	database.runPendingCommandWALCleanup()
	if got := backend.cleanupCalls; got != 1 {
		t.Fatalf("cleanup calls after coalesced request=%d want 1", got)
	}
	if !database.commandWALCleanupPending.Load() {
		t.Fatal("retryable cleanup outcome dropped debt")
	}
	if got := database.commandWALCleanupCoalesced.Load(); got != 2 {
		t.Fatalf("coalesced requests=%d want 2", got)
	}

	backend.cleanupComplete = true
	database.runPendingCommandWALCleanup()
	if got := backend.cleanupCalls; got != 2 {
		t.Fatalf("cleanup calls after later event=%d want 2", got)
	}
	if database.commandWALCleanupPending.Load() {
		t.Fatal("cleanup debt remained after retry succeeded")
	}
	if got := database.commandWALCleanupCompletions.Load(); got != 1 {
		t.Fatalf("cleanup completions=%d want 1", got)
	}
}

func TestAutoCheckpointCoveredPrefixCleanupFailureRetainsDebtAndReports(t *testing.T) {
	want := errors.New("namespace sync failed")
	backend := &checkpointBoundaryBackend{cleanupErr: want}
	database := &DB{backend: backend}
	database.requestCommandWALCleanup()
	database.runPendingCommandWALCleanup()

	if !database.commandWALCleanupPending.Load() {
		t.Fatal("unexpected cleanup failure dropped debt")
	}
	if err := database.backgroundError(); !errors.Is(err, want) {
		t.Fatalf("background error=%v want %v", err, want)
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
