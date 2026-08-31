package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

type checkpointTestLeafPageLog struct {
	flushes atomic.Uint64
	syncs   atomic.Uint64
}

func TestCheckpointReleasesCommandWALAdmissionBeforeCleanupMaintenance(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()
	b := d.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	d.maintenanceMu.Lock()
	d.commandWALRawPublishMu.Lock()
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- d.Checkpoint() }()

	deadline := time.Now().Add(time.Second)
	for d.commandWALRawAdmissionMu.TryLock() {
		d.commandWALRawAdmissionMu.Unlock()
		if time.Now().After(deadline) {
			d.commandWALRawPublishMu.Unlock()
			d.maintenanceMu.Unlock()
			<-checkpointDone
			t.Fatal("Checkpoint did not acquire command-WAL admission")
		}
		runtime.Gosched()
	}
	d.commandWALRawPublishMu.Unlock()
	for {
		if d.commandWALRawAdmissionMu.TryLock() {
			d.commandWALRawAdmissionMu.Unlock()
			break
		}
		if time.Now().After(deadline) {
			d.maintenanceMu.Unlock()
			<-checkpointDone
			t.Fatal("Checkpoint retained command-WAL admission while cleanup waited for maintenance")
		}
		runtime.Gosched()
	}
	d.maintenanceMu.Unlock()
	if err := <-checkpointDone; err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

func (l *checkpointTestLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, nil
}

func (l *checkpointTestLeafPageLog) Flush() error {
	l.flushes.Add(1)
	return nil
}

func (l *checkpointTestLeafPageLog) Sync() error {
	l.syncs.Add(1)
	return nil
}

func TestCheckpointSyncBoundaryDoesNotAdvanceCommitSeq(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:       dir,
		ChunkSize: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	b := d.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	state := d.State()
	if state == nil {
		t.Fatal("missing state after write")
	}
	seq := state.CommitSeq

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := d.State().CommitSeq; got != seq {
		t.Fatalf("Checkpoint advanced CommitSeq: got=%d want=%d", got, seq)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{
		Dir:       dir,
		ChunkSize: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen=%q want %q", got, []byte("v"))
	}
}

func TestMaintainCommandWALCoveredPrefixWithoutCommandWALFallsBackToCheckpoint(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var checkpoints atomic.Uint64
	d.testCheckpointAfterPoisonPreflightHook = func() { checkpoints.Add(1) }

	b := d.NewBatch()
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if err := d.MaintainCommandWALCoveredPrefix(); err != nil {
		t.Fatalf("MaintainCommandWALCoveredPrefix: %v", err)
	}
	if got := checkpoints.Load(); got != 1 {
		t.Fatalf("checkpoint calls=%d want 1", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after reopen=%q want %q", got, []byte("v"))
	}
}

func TestMaintainCommandWALCoveredPrefixPinsTeardown(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		Durability:             DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	maintained := make(chan error, 1)
	d.teardownMu.Lock()
	go func() { maintained <- d.MaintainCommandWALCoveredPrefix() }()
	select {
	case err := <-maintained:
		d.teardownMu.Unlock()
		t.Fatalf("covered-prefix maintenance bypassed teardown admission: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	d.teardownMu.Unlock()
	if err := <-maintained; err != nil {
		t.Fatalf("MaintainCommandWALCoveredPrefix: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMaintainCommandWALCoveredPrefixAcquiresMaintenanceBeforeTeardown(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		Durability:             DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	called := false
	d.testStorageMaintenanceAfterLockHook = func(operation string) error {
		called = true
		if operation != "command-wal-covered-prefix" {
			return fmt.Errorf("maintenance operation=%q", operation)
		}
		if !d.teardownMu.TryLock() {
			return errors.New("teardown lock held before maintenance admission")
		}
		d.teardownMu.Unlock()
		return nil
	}
	if err := d.MaintainCommandWALCoveredPrefix(); err != nil {
		t.Fatalf("MaintainCommandWALCoveredPrefix: %v", err)
	}
	if !called {
		t.Fatal("maintenance admission hook was not called")
	}
}

func TestMaintainCommandWALCoveredPrefixSnapshotsRootPublicationUnderDBMu(t *testing.T) {
	d, err := Open(Options{
		Dir:                    t.TempDir(),
		CommandWAL:             true,
		Durability:             DurabilityWALOnRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	// Online vacuum swaps rootPublication while holding db.mu. The capability
	// snapshot must use that same lock rather than racing the replacement.
	d.mu.Lock()
	maintained := make(chan error, 1)
	go func() { maintained <- d.MaintainCommandWALCoveredPrefix() }()
	select {
	case err := <-maintained:
		d.mu.Unlock()
		t.Fatalf("covered-prefix maintenance bypassed root-publication snapshot: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	d.mu.Unlock()
	if err := <-maintained; err != nil {
		t.Fatalf("MaintainCommandWALCoveredPrefix: %v", err)
	}
}

func TestMaintainCommandWALCoveredPrefixRetriesClosedSegmentsAfterCoverageAdvance(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                          dir,
		CommandWAL:                   true,
		Durability:                   DurabilityWALOnRelaxed,
		CommandWALSegmentTargetBytes: 1,
		DisableBackgroundPrune:       true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Set([]byte("covered-prefix"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := d.MaintainCommandWALCoveredPrefix(); err != nil {
		t.Fatalf("first MaintainCommandWALCoveredPrefix: %v", err)
	}
	if d.commandWALClosedBytes.Load() <= 0 {
		t.Fatalf("closed command WAL bytes=%d, want retained rotated segment", d.commandWALClosedBytes.Load())
	}

	state := d.State()
	if state == nil || state.AppliedCommandLSN == 0 {
		t.Fatalf("state after first maintenance=%+v, want applied command WAL coverage", state)
	}
	if err := d.publishCommandWALRoots(state.RootPageID, state.SystemRootPageID, state.AppliedCommandLSN, nil, true); err != nil {
		t.Fatalf("advance durable root coverage: %v", err)
	}
	if err := d.rootPublication.coordinator.WaitThrough(context.Background(), d.State().CommitSeq); err != nil {
		t.Fatalf("wait durable root coverage: %v", err)
	}
	if err := d.RefreshCommandWALCheckpointFallback(); err != nil {
		t.Fatalf("advance fallback durable root coverage without WAL append: %v", err)
	}
	proof, err := d.captureDurableWALCleanupProofV1()
	if err != nil {
		t.Fatalf("capture cleanup proof after coverage advance: %v", err)
	}
	if proof.cleanupThrough < state.AppliedCommandLSN || proof.rootCount != 2 {
		t.Fatalf("coverage proof: through=%d roots=%d, want two roots through %d", proof.cleanupThrough, proof.rootCount, state.AppliedCommandLSN)
	}
	pending, err := d.PrepareCommandWALCoveredPrefixCleanup()
	if err != nil {
		t.Fatalf("PrepareCommandWALCoveredPrefixCleanup: %v", err)
	}
	if !pending {
		t.Fatal("covered closed segment did not create cleanup debt")
	}
	if d.commandWALClosedBytes.Load() <= 0 {
		t.Fatal("prefix preparation performed cleanup synchronously")
	}
	if err := d.Set([]byte("post-cut"), []byte("retained")); err != nil {
		t.Fatalf("post-cut Set: %v", err)
	}
	complete, err := d.CleanupCommandWALCoveredPrefix()
	if err != nil {
		t.Fatalf("CleanupCommandWALCoveredPrefix: %v", err)
	}
	if !complete {
		t.Fatal("cleanup proof remained unavailable after durable coverage advanced")
	}
	if d.commandWALClosedBytes.Load() != 0 {
		t.Fatalf("closed command WAL bytes=%d, want covered segment removed without new append", d.commandWALClosedBytes.Load())
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Dir: dir, CommandWAL: true, Durability: DurabilityWALOnRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Get([]byte("covered-prefix"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get after reopen=%q want value", got)
	}
	got, err = reopened.Get([]byte("post-cut"))
	if err != nil {
		t.Fatalf("Get post-cut value after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("retained")) {
		t.Fatalf("Get post-cut value after reopen=%q want retained", got)
	}
	if entries, err := os.ReadDir(WALDirPath(dir)); err != nil || len(entries) == 0 {
		t.Fatalf("command WAL after cleanup entries=%v err=%v, want active successor", entries, err)
	}
}

func TestEmptyBatchWriteSyncWaitsExistingDurableFrontierWithoutInventingDependencies(t *testing.T) {
	d, err := Open(Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	leafLog := &checkpointTestLeafPageLog{}
	d.SetLeafPageLog(leafLog)
	seq := d.State().CommitSeq

	b := d.NewBatch()
	if err := b.WriteSync(); err != nil {
		t.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}
	if got := d.State().CommitSeq; got != seq {
		t.Fatalf("empty WriteSync advanced CommitSeq: got=%d want=%d", got, seq)
	}
	if got := leafLog.syncs.Load(); got != 0 {
		t.Fatalf("leaf log syncs=%d want 0 for an unreferenced log installed after the durable frontier", got)
	}
	if got := leafLog.flushes.Load(); got != 0 {
		t.Fatalf("leaf log flushes=%d want 0", got)
	}
}
