package db

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

type checkpointTestLeafPageLog struct {
	flushes atomic.Uint64
	syncs   atomic.Uint64
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
