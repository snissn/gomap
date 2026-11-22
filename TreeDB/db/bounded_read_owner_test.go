package db

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestCOWStableReadSnapshotAdmissionAndRelease(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	refusal := errors.New("admission denied")
	if s, err := d.AcquireStableSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return refusal }); s != nil || err != refusal {
		t.Fatalf("refused capture: %v %v", s, err)
	}
	if d.stableIndexCaptures.Load() != 0 {
		t.Fatal("refusal acquired maintenance owner")
	}
	if err := d.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	s, err := d.AcquireStableSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if d.stableIndexCaptures.Load() != 1 {
		t.Fatal("stable read owner missing")
	}
	if err := d.Set([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := s.GetEntryExactWithFixedScratch([]byte("key"), keyScratch[:], leafScratch[:], nil)
	if err != nil || !bytes.Equal(entry.Value, []byte("old")) {
		t.Fatalf("captured basis: %q %v", entry.Value, err)
	}
	s.Close()
	s.Close()
	if d.stableIndexCaptures.Load() != 0 {
		t.Fatal("stable read owner leaked")
	}
}

func TestCOWBuildGroupRejectsChangedUserBasisBeforeApply(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	basis, err := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer basis.Close()
	g, err := d.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	if g.baseRoot != basis.treeRoot || g.idx != basis.idx {
		t.Fatal("wrong captured build basis")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	before := d.rootPublication.coordinator.Stats()
	if g, err := d.BeginRootPublicationBuildGroupFromSnapshot(basis); g != nil || !errors.Is(err, ErrRootPublicationBasisMismatch) {
		t.Fatalf("basis mismatch: %v %v", g, err)
	}
	after := d.rootPublication.coordinator.Stats()
	if after.ActiveBuilders != before.ActiveBuilders || after.VisibleCommitSeq != before.VisibleCommitSeq || after.PendingCommits != before.PendingCommits {
		t.Fatal("refused basis changed publication")
	}
	if got, err := d.Get([]byte("key")); err != nil || string(got) != "new" {
		t.Fatalf("lost current value: %q %v", got, err)
	}
	// The refusal released all build guards: a fresh exact basis progresses.
	fresh, err := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	g, err = d.BeginRootPublicationBuildGroupFromSnapshot(fresh)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
}

func TestCOWFixedScratchPointKeepsOneReadPinAcrossClose(t *testing.T) {
	d, err := Open(compactPersistenceProofOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	expected, _ := writeCompactPersistenceFixture(t, d)
	requireCompactPersistenceLeafLogRoot(t, d)
	key := compactPersistenceKey(0)
	want := expected[string(key)]
	s, err := d.AcquireStableSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	limits := valuelog.COWReadLimits{MaxRecordBytes: 64 << 10, MaxRawBytes: 64 << 10, MaxValueBytes: page.PageSize}
	entered := make(chan uint64, 1)
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	done := make(chan struct{})
	var readErr error
	go func() {
		defer close(done)
		var keyScratch, leafScratch [page.PageSize]byte
		readLeaf := func(_ page.LeafLogPtr, dst []byte, record PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error) {
			entered <- s.readState.Load() &^ snapshotReadClosedBit
			<-resume
			data, err := record.ReadPage(make([]byte, record.Shape().RecordBytes), nil, dst, nil)
			return data, tree.LeafLogPageReadState{}, err
		}
		entry, err := s.GetEntryExactWithPinnedCOWLeaf(key, keyScratch[:], leafScratch[:], limits, readLeaf)
		readErr = err
		if err == nil && (entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 || !bytes.Equal(entry.Value, want)) {
			readErr = fmt.Errorf("persisted inline value: got %q flags=%d, want %q", entry.Value, entry.Flags, want)
		}
	}()
	defer func() {
		release()
		<-done
	}()

	select {
	case pins := <-entered:
		if pins != 1 {
			t.Fatalf("point callback holds %d backend read pins, want 1", pins)
		}
	case <-done:
		t.Fatalf("logged-leaf callback was not invoked: %v", readErr)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.readState.Load(); got != snapshotReadClosedBit|1 || s.finalized.Load() || d.stableIndexCaptures.Load() != 1 {
		t.Fatalf("close released the admitted reader: read state=%x finalized=%t captures=%d", got, s.finalized.Load(), d.stableIndexCaptures.Load())
	}
	var keyScratch, leafScratch [page.PageSize]byte
	if _, err = s.GetEntryExactWithPinnedCOWLeaf(key, keyScratch[:], leafScratch[:], limits, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after Close: got %v, want %v", err, ErrClosed)
	}
	release()
	<-done
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := s.readState.Load(); got != snapshotReadClosedBit || !s.cleanupFinished || s.cleanupErr != nil || d.stableIndexCaptures.Load() != 0 {
		t.Fatalf("read did not finalize the closed capture: read state=%x finished=%t cleanup=%v captures=%d", got, s.cleanupFinished, s.cleanupErr, d.stableIndexCaptures.Load())
	}
}
