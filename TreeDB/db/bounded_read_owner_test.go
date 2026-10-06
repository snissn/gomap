package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
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
