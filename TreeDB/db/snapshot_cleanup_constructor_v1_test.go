package db

import (
	"testing"
)

func TestPrivatePointConstructorChurnAndPublicControlNoRefund(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	idx := database.idx.Load()
	owner := idx.creator.OwnerStats()
	_ = owner
	// Capture once so registry/reader initialization is outside the fixed holder plateau.
	r, err := database.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	if err = closeOneShotReadV1(&r); err != nil {
		t.Fatal(err)
	}
	base := idx.creator.OwnerStats()
	for i := 0; i < 64; i++ {
		r, err = database.acquireOneShotReadOrErr()
		if err != nil {
			t.Fatal(err)
		}
		if r.snapshot.originalCleanup != &r.snapshot.originalCleanupStorage {
			t.Fatal("private original control is not inline")
		}
		if r.snapshot.originalCleanup.privateBacking == nil {
			t.Fatal("point holder was public-stamped")
		}
		privateCapsule := r.snapshot.originalCleanup
		if err = privateCapsule.RetainOriginalCleanupV1(); err != nil {
			t.Fatal(err)
		}
		if err = closeOneShotReadV1(&r); err != nil {
			t.Fatal(err)
		}
		if !privateCapsule.ObserveOriginalCleanupV1().Complete() || privateCapsule.creator == nil {
			t.Fatal("private original observer lost its completion or creator")
		}
		if idx.creator.OwnerStats().Live <= base.Live {
			t.Fatal("combined private backing refunded before the last observer")
		}
		privateCapsule.ReleaseOriginalCleanupV1()
		privateCapsule = nil
		if idx.creator.OwnerStats().Live != base.Live {
			t.Fatal("combined private backing survived the last observer")
		}
		if r != nil {
			t.Fatal("private caller alias survived drain")
		}
	}
	got := idx.creator.OwnerStats()
	if got.Live != base.Live || got.Births <= base.Births {
		t.Fatalf("private fixed births retained across churn: base=%+v got=%+v", base, got)
	}
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("public capture")
	}
	if s.originalCleanup.privateBacking != nil {
		t.Fatal("public handle got refundable stamp")
	}
	public := idx.creator.OwnerStats()
	if s.originalCleanup != &s.originalCleanupStorage {
		t.Fatal("public original control is not inline")
	}
	capsule := s.originalCleanup
	if err = capsule.RetainOriginalCleanupV1(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if !capsule.ObserveOriginalCleanupV1().Complete() || capsule.creator == nil || s.treePager != nil || s.idx != nil {
		t.Fatal("independent original observer invalid after Snapshot scrub")
	}
	capsule.ReleaseOriginalCleanupV1()
	capsule = nil
	if idx.creator.OwnerStats().Live != public.Live {
		t.Fatal("caller-retained public control got full refund")
	}
	fresh := database.AcquireSnapshot()
	if fresh == s {
		t.Fatal("stale public handle was reused")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if fresh.closed.Load() {
		t.Fatal("old Close consumed new handle")
	}
	_ = fresh.Close()
}
func TestScanConstructorPrivateOriginAndUnownedRefusal(t *testing.T) {
	indexes := newSnapshotFixtureIndexes(t, 1)
	database := &DB{snapPool: NewSnapshotPool()}
	s, err := database.newScanSnapshotV1(indexes[0].pager, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.idx.creator != nil || !s.scanOnly || s.originalCleanup.privateBacking == nil || !s.pagerCreator.SameOwner(indexes[0].creator) {
		t.Fatal("scanner changed creator or fabricated public registry ownership")
	}
	if err = database.closePrivateSnapshotV1(&s); err != nil {
		t.Fatal(err)
	}
	if s != nil {
		t.Fatal("scanner retained caller alias")
	}
	if _, err = database.newSnapshotHolder(&indexGen{}); err == nil {
		t.Fatal("unowned origin admitted")
	}
}
