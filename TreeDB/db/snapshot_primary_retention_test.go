package db

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
)

func TestPrimaryMetadataCaptureFinalizesAfterLastRead(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.Set([]byte("key"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	s := d.AcquireSnapshot()
	a := s.primaryRoot.arena
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	e, err := retainedalloc.EnrollPair(a.MetadataOwner(), d.valueLogIdentityPins.MetadataOwner(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AdoptPrimaryMetadataEnrollment(e); err != nil {
		t.Fatal(err)
	}
	if err = s.beginRead(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().ExternalBytes == 0 {
		t.Fatal("Close refunded active read")
	}
	if err = d.Set([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	s.endRead()
	if budget.Stats().ExternalBytes != 0 {
		t.Fatal("last read did not settle exact queue/governor")
	}
	if err = d.Set([]byte("later"), []byte("value")); err != nil {
		t.Fatal(err)
	}
}
func TestPrimaryMetadataCaptureWrongOwnerRefusesWithoutTransfer(t *testing.T) {
	open := func() *DB {
		d, e := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { d.Close() })
		return d
	}
	a, b := open(), open()
	s := a.AcquireSnapshot()
	defer s.Close()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	e, err := retainedalloc.EnrollPair(b.idx.Load().primary.MetadataOwner(), b.valueLogIdentityPins.MetadataOwner(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.AdoptPrimaryMetadataEnrollment(e); err == nil {
		t.Fatal("wrong physical owner accepted")
	}
	if budget.Stats().ExternalBytes == 0 {
		t.Fatal("refusal consumed enrollment")
	}
	e.Close()
	if budget.Stats().ExternalBytes != 0 {
		t.Fatal("caller unwind leaked")
	}
}

func TestPrimarySnapshotPhysicalOwnerSurvivesDBCloseAndLastRead(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "bounded"}[bounded], func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if err = d.SetSync([]byte("key"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			var s *Snapshot
			if bounded {
				s, err = d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
			} else {
				s = d.AcquireSnapshot()
			}
			if err != nil || s == nil {
				t.Fatal(err)
			}
			a, owner := s.primaryRoot.arena, s.primaryOwner
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			defer budget.Close()
			e, err := retainedalloc.EnrollPair(a.MetadataOwner(), d.valueLogIdentityPins.MetadataOwner(), budget)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.AdoptPrimaryMetadataEnrollment(e); err != nil {
				t.Fatal(err)
			}
			if err = s.beginRead(); err != nil {
				t.Fatal(err)
			}
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
			if a.MetadataOwner().PhysicalClosed() || owner.refs == 0 || budget.Stats().ExternalBytes == 0 {
				t.Fatal("DB close stole active PRIMARY physical owner")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if a.MetadataOwner().PhysicalClosed() {
				t.Fatal("snapshot close stole active read physical owner")
			}
			s.endRead()
			if !a.MetadataOwner().PhysicalClosed() || owner.refs != 0 || s.primaryOwner != nil || s.primaryRoot != nil || budget.Stats().ExternalBytes != 0 {
				t.Fatal("last read did not finish physical owner and governor")
			}
			if err = s.Close(); err != nil || budget.Stats().ExternalBytes != 0 {
				t.Fatal("repeated close changed terminal result")
			}
		})
	}
}

func TestPrimaryOneShotFailedCleanupPreservesOriginalSnapshot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	r, err := d.acquireOneShotReadOrErr()
	if err != nil {
		t.Fatal(err)
	}
	owner, original := r.snapshot.primaryOwner, r.snapshot.primaryRoot
	// Corrupt the private finalizer operand to exercise failed original cleanup.
	bad := *original
	bad.ref.Incarnation++
	r.snapshot.primaryRoot = &bad
	if err = r.close(); err == nil {
		t.Fatal("failed cleanup missing")
	}
	if owner.failedSnapshots != &r.snapshot || r.snapshot.primaryRoot != &bad || r.snapshot.primaryOwner != owner || r.snapshot.treePager == nil {
		t.Fatal("one-shot reuse/scrub discarded failed original custody")
	}
	// Restore the actual bank operand only for test teardown; the failed owner
	// deliberately retains its existing edge and reports cleanup debt to DB.Close.
	r.snapshot.primaryRoot = original
	if _, err := original.arena.Drop(original.ref, nil); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()                           // Historical injected debt remains observable.
	_, _, _ = owner.releaseWithOutcome(nil) // Dispose the fixture's physical edge.
}
