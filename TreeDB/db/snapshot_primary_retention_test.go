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
