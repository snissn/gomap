package db

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/lifecycle"
)

func TestSnapshotAllocationAdmissionBeforeWrapperAndRegistry(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	refusal := errors.New("test admission refusal")
	admit := func(sizes SnapshotAllocationSizes) error {
		if sizes.Wrapper != uint64(unsafe.Sizeof(Snapshot{})) {
			t.Fatal("wrapper bound")
		}
		return refusal
	}
	if allocations := testing.AllocsPerRun(100, func() {
		s, err := d.AcquireSnapshotWithAllocationAdmission(admit)
		if s != nil || err != refusal {
			t.Fatal("admission refusal lost")
		}
	}); allocations != 0 {
		t.Fatalf("refused capture allocated %g", allocations)
	}
	if d.MinPinnedSnapshotCommitSeq() != ^uint64(0) {
		t.Fatal("refusal registered a reader")
	}
}

func TestSnapshotAllocationAdmissionFixedCohortPressure(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var snapshots [lifecycle.FastReaderShardCount]*Snapshot
	defer func() {
		for _, s := range snapshots {
			if s != nil {
				s.Close()
			}
		}
	}()
	admit := func(SnapshotAllocationSizes) error { return nil }
	for i := range snapshots {
		if err := d.Set([]byte("key"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		snapshots[i], err = d.AcquireSnapshotWithAllocationAdmission(admit)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Set([]byte("key"), []byte("next")); err != nil {
		t.Fatal(err)
	}
	if s, err := d.AcquireSnapshotWithAllocationAdmission(admit); s != nil || err != ErrSnapshotCapacity {
		t.Fatalf("cohort pressure: snapshot=%v err=%v", s, err)
	}
	// Legacy capture still uses its established growable fallback.
	legacy := d.AcquireSnapshot()
	if legacy == nil {
		t.Fatal("bounded pressure changed legacy capture")
	}
	legacy.Close()
	snapshots[0].Close()
	snapshots[0] = nil
	restored, err := d.AcquireSnapshotWithAllocationAdmission(admit)
	if err != nil {
		t.Fatal(err)
	}
	restored.Close()
}

func TestSnapshotAllocationAdmissionRequiresSharedLeafPinSet(t *testing.T) {
	idx := newSnapshotFixtureIndexes(t, 1)[0]
	state := &DBState{CommitSeq: 1, RootPageID: 1, LeafGenerations: &leafGenerationView{GenerationOrder: []uint64{1}}}
	d := &DB{snapPool: NewSnapshotPool()}
	d.publishSnapshotView(idx, state, nil)
	called := false
	s, err := d.AcquireSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { called = true; return nil })
	if s != nil || err != ErrSnapshotCapacity || called {
		t.Fatalf("manual pin refusal: snapshot=%v err=%v admitted=%v", s, err, called)
	}
	if d.leafGenerationPinCountForTesting(1) != 0 {
		t.Fatal("manual refusal created leaf pins")
	}
	refs := d.leafGenerationPins.refsForGenerationIDs([]uint64{1})
	state.LeafGenerations.PinRefs = refs
	state.LeafGenerations.PinSet = newLeafGenerationPinSet(refs)
	var sizes SnapshotAllocationSizes
	s, err = d.AcquireSnapshotWithAllocationAdmission(func(got SnapshotAllocationSizes) error { sizes = got; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if sizes.PinSet != uint64(unsafe.Sizeof(leafGenerationPinSet{})) || sizes.Refs != uint64(cap(refs))*uint64(unsafe.Sizeof((*leafGenerationPinRef)(nil))) || sizes.PinRefCount != 1 || sizes.PinRef != uint64(unsafe.Sizeof(leafGenerationPinRef{})) || sizes.IDs != 8 {
		t.Fatalf("shared pin capacity omitted: %+v", sizes)
	}
	if cap(s.leafGenerationRefs) != 0 || s.leafGenerationPinSet != state.LeafGenerations.PinSet {
		t.Fatal("bounded capture recreated shared pins")
	}
	s.Close()
	if state.LeafGenerations.PinSet.holders != 0 {
		t.Fatal("shared pin holder survived close")
	}
}
