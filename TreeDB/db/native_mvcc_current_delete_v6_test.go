package db

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"testing"
)

// An actual persisted-tree reader control. Native publication, snapshot
// physical cleanup and the protected whole-publication ABI are separate gates.
func TestNativeMVCCSnapshotProjectionOriginalPagesV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, key := range []string{"a", "b", "c"} {
		if err := d.Set([]byte(key), []byte("old-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	s, err := d.AcquireStableSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reader := &NativeMVCCFrameReaderV6{Snapshot: s}
	c, err := s.NewNativeMVCCProjectionV6(nil, nil, nil, reader, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// A legal foreground update changes the live owner while this reader retains
	// its original physical snapshot. No frozen fixture map or Get adapter exists.
	if err := d.Set([]byte("c"), []byte("new-c")); err != nil {
		t.Fatal(err)
	}
	var previous tree.NativeMVCCSelectionV6
	for i, key := range []string{"c", "b", "a"} {
		op := tree.ProjectionPrev
		if i == 0 {
			op = tree.ProjectionLast
		}
		if err := c.Begin(op, nil, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); err != nil {
			t.Fatal(err)
		}
		ready := false
		for turn := 0; turn < 100 && !ready; turn++ {
			w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 16384}
			ready, err = c.Step(w)
			if err != nil {
				t.Fatal(err)
			}
			if w.Records > 32 || w.Bytes > 16384 {
				t.Fatal("work escaped quantum")
			}
		}
		if !ready {
			t.Fatal("actual native page reader did not progress")
		}
		if i != 0 {
			if valid, err := s.ValidateNativeMVCCSelectionV6(previous, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); valid || err == nil {
				t.Fatalf("advanced source accepted previous selection: %t %v", valid, err)
			}
		}
		selection, selected, err := c.NativeMVCCSelectionV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if !selected || err != nil || selection.LeafReference().Page < 2 {
			t.Fatalf("actual selection: %t %v ref=%v", selected, err, selection.LeafReference())
		}
		if valid, err := s.ValidateNativeMVCCSelectionV6(selection, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !valid || err != nil {
			t.Fatalf("source identity: %t %v", valid, err)
		}
		previous = selection
		e, valid, err := c.Record(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil || !valid || !bytes.Equal(e.Key, []byte(key)) || !bytes.Equal(e.Value, []byte("old-"+key)) {
			t.Fatalf("record: %q %q %t %v", e.Key, e.Value, valid, err)
		}
	}
	closed := false
	for turn := 0; turn < 100 && !closed; turn++ {
		closed, err = c.CloseStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !closed {
		t.Fatal("consumer close did not progress")
	}
	if got, err := d.Get([]byte("c")); err != nil || string(got) != "new-c" {
		t.Fatalf("foreground value: %q %v", got, err)
	}
}

func TestNativeMVCCFrameRefusalAndPartialCopyV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	s, err := d.AcquireStableSnapshotWithAllocationAdmission(func(SnapshotAllocationSizes) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := NativeMVCCFrameReaderV6{Snapshot: s}
	var dst [page.PageSize]byte
	if ready, err := r.BeginProjectionFrame(page.PageChildRef(s.treeRoot), dst[:], &iterator.OrdinalScanWork{}); ready || err != nil || r.active {
		t.Fatalf("before-effect begin refusal: %t %v", ready, err)
	}
	if ready, err := r.BeginProjectionFrame(page.PageChildRef(s.treeRoot), dst[:], &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatalf("begin %t %v", ready, err)
	}
	if _, ready, err := r.StepProjectionFrame(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1024}); ready || err != nil {
		t.Fatalf("partial %t %v", ready, err)
	}
	if r.copied <= 0 || r.copied >= page.PageSize {
		t.Fatal("no actual partial fixed copy")
	}
	previous := r.copied
	if _, ready, err := r.StepProjectionFrame(&iterator.OrdinalScanWork{}); ready || err != nil || r.copied != previous {
		t.Fatalf("copy refusal %t %v", ready, err)
	}
	if ready, err := r.CancelProjectionFrame(&iterator.OrdinalScanWork{}); ready || err != nil || !r.active {
		t.Fatalf("cleanup refusal %t %v", ready, err)
	}
	if ready, err := r.CancelProjectionFrame(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil || r.active || r.image != nil {
		t.Fatalf("cancel %t %v", ready, err)
	}
}
