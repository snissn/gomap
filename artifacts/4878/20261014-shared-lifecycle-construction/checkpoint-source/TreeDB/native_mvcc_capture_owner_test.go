package treedb

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"testing"
	"unsafe"
)

// This characterizes the actual capture lifetime seam. It grants no public
// prune/publication/ACK evidence; the unchanged whole fixture remains separate.
func TestNativeMVCCOwnedCaptureOriginalSnapshotLifetime(t *testing.T) {
	opts := OptionsFor(ProfileNoWALFast, t.TempDir())
	opts.DisableSideStores = true
	opts.BackgroundCheckpointInterval = -1
	opts.ValueLog.PointerThreshold = 1
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("old"), []byte("original-persistent-value")); err != nil {
		t.Fatal(err)
	}
	var capture NativeMVCCCaptureV6
	// The public caller retains its preborn control in an original budget lease.
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	b, ready, err := d.NativeMVCCBudgetV6(w)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	containing, err := b.AcquireExternal(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(capture))))
	if err != nil {
		t.Fatal(err)
	}
	ready = false
	for call := 0; call < 4096; call++ {
		w = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		_, done, e := d.StepNativeMVCCOwnedCaptureV6(&capture, w)
		if e != nil {
			t.Fatal("capture", call, e)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual work exceeded", w)
		}
		if done {
			ready = true
			break
		}
	}
	if !ready || capture.Snapshot == nil {
		t.Fatal("capture did not become readable")
	}
	got, err := capture.Snapshot.Get([]byte("old"))
	if err != nil || !bytes.Equal(got, []byte("original-persistent-value")) {
		t.Fatal(got, err)
	}
	if err = d.SetSync([]byte("old"), []byte("new-persistent-value")); err != nil {
		t.Fatal(err)
	}
	got, err = capture.Snapshot.Get([]byte("old"))
	if err != nil || !bytes.Equal(got, []byte("original-persistent-value")) {
		t.Fatal("held physical Snapshot", got, err)
	}
	closed := false
	for call := 0; call < 4096; call++ {
		done, e := capture.CloseNativeMVCCOwnedCaptureV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if e != nil {
			t.Fatal("actual cleanup", call, e)
		}
		if done {
			closed = true
			break
		}
	}
	if !closed || capture.Snapshot != nil {
		t.Fatal("capture physical cleanup incomplete")
	}
	containing.Close()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	if st := b.Stats(); st.TotalBytes != 0 || st.ExternalLeases != 0 {
		t.Fatal("capture/producer governor leaked", st)
	}
}
