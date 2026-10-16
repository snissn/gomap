package db

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// Pre-adoption only: neither vector reaches allocator activation or publishes.
func TestNativePublishedOutputAdmissionBeforeAdoptionV6(t *testing.T) {
	for _, limit := range []uint64{1, 2, 5} {
		t.Run(string(rune('0'+limit)), func(t *testing.T) {
			d := &DB{}
			old := &DBState{CommitSeq: 0}
			view := &snapshotView{state: old}
			d.state.Store(old)
			d.snapshotViewRO.Store(view)
			install := &rootPublicationVisibleInstallV1{db: d, idx: &indexGen{}, next: page.MetaPageBody{CommitSeq: 1}, conditionalMutation: conditionalCommitMutation{physicalOnly: true}}
			calls := 0
			w := iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
			ready, err := install.activateWithWorkV6(&w, func(*iterator.OrdinalScanWork) (bool, error) { calls++; return false, nil })
			if ready || err != nil || calls != 0 || install.activated || d.state.Load() != old || d.snapshotViewRO.Load() != view || d.meta.CommitSeq != 0 || install.post.oldState != nil {
				t.Fatal("refusal crossed adoption/publication", ready, err, calls)
			}
			if limit == 1 && w.Records != 0 || limit >= 2 && w.Records != 2 {
				t.Fatal("allocation traffic not preadmitted", w)
			}
		})
	}
	w := iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	if !admitActivationHeadersV6(&w) || w.Records != 2 || w.Bytes != uint64(unsafe.Sizeof(DBState{}))+uint64(unsafe.Sizeof(snapshotView{})) {
		t.Fatal("published allocation traffic", w)
	}
}

func TestNativeSnapshotPublicationUsesSameLeafOperandV6(t *testing.T) {
	d := &DB{}
	generations := &leafGenerationView{GenerationOrder: []uint64{1, 2}}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	scope, ready := d.beginSnapshotPublicationV6(generations, &w)
	if !ready {
		t.Fatal("same operand refused")
	}
	if scope.generations != generations || d.state.Load() != nil || d.snapshotViewRO.Load() != nil {
		t.Fatal("private header constructed or operand replaced")
	}
	scope.finish(false)
}
