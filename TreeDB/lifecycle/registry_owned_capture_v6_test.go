package lifecycle

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"math"
	"testing"
)

func TestOwnedReaderRegistrationOneChargedCell(t *testing.T) {
	r := NewReaderRegistry()
	empty := &iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1024}
	if id, attempted, e := r.RegisterOneFastWithWorkV6(90, 3, empty); id != 0 || attempted || e != nil {
		t.Fatalf("empty %d %v %v", id, attempted, e)
	}
	if r.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("empty admission mutated registry")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1024}
	id, attempted, e := r.RegisterOneFastWithWorkV6(90, 3, w)
	if e != nil || !attempted || id != makeFastHandle(3) || w.Records != 1 {
		t.Fatalf("claim %d %v %v %+v", id, attempted, e, w)
	}
	w = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1024}
	if id2, _, e := r.RegisterOneFastWithWorkV6(89, 3, w); e != nil || id2 != 0 || w.Records != 1 {
		t.Fatal("foreign cohort was replaced")
	}
	w = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1024}
	joined, _, e := r.RegisterOneFastWithWorkV6(90, 3, w)
	if e != nil || joined != id || r.fastShards[3].count.Load() != 2 {
		t.Fatal("same sequence did not share exact cohort")
	}
	r.Unregister(id)
	r.Unregister(joined)
	w = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1024}
	low, _, e := r.RegisterOneFastWithWorkV6(8, 3, w)
	if e != nil || low != id || r.MinPinnedSeq() != 8 {
		t.Fatal("recycled cell exposed stale higher minimum")
	}
	r.Unregister(low)
	if len(r.seqs) != 0 || len(r.free) != 0 || r.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("bounded registration allocated fallback storage")
	}
}
