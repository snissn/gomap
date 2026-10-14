package lifecycle

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"math"
	"testing"
)

func TestReaderRegistryOwnedFallbackRetirementV5(t *testing.T) {
	r := NewReaderRegistry()
	ids := make([]int64, 0, 17)
	for i := 0; i < 17; i++ {
		ids = append(ids, r.Register(uint64(i+1)))
	}
	id := ids[16]
	if id <= 0 || cap(r.free) < len(r.seqs) {
		t.Fatal("actual fallback free capacity missing")
	}
	if ready, e := r.UnregisterWithWorkV5(id, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}); ready || e != nil {
		t.Fatalf("unadmitted fallback %v %v", ready, e)
	}
	if ready, e := r.UnregisterWithWorkV5(id, &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}); !ready || e != nil {
		t.Fatalf("fallback %v %v", ready, e)
	}
	for _, id := range ids[:16] {
		if ready, e := r.UnregisterWithWorkV5(id, &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}); !ready || e != nil {
			t.Fatalf("fast %v %v", ready, e)
		}
	}
	if r.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("real registry edge leaked")
	}
}
