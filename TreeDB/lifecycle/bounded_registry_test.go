package lifecycle

import "testing"

func TestReaderRegistryFastOnlyRefusalAndDrainProgress(t *testing.T) {
	r := NewReaderRegistry()
	var handles [FastReaderShardCount]int64
	for i := range handles {
		handles[i], _ = r.RegisterFastWithHint(uint64(i+1), -1)
		if handles[i] >= 0 {
			t.Fatal("fast cohort not admitted")
		}
	}
	if allocations := testing.AllocsPerRun(100, func() {
		id, _ := r.RegisterFastWithHint(100, -1)
		if id != 0 {
			t.Fatal("overflow admitted")
		}
	}); allocations != 0 {
		t.Fatalf("refusal allocated %g", allocations)
	}
	if len(r.seqs) != 0 || len(r.free) != 0 {
		t.Fatal("bounded registration touched fallback storage")
	}
	joined, _ := r.RegisterFastWithHint(1, -1)
	if joined != handles[0] {
		t.Fatal("same sequence did not share cohort")
	}
	r.Unregister(joined)
	r.Unregister(handles[0])
	next, _ := r.RegisterFastWithHint(100, -1)
	if next >= 0 {
		t.Fatal("draining cohort failed to restore progress")
	}
	r.Unregister(next)
	for _, h := range handles[1:] {
		r.Unregister(h)
	}
	if len(r.seqs) != 0 || len(r.free) != 0 {
		t.Fatal("fast release grew fallback storage")
	}
}
