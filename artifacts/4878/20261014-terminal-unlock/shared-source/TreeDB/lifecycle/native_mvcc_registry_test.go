package lifecycle

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
)

func nativeRegistryWork() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}

func TestNativeMVCCRegistryOriginalCohorts(t *testing.T) {
	r := NewReaderRegistry()
	var original [FastReaderShardCount]int64
	for i := range original {
		original[i], _ = r.RegisterFastWithHint(uint64(i+1), i)
	}
	for _, sequence := range []uint64{3, 100} {
		var state NativeMVCCRegistration
		if ready, err := r.BeginNativeMVCCRegistration(&state, sequence, 0, &iterator.OrdinalScanWork{}); ready || err != nil || state.registry != nil {
			t.Fatal("refusal created authority")
		}
		if ready, err := r.BeginNativeMVCCRegistration(&state, sequence, 0, nativeRegistryWork()); !ready || err != nil {
			t.Fatal(ready, err)
		}
		var id int64
		for turn := 0; ; turn++ {
			if turn == 100 {
				t.Fatal("stalled")
			}
			before := state
			if _, _, done, err := r.StepNativeMVCCRegistration(&state, &iterator.OrdinalScanWork{}); done || err != nil || state != before {
				t.Fatal("refusal advanced probe")
			}
			work := nativeRegistryWork()
			var done bool
			var err error
			id, _, done, err = r.StepNativeMVCCRegistration(&state, work)
			if err != nil || work.Records > 32 || work.Bytes > 1<<20 {
				t.Fatal("unbounded registration", err)
			}
			if done {
				break
			}
		}
		if sequence == 3 && id != original[2] || sequence == 100 && id != 0 {
			t.Fatal("original cohort sharing/refusal changed", id)
		}
		if len(r.seqs) != 0 || len(r.free) != 0 {
			t.Fatal("fallback registry grew")
		}
		if done, err := r.CancelNativeMVCCRegistration(&state, nativeRegistryWork()); !done || err != nil {
			t.Fatal(done, err)
		}
	}
	for _, id := range original {
		r.Unregister(id)
	}
	var state NativeMVCCRegistration
	r.BeginNativeMVCCRegistration(&state, 100, 0, nativeRegistryWork())
	for turn := 0; ; turn++ {
		if turn == 100 {
			t.Fatal("claim stalled")
		}
		id, _, done, err := r.StepNativeMVCCRegistration(&state, nativeRegistryWork())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if id >= 0 || r.MinPinnedSeq() != 100 {
				t.Fatal("current claim not visible")
			}
			break
		}
	}
	if ready, err := r.CancelNativeMVCCRegistration(&state, nativeRegistryWork()); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if r.MinPinnedSeq() != ^uint64(0) {
		t.Fatal("original registry pin survived physical release")
	}
}
