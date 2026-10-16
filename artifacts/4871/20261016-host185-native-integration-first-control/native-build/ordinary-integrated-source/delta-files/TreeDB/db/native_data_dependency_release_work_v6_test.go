package db

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestNativeDataDependencyReleaseRechecksCurrentGenerationV6(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "current", true: "retired-after-eligibility"}[retire], func(t *testing.T) {
			d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			idx := d.idx.Load()
			charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dataDependencyLeaseV6{})))
			if err := idx.metadata.AddPending(charge); err != nil {
				t.Fatal(err)
			}
			lease := &dataDependencyLeaseV6{db: d, generation: idx, registryID: idx.registry.Register(1), metadata: idx.metadata, charge: charge}
			lease.failure.payload = lease
			idx.retainWrapperBorrow()
			if !lease.SupportsBoundedDependencyDirectoryReleaseV6() {
				t.Fatal("current original lease was excluded")
			}
			borrows, id, bytes := idx.wrapperBorrowCountV6(), lease.registryID, idx.metadata.Bytes()
			if consumed, ready, _, err := lease.ReleaseDependencyDirectoryWorkV6(&iterator.OrdinalScanWork{}); consumed || ready || err != nil || lease.registryID != id || idx.wrapperBorrowCountV6() != borrows || idx.metadata.Bytes() != bytes {
				t.Fatalf("zero-work refusal changed custody: %v %v %v", consumed, ready, err)
			}
			if retire {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				borrows = idx.wrapperBorrowCountV6()
				if consumed, ready, _, err := lease.ReleaseDependencyDirectoryWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); consumed || ready || !errors.Is(err, retainedalloc.ErrWorkRequired) || lease.registryID != id || idx.wrapperBorrowCountV6() != borrows || lease.generation != idx || lease.charge != charge {
					t.Fatalf("late retirement consumed original custody: %v %v %v", consumed, ready, err)
				}
			}
			w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			if retire {
				w = nil
			}
			consumed, ready, completion, err := lease.ReleaseDependencyDirectoryWorkV6(w)
			if !consumed || !ready || err != nil || !lease.closed || lease.generation != nil || lease.db != nil || lease.metadata != nil || lease.charge != 0 || lease.failure.payload != nil {
				t.Fatalf("original completion did not scrub custody: %v %v %v", consumed, ready, err)
			}
			if completion.Charge == 0 {
				t.Fatal("original admitted lease lost completion")
			}
			completion.Metadata.RemovePending(completion.Charge)
		})
	}
}
