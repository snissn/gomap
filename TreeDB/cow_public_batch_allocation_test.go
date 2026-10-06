package treedb

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestCOWPublicCommandBatchOwnedStagingAndWrapperAdmission(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			database, _, _ := cowPublicContractOpen(t, profile)
			before := database.cached.COWMemoryStats()
			command := database.NewBatch().(*commandWALPublicBatch)
			if !command.isCOW() || command.cowState.lease == nil || !command.payloadBypass || command.payload.RetainedCap() != 0 {
				t.Fatal("COW wrapper constructed a second payload owner")
			}
			opened := database.cached.COWMemoryStats()
			if opened.ExternalLeases <= before.ExternalLeases || opened.ExternalBytes <= before.ExternalBytes {
				t.Fatal("public wrapper not admitted")
			}
			if _, _, err := command.SetViewWithReplayBytes([]byte("adapter"), []byte("rejected")); !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatal(err)
			}
			if _, err := command.DeleteViewWithReplayBytes([]byte("adapter")); !errors.Is(err, caching.ErrCOWUnsupported) {
				t.Fatal(err)
			}
			if size, err := command.GetByteSize(); err != nil || size != 0 {
				t.Fatalf("unsupported adapter staged data: %d %v", size, err)
			}
			key, value := []byte("owned"), []byte("original")
			if err := command.Set(key, value); err != nil {
				t.Fatal(err)
			}
			key[0], value[0] = 'X', 'X'
			if command.payload.RetainedCap() != 0 {
				t.Fatal("COW staging duplicated payload")
			}
			if err := command.Write(); err != nil {
				t.Fatal(err)
			}
			got, err := database.Get([]byte("owned"))
			if err != nil || !bytes.Equal(got, []byte("original")) {
				t.Fatalf("owned staging: %q %v", got, err)
			}
			closing := database.cached.COWMemoryStats()
			if err := command.Close(); err != nil {
				t.Fatal(err)
			}
			after := database.cached.COWMemoryStats()
			if closing.ExternalLeases-after.ExternalLeases != 2 {
				t.Fatal("closed public batch kept staging owners")
			}
			// Exhaust the actual shared authority. Refused constructors and every
			// method use immutable carriers, including repeated/concurrent Close.
			s := database.cached.COWMemoryStats()
			limits := memtable.DefaultCOWLimits()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			pressure, err := database.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - s.ReservedBytes - overhead)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			refusedKey := []byte("x")
			allocs := testing.AllocsPerRun(10, func() {
				denied := database.NewBatch().(*commandWALPublicBatch)
				if denied != &cowDeniedPublicBatch {
					panic("allocated refused wrapper")
				}
				if !errors.Is(denied.Set(refusedKey, nil), memtable.ErrCOWCapacity) {
					panic("lost capacity error")
				}
				denied.Reset()
				_ = denied.Close()
			})
			if allocs != 0 {
				t.Fatalf("refused public construction allocated %g", allocs)
			}
		})
	}
}
