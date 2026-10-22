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
			if !command.isCOW() || command.cowState.lease == nil || !command.payloadBypass || command.commandWALPublicOrdinaryState != nil {
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
			if command.commandWALPublicOrdinaryState != nil {
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

func TestCOWPublicCommandBatchAuthorizationFailureCloseReleasesOwners(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		for _, mode := range []string{"cow_btree", "cow_ordered"} {
			t.Run(string(profile)+"/"+mode, func(t *testing.T) {
				database, opts, _ := cowPublicContractOpen(t, profile, func(o *Options) { o.MemtableMode = mode })
				baseline := database.cached.COWMemoryStats()
				nextLSN := database.backend.CommandWALNextLSN()
				inner := database.cached.NewBatchWithSize(1)
				defer inner.Close()
				stats := database.cached.COWMemoryStats()
				limits := opts.COWMemtableLimits
				if limits == (memtable.COWLimits{}) {
					limits = memtable.DefaultCOWLimits()
				}
				overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				if stats.ReservedBytes+overhead >= limits.MaxInFlightBytes {
					t.Fatal("fixture lacks producer headroom")
				}
				filler, err := database.cached.AcquireCOWAllocation(limits.MaxInFlightBytes - stats.ReservedBytes - overhead)
				if err != nil {
					t.Fatal(err)
				}
				defer filler.Close()
				inner.Reserve(8192)
				// Reserve has no error return. The existing authorizer exposes its sticky
				// refusal without invoking any producer, assigning a revision or appending.
				if err := inner.AuthorizeCOWAppend(database.txnOwner); !errors.Is(err, memtable.ErrCOWCapacity) {
					t.Fatalf("Reserve did not retain capacity refusal: %v", err)
				}
				filler.Close()
				command := newCommandWALPublicBatch(database, inner, 1)
				defer command.Close()
				if command == &cowDeniedPublicBatch || command == &cowClosedPublicBatch || command.cowState == nil || command.cowState.lease == nil || !errors.Is(command.cowState.batchError, memtable.ErrCOWCapacity) {
					t.Fatal("fixture did not admit an owned wrapper with failed inner authorization")
				}
				opened := database.cached.COWMemoryStats()
				if opened.ExternalLeases != baseline.ExternalLeases+2 {
					t.Fatalf("wrapper and inner not independently owned: before=%+v opened=%+v", baseline, opened)
				}
				command.Reset()
				if stats := database.cached.COWMemoryStats(); stats.TotalBytes != opened.TotalBytes || stats.ExternalLeases != opened.ExternalLeases {
					t.Fatal("Reset changed a refused batch's owners")
				}
				for closeIndex := 0; closeIndex < 2; closeIndex++ {
					if err := command.Close(); err != nil {
						t.Fatal(err)
					}
					after := database.cached.COWMemoryStats()
					if after.TotalBytes != baseline.TotalBytes || after.ReservedBytes != baseline.ReservedBytes || after.ExternalBytes != baseline.ExternalBytes || after.ExternalLeases != baseline.ExternalLeases {
						t.Fatalf("Close %d retained refused batch owners: before=%+v after=%+v", closeIndex+1, baseline, after)
					}
					if database.backend.CommandWALNextLSN() != nextLSN {
						t.Fatal("refused batch Close assigned a command")
					}
				}
			})
		}
	}
}
