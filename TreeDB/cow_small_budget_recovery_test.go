package treedb

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// A valid small caller limit must admit the actual original queue and journal
// arrays, coalesce raw coverage, refuse new work under producer pressure before
// assignment, and seal the accepted endpoint using reserved maintenance.
func TestCOWPublicSmallBudgetAcceptedPrefixDrainsAndReopens(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			database, opts, _ := cowPublicContractOpen(t, profile, func(o *Options) {
				o.COWMemtableLimits = DefaultCOWMemtableLimits()
				o.COWMemtableLimits.MaxInFlightBytes = 1 << 20
				o.ValueLog.ForcePointers = false
				o.ValueLog.PointerThreshold = 1 << 30
				o.DisableBackgroundPrune = true
				o.BackgroundCheckpointInterval = -1
			})
			prefix, initial, err := database.backend.PrivateCOWAcceptedPrefix(database.txnOwner)
			if err != nil {
				t.Fatal(err)
			}
			var last string
			for accepted := 0; accepted < 512; accepted++ {
				last = fmt.Sprint(accepted)
				if err := database.Set([]byte("bounded"), []byte(last)); err != nil {
					t.Fatalf("bounded queue did not drain at write %d: %v", accepted, err)
				}
			}
			accepted := prefix.LogicalFrontiers()
			if accepted.V != initial.V+512 || accepted.S != initial.S {
				t.Fatalf("raw coverage did not coalesce before checkpoint: initial=%+v accepted=%+v", initial, accepted)
			}
			if profile != ProfileNoWALFast && accepted.C != initial.C+512 {
				t.Fatalf("raw command coverage=%d want %d", accepted.C, initial.C+512)
			}
			cache := database.cached
			stats := cache.COWMemoryStats()
			overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			if stats.ReservedBytes+overhead >= 1<<20 {
				t.Fatal("fixture has no producer admission headroom")
			}
			pressure, err := cache.AcquireCOWAllocation((1 << 20) - stats.ReservedBytes - overhead)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			next := database.backend.CommandWALNextLSN()
			if err := database.Set([]byte("bounded"), []byte("refused")); !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("producer pressure did not refuse before assignment: %v", err)
			}
			if database.backend.CommandWALNextLSN() != next {
				t.Fatal("capacity refusal assigned a canonical command")
			}
			if refused := prefix.LogicalFrontiers(); refused != accepted {
				t.Fatalf("capacity refusal changed accepted frontiers: before=%+v after=%+v", accepted, refused)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("reserved maintenance could not drain under producer pressure: %v", err)
			}
			if sealed := prefix.LogicalFrontiers(); sealed.S.LogicalSequence != accepted.V || sealed.S.CanonicalLSN != accepted.C {
				t.Fatalf("reserved maintenance did not seal accepted endpoint: accepted=%+v sealed=%+v", accepted, sealed)
			}
			pressure.Close()
			if got, err := database.Get([]byte("bounded")); err != nil || string(got) != last {
				t.Fatalf("refusal changed accepted value: %q, %v; want %q", got, err, last)
			}
			if err := database.Set([]byte("bounded"), []byte("successor")); err != nil {
				t.Fatalf("drain did not restore producer admission: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Get([]byte("bounded")); err != nil || string(got) != "successor" {
				t.Fatalf("recovered accepted prefix=%q, %v", got, err)
			}
			if err := reopened.Checkpoint(); err != nil {
				t.Fatalf("recovered empty prefix checkpoint: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("recovered empty prefix Close: %v", err)
			}
		})
	}
}
