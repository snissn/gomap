package treedb

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// A valid small caller limit must admit the actual original queue and journal
// arrays, drain a full queue outside publication gates, refuse new work under
// producer pressure before assignment, and drain using reserved maintenance.
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
			var last string
			for accepted := 0; accepted < 512; accepted++ {
				last = fmt.Sprint(accepted)
				if err := database.Set([]byte("bounded"), []byte(last)); err != nil {
					t.Fatalf("bounded queue did not drain at write %d: %v", accepted, err)
				}
			}
			runs, err := strconv.ParseUint(database.Stats()["treedb.cache.checkpoint.runs"], 10, 64)
			if err != nil || runs == 0 {
				t.Fatalf("full queue did not checkpoint outside publication gates: runs=%d err=%v", runs, err)
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
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("reserved maintenance could not drain under producer pressure: %v", err)
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
