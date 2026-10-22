package memtable

import (
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWOrderedAbsentRawMigrationUsesOnlyOrderedNodeReceipt(t *testing.T) {
	for _, rawEntries := range []int{0, 1024} {
		t.Run(fmt.Sprintf("raw_entries_%d", rawEntries), func(t *testing.T) {
			cowTestIsolateAllocationWitness(t)
			limits := DefaultCOWLimits()
			limits.MaxGenerationBytes = 4 << 20
			limits.MaxTotalBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = 16<<20, 16<<20, 16<<20
			budget, err := NewCOWBudget(limits)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCOWOrderedWriter(budget)
			if err != nil {
				t.Fatal(err)
			}
			seed := make([]COWMutation, rawEntries)
			for i := range seed {
				seed[i] = cowOrderedLifecycleMutation(t, fmt.Sprintf("raw/%04d", i), 3, "raw")
				seed[i].MVCC = false
			}
			old := cowTestPublish(t, writer, seed)
			if old.tree.Len() != rawEntries || old.ordered.len() != 0 || (rawEntries != 0 && old.tree.Height() < 2) {
				t.Fatal("fixture did not retain the intended raw compatibility tree")
			}
			mutations := make([]COWMutation, 16)
			for i := range mutations {
				// These canonical versions are absent from the populated raw tree.
				// Both interleaved prefixes and repeated history writes must avoid
				// a raw search-path copy merely to discover that absence.
				mutations[i] = cowOrderedLifecycleMutation(t, fmt.Sprintf("raw/%04d/missing", i*61), uint64(i+1), "ordered")
			}
			upper, err := writer.Estimate(mutations, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			orderedUpper, err := writer.ordered.estimate(mutations)
			if err != nil || upper.Nodes != orderedUpper.Nodes {
				t.Fatalf("absent raw keys unexpectedly reserved deletion nodes: total=%d ordered=%d err=%v", upper.Nodes, orderedUpper.Nodes, err)
			}
			// Keep only the exact original preparation promise available. This
			// checks the existing authority; the test never enlarges a quota or
			// obtains additional capacity after private allocation begins.
			remaining := limits.MaxTotalBytes - budget.Stats().TotalBytes - upper.Total()
			leaseHeader := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
			if remaining <= leaseHeader {
				t.Fatal("fixture has no external pressure allowance")
			}
			pressure, err := budget.AcquireExternal(remaining - leaseHeader)
			if err != nil {
				t.Fatal(err)
			}
			before := budget.Stats()
			if before.TotalBytes+upper.Total() != limits.MaxTotalBytes {
				t.Fatal("pressure did not leave precisely the original preparation bound")
			}
			runtime.GC()
			var allocatedBefore, allocatedAfter runtime.MemStats
			runtime.ReadMemStats(&allocatedBefore)
			prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
			runtime.ReadMemStats(&allocatedAfter)
			if err != nil {
				t.Fatal(err)
			}
			if actual := allocatedAfter.TotalAlloc - allocatedBefore.TotalAlloc; actual > upper.Total() {
				t.Errorf("preparation allocated %d bytes above its original %d-byte promise", actual, upper.Total())
			}
			if raw := prepared.private.AllocationCharge(); raw != 0 {
				t.Errorf("absent raw migration allocated %d charged raw node/backing bytes", raw)
			}
			if prepared.Charge().Nodes != prepared.ordered.nodeAllocationCharge() {
				t.Error("node receipt contains work outside the ordered postimage")
			}
			if prepared.Root().tree.Len() != rawEntries || old.tree.Len() != rawEntries || old.ordered.len() != 0 {
				t.Error("private preparation changed retained raw membership")
			}
			retirement := prepared.Cancel()
			retirement.Drain()
			if got := budget.Stats(); got.TotalBytes != before.TotalBytes || got.ReservedBytes != before.ReservedBytes || got.HistoryBytes != before.HistoryBytes {
				t.Fatalf("cancellation did not refund the original reservation: got=%+v before=%+v", got, before)
			}
			prepared, err = writer.Prepare(mutations, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			current := prepared.Publish()
			if current.tree.Len() != rawEntries || current.ordered.len() != len(mutations) || current.Len() != rawEntries+len(mutations) {
				t.Fatal("publication lost a raw or ordered physical version")
			}
			for _, mutation := range mutations {
				if _, found := old.Get(mutation.Key); found {
					t.Fatal("new ordered version appeared in the held opening root")
				}
				if got, found := current.Get(mutation.Key); !found || got.Value != string(mutation.Value) || got.Revision != mutation.Revision {
					t.Fatal("published ordered version or native revision differs")
				}
			}
			for _, mutation := range seed {
				if got, found := current.Get(mutation.Key); !found || got.Value != "raw" {
					t.Fatal("ordered publication changed a raw compatibility record")
				}
			}
			cowTestRelease(current)
			cowTestRelease(old)
			cowTestClose(writer)
			pressure.Close()
			budget.Close()
			if got := budget.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.Generations != 0 {
				t.Fatalf("raw guard lifecycle retained owners: %+v", got)
			}
		})
	}
}

func TestCOWOrderedRawMigrationGuardUsesStagedMembership(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCOWOrderedWriter(budget)
	if err != nil {
		t.Fatal(err)
	}
	seed := make([]COWMutation, 256)
	for i := range seed {
		seed[i] = cowOrderedLifecycleMutation(t, fmt.Sprintf("raw/%04d", i), 7, "old raw")
		seed[i].MVCC = false
	}
	old := cowTestPublish(t, writer, seed)
	newPhysical := cowOrderedLifecycleMutation(t, "new raw then ordered", 17, "first raw")
	newPhysical.MVCC = false
	qualified := newPhysical
	qualified.MVCC, qualified.Timestamp, qualified.Value = true, 17, []byte("qualified")
	erase := COWMutation{Key: qualified.Key, Remove: true}
	final := qualified
	final.Value, final.Revision = []byte("final ordered"), qualified.Revision+10
	migrate := seed[73]
	migrate.MVCC, migrate.Timestamp, migrate.Value = true, 7, []byte("migrated old raw")
	rawOnly := COWMutation{Key: []byte("ordinary raw key"), Value: []byte("new raw"), Flags: node.FlagInline}
	entries := []COWMutation{newPhysical, qualified, erase, final, migrate, rawOnly}
	upper, err := writer.Estimate(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := writer.Prepare(entries, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.private.AllocationCharge() == 0 || prepared.Charge().Nodes > upper.Nodes {
		t.Fatal("real raw insertion/migration was skipped or exceeded its admitted nodes")
	}
	current := prepared.Publish()
	if current.tree.Len() != len(seed) || current.ordered.len() != 2 || current.Len() != len(seed)+2 {
		t.Fatalf("staged migration duplicated or lost membership: raw=%d ordered=%d total=%d", current.tree.Len(), current.ordered.len(), current.Len())
	}
	if cowHasKey(current.tree, final.Key) || cowHasKey(current.tree, migrate.Key) {
		t.Fatal("qualified version retained a stale raw incarnation")
	}
	if got, found := current.Get(final.Key); !found || got.Value != "final ordered" || got.Revision != final.Revision {
		t.Fatal("repeated qualification/erase did not preserve the final value and revision")
	}
	if got, found := current.Get(migrate.Key); !found || got.Value != "migrated old raw" || got.Revision != migrate.Revision {
		t.Fatal("existing raw migration lost metadata")
	}
	if got, found := current.Get(rawOnly.Key); !found || got.Value != "new raw" {
		t.Fatal("unqualified raw insertion was lost")
	}
	if _, found := old.Get(final.Key); found {
		t.Fatal("newly staged raw/ordered key changed the held root")
	}
	if got, found := old.Get(migrate.Key); !found || got.Value != "old raw" || got.Revision != seed[73].Revision {
		t.Fatal("migration mutated a held raw value")
	}
	cowTestRelease(current)
	cowTestRelease(old)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 || got.ReservedBytes != 0 || got.Generations != 0 {
		t.Fatalf("staged-membership lifecycle retained owners: %+v", got)
	}
}
