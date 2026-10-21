package memtable

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWOrderedRawMigrationPreparationAccounting(t *testing.T) {
	for _, shape := range []string{"one", "consecutive", "alternating"} {
		t.Run(shape, func(t *testing.T) {
			cowTestIsolateAllocationWitness(t)
			budget, err := NewCOWBudget(DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCOWOrderedWriter(budget)
			if err != nil {
				t.Fatal(err)
			}
			seed := make([]COWMutation, 1024)
			for i := range seed {
				key, err := mvcckey.Encode([]byte(fmt.Sprintf("raw-%04d", i)), 17)
				if err != nil {
					t.Fatal(err)
				}
				seed[i] = COWMutation{Key: key, Value: []byte("raw"), Flags: node.FlagInline, Revision: 1}
			}
			old := cowTestPublish(t, writer, seed)
			if old.tree.Height() < 2 || old.tree.Len() != len(seed) || old.ordered.len() != 0 {
				t.Fatal("fixture did not create a multi-level raw compatibility tree")
			}
			var mutations, removes []COWMutation
			for i, entry := range seed {
				selected := i == 31
				if shape == "consecutive" {
					selected = i < 128
				} else if shape == "alternating" {
					selected = i%2 == 0
				}
				if !selected {
					continue
				}
				entry.MVCC, entry.Timestamp, entry.Value, entry.Revision = true, 17, []byte("ordered"), 2
				mutations = append(mutations, entry)
				removes = append(removes, COWMutation{Key: entry.Key, Remove: true})
			}
			charge, err := writer.Estimate(mutations, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			orderedCharge, err := writer.ordered.estimate(mutations)
			if err != nil {
				t.Fatal(err)
			}
			removeCharge, err := writer.Estimate(removes, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// Migration removes precisely these raw keys. Its raw structural
			// allowance must cover the same deletion work as explicit erasure,
			// including sibling copies and merges rather than replacement paths.
			if charge.Nodes < orderedCharge.Nodes || charge.Nodes-orderedCharge.Nodes < removeCharge.Nodes {
				t.Fatalf("raw migration admitted less deletion work: migration=%d ordered=%d physical-remove=%d", charge.Nodes, orderedCharge.Nodes, removeCharge.Nodes)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			if actual := after.TotalAlloc - before.TotalAlloc; actual > charge.Total() {
				t.Errorf("migration allocated %d bytes above preadmitted %d", actual, charge.Total())
			}
			if old.tree.Len() != len(seed) || old.ordered.len() != 0 {
				t.Fatal("private migration changed the retained opening representation")
			}
			current := prepared.Publish()
			if current.Len() != len(seed) || current.tree.Len() != len(seed)-len(mutations) || current.ordered.len() != len(mutations) {
				t.Fatalf("migration duplicated or lost a physical version: total=%d raw=%d ordered=%d", current.Len(), current.tree.Len(), current.ordered.len())
			}
			for _, mutation := range mutations {
				prior, found := old.Get(mutation.Key)
				if !found || prior.Value != "raw" || prior.Revision != 1 {
					t.Fatal("migration changed a retained opening record")
				}
				next, found := current.Get(mutation.Key)
				if !found || next.Value != "ordered" || next.Revision != 2 {
					t.Fatal("migration lost the replacement value or native revision")
				}
			}
			cowTestRelease(old)
			cowTestRelease(current)
			cowTestClose(writer)
			budget.Close()
			if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.Generations != 0 {
				t.Fatalf("migration leaked original owners: %+v", stats)
			}
		})
	}
}
