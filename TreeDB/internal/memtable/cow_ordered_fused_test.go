package memtable

import (
	"reflect"
	"testing"
)

func TestCOWOrderedFusedGroupsKeepOldCutsAndFreshReceipts(t *testing.T) {
	budget, writer := cowOrderedLifecycleWriter(t)
	mutation := func(ts uint64) COWMutation {
		return cowOrderedStorageMutation(t, "shared\x00key", ts)
	}
	seed := []COWMutation{mutation(1), mutation(2), cowOrderedStorageMutation(t, "untouched", 1)}
	roots := []*COWRoot{cowTestPublish(t, writer, seed)}
	collect := func(root *COWRoot) []COWRecord {
		var rows []COWRecord
		root.ScanOwned(func(row COWRecord) bool { rows = append(rows, row); return true })
		return rows
	}
	witnesses := [][]COWRecord{collect(roots[0])}
	defer func() {
		for _, root := range roots {
			cowTestRelease(root)
		}
		cowTestClose(writer)
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 {
			t.Errorf("final owners retained charge: %+v", stats)
		}
	}()
	remove := func(ts uint64) COWMutation {
		e := mutation(ts)
		e.MVCC, e.Timestamp, e.Remove = false, 0, true
		return e
	}
	replace := mutation(1)
	replace.Value = []byte("replacement through a private existing history")
	groups := [][]COWMutation{
		{remove(2), mutation(3), replace, remove(3), mutation(4)},
		{mutation(2), mutation(5), mutation(4)},
		{remove(1), remove(2), remove(4), remove(5), mutation(7), mutation(6)},
	}
	for index, entries := range groups {
		upper, err := writer.Estimate(entries, COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := writer.Prepare(entries, COWPrepareOptions{})
		if err != nil {
			t.Fatal(err)
		}
		actual := prepared.ordered.nodeAllocationCharge() + prepared.private.AllocationCharge()
		if actual != prepared.Charge().Nodes || actual > upper.Nodes {
			t.Fatalf("group %d node receipt=%d reconciled=%d envelope=%d", index, actual, prepared.Charge().Nodes, upper.Nodes)
		}
		root := prepared.Publish()
		roots = append(roots, root)
		witnesses = append(witnesses, collect(root))
		for cut, previous := range roots {
			if got := collect(previous); !reflect.DeepEqual(got, witnesses[cut]) {
				t.Fatalf("group %d changed retained cut %d", index, cut)
			}
		}
		final := make(map[string]COWMutation)
		for _, entry := range entries {
			final[string(entry.Key)] = entry
		}
		for _, entry := range final {
			got, found := root.Get(entry.Key)
			if entry.Remove {
				if found {
					t.Fatalf("group %d retained a physically removed version", index)
				}
				continue
			}
			if !found || got.Value != string(entry.Value) {
				t.Fatalf("group %d lost its final put: %+v found=%t", index, got, found)
			}
		}
		// A sealed numeric map keeps its diagnostic receipt, but a subsequent
		// writer must start a fresh receipt and fresh isolation identity.
		root.ordered.keys.Scan(func(_ string, h *cowOrderedHistory) bool {
			if h.versions != nil {
				fork := h.fork()
				if fork.versions == h.versions || fork.versions.AllocationCharge() != 0 {
					t.Fatal("new preparation inherited a consumed header or old receipt")
				}
			}
			return true
		})
	}
}
