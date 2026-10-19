package memtable

import (
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWCapturedOwnedRetainAfterClosePreservesOriginalLastOwner(t *testing.T) {
	budget, err := NewCOWBudget(DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := budget.NewNativeCOWWriter(128)
	if err != nil {
		t.Fatal(err)
	}
	root := cowTestPublish(t, writer, []COWMutation{{Key: []byte("key"), Value: []byte("owned"), Flags: node.FlagInline}})
	cowTestClose(writer)
	writer = nil
	budget.Close()
	before := budget.Stats()
	if root.Retain() || !root.RetainOwned() || before != budget.Stats() || before.Sources != 1 {
		t.Fatal("owned successor changed original admission or ordinary Close refusal", before, budget.Stats())
	}
	// The independently retained root edge survives release of the opening edge.
	// Concurrent maintenance transfers remain covered by that held owner.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			if !root.RetainOwned() {
				t.Error("owned root could not be shared")
				return
			}
			cowTestRelease(root)
		}
	}()
	cowTestRelease(root)
	wg.Wait()
	if row, ok := root.Get([]byte("key")); !ok || row.Value != "owned" || budget.Stats() != before {
		t.Fatal("successor lost exact original root/source", row, ok, budget.Stats())
	}
	cowTestRelease(root)
	if root.RetainOwned() || budget.Stats().TotalBytes != 0 || budget.Stats().Sources != 0 {
		t.Fatal("released root revived or original generation was not refunded", budget.Stats())
	}
	root = nil
	if root.RetainOwned() {
		t.Fatal("nil root retained")
	}
}

func TestCOWCapturedOwnedScanNeedsNoNewAdmission(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, writer, []COWMutation{
		{Key: []byte("a"), Value: []byte("old"), Flags: node.FlagInline},
		{Key: []byte("b"), Flags: node.FlagTombstone},
		{Key: []byte("c"), Value: []byte("last"), Flags: node.FlagInline},
	})
	next := cowTestPublish(t, writer, []COWMutation{{Key: []byte("a"), Value: []byte("new"), Flags: node.FlagInline}})
	cowTestClose(writer)
	writer = nil
	budget.Close() // even new cursor/view admission is now unavailable
	before := budget.Stats()
	var keys [3]string
	count := 0
	old.ScanOwned(func(row COWRecord) bool {
		keys[count] = row.Key
		count++
		if row.Key == "a" && row.Value != "old" {
			t.Fatal("scan followed a later root", row)
		}
		if row.Key == "b" && row.Flags&node.FlagTombstone == 0 {
			t.Fatal("scan lost physical deletion effect", row)
		}
		return true
	})
	if count != 3 || keys != [3]string{"a", "b", "c"} || before != budget.Stats() {
		t.Fatal(keys, count, before, budget.Stats())
	}
	clear(keys[:]) // borrowed generation aliases end before last root release
	count = 0
	old.ScanOwned(func(COWRecord) bool { count++; return false })
	if count != 1 {
		t.Fatal("stop callback was ignored", count)
	}
	if n := testing.AllocsPerRun(10, func() {
		old.ScanOwned(func(COWRecord) bool { return true })
	}); n != 0 {
		t.Fatal("owned traversal allocated a new cursor", n)
	}
	cowTestRelease(old)
	old = nil
	if budget.Stats().HistoryBytes == 0 {
		t.Fatal("remaining root lost shared generation")
	}
	cowTestRelease(next)
	next = nil
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
