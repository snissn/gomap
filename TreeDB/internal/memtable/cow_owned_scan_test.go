package memtable

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

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
