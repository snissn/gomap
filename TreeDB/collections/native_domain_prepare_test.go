package collections

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestNativeDomainSuccessorSharesOriginalPendingRows(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]memtable.COWMutation, 4096)
	for i := range entries {
		entries[i] = memtable.COWMutation{Key: []byte(fmt.Sprintf("%08d", i)), Value: []byte("old"), Flags: node.FlagInline}
	}
	old, err := prepareNativeDomainUpdate(nil, "primary", 17, 1, 0, entries, budget)
	if err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		entries[i].Value[0] = '!'
	}
	entries = nil
	before := budget.Stats()
	next, err := prepareNativeDomainUpdate(old, "primary", 17, 1, 0, []memtable.COWMutation{
		{Key: []byte("00000000"), Value: []byte("new"), Flags: node.FlagInline},
		{Key: []byte("00000001"), Flags: node.FlagTombstone},
	}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := old.pending.Get([]byte("00000000")); got.Value != "old" {
		t.Fatal("old source changed", got)
	}
	if got, _ := next.pending.Get([]byte("00000000")); got.Value != "new" {
		t.Fatal("new row missing", got)
	}
	if got, _ := next.pending.Get([]byte("00000001")); got.Flags&node.FlagTombstone == 0 {
		t.Fatal("base-shadowing tombstone missing", got)
	}
	if got, _ := next.pending.Get([]byte("00004095")); got.Value != "old" {
		t.Fatal("untouched row lost", got)
	}
	if got := budget.Stats(); got.Generations != before.Generations {
		t.Fatal("successor allocated another source generation", before, got)
	}
	completion, err := (nativeDomainLifetime{}).Release(old)
	old = nil
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	if got, _ := next.pending.Get([]byte("00004095")); got.Value != "old" {
		t.Fatal("older descriptor release lost shared source", got)
	}
	budget.Close()
	completion, err = (nativeDomainLifetime{}).Release(next)
	next = nil
	if err != nil {
		t.Fatal(err)
	}
	completion.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
