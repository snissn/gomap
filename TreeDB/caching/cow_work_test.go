package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"testing"
)

func TestCOWLogicalWorkUnsupportedVersusNoHeadroom(t *testing.T) {
	if got := (*DB)(nil).MVCCWriteWorkLimit(); got != 0 {
		t.Fatalf("unsupported=%d", got)
	}
	limits := memtable.DefaultCOWLimits()
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	maintenance, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer maintenance.Close()
	// A supported configuration whose maintenance envelope consumes the available
	// limit must refuse shaping, rather than masquerade as an unsupported route.
	db := &DB{cow: &cowCache{ordered: true, budget: budget, maintenance: maintenance}}
	if got := db.MVCCWriteWorkLimit(); got != 1 {
		t.Fatalf("no-headroom=%d", got)
	}
	if work := db.EstimateMVCCWriteWork(1, 0); work <= 1 {
		t.Fatalf("valid row could bypass refusal marker: %d", work)
	}
}
