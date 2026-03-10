package memtable

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyResetWithCapacity_ShrinksEntriesAfterSpike(t *testing.T) {
	const (
		capacityBytes           = 4 << 10
		estimatedBytesPerEntry  = 96
		desiredEntriesMultiple  = 10
		shrinkThresholdMultiple = 4
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	if base <= 0 {
		t.Fatalf("expected base entry count > 0, got %d", base)
	}

	for i := 0; i < base*desiredEntriesMultiple; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got <= base*shrinkThresholdMultiple {
		t.Fatalf("test setup did not grow entries enough: cap(entries)=%d want>%d", got, base*shrinkThresholdMultiple)
	}

	mt.ResetWithCapacity(capacityBytes, estimatedBytesPerEntry)
	if got := cap(mt.entries); got > base*shrinkThresholdMultiple {
		t.Fatalf("entries not shrunk after reset: cap(entries)=%d want<=%d", got, base*shrinkThresholdMultiple)
	}
	if mt.count != 0 {
		t.Fatalf("expected count=0 after reset, got %d", mt.count)
	}
}
