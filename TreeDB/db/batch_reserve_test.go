package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func testBatchEntriesCap(inner *batch.Batch) int {
	if inner == nil {
		return 0
	}
	return inner.EntriesCap()
}

func TestBatchReserveForwardsToInternalBatch(t *testing.T) {
	internal := batch.New(nil, page.DefaultInlineThreshold)
	b := &Batch{batch: internal}

	const reserveHint = 256
	b.Reserve(reserveHint)

	if got := testBatchEntriesCap(internal); got < reserveHint {
		t.Fatalf("internal batch cap=%d want >= %d", got, reserveHint)
	}
}

func TestBatchReserveNilSafe(t *testing.T) {
	var b *Batch
	b.Reserve(64)

	b = &Batch{}
	b.Reserve(64)
}
