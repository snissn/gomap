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

func TestNormalizePublicBatchReserveHint_PreservesSmallEntryHints(t *testing.T) {
	const hint = 512
	if got := normalizePublicBatchReserveHint(hint); got != hint {
		t.Fatalf("normalizePublicBatchReserveHint(%d)=%d want %d", hint, got, hint)
	}
}

func TestNormalizePublicBatchReserveHint_ConvertsLargeByteBudgets(t *testing.T) {
	const sizeHint = 100_000
	want := sizeHint / publicBatchHintBytesPerEntry
	if sizeHint%publicBatchHintBytesPerEntry != 0 {
		want++
	}
	if got := normalizePublicBatchReserveHint(sizeHint); got != want {
		t.Fatalf("normalizePublicBatchReserveHint(%d)=%d want %d", sizeHint, got, want)
	}
}

func TestNormalizePublicBatchReserveHint_HandlesEdgeCases(t *testing.T) {
	if got := normalizePublicBatchReserveHint(-1); got != 0 {
		t.Fatalf("normalizePublicBatchReserveHint(-1)=%d want 0", got)
	}
	if got := normalizePublicBatchReserveHint(0); got != 0 {
		t.Fatalf("normalizePublicBatchReserveHint(0)=%d want 0", got)
	}
	if got := normalizePublicBatchReserveHint(int(^uint(0) >> 1)); got != publicBatchReserveEntriesMax {
		t.Fatalf("normalizePublicBatchReserveHint(maxint)=%d want %d", got, publicBatchReserveEntriesMax)
	}
}

func TestNewBatchWithSize_NormalizesLargePublicHint(t *testing.T) {
	db := &DB{}
	b := db.NewBatchWithSize(100_000).(*Batch)
	defer func() { _ = b.Close() }()

	got := testBatchEntriesCap(b.batch)
	if got < normalizePublicBatchReserveHint(100_000) {
		t.Fatalf("internal batch cap=%d want >= %d", got, normalizePublicBatchReserveHint(100_000))
	}
	if got >= 100_000 {
		t.Fatalf("internal batch cap=%d want < %d", got, 100_000)
	}
}

func TestNewBatchWithEntryReserve_PreservesExplicitEntryCounts(t *testing.T) {
	db := &DB{}
	const reserveHint = publicBatchReserveEntryHintCutover + 1
	b := db.newBatchWithEntryReserve(reserveHint).(*Batch)
	defer func() { _ = b.Close() }()

	if got := testBatchEntriesCap(b.batch); got < reserveHint {
		t.Fatalf("internal batch cap=%d want >= %d", got, reserveHint)
	}
}
