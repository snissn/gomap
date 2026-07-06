package db

import (
	"math"
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
	if got := NormalizePublicBatchReserveHint(hint); got != hint {
		t.Fatalf("NormalizePublicBatchReserveHint(%d)=%d want %d", hint, got, hint)
	}
}

func TestNormalizePublicBatchReserveHint_ConvertsLargeByteBudgets(t *testing.T) {
	const sizeHint = 100_000
	want := sizeHint / publicBatchHintApproxBytesPerEntry
	if sizeHint%publicBatchHintApproxBytesPerEntry != 0 {
		want++
	}
	if got := NormalizePublicBatchReserveHint(sizeHint); got != want {
		t.Fatalf("NormalizePublicBatchReserveHint(%d)=%d want %d", sizeHint, got, want)
	}
}

func TestNormalizePublicBatchReserveHint_HandlesEdgeCases(t *testing.T) {
	if got := NormalizePublicBatchReserveHint(-1); got != 0 {
		t.Fatalf("NormalizePublicBatchReserveHint(-1)=%d want 0", got)
	}
	if got := NormalizePublicBatchReserveHint(0); got != 0 {
		t.Fatalf("NormalizePublicBatchReserveHint(0)=%d want 0", got)
	}
	if got := NormalizePublicBatchReserveHint(math.MaxInt); got != publicBatchHintNormalizedEntryCap {
		t.Fatalf("NormalizePublicBatchReserveHint(maxint)=%d want %d", got, publicBatchHintNormalizedEntryCap)
	}
}

func TestNormalizePublicBatchReserveAndByteHints_PreservesSmallEntryHints(t *testing.T) {
	const (
		sizeHint              = 512
		estimatedBytesPerHint = 192
	)
	entryHint, byteHint := NormalizePublicBatchReserveAndByteHints(sizeHint, estimatedBytesPerHint)
	if entryHint != sizeHint {
		t.Fatalf("entryHint=%d want %d", entryHint, sizeHint)
	}
	if want := sizeHint * estimatedBytesPerHint; byteHint != want {
		t.Fatalf("byteHint=%d want %d", byteHint, want)
	}
}

func TestNormalizePublicBatchReserveAndByteHints_PreservesLargeByteBudget(t *testing.T) {
	const (
		sizeHint              = 100_000
		estimatedBytesPerHint = 192
	)
	entryHint, byteHint := NormalizePublicBatchReserveAndByteHints(sizeHint, estimatedBytesPerHint)
	if want := NormalizePublicBatchReserveHint(sizeHint); entryHint != want {
		t.Fatalf("entryHint=%d want %d", entryHint, want)
	}
	if byteHint != sizeHint {
		t.Fatalf("byteHint=%d want preserved byte budget %d", byteHint, sizeHint)
	}
}

func TestNormalizePublicBatchReserveAndByteHints_CapsHugeByteBudget(t *testing.T) {
	entryHint, byteHint := NormalizePublicBatchReserveAndByteHints(math.MaxInt, 192)
	if entryHint != publicBatchHintNormalizedEntryCap {
		t.Fatalf("entryHint=%d want %d", entryHint, publicBatchHintNormalizedEntryCap)
	}
	if want := publicBatchHintNormalizedEntryCap * publicBatchHintApproxBytesPerEntry; byteHint != want {
		t.Fatalf("byteHint=%d want capped byte budget %d", byteHint, want)
	}
}

func TestNewBatchWithSize_NormalizesLargePublicHint(t *testing.T) {
	db := &DB{}
	want := NormalizePublicBatchReserveHint(100_000)
	b := db.NewBatchWithSize(100_000).(*Batch)
	defer func() { _ = b.Close() }()

	got := testBatchEntriesCap(b.batch)
	if got < want {
		t.Fatalf("internal batch cap=%d want >= %d", got, want)
	}
	if got >= 100_000 {
		t.Fatalf("internal batch cap=%d want < %d", got, 100_000)
	}
}

func TestNewBatchWithEntryReserve_PreservesExplicitEntryCounts(t *testing.T) {
	db := &DB{}
	const reserveHint = publicBatchHintExactEntryReserveMax + 1
	b := db.newBatchWithEntryReserve(reserveHint).(*Batch)
	defer func() { _ = b.Close() }()

	if got := testBatchEntriesCap(b.batch); got < reserveHint {
		t.Fatalf("internal batch cap=%d want >= %d", got, reserveHint)
	}
}
