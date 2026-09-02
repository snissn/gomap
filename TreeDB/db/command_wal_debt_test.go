package db

import (
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALDependencyDebtCoalescesOnlyAdjacentEmptyEntries(t *testing.T) {
	var debt CommandWALDependencyDebt
	for _, lsn := range []uint64{1, 2, 3} {
		if err := debt.add(lsn, nil); err != nil {
			t.Fatalf("add empty LSN %d: %v", lsn, err)
		}
	}
	debt.mu.Lock()
	if got := len(debt.entries); got != 1 {
		debt.mu.Unlock()
		t.Fatalf("physical empty debt entries=%d, want 1", got)
	}
	if got := debt.entries[0]; got.firstLSN != 1 || got.lastLSN != 3 {
		debt.mu.Unlock()
		t.Fatalf("coalesced range=%d..%d, want 1..3", got.firstLSN, got.lastLSN)
	}
	debt.mu.Unlock()
	if got := debt.entryCountThrough(2); got != 2 {
		t.Fatalf("logical entries through 2=%d, want 2", got)
	}
	debt.noteRetryThrough(2)
	stats := debt.stats(time.Now())
	if got := stats.entries; got != 3 {
		t.Fatalf("stats logical entries=%d, want 3", got)
	}
	if got := stats.retries; got != 2 {
		t.Fatalf("stats logical retries=%d, want 2", got)
	}
	debt.releaseThrough(2)
	debt.mu.Lock()
	if got := debt.entries[0]; got.firstLSN != 3 || got.lastLSN != 3 {
		debt.mu.Unlock()
		t.Fatalf("partial release range=%d..%d, want 3..3", got.firstLSN, got.lastLSN)
	}
	debt.mu.Unlock()
	stats = debt.stats(time.Now())
	if got := stats.retries; got != 0 {
		t.Fatalf("remaining retries after partial release=%d, want 0", got)
	}
	if err := debt.add(4, nil); err != nil {
		t.Fatalf("add after partial release: %v", err)
	}
	debt.mu.Lock()
	if got := debt.entries[0]; got.firstLSN != 3 || got.lastLSN != 4 {
		debt.mu.Unlock()
		t.Fatalf("post-release coalesced range=%d..%d, want 3..4", got.firstLSN, got.lastLSN)
	}
	debt.mu.Unlock()
	debt.releaseThrough(4)
	if got := debt.entryCountThrough(4); got != 0 {
		t.Fatalf("logical entries after full release=%d, want 0", got)
	}
}

func TestCommandWALDependencyDebtPartialReleaseDoesNotSplitPhysicalEntry(t *testing.T) {
	debt := CommandWALDependencyDebt{entries: []commandWALDependencyDebtEntry{{
		firstLSN:  10,
		lastLSN:   12,
		resources: []*rootpublication.StableResourceSet{{}},
	}}}
	debt.releaseThrough(11)
	debt.mu.Lock()
	defer debt.mu.Unlock()
	if got := debt.entries; len(got) != 1 || got[0].firstLSN != 10 || got[0].lastLSN != 12 {
		t.Fatalf("partial release split physical entry: %+v, want unchanged 10..12", got)
	}
}

func TestCommandWALDependencyDebtAddAfterRetryStartsFreshRange(t *testing.T) {
	var debt CommandWALDependencyDebt
	for _, lsn := range []uint64{1, 2} {
		if err := debt.add(lsn, nil); err != nil {
			t.Fatalf("add LSN %d: %v", lsn, err)
		}
	}
	debt.noteRetryThrough(2)
	if err := debt.add(3, nil); err != nil {
		t.Fatalf("add after retry: %v", err)
	}
	debt.mu.Lock()
	if got := debt.entries; len(got) != 2 || got[0].firstLSN != 1 || got[0].lastLSN != 2 || got[0].retriesPerLSN != 1 || got[1].firstLSN != 3 || got[1].lastLSN != 3 || got[1].retriesPerLSN != 0 {
		debt.mu.Unlock()
		t.Fatalf("entries after retry then add=%+v, want retry range 1..2 plus fresh 3", got)
	}
	debt.mu.Unlock()
	debt.noteRetryThrough(2)
	debt.releaseThrough(2)
	stats := debt.stats(time.Now())
	if got := stats.entries; got != 1 {
		t.Fatalf("remaining logical entries=%d, want 1", got)
	}
	if got := stats.retries; got != 0 {
		t.Fatalf("remaining logical retries=%d, want 0", got)
	}
	if err := debt.add(4, nil); err != nil {
		t.Fatalf("add after partial retry/release: %v", err)
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()
	if got := debt.entries; len(got) != 1 || got[0].firstLSN != 3 || got[0].lastLSN != 4 || got[0].retriesPerLSN != 0 {
		t.Fatalf("fresh tail after partial retry/release=%+v, want retry-free 3..4", got)
	}
}

func TestCommandWALDependencyDebtNestedRetryAndPartialReleaseAccounting(t *testing.T) {
	var debt CommandWALDependencyDebt
	for lsn := uint64(1); lsn <= 5; lsn++ {
		if err := debt.add(lsn, nil); err != nil {
			t.Fatalf("add LSN %d: %v", lsn, err)
		}
	}
	debt.noteRetryThrough(3) // retries: 1,1,1,0,0
	debt.noteRetryThrough(2) // retries: 2,2,1,0,0
	if got := debt.stats(time.Now()).retries; got != 5 {
		t.Fatalf("nested retry total=%d, want 5", got)
	}
	debt.releaseThrough(1) // retries: 2,1,0,0
	if got := debt.stats(time.Now()).retries; got != 3 {
		t.Fatalf("retry total after release through 1=%d, want 3", got)
	}
	debt.releaseThrough(2) // retries: 1,0,0
	if got := debt.stats(time.Now()).retries; got != 1 {
		t.Fatalf("retry total after release through 2=%d, want 1", got)
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()
	if got := debt.entries; len(got) != 2 || got[0].firstLSN != 3 || got[0].lastLSN != 3 || got[0].retriesPerLSN != 1 || got[1].firstLSN != 4 || got[1].lastLSN != 5 || got[1].retriesPerLSN != 0 {
		t.Fatalf("nested retry ranges=%+v, want retried 3 plus clean 4..5", got)
	}
}

func BenchmarkCommandWALDependencyDebtAddEmpty(b *testing.B) {
	var debt CommandWALDependencyDebt
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := debt.add(uint64(i+1), nil); err != nil {
			b.Fatal(err)
		}
	}
}
