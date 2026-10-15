package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAppendWorkRechecksCapacityAfterReusedAllocation(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = p.Alloc(4); err != nil {
		t.Fatal(err)
	}
	a := New(p, 0)
	if err = a.EnableCOWV1(MustNewFreelistGenerationV1(1, 4, []uint64{2}, nil), NewReservationLedger()); err != nil {
		t.Fatal(err)
	}
	// Existing transaction capacity is fixture setup, not a production admission
	// exemption. The public probe uses only actual producer-issued capacity.
	a.cow.txn.allocated = make([]allocatedPage, 0, 1)
	var s AppendWork
	_, ready, err := s.Step(a, &iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20})
	if err != nil || ready || s.phase != 1 || s.growth != nil {
		t.Fatalf("not actual pre-growth pause: ready=%t err=%v phase=%d", ready, err, s.phase)
	}
	beforeWater, beforeCount := a.cow.txn.highWater, p.PageCount()
	reused, err := a.Alloc(0)
	if err != nil || reused != 2 || a.cow.txn.highWater != beforeWater || p.PageCount() != beforeCount {
		t.Fatal("reused allocation", reused, err)
	}
	var appended uint64
	for i := 0; i < 64; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		appended, ready, err = s.Step(a, &w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("resumed allocation escaped capacity admission", ready, err, w)
		}
		if ready {
			break
		}
	}
	if !ready || appended != beforeWater || a.cow.txn.highWater != beforeWater+1 || p.PageCount() != beforeCount+1 || len(a.cow.txn.allocated) != 2 {
		t.Fatal("retained growth did not preserve reused allocation", appended, ready, err)
	}
	// Pause after the private rollback captures its original trie. An ordinary
	// writer then changes that trie and highwater. Resumption must reprepare
	// against that writer rather than restoring the stale capture.
	ready, err = s.Cancel(&iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20})
	if ready || err != nil || s.retirement.phase == 0 {
		t.Fatal("missing actual installed retirement pause", ready, err)
	}
	newer, err := a.Alloc(0)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Free(reused); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err = s.Cancel(&w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("installed cancellation exceeded allowance", ready, err, w)
		}
		if ready {
			break
		}
	}
	if !ready || s.allocator != nil || a.cow.txn.highWater != newer+1 || p.PageCount() != newer+1 {
		t.Fatal("installed cancellation lost live allocator", ready, newer)
	}
	chunk := lookupChunk(a.cow.txn.root, appended>>freelistChunkShift)
	if chunk == nil || chunk.retired[appended&(freelistChunkSize-1)] == 0 || chunk.retired[reused&(freelistChunkSize-1)] == 0 || chunk.retired[newer&(freelistChunkSize-1)] != 0 {
		t.Fatal("rollback lost retirement or retired foreground page")
	}
	// The independently decoded generation must include both retirements and
	// retain the foreground writer's allocation.
	candidate, err := a.cow.txn.MaterializeCandidate(2, 2, candidateIDFromString("append-cancel"), NewCandidatePageSinkV1())
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryPageStoreV1()
	for _, output := range candidate.pages {
		store.Pages[output.PageID] = output.Data
	}
	decoded, err := LoadGenerationV1(store, candidate.GenerationRef())
	if err != nil || decoded.RetiredCount() != 2 || decoded.Allocatable(newer) {
		t.Fatal("cancelled output did not survive generation decode", err)
	}
}
