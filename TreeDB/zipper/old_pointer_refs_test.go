package zipper

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func pointerApplyBatch(t *testing.T, start, end int, fileID uint32) *batch.Batch {
	t.Helper()
	b := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	for i := start; i < end; i++ {
		ptr := page.ValuePtr{FileID: fileID, Offset: uint64(i + 1), Length: 64}
		if err := b.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	return b
}

func TestApplyWithOptionsCollectsOldPointerRefsForPointsAndRange(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	root := newParallelApplyEmptyLeafRoot(t, z)
	fileID := page.ValueLogFileID(1)
	seed := pointerApplyBatch(t, 0, 256, fileID)
	root, _, _, err := z.Apply(root, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	delta := pointerApplyBatch(t, 0, 64, page.ValueLogFileID(2))
	defer func() { _ = delta.Close() }()
	for i := 64; i < 128; i++ {
		if err := delta.Delete([]byte(fmt.Sprintf("key-%06d", i))); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	if err := delta.DeleteRange([]byte("key-000128"), []byte("key-000192")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	result, err := z.ApplyWithOptions(root, delta, ApplyOptions{CollectOldPointerRefs: true})
	if err != nil {
		t.Fatalf("ApplyWithOptions: %v", err)
	}
	if !result.OldPointerRefsCollected {
		t.Fatal("old pointer refs were not marked collected")
	}
	if got, want := result.OldPointerRefs.Count(fileID), uint64(192); got != want {
		t.Fatalf("old pointer refs=%d want %d", got, want)
	}
	if got, want := result.OldEntriesRemoved, uint64(192); got != want {
		t.Fatalf("old entries removed=%d want %d", got, want)
	}
	if got := result.OldPointerRefs.Count(page.ValueLogFileID(2)); got != 0 {
		t.Fatalf("new pointer file appeared in old refs: %d", got)
	}
}

func TestApplyWithOptionsParallelCollectsOldPointerRefsExactlyOnce(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	root := newParallelApplyEmptyLeafRoot(t, z)
	fileID := page.ValueLogFileID(11)
	seed := pointerApplyBatch(t, 0, 12000, fileID)
	root, _, _, err := z.Apply(root, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	update := pointerApplyBatch(t, 0, 9000, page.ValueLogFileID(12))
	defer func() { _ = update.Close() }()
	pool := NewApplyWorkerPool(4)
	defer pool.Close()
	result, err := z.ApplyWithOptions(root, update, ApplyOptions{
		CollectOldPointerRefs:     true,
		PrepareReadOnly:           true,
		ReadOnlyPrepareWorkers:    4,
		ParallelApplyConcurrency:  4,
		ParallelApplyWorkerPool:   pool,
		ParallelApplyMinSpans:     1,
		ParallelApplyMinSpanOps:   1,
		ParallelApplyMinSpanBytes: 1,
	})
	if err != nil {
		t.Fatalf("parallel ApplyWithOptions: %v", err)
	}
	if !result.ParallelApplyUsed {
		t.Fatal("parallel apply path was not used")
	}
	if got, want := result.OldPointerRefs.Count(fileID), uint64(9000); got != want {
		t.Fatalf("parallel old pointer refs=%d want %d", got, want)
	}
	if got, want := result.OldEntriesRemoved, uint64(9000); got != want {
		t.Fatalf("parallel old entries removed=%d want %d", got, want)
	}
}

func TestApplyWithOptionsSpanNativeCollectsOldPointerRefsExactlyOnce(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	root := newParallelApplyEmptyLeafRoot(t, z)
	fileID := page.ValueLogFileID(21)
	seed := pointerApplyBatch(t, 0, 2000, fileID)
	root, _, _, err := z.Apply(root, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	update := pointerApplyBatch(t, 250, 1750, page.ValueLogFileID(22))
	defer func() { _ = update.Close() }()
	pool := NewApplyWorkerPool(4)
	defer pool.Close()
	result, err := z.ApplyWithOptions(root, update, ApplyOptions{
		CollectOldPointerRefs:    true,
		SpanNativeApply:          true,
		ParallelApplyConcurrency: 4,
		ParallelApplyWorkerPool:  pool,
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeUsed {
		t.Fatal("span-native apply path was not used")
	}
	if got, want := result.OldPointerRefs.Count(fileID), uint64(1500); got != want {
		t.Fatalf("span-native old pointer refs=%d want %d", got, want)
	}
	if got, want := result.OldEntriesRemoved, uint64(1500); got != want {
		t.Fatalf("span-native old entries removed=%d want %d", got, want)
	}
}

func TestApplyWithOptionsSpanNativeCollectsMixedOldPointerRefsByUpdatedKey(t *testing.T) {
	_, z := newParallelApplyTestZipper(t)
	root := newParallelApplyEmptyLeafRoot(t, z)
	fileA := page.ValueLogFileID(31)
	fileB := page.ValueLogFileID(32)
	fileC := page.ValueLogFileID(33)
	seed := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < 20000; i++ {
		fileID := fileB
		if i%3 == 0 {
			fileID = fileA
		}
		ptr := page.ValuePtr{FileID: fileID, Offset: uint64(i + 1), Length: 64}
		if err := seed.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("seed SetPointer %d: %v", i, err)
		}
	}
	var err error
	root, _, _, err = z.Apply(root, seed)
	_ = seed.Close()
	if err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	update := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
	defer func() { _ = update.Close() }()
	wantA, wantB := uint64(0), uint64(0)
	for i := 11; i < 19900; i += 7 {
		if i%3 == 0 {
			wantA++
		} else {
			wantB++
		}
		ptr := page.ValuePtr{FileID: fileC, Offset: uint64(i + 1), Length: 64}
		if err := update.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("update SetPointer %d: %v", i, err)
		}
	}
	pool := NewApplyWorkerPool(4)
	defer pool.Close()
	result, err := z.ApplyWithOptions(root, update, ApplyOptions{
		CollectOldPointerRefs:    true,
		SpanNativeApply:          true,
		ParallelApplyConcurrency: 4,
		ParallelApplyWorkerPool:  pool,
	})
	if err != nil {
		t.Fatalf("span-native ApplyWithOptions: %v", err)
	}
	if !result.SpanNativeUsed {
		t.Fatal("span-native apply path was not used")
	}
	if got := result.OldPointerRefs.Count(fileA); got != wantA {
		t.Fatalf("file A old pointer refs=%d want %d", got, wantA)
	}
	if got := result.OldPointerRefs.Count(fileB); got != wantB {
		t.Fatalf("file B old pointer refs=%d want %d", got, wantB)
	}
	if got := result.OldPointerRefs.Count(fileC); got != 0 {
		t.Fatalf("new pointer file appeared in old refs: %d", got)
	}
	if got, want := result.OldEntriesRemoved, wantA+wantB; got != want {
		t.Fatalf("mixed old entries removed=%d want %d", got, want)
	}
	livePages, err := tree.New(z.pager, nil, result.RootID).CollectPageIDs()
	if err != nil {
		t.Fatalf("collect result pages: %v", err)
	}
	live := make(map[uint64]struct{}, len(livePages))
	for _, pageID := range livePages {
		live[pageID] = struct{}{}
	}
	for _, pageID := range result.PendingRetiredPages {
		if _, ok := live[pageID]; ok {
			t.Fatalf("result page %d also appears in pending retired pages", pageID)
		}
	}
}
