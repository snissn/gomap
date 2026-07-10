package zipper

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
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
}
