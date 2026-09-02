package db

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestEstimateValueLogLiveBytesByChunk_SplitsPointerRecordsAcrossChunks(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 260_000, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 1500)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	chunkBytes := int64(1024)
	liveByChunk, err := db.estimateValueLogLiveBytesByChunk(context.Background(), chunkBytes)
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesByChunk: %v", err)
	}
	if len(liveByChunk) != 2 {
		t.Fatalf("chunk entries=%d want 2 (got=%v)", len(liveByChunk), liveByChunk)
	}

	recordLen1, err := db.valueLogRecordLengthForRewrite(ptrs[0])
	if err != nil {
		t.Fatalf("record length 1: %v", err)
	}
	recordLen2, err := db.valueLogRecordLengthForRewrite(ptrs[1])
	if err != nil {
		t.Fatalf("record length 2: %v", err)
	}
	chunk1, err := valueLogChunkOffsetForPtr(ptrs[0], chunkBytes)
	if err != nil {
		t.Fatalf("chunk offset 1: %v", err)
	}
	chunk2, err := valueLogChunkOffsetForPtr(ptrs[1], chunkBytes)
	if err != nil {
		t.Fatalf("chunk offset 2: %v", err)
	}
	if chunk1 == chunk2 {
		t.Fatalf("expected separate chunk offsets, got %d and %d", chunk1, chunk2)
	}
	if got, want := liveByChunk[valueLogChunkKey{fileID: ptrs[0].FileID, chunkOffset: chunk1}], int64(recordLen1); got != want {
		t.Fatalf("chunk1 live bytes=%d want %d", got, want)
	}
	if got, want := liveByChunk[valueLogChunkKey{fileID: ptrs[1].FileID, chunkOffset: chunk2}], int64(recordLen2); got != want {
		t.Fatalf("chunk2 live bytes=%d want %d", got, want)
	}
}

func TestEstimateValueLogLiveBytesByChunk_DedupsGroupedPointers(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 260_100, 2, func(i int) []byte {
		return bytes.Repeat([]byte("x"), 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 260_101, 1, func(i int) []byte {
		return bytes.Repeat([]byte("y"), 256)
	})

	base := ptrs[0]
	recordLenHint := page.ValuePtrRecordLength(base)
	b := db.NewBatch().(*Batch)
	for i := 0; i < 3; i++ {
		ptr := base
		ptr.Length = page.ValuePtrMarkGrouped(recordLenHint, uint8(i))
		if err := b.SetPointer([]byte{byte('k'), byte('0' + i)}, ptr); err != nil {
			t.Fatalf("set grouped key %d: %v", i, err)
		}
	}
	if err := b.SetPointer([]byte("k_active"), ptrs2[0]); err != nil {
		t.Fatalf("set k_active: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	chunkBytes := int64(1 << 20)
	liveByChunk, err := db.estimateValueLogLiveBytesByChunk(context.Background(), chunkBytes)
	if err != nil {
		t.Fatalf("estimateValueLogLiveBytesByChunk: %v", err)
	}
	recordLen, err := db.valueLogRecordLengthForRewrite(base)
	if err != nil {
		t.Fatalf("record length: %v", err)
	}
	chunkOffset, err := valueLogChunkOffsetForPtr(base, chunkBytes)
	if err != nil {
		t.Fatalf("chunk offset: %v", err)
	}
	if got, want := liveByChunk[valueLogChunkKey{fileID: base.FileID, chunkOffset: chunkOffset}], int64(recordLen); got != want {
		t.Fatalf("grouped chunk live bytes=%d want %d", got, want)
	}
}

func TestValueLogRewriteChunkPlan_SelectsStaleChunk(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs1 := appendPointersInNewSegment(t, dir, 0, 1, 260_200, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 256)
	})
	ptrs2 := appendPointersInNewSegment(t, dir, 0, 2, 260_201, 1, func(i int) []byte {
		return bytes.Repeat([]byte("z"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs1[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs2[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	plan, err := db.ValueLogRewriteChunkPlan(context.Background(), ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    1,
		MaxSourceBytes:       4 << 20,
		MinSegmentStaleRatio: 0.30,
		MinSegmentStaleBytes: 1,
	}, 1<<20)
	if err != nil {
		t.Fatalf("ValueLogRewriteChunkPlan: %v", err)
	}
	if len(plan.SourceChunks) != 1 {
		t.Fatalf("selected chunks=%d want 1 (%v)", len(plan.SourceChunks), plan.SourceChunks)
	}
	if got, want := plan.SourceChunks[0].FileID, ptrs1[0].FileID; got != want {
		t.Fatalf("selected chunk file=%d want %d", got, want)
	}
	if plan.SourceChunks[0].BytesStale <= 0 {
		t.Fatalf("selected chunk stale bytes=%d want >0", plan.SourceChunks[0].BytesStale)
	}
	if plan.SourceChunks[0].BytesLive <= 0 {
		t.Fatalf("selected chunk live bytes=%d want >0", plan.SourceChunks[0].BytesLive)
	}
}

func TestSelectRewriteSourceChunksWithStats_PrefersHighStaleChunk(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "value-l0-000001.log")
	path2 := filepath.Join(dir, "value-l0-000002.log")
	if err := os.WriteFile(path1, bytes.Repeat([]byte("a"), 128), 0o600); err != nil {
		t.Fatalf("write path1: %v", err)
	}
	if err := os.WriteFile(path2, bytes.Repeat([]byte("b"), 128), 0o600); err != nil {
		t.Fatalf("write path2: %v", err)
	}

	files := map[uint32]*valuelog.File{
		1: {Path: path1},
		2: {Path: path2},
	}
	liveByChunk := map[valueLogChunkKey]int64{
		{fileID: 1, chunkOffset: 0}:  8,
		{fileID: 1, chunkOffset: 64}: 48,
		{fileID: 2, chunkOffset: 0}:  32,
		{fileID: 2, chunkOffset: 64}: 56,
	}

	selected, _ := selectRewriteSourceChunksWithStats(ValueLogRewriteOnlineOptions{
		MaxSourceSegments:    2,
		MaxSourceBytes:       80,
		MinSegmentStaleBytes: 1,
	}, files, map[uint32]struct{}{}, liveByChunk, 64)
	if len(selected) != 2 {
		t.Fatalf("selected chunks=%d want 2 (%v)", len(selected), selected)
	}
	if got, want := selected[0].FileID, uint32(1); got != want {
		t.Fatalf("first chunk file=%d want %d", got, want)
	}
	if got, want := selected[0].ChunkOffset, int64(0); got != want {
		t.Fatalf("first chunk offset=%d want %d", got, want)
	}
	if got, want := selected[1].FileID, uint32(2); got != want {
		t.Fatalf("second chunk file=%d want %d", got, want)
	}
	if got, want := selected[1].ChunkOffset, int64(0); got != want {
		t.Fatalf("second chunk offset=%d want %d", got, want)
	}
}

func TestValueLogRewriteOnline_SourceChunks_RestrictsPointerRewriteSet(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeNoErr(t, db)

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 260_300, 2, func(i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 1500)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrs[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrs[1]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	closeNoErr(t, b)

	chunkBytes := int64(1024)
	chunk1, err := valueLogChunkOffsetForPtr(ptrs[0], chunkBytes)
	if err != nil {
		t.Fatalf("chunk offset 1: %v", err)
	}
	chunk2, err := valueLogChunkOffsetForPtr(ptrs[1], chunkBytes)
	if err != nil {
		t.Fatalf("chunk offset 2: %v", err)
	}
	if chunk1 == chunk2 {
		t.Fatalf("expected separate chunk offsets, got %d and %d", chunk1, chunk2)
	}

	stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
		SourceChunks: []ValueLogRewritePlanChunk{{
			FileID:      ptrs[0].FileID,
			ChunkOffset: chunk1,
			BytesTotal:  chunkBytes,
		}},
		SourceChunkBytes: chunkBytes,
		BatchSize:        8,
	})
	if err != nil {
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if stats.SourceChunksRequested != 1 {
		t.Fatalf("source chunks requested=%d want 1", stats.SourceChunksRequested)
	}
	if stats.RecordsCopied != 1 {
		t.Fatalf("records copied=%d want 1", stats.RecordsCopied)
	}

	ptrK1, flagsK1 := readProjectedPointerByKey(t, db, []byte("k1"))
	ptrK2, flagsK2 := readProjectedPointerByKey(t, db, []byte("k2"))
	if flagsK1&node.FlagPointer == 0 || flagsK2&node.FlagPointer == 0 {
		t.Fatalf("expected pointer flags for rewritten keys: k1=%#x k2=%#x", flagsK1, flagsK2)
	}
	if ptrK1.FileID == ptrs[0].FileID {
		t.Fatalf("expected k1 pointer to move off source segment %d", ptrs[0].FileID)
	}
	if ptrK2.FileID != ptrs[1].FileID {
		t.Fatalf("expected k2 pointer to remain on non-selected chunk in source segment %d, got %d", ptrs[1].FileID, ptrK2.FileID)
	}
}
