package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkValueLogRewriteOnline_ValuePointers(b *testing.B) {
	b.Run("Paged/N1536/B512", func(b *testing.B) { benchmarkValuePointerRewrite(b, 2048, 1024, 512, false) })
	// Fixed B isolates repeated candidate-closure work as logical population N
	// grows. Large cells are complete runs; invoke -benchtime=1x explicitly.
	for _, n := range []int{30_000, 300_000, 3_000_000} {
		b.Run(fmt.Sprintf("CompressedOuter/N%d/B8192", n), func(b *testing.B) { benchmarkValuePointerRewrite(b, n*2, n/2, 8192, true) })
	}
	b.Run("CompressedOuter/N30000/B256", func(b *testing.B) { benchmarkValuePointerRewrite(b, 60_000, 15_000, 256, true) })
}

func benchmarkValuePointerRewrite(b *testing.B, seg1Records, seg2Records, batchSize int, outer bool) {
	var totalCopied int64
	var totalBytes int64
	var totalRefreshScans uint64
	var totalRewriteAllocs, totalRewriteAllocBytes uint64
	var totalFullScans, totalLeafScans, totalPages, totalBodies, totalCRC, totalPublications uint64

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, sourceIDs, cleanup := setupValuePointerRewriteBench(b, seg1Records, seg2Records, outer)
		refreshBefore := db.valueLogManager.RefreshScanCount()
		fullBefore := db.durableRootCandidateFullScans.Load()
		leafBefore := db.durableRootCandidateLeafOnlyScans.Load()
		pagesBefore := db.durableRootCandidatePagesVisited.Load()
		bodiesBefore := db.durableRootCandidateOuterBodies.Load()
		crcBefore := db.valueLogManager.ReadStats().RecordCRCChecks
		seqBefore := db.currentCommitSeq()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		b.StartTimer()

		stats, err := db.ValueLogRewriteOnline(context.Background(), ValueLogRewriteOnlineOptions{
			SourceFileIDs: sourceIDs,
			BatchSize:     batchSize,
		})
		b.StopTimer()
		if err != nil {
			cleanup()
			b.Fatalf("ValueLogRewriteOnline: %v", err)
		}
		totalFullScans += db.durableRootCandidateFullScans.Load() - fullBefore
		totalLeafScans += db.durableRootCandidateLeafOnlyScans.Load() - leafBefore
		totalPages += db.durableRootCandidatePagesVisited.Load() - pagesBefore
		totalBodies += db.durableRootCandidateOuterBodies.Load() - bodiesBefore
		totalCRC += db.valueLogManager.ReadStats().RecordCRCChecks - crcBefore
		totalPublications += db.currentCommitSeq() - seqBefore
		totalCopied += int64(stats.ValueRecordsCopied)
		totalBytes += stats.ValueBytesCopied
		totalRefreshScans += db.valueLogManager.RefreshScanCount() - refreshBefore
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		if memAfter.Mallocs > memBefore.Mallocs {
			totalRewriteAllocs += memAfter.Mallocs - memBefore.Mallocs
		}
		if memAfter.TotalAlloc > memBefore.TotalAlloc {
			totalRewriteAllocBytes += memAfter.TotalAlloc - memBefore.TotalAlloc
		}
		cleanup()
	}

	if b.N > 0 {
		b.ReportMetric(float64(totalFullScans)/float64(b.N), "candidate_full_scans/op")
		b.ReportMetric(float64(totalLeafScans)/float64(b.N), "candidate_leaf_scans/op")
		b.ReportMetric(float64(totalPages)/float64(b.N), "candidate_pages/op")
		b.ReportMetric(float64(totalBodies)/float64(b.N), "candidate_outer_bodies/op")
		b.ReportMetric(float64(totalCRC)/float64(b.N), "record_crc_checks/op")
		b.ReportMetric(float64(totalPublications)/float64(b.N), "publications/op")
		b.ReportMetric(float64(totalCopied)/float64(b.N), "value_records/op")
		b.ReportMetric(float64(totalBytes)/float64(b.N), "value_bytes/op")
		b.ReportMetric(float64(totalRefreshScans)/float64(b.N), "refresh_scans/op")
		b.ReportMetric(float64(totalRewriteAllocs)/float64(b.N), "rewrite_allocs/op")
		b.ReportMetric(float64(totalRewriteAllocBytes)/float64(b.N), "rewrite_alloc_bytes/op")
	}
}

func setupValuePointerRewriteBench(tb testing.TB, seg1Records, seg2Records int, outer bool) (*DB, []uint32, func()) {
	tb.Helper()
	if outer && !rootpublication.StableNamespaceCreationSupported() {
		tb.Skip("rotated producer creation requires stable creation authority")
	}
	dir, err := os.MkdirTemp("", "treedb-vlog-rewrite-value-bench-*")
	if err != nil {
		tb.Fatalf("MkdirTemp: %v", err)
	}

	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		LeafPrefixCompression:      true,
		IndexOuterLeavesInValueLog: outer,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		_ = os.RemoveAll(dir)
		tb.Fatalf("Open: %v", err)
	}

	var leafLog LeafPageLog
	if outer {
		leafLog, err = NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionAuto})
		if err != nil {
			_ = db.Close()
			_ = os.RemoveAll(dir)
			tb.Fatal(err)
		}
		db.SetLeafPageLog(leafLog)
	}

	ptrs1 := appendPointersInNewSegmentBench(tb, dir, 0, 1, 1_000_000, seg1Records, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i % 251)}, 768)
	})
	ptrs2 := appendPointersInNewSegmentBench(tb, dir, 0, 2, uint64(1_000_000+seg1Records), seg2Records, func(i int) []byte {
		return bytes.Repeat([]byte{byte((i + 7) % 251)}, 768)
	})

	bt, ok := db.NewBatch().(*Batch)
	if !ok {
		_ = db.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("NewBatch type assertion failed")
	}
	// Keep only a subset of segment-1 pointers live so rewrite has deterministic
	// stale bytes in the selected source.
	for i := range ptrs1 {
		if i%4 != 0 {
			continue
		}
		if err := bt.SetPointer([]byte(fmt.Sprintf("s1-live-%06d", i)), ptrs1[i]); err != nil {
			_ = bt.Close()
			_ = db.Close()
			_ = os.RemoveAll(dir)
			tb.Fatalf("SetPointer(s1): %v", err)
		}
	}
	if outer {
		if err := bt.WriteSync(); err != nil {
			tb.Fatalf("first-generation seed: %v", err)
		}
		_ = bt.Close()
		if err := leafLog.(*rewriteWriter).rotateLeaf(); err != nil {
			tb.Fatal(err)
		}
		bt = db.NewBatch().(*Batch)
	}
	for i := range ptrs2 {
		if err := bt.SetPointer([]byte(fmt.Sprintf("s2-live-%06d", i)), ptrs2[i]); err != nil {
			_ = bt.Close()
			_ = db.Close()
			_ = os.RemoveAll(dir)
			tb.Fatalf("SetPointer(s2): %v", err)
		}
	}
	if err := bt.Write(); err != nil {
		_ = bt.Close()
		_ = db.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("seed Write: %v", err)
	}
	_ = bt.Close()

	if err := db.RefreshValueLogSet(); err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("RefreshValueLogSet: %v", err)
	}

	sourceIDs := []uint32{ptrs1[0].FileID}
	cleanup := func() {
		_ = db.Close()
		if leafLog != nil {
			_ = leafLog.(*rewriteWriter).Close()
		}
		_ = os.RemoveAll(dir)
	}
	return db, sourceIDs, cleanup
}

func appendPointersInNewSegmentBench(tb testing.TB, dir string, lane, seq uint32, ridBase uint64, n int, valueAt func(i int) []byte) []page.ValuePtr {
	tb.Helper()
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		tb.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		tb.Fatalf("encode file id lane=%d seq=%d: %v", lane, seq, err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		tb.Fatalf("new writer: %v", err)
	}
	ptrs := make([]page.ValuePtr, 0, n)
	for i := 0; i < n; i++ {
		ptr, err := w.Append(0, nil, ridBase+uint64(i), valueAt(i))
		if err != nil {
			_ = w.Close()
			tb.Fatalf("append rid=%d: %v", ridBase+uint64(i), err)
		}
		ptrs = append(ptrs, ptr)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}
	registerTestValueLogProducer(tb, dir, path, fileID)
	return ptrs
}

func BenchmarkCollectRewriteSwapPointerMatches_DeltaTracking(b *testing.B) {
	const swapCount = 4096

	db, swaps, cleanup := setupRewriteSwapMatchBench(b, swapCount)
	defer cleanup()

	snap := db.AcquireSnapshot()
	if snap == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		b.Fatalf("missing snapshot state")
	}
	defer func() { _ = snap.Close() }()

	bench := func(b *testing.B, trackDelta bool) {
		batch := batchpkg.Acquire(db.valueLogManager, db.InlineThreshold())
		defer batchpkg.Release(batch)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batch.Reset()
			delta, err := collectRewriteSwapPointerMatches(&snap.tree, batch, swaps, trackDelta)
			if err != nil {
				b.Fatalf("collectRewriteSwapPointerMatches: %v", err)
			}
			if trackDelta && delta == nil {
				b.Fatalf("expected non-nil delta when tracking enabled")
			}
			if !trackDelta && delta != nil {
				b.Fatalf("expected nil delta when tracking disabled")
			}
			if trackDelta {
				releaseValueLogRefDelta(delta)
			}
		}
	}

	b.Run("NoDelta", func(b *testing.B) { bench(b, false) })
	b.Run("WithDelta", func(b *testing.B) { bench(b, true) })
}

func setupRewriteSwapMatchBench(tb testing.TB, swapCount int) (*DB, []rewriteSwap, func()) {
	tb.Helper()
	dir, err := os.MkdirTemp("", "treedb-vlog-rewrite-swap-bench-*")
	if err != nil {
		tb.Fatalf("MkdirTemp: %v", err)
	}
	db, err := Open(Options{
		Dir:                    dir,
		Durability:             DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
		},
	})
	if err != nil {
		_ = os.RemoveAll(dir)
		tb.Fatalf("Open: %v", err)
	}

	bt, ok := db.NewBatch().(*Batch)
	if !ok {
		_ = db.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("NewBatch type assertion failed")
	}

	swaps := make([]rewriteSwap, 0, swapCount)
	for i := 0; i < swapCount; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i+1))
		oldPtr := page.ValuePtr{
			Offset: uint64(4 + (i * 96)),
			Length: 96,
			FileID: page.ValueLogFileID(uint32((i % 64) + 1)),
		}
		newPtr := page.ValuePtr{
			Offset: oldPtr.Offset,
			Length: oldPtr.Length,
			FileID: page.ValueLogFileID(uint32((i % 64) + 101)),
		}
		if err := bt.SetPointer(key[:], oldPtr); err != nil {
			_ = bt.Close()
			_ = db.Close()
			_ = os.RemoveAll(dir)
			tb.Fatalf("SetPointer(%d): %v", i, err)
		}
		keyCopy := append([]byte(nil), key[:]...)
		swaps = append(swaps, rewriteSwap{
			key:    keyCopy,
			oldPtr: oldPtr,
			newPtr: newPtr,
		})
	}
	if err := bt.Write(); err != nil {
		_ = bt.Close()
		_ = db.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("seed Write: %v", err)
	}
	_ = bt.Close()

	cleanup := func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}
	return db, swaps, cleanup
}
