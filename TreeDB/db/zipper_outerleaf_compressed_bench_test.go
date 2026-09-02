package db

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type benchmarkPanicValueReader struct{}

func (benchmarkPanicValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	panic("unexpected value pointer read in zipper benchmark")
}

func (benchmarkPanicValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	panic("unexpected value pointer read in zipper benchmark")
}

func BenchmarkZipperApplyOuterLeafCompressedRandomOverwrite(b *testing.B) {
	const (
		keyCount  = 1 << 16
		batchSize = 8192
		valueSize = 128
	)

	dir := b.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		LeafPageReadCacheEntries:   -1,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		DisablePiggybackCompaction: true,
		ValueLog:                   ValueLogOptions{Compression: ValueLogCompressionBlock, BlockCodec: ValueLogBlockSnappy},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	d.valueLogManager.SetDisableReadChecksum(true)

	leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{
		Compression: ValueLogCompressionBlock,
		BlockCodec:  ValueLogBlockSnappy,
	})
	if err != nil {
		b.Fatalf("NewStandaloneLeafPageLog: %v", err)
	}
	defer leafLog.Close()
	d.SetLeafPageLog(leafLog)

	idx := d.idx.Load()
	if idx == nil || idx.zipper == nil {
		b.Fatal("missing index zipper")
	}
	z := idx.zipper
	p := idx.pager

	rootID, err := p.Alloc(1)
	if err != nil {
		b.Fatalf("alloc root: %v", err)
	}
	data, err := p.Get(rootID)
	if err != nil {
		b.Fatalf("get root: %v", err)
	}
	root := node.NewNode(data)
	root.SetPageID(rootID)
	root.SetType(page.PageTypeLeaf)
	root.UpdateChecksum()

	keys := make([][]byte, keyCount)
	loadValue := bytes.Repeat([]byte{'v'}, valueSize)
	loadBatch := batch.NewRetainingLargeEntries(benchmarkPanicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < keyCount; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		keys[i] = key
		loadBatch.Set(key, loadValue)
	}
	rootID, _, _, err = z.Apply(rootID, loadBatch)
	loadBatch.Close()
	if err != nil {
		b.Fatalf("initial apply: %v", err)
	}
	if err := leafLog.Flush(); err != nil {
		b.Fatalf("flush initial leaf log: %v", err)
	}
	if err := d.valueLogManager.Refresh(); err != nil {
		b.Fatalf("refresh initial leaf log: %v", err)
	}

	updateValue := bytes.Repeat([]byte{'u'}, valueSize)
	updateBatch := batch.NewRetainingLargeEntries(benchmarkPanicValueReader{}, page.DefaultInlineThreshold)
	for i := 0; i < batchSize; i++ {
		idx := (i*7919 + 17) & (keyCount - 1)
		updateBatch.Set(keys[idx], updateValue)
	}
	updateBatch.SortedEntries()
	defer updateBatch.Close()

	var leafMerges, leafLogLoads, leafLogWrites, leafBytesRead, leafBytesWritten int64
	var leafRecordBytesRead, leafRecordBytesWritten, leafCacheHits, leafReaderCalls, leafScratchReads, leafViewReads int64
	b.ReportAllocs()
	b.SetBytes(int64(batchSize * (8 + valueSize)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, metrics, err := z.Apply(rootID, updateBatch)
		if err != nil {
			b.Fatalf("apply overwrite batch: %v", err)
		}
		leafMerges += int64(metrics.ZipperLeafMerges)
		leafLogLoads += int64(metrics.ZipperLeafLogNodeLoads)
		leafLogWrites += int64(metrics.ZipperLeafLogPagesWritten)
		leafBytesRead += int64(metrics.ZipperLeafLogNodeBytesRead)
		leafBytesWritten += int64(metrics.ZipperLeafLogPageBytesWritten)
		leafRecordBytesRead += int64(metrics.ZipperLeafLogRecordHintBytesRead)
		leafRecordBytesWritten += int64(metrics.ZipperLeafLogRecordHintBytesWritten)
		leafCacheHits += int64(metrics.ZipperLeafLogCacheHits)
		leafReaderCalls += int64(metrics.ZipperLeafLogReaderCalls)
		leafScratchReads += int64(metrics.ZipperLeafLogScratchReads)
		leafViewReads += int64(metrics.ZipperLeafLogViewReads)
	}
	b.StopTimer()

	n := float64(b.N)
	if n > 0 {
		b.ReportMetric(float64(batchSize), "batch_ops/op")
		b.ReportMetric(float64(leafMerges)/n, "leaf_merges/op")
		b.ReportMetric(float64(leafLogLoads)/n, "leaflog_loads/op")
		b.ReportMetric(float64(leafLogWrites)/n, "leaflog_writes/op")
		b.ReportMetric(float64(leafBytesRead)/n, "leaflog_read_B/op")
		b.ReportMetric(float64(leafBytesWritten)/n, "leaflog_write_B/op")
		b.ReportMetric(float64(leafRecordBytesRead)/n, "leaflog_record_read_B/op")
		b.ReportMetric(float64(leafRecordBytesWritten)/n, "leaflog_record_write_B/op")
		b.ReportMetric(float64(leafCacheHits)/n, "leaflog_cache_hits/op")
		b.ReportMetric(float64(leafReaderCalls)/n, "leaflog_reader_calls/op")
		b.ReportMetric(float64(leafScratchReads)/n, "leaflog_scratch_reads/op")
		b.ReportMetric(float64(leafViewReads)/n, "leaflog_view_reads/op")
	}
}
