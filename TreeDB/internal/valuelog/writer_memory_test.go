package valuelog

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestNewWriter_LazyScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if cap(writer.scratch) != 0 {
		t.Fatalf("cap(scratch)=%d want 0", cap(writer.scratch))
	}
	if cap(writer.appendBuf) != 0 {
		t.Fatalf("cap(appendBuf)=%d want 0", cap(writer.appendBuf))
	}
	if cap(writer.rawScratch) != 0 {
		t.Fatalf("cap(rawScratch)=%d want 0", cap(writer.rawScratch))
	}
	if cap(writer.encScratch) != 0 {
		t.Fatalf("cap(encScratch)=%d want 0", cap(writer.encScratch))
	}
	if cap(writer.blockScratch) != 0 {
		t.Fatalf("cap(blockScratch)=%d want 0", cap(writer.blockScratch))
	}

	sinkWriter := NewWriterWithSink(io.Discard, page.ValueLogFileID(2))
	if cap(sinkWriter.scratch) != 0 {
		t.Fatalf("cap(sink scratch)=%d want 0", cap(sinkWriter.scratch))
	}
}

func TestWriterFlush_TrimsOversizedScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	oversized := writerScratchTrimCap + 1
	writer.scratch = make([]byte, 32, oversized)
	writer.rawScratch = make([]byte, 16, oversized)
	writer.encScratch = make([]byte, 8, oversized)
	writer.blockScratch = make([]byte, 4, oversized)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = oversized

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(writer.scratch) != 0 || cap(writer.scratch) != writerScratchKeepCap {
		t.Fatalf("scratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), writerScratchKeepCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != writerScratchKeepCap {
		t.Fatalf("rawScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), writerScratchKeepCap)
	}
	if len(writer.encScratch) != 0 || cap(writer.encScratch) != writerScratchKeepCap {
		t.Fatalf("encScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.encScratch), cap(writer.encScratch), writerScratchKeepCap)
	}
	if len(writer.blockScratch) != 0 || cap(writer.blockScratch) != writerScratchKeepCap {
		t.Fatalf("blockScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.blockScratch), cap(writer.blockScratch), writerScratchKeepCap)
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared after Flush: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestWriterFlush_KeepsScratchWithinHysteresis(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	scratchCap := writerScratchTrimCap
	rawCap := writerScratchTrimCap - 1
	writer.scratch = make([]byte, 32, scratchCap)
	writer.rawScratch = make([]byte, 16, rawCap)

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(writer.scratch) != 0 || cap(writer.scratch) != scratchCap {
		t.Fatalf("scratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), scratchCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != rawCap {
		t.Fatalf("rawScratch after Flush = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), rawCap)
	}
}

func TestWriterRotateTo_TrimsOversizedScratchBuffers(t *testing.T) {
	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))

	oversized := writerScratchTrimCap + 1
	writer.scratch = make([]byte, 32, oversized)
	writer.rawScratch = make([]byte, 16, oversized)
	writer.encScratch = make([]byte, 8, oversized)
	writer.blockScratch = make([]byte, 4, oversized)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = oversized

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000002.log")
	if err := writer.RotateTo(path, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if len(writer.scratch) != 0 || cap(writer.scratch) != writerScratchKeepCap {
		t.Fatalf("scratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.scratch), cap(writer.scratch), writerScratchKeepCap)
	}
	if len(writer.rawScratch) != 0 || cap(writer.rawScratch) != writerScratchKeepCap {
		t.Fatalf("rawScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.rawScratch), cap(writer.rawScratch), writerScratchKeepCap)
	}
	if len(writer.encScratch) != 0 || cap(writer.encScratch) != writerScratchKeepCap {
		t.Fatalf("encScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.encScratch), cap(writer.encScratch), writerScratchKeepCap)
	}
	if len(writer.blockScratch) != 0 || cap(writer.blockScratch) != writerScratchKeepCap {
		t.Fatalf("blockScratch after RotateTo = len %d cap %d, want len 0 cap %d", len(writer.blockScratch), cap(writer.blockScratch), writerScratchKeepCap)
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared after RotateTo: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestWriterRotateToFromSink_LazyAppendBuffer(t *testing.T) {
	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000002.log")
	if err := writer.RotateTo(path, page.ValueLogFileID(2)); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	defer func() { _ = writer.Close() }()

	if cap(writer.appendBuf) != 0 {
		t.Fatalf("cap(appendBuf)=%d want 0", cap(writer.appendBuf))
	}
}

func TestWriterCloseSinkFlushesAndReleasesBuffers(t *testing.T) {
	var sink bytes.Buffer
	writer := NewWriterWithSink(&sink, page.ValueLogFileID(1))
	if err := writer.writeBytes([]byte("buffered")); err != nil {
		t.Fatalf("writeBytes: %v", err)
	}
	writer.appendBuf = append(make([]byte, 0, defaultBufferSize), "append"...)
	writer.scratch = make([]byte, 1, 16)
	writer.rawScratch = make([]byte, 1, 16)
	writer.encScratch = make([]byte, 1, 16)
	writer.blockScratch = make([]byte, 1, 16)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = 16

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, want := sink.String(), "bufferedappend"; got != want {
		t.Fatalf("sink contents=%q want %q", got, want)
	}
	if writer.appendBuf != nil {
		t.Fatalf("appendBuf retained after Close: cap=%d", cap(writer.appendBuf))
	}
	if writer.scratch != nil || writer.rawScratch != nil || writer.encScratch != nil || writer.blockScratch != nil {
		t.Fatalf("scratch buffers retained after Close")
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter retained after Close: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func drainWriterAppendBufPoolForTest() {
	for {
		select {
		case buf := <-writerAppendBufPool:
			noteWriterAppendBufPoolTake(cap(buf))
		default:
			return
		}
	}
}

func TestWriterAppendBufPool_BoundedDefaultBuffers(t *testing.T) {
	drainWriterAppendBufPoolForTest()
	t.Cleanup(drainWriterAppendBufPoolForTest)

	for i := 0; i < writerAppendBufPoolEntries+3; i++ {
		putWriterAppendBuf(make([]byte, 0, defaultBufferSize))
	}
	if got := len(writerAppendBufPool); got != writerAppendBufPoolEntries {
		t.Fatalf("writer append pool entries=%d want=%d", got, writerAppendBufPoolEntries)
	}

	for i := 0; i < writerAppendBufPoolEntries; i++ {
		buf := getWriterAppendBuf(defaultBufferSize)
		if cap(buf) != defaultBufferSize {
			t.Fatalf("pooled append buffer cap=%d want=%d", cap(buf), defaultBufferSize)
		}
	}
	if got := len(writerAppendBufPool); got != 0 {
		t.Fatalf("writer append pool entries after get=%d want=0", got)
	}
}

func TestWriterAppendBufStats_PoolAndDropCounters(t *testing.T) {
	drainWriterAppendBufPoolForTest()
	t.Cleanup(drainWriterAppendBufPoolForTest)

	before := WriterAppendBufferStatsSnapshot()
	putWriterAppendBuf(make([]byte, 0, defaultBufferSize))
	afterPut := WriterAppendBufferStatsSnapshot()
	if got, want := afterPut.PoolRetainedBytes-before.PoolRetainedBytes, uint64(defaultBufferSize); got != want {
		t.Fatalf("pool retained bytes delta=%d want=%d", got, want)
	}
	if got := afterPut.PutsTotal - before.PutsTotal; got != 1 {
		t.Fatalf("puts delta=%d want=1", got)
	}

	buf := getWriterAppendBuf(defaultBufferSize)
	afterGet := WriterAppendBufferStatsSnapshot()
	if cap(buf) != defaultBufferSize {
		t.Fatalf("pooled append buffer cap=%d want=%d", cap(buf), defaultBufferSize)
	}
	if got := afterGet.HitsTotal - afterPut.HitsTotal; got != 1 {
		t.Fatalf("hits delta=%d want=1", got)
	}
	if got, want := afterPut.PoolRetainedBytes-afterGet.PoolRetainedBytes, uint64(defaultBufferSize); got != want {
		t.Fatalf("pool retained bytes released delta=%d want=%d", got, want)
	}

	putWriterAppendBuf(make([]byte, 0, defaultBufferSize+1))
	afterDrop := WriterAppendBufferStatsSnapshot()
	if got := afterDrop.DropsTotal - afterGet.DropsTotal; got != 1 {
		t.Fatalf("drops delta=%d want=1", got)
	}
	if got, want := afterDrop.DroppedBytesTotal-afterGet.DroppedBytesTotal, uint64(defaultBufferSize+1); got != want {
		t.Fatalf("dropped bytes delta=%d want=%d", got, want)
	}
}

func BenchmarkWriterAppendBufPoolMultiWriterSyncCycles(b *testing.B) {
	const writers = 16

	drainWriterAppendBufPoolForTest()
	b.Cleanup(drainWriterAppendBufPoolForTest)

	dir := b.TempDir()
	ws := make([]*Writer, 0, writers)
	for i := 0; i < writers; i++ {
		path := filepath.Join(dir, "value-"+string(rune('a'+i))+".log")
		w, err := NewWriter(path, page.ValueLogFileID(uint32(i+1)))
		if err != nil {
			b.Fatalf("NewWriter(%d): %v", i, err)
		}
		w.syncFn = func(*os.File) error { return nil }
		ws = append(ws, w)
	}
	defer func() {
		for _, w := range ws {
			_ = w.Close()
		}
	}()

	payload := []byte("x")
	before := WriterAppendBufferStatsSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, w := range ws {
			if err := w.writeBytes(payload); err != nil {
				b.Fatal(err)
			}
		}
		for _, w := range ws {
			if err := w.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	after := WriterAppendBufferStatsSnapshot()
	if b.N > 0 {
		b.ReportMetric(float64(after.AllocCallsTotal-before.AllocCallsTotal)/float64(b.N), "appendbuf_allocs/op")
		b.ReportMetric(float64(after.AllocatedBytesTotal-before.AllocatedBytesTotal)/float64(b.N)/(1<<20), "appendbuf_alloc_MiB/op")
		b.ReportMetric(float64(after.DropsTotal-before.DropsTotal)/float64(b.N), "appendbuf_drops/op")
	}
}

func TestWriterEnsureAppendBufCapPreservesPendingBytes(t *testing.T) {
	writer := &Writer{
		appendBuf: append(make([]byte, 0, 4), "pending"...),
	}

	writer.ensureAppendBufCap(64)

	if string(writer.appendBuf) != "pending" {
		t.Fatalf("appendBuf=%q want pending bytes preserved", string(writer.appendBuf))
	}
	if cap(writer.appendBuf) < 64 {
		t.Fatalf("cap(appendBuf)=%d want at least 64", cap(writer.appendBuf))
	}
}

func TestWriterEnsureAppendBufCapReturnsDefaultBufferOnGrowth(t *testing.T) {
	drainWriterAppendBufPoolForTest()
	t.Cleanup(drainWriterAppendBufPoolForTest)

	writer := &Writer{
		appendBuf: append(make([]byte, 0, defaultBufferSize), "pending"...),
	}
	writer.ensureAppendBufCap(defaultBufferSize + 1)

	if string(writer.appendBuf) != "pending" {
		t.Fatalf("appendBuf=%q want pending bytes preserved", string(writer.appendBuf))
	}
	if cap(writer.appendBuf) < defaultBufferSize+1 {
		t.Fatalf("cap(appendBuf)=%d want at least %d", cap(writer.appendBuf), defaultBufferSize+1)
	}
	if got := len(writerAppendBufPool); got != 1 {
		t.Fatalf("writer append pool entries=%d want=1", got)
	}
}

func TestWriterFlushKeepsIdleAppendBufferForReuse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	writer.appendBuf = make([]byte, 0, defaultBufferSize)
	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if cap(writer.appendBuf) != defaultBufferSize {
		t.Fatalf("appendBuf cap after Flush=%d want=%d", cap(writer.appendBuf), defaultBufferSize)
	}
}

func TestWriterSyncReleasesIdleAppendBuffer(t *testing.T) {
	drainWriterAppendBufPoolForTest()
	t.Cleanup(drainWriterAppendBufPoolForTest)

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = writer.Close() }()

	writer.appendBuf = append(make([]byte, 0, defaultBufferSize), "pending"...)
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if writer.appendBuf != nil {
		t.Fatalf("appendBuf retained after Sync: len=%d cap=%d", len(writer.appendBuf), cap(writer.appendBuf))
	}
	if got := len(writerAppendBufPool); got != 1 {
		t.Fatalf("writer append pool entries after Sync=%d want=1", got)
	}
}

func TestWriterClose_ReleasesScratchBuffers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	writer.scratch = make([]byte, 32, writerScratchTrimCap+1)
	writer.rawScratch = make([]byte, 16, writerScratchTrimCap+1)
	writer.encScratch = make([]byte, 8, writerScratchTrimCap+1)
	writer.blockScratch = make([]byte, 4, writerScratchTrimCap+1)
	writer.appendBuf = make([]byte, 32, defaultBufferSize)
	writer.encLimiter.buf = writer.encScratch
	writer.encLimiter.limit = writerScratchTrimCap + 1

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if writer.scratch != nil {
		t.Fatalf("scratch not released on Close")
	}
	if writer.rawScratch != nil {
		t.Fatalf("rawScratch not released on Close")
	}
	if writer.encScratch != nil {
		t.Fatalf("encScratch not released on Close")
	}
	if writer.blockScratch != nil {
		t.Fatalf("blockScratch not released on Close")
	}
	if writer.appendBuf != nil {
		t.Fatalf("appendBuf not released on Close")
	}
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared on Close: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestWriterBlockCompressionSingleRecordAvoidsRawConcatScratch(t *testing.T) {
	writer := NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	writer.SetBlockCompression(BlockCodecSnappy, true)

	value := bytes.Repeat([]byte("a"), 4096)
	wantValue := bytes.Clone(value)
	_, stats, err := writer.AppendFrameWithStatsInto(0, nil, []Record{{RID: 1, Value: value}}, make([]page.ValuePtr, 1))
	if err != nil {
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Kept {
		t.Fatalf("expected single-record block compression to be kept, stats=%+v", stats)
	}
	if cap(writer.rawScratch) != 0 {
		t.Fatalf("rawScratch cap=%d want 0 for single-record block compression", cap(writer.rawScratch))
	}
	if !bytes.Equal(value, wantValue) {
		t.Fatal("single-record block compression mutated source value")
	}
}

func TestFramePreparerBlockCompressionSingleRecordAvoidsRawConcatScratch(t *testing.T) {
	prep := NewFramePreparer()
	prep.SetBlockCompression(BlockCodecSnappy, true)

	value := bytes.Repeat([]byte("b"), 4096)
	wantValue := bytes.Clone(value)
	_, stats, err := prep.PrepareFrameInto(nil, 0, nil, []Record{{RID: 1, Value: value}})
	if err != nil {
		t.Fatalf("PrepareFrameInto: %v", err)
	}
	if !stats.Kept {
		t.Fatalf("expected single-record block compression to be kept, stats=%+v", stats)
	}
	if cap(prep.rawScratch) != 0 {
		t.Fatalf("rawScratch cap=%d want 0 for single-record block compression", cap(prep.rawScratch))
	}
	if !bytes.Equal(value, wantValue) {
		t.Fatal("single-record block compression mutated source value")
	}
}
