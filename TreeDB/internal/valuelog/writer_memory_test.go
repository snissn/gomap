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
	if writer.encLimiter.buf != nil || writer.encLimiter.limit != 0 {
		t.Fatalf("encLimiter not cleared on Close: buf=%v limit=%d", writer.encLimiter.buf != nil, writer.encLimiter.limit)
	}
}

func TestAppendFrame_BlockCompressedBufferedPath_DoesNotGrowScratch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	records := make([]Record, 4)
	want := make([][]byte, len(records))
	for i := range records {
		v := make([]byte, 2048)
		for j := range v {
			v[j] = byte('a' + i)
		}
		copy(v, []byte("record-block-compressed"))
		records[i] = Record{RID: uint64(i + 1), Value: v}
		want[i] = append([]byte(nil), v...)
	}

	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, make([]page.ValuePtr, len(records)))
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected block-compressed frame to be kept")
	}
	if cap(writer.scratch) != 0 {
		_ = writer.Close()
		t.Fatalf("scratch cap after kept block-compressed append = %d, want 0", cap(writer.scratch))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = f.Close() }()

	for i, ptr := range ptrs {
		got, err := ReadAt(f, ptr, true)
		if err != nil {
			t.Fatalf("ReadAt %d: %v", i, err)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("ReadAt %d mismatch", i)
		}
	}
}
