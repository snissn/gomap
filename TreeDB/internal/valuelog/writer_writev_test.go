package valuelog

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendRawFramesWritevInto(t *testing.T) {
	if !writevSupported {
		t.Skip("writev not supported on this platform")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")

	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	// Force multiple writev flushes inside the batch append.
	writer.appendMax = 256 << 10

	const (
		k        = 4
		nRecords = 40
		valSize  = 20 << 10
	)
	records := make([]Record, 0, nRecords)
	for i := 0; i < nRecords; i++ {
		records = append(records, Record{
			RID:   uint64(i + 1),
			Value: bytes.Repeat([]byte{byte(i%251) + 1}, valSize),
		})
	}

	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendRawFramesWritevInto(records, k, dst)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("append: %v", err)
	}
	if stats.Records != len(records) {
		_ = writer.Close()
		t.Fatalf("expected %d records, got %d", len(records), stats.Records)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reader, err := NewReader(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	for i := 0; i < len(records); i++ {
		rid, val, gotPtr, err := reader.ReadNext()
		if err != nil {
			_ = reader.Close()
			t.Fatalf("read next: %v", err)
		}
		if rid != records[i].RID {
			_ = reader.Close()
			t.Fatalf("rid mismatch at %d: got %d want %d", i, rid, records[i].RID)
		}
		if !bytes.Equal(val, records[i].Value) {
			_ = reader.Close()
			t.Fatalf("value mismatch at %d", i)
		}
		if gotPtr != ptrs[i] {
			_ = reader.Close()
			t.Fatalf("ptr mismatch at %d: got %+v want %+v", i, gotPtr, ptrs[i])
		}
	}
	if _, _, _, err := reader.ReadNext(); !errors.Is(err, io.EOF) {
		_ = reader.Close()
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = reader.Close()
}

func TestShouldUseRawWritev_DefaultsPrefer4KiBMediumBatch(t *testing.T) {
	if !writevSupported {
		t.Skip("writev not supported on this platform")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	const (
		k       = 8
		n       = 16
		valSize = 4 << 10
	)
	records := make([]Record, n)
	rawBytes := 0
	for i := 0; i < n; i++ {
		value := bytes.Repeat([]byte{byte(i + 1)}, valSize)
		records[i] = Record{RID: uint64(i + 1), Value: value}
		rawBytes += len(value)
	}
	if !writer.shouldUseRawWritev(records, k, rawBytes) {
		t.Fatalf("expected writev strategy to enable 4KiB medium-batch path")
	}
}

func TestShouldUseRawWritev_RespectsBatchAndMinAvgKnobs(t *testing.T) {
	if !writevSupported {
		t.Skip("writev not supported on this platform")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	makeRecords := func(n, valueBytes int) ([]Record, int) {
		records := make([]Record, n)
		raw := 0
		for i := 0; i < n; i++ {
			value := bytes.Repeat([]byte{byte(i + 1)}, valueBytes)
			records[i] = Record{RID: uint64(i + 1), Value: value}
			raw += len(value)
		}
		return records, raw
	}

	recordsSmallBatch, rawSmallBatch := makeRecords(4, 4<<10)
	if writer.shouldUseRawWritev(recordsSmallBatch, 4, rawSmallBatch) {
		t.Fatalf("expected writev strategy to reject undersized batch")
	}

	writer.SetRawWritevStrategy(16<<10, 8)
	records4K, raw4K := makeRecords(16, 4<<10)
	if writer.shouldUseRawWritev(records4K, 8, raw4K) {
		t.Fatalf("expected min-avg floor to disable 4KiB records")
	}
}

func TestShouldUseRawWritev_PrefersFallbackWithBufferedAppendData(t *testing.T) {
	if !writevSupported {
		t.Skip("writev not supported on this platform")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer func() { _ = writer.Close() }()

	const (
		k       = 8
		n       = 16
		valSize = 4 << 10
	)
	records := make([]Record, n)
	rawBytes := 0
	for i := 0; i < n; i++ {
		value := bytes.Repeat([]byte{byte(i + 1)}, valSize)
		records[i] = Record{RID: uint64(i + 1), Value: value}
		rawBytes += len(value)
	}

	// Non-empty appendBuf adds a mandatory flush to the writev path; for this
	// workload fallback can coalesce existing + new data into one buffered flush.
	writer.appendBuf = []byte("x")
	if writer.shouldUseRawWritev(records, k, rawBytes) {
		t.Fatalf("expected fallback path when append buffer is already non-empty")
	}
}

func TestPredictRawFallbackFlushes_LargeFrameUsesSingleDirectWrite(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("a"), 4096)},
	}
	const (
		k            = 1
		maxBytes     = 512
		withBuffered = 128
	)

	gotBuffered := predictRawFallbackFlushes(records, k, maxBytes, withBuffered)
	if gotBuffered != 2 {
		t.Fatalf("predictRawFallbackFlushes(with buffered)=%d want 2", gotBuffered)
	}

	gotEmpty := predictRawFallbackFlushes(records, k, maxBytes, 0)
	if gotEmpty != 1 {
		t.Fatalf("predictRawFallbackFlushes(no buffered)=%d want 1", gotEmpty)
	}
}

func BenchmarkRawWritevStrategy_ValueSizes(b *testing.B) {
	if !writevSupported {
		b.Skip("writev not supported on this platform")
	}

	dir := b.TempDir()
	path := filepath.Join(dir, "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		b.Fatalf("new writer: %v", err)
	}
	b.Cleanup(func() { _ = writer.Close() })

	makeRecords := func(n, valueBytes int) ([]Record, int) {
		records := make([]Record, n)
		raw := 0
		for i := 0; i < n; i++ {
			value := bytes.Repeat([]byte{byte(i + 1)}, valueBytes)
			records[i] = Record{RID: uint64(i + 1), Value: value}
			raw += len(value)
		}
		return records, raw
	}

	for _, valueBytes := range []int{1 << 10, 4 << 10, 8 << 10, 16 << 10} {
		records, rawBytes := makeRecords(64, valueBytes)
		b.Run("value_"+strconv.Itoa(valueBytes), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = writer.shouldUseRawWritev(records, 8, rawBytes)
			}
		})
	}
}
