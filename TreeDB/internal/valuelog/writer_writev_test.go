package valuelog

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
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
