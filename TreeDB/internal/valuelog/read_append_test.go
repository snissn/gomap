package valuelog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func writeCompressedGroupedFrameForReadAppendTest(t *testing.T, records []Record) (*os.File, []page.ValuePtr) {
	t.Helper()

	dir := t.TempDir()
	fileID := page.ValueLogFileID(1)
	path := filepath.Join(dir, "value-l0-000001.log")

	writer, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	writer.SetBlockCompression(BlockCodecSnappy, true)

	dst := make([]page.ValuePtr, len(records))
	ptrs, stats, err := writer.AppendFrameWithStatsInto(0, nil, records, dst)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Kept {
		_ = writer.Close()
		t.Fatalf("expected compressed grouped frame to be kept")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, ptrs
}

func TestReadAtWithDictAppend_CompressedGroupedSingleRecord_ReusesProvidedBuffer(t *testing.T) {
	value := bytes.Repeat([]byte("single-record-compressible-payload-"), 128)
	f, ptrs := writeCompressedGroupedFrameForReadAppendTest(t, []Record{
		{RID: 1, Value: value},
	})

	dst := make([]byte, len("prefix:"), len("prefix:")+len(value)+64)
	copy(dst, []byte("prefix:"))

	out, err := ReadAtWithDictAppend(f, ptrs[0], false, nil, nil, nil, templ.DecodeOptions{}, dst)
	if err != nil {
		t.Fatalf("ReadAtWithDictAppend: %v", err)
	}
	if !bytes.Equal(out[len("prefix:"):], value) {
		t.Fatalf("value mismatch")
	}
	if string(out[:len("prefix:")]) != "prefix:" {
		t.Fatalf("prefix mismatch: got %q", out[:len("prefix:")])
	}
	if &out[0] != &dst[0] {
		t.Fatalf("expected append read to reuse provided destination buffer")
	}
}

func TestReadAtWithDictAppend_CompressedGroupedMultiRecord_ReusesProvidedBuffer(t *testing.T) {
	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("alpha-"), 96)},
		{RID: 2, Value: bytes.Repeat([]byte("bravo-"), 96)},
		{RID: 3, Value: bytes.Repeat([]byte("charlie-"), 96)},
	}
	f, ptrs := writeCompressedGroupedFrameForReadAppendTest(t, records)

	dst := make([]byte, len("prefix:"), len("prefix:")+len(records[1].Value)+64)
	copy(dst, []byte("prefix:"))

	out, err := ReadAtWithDictAppend(f, ptrs[1], false, nil, nil, nil, templ.DecodeOptions{}, dst)
	if err != nil {
		t.Fatalf("ReadAtWithDictAppend: %v", err)
	}
	if !bytes.Equal(out[len("prefix:"):], records[1].Value) {
		t.Fatalf("value mismatch")
	}
	if string(out[:len("prefix:")]) != "prefix:" {
		t.Fatalf("prefix mismatch: got %q", out[:len("prefix:")])
	}
	if &out[0] != &dst[0] {
		t.Fatalf("expected append read to reuse provided destination buffer")
	}
}

func TestFileReadAppend_CompressedGroupedFallback_ReusesProvidedBuffer(t *testing.T) {
	value := bytes.Repeat([]byte("file-readappend-compressible-payload-"), 96)
	f, ptrs := writeCompressedGroupedFrameForReadAppendTest(t, []Record{
		{RID: 1, Value: value},
	})

	vf := &File{
		ID:   page.ValueLogFileID(1),
		File: f,
	}
	vf.mmapData.Store([]byte(nil))

	dst := make([]byte, len("prefix:"), len("prefix:")+len(value)+64)
	copy(dst, []byte("prefix:"))

	out, err := vf.ReadAppend(ptrs[0], false, dst)
	if err != nil {
		t.Fatalf("ReadAppend: %v", err)
	}
	if !bytes.Equal(out[len("prefix:"):], value) {
		t.Fatalf("value mismatch")
	}
	if &out[0] != &dst[0] {
		t.Fatalf("expected file ReadAppend fallback to reuse provided destination buffer")
	}
}
