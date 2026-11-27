package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestInspectCOWLeafRecordPinnedIntoBufferedReadParity(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "leaf_vlog")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "value-l255-000001.log")
	fileID, err := EncodeFileID(255, 1)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatal(err)
	}
	leaf := make([]byte, page.PageSize)
	for i := range leaf {
		leaf[i] = byte((i*31 + 7) & 0xff)
	}
	ptrs, err := w.AppendFrame(0, nil, []Record{{RID: 1, Value: leaf}})
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	ptr := ptrs[0]
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	rawFile, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rawFile.Close()

	// Unmapped currentWritable File exercises the single-ReadAt buffered path.
	f := &File{ID: fileID, Path: path, File: rawFile, manager: &Manager{}}
	f.currentWritable.Store(true)
	limits := COWReadLimits{MaxRecordBytes: 64 << 20, MaxRawBytes: 64 << 20, MaxValueBytes: 64 << 20}
	scratch := make([]byte, 16*1024)

	shape, bufInput, fromMmap, err := f.InspectCOWLeafRecordPinnedInto(ptr, limits, scratch)
	if err != nil {
		t.Fatalf("InspectCOWLeafRecordPinnedInto: %v", err)
	}
	if fromMmap {
		t.Fatal("expected fromMmap=false for unmapped file")
	}
	if len(bufInput) == 0 {
		t.Fatal("expected non-empty single-ReadAt buffered input")
	}
	if f.totalMmapReadHits() != 0 {
		t.Fatalf("expected 0 mmap read hits, got %d", f.totalMmapReadHits())
	}

	payloadScratch := make([]byte, int(shape.PayloadBytes()))
	rawScratch := make([]byte, int(shape.RawBytes))
	pageDst := make([]byte, page.PageSize)
	got, err := f.ReadCOWLeafPageBuffered(ptr, shape, bufInput, payloadScratch, rawScratch, pageDst, nil)
	if err != nil {
		t.Fatalf("ReadCOWLeafPageBuffered: %v", err)
	}
	if !bytes.Equal(got, leaf) {
		t.Fatal("decoded buffered leaf bytes mismatch")
	}
	if f.totalMmapReadHits() != 0 {
		t.Fatalf("ReadCOWLeafPageBuffered unexpectedly bumped mmap read hits: %d", f.totalMmapReadHits())
	}

	// Corrupting one byte in bufInput must fail full CRC32 verification.
	corrupt := append([]byte(nil), bufInput...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := f.ReadCOWLeafPageBuffered(ptr, shape, corrupt, payloadScratch, rawScratch, pageDst, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted buffered input err=%v want ErrCorrupt", err)
	}

	// Tight limits must still be rejected cleanly.
	tight := COWReadLimits{MaxRecordBytes: 1, MaxRawBytes: 1, MaxValueBytes: 1}
	if _, _, _, err := f.InspectCOWLeafRecordPinnedInto(ptr, tight, scratch); !errors.Is(err, ErrCOWReadCapacity) {
		t.Fatalf("tight limits err=%v want ErrCOWReadCapacity", err)
	}
}
