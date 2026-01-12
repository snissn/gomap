package slab

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func newZSTDConfig(t *testing.T) compressionConfig {
	t.Helper()
	cfg, err := normalizeCompressionOptions(CompressionOptions{Kind: CompressionZSTD})
	if err != nil {
		t.Fatalf("normalize compression: %v", err)
	}
	return cfg
}

func TestFrameGroupRoundTrip(t *testing.T) {
	cfg := newZSTDConfig(t)
	values := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}

	record, k, err := buildFrameGroupRecord(values, &cfg)
	if err != nil {
		t.Fatalf("buildFrameGroupRecord err=%v", err)
	}
	if k != len(values) {
		t.Fatalf("expected k=%d got %d", len(values), k)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.slab")
	slab, err := OpenSlab(path, 1)
	if err != nil {
		t.Fatalf("open slab: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	offset, err := slab.WriteBatch(record)
	if err != nil {
		t.Fatalf("write batch: %v", err)
	}

	bodyLen := uint32(len(record) - 4) // exclude checksum like normal pointers
	for i := range values {
		ptr := page.ValuePtr{
			Offset: uint64(offset + 4),
			Length: page.ValuePtrMarkGrouped(page.ValuePtrMarkCompressed(bodyLen), uint8(i)),
			FileID: slab.ID,
		}
		raw, err := slab.ReadUnsafe(int64(ptr.Offset), true)
		if err != nil {
			t.Fatalf("read unsafe: %v", err)
		}
		got, err := decodeValue(ptr, raw, &cfg)
		if err != nil {
			t.Fatalf("decodeValue: %v", err)
		}
		if string(got) != string(values[i]) {
			t.Fatalf("value mismatch idx=%d got=%q want=%q", i, got, values[i])
		}
	}
}

func TestFrameGroupPartial(t *testing.T) {
	cfg := newZSTDConfig(t)
	values := [][]byte{[]byte("solo")}
	record, k, err := buildFrameGroupRecord(values, &cfg)
	if err != nil {
		t.Fatalf("buildFrameGroupRecord err=%v", err)
	}
	if k != 1 {
		t.Fatalf("expected k=1 got %d", k)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "partial.slab")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create slab: %v", err)
	}
	_ = f.Close()
	slab, err := OpenSlab(path, 2)
	if err != nil {
		t.Fatalf("open slab: %v", err)
	}
	t.Cleanup(func() { _ = slab.Close() })

	offset, err := slab.WriteBatch(record)
	if err != nil {
		t.Fatalf("write batch: %v", err)
	}
	bodyLen := uint32(len(record) - 4)
	ptr := page.ValuePtr{
		Offset: uint64(offset + 4),
		Length: page.ValuePtrMarkGrouped(page.ValuePtrMarkCompressed(bodyLen), 0),
		FileID: slab.ID,
	}
	raw, err := slab.ReadUnsafe(int64(ptr.Offset), true)
	if err != nil {
		t.Fatalf("read unsafe: %v", err)
	}
	got, err := decodeValue(ptr, raw, &cfg)
	if err != nil {
		t.Fatalf("decodeValue: %v", err)
	}
	if string(got) != "solo" {
		t.Fatalf("value mismatch got=%q want=solo", got)
	}
}
