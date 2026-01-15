package slab

import (
	"bytes"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/compression"
)

func TestSlabV2_ExactBoundary(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionNone,
		},
		OmitSlabKeys: true,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Force V2 by providing a mock profile and rotating.
	sm.compressionTrainer.AcceptProfile(&compression.ActiveProfile{
		Dict: make([]byte, 32768),
		K:    1,
	})
	if err := sm.rotateLocked(); err != nil {
		t.Fatal(err)
	}

	if sm.activeSlab.version != Version2 {
		t.Fatalf("expected V2 slab after rotation, got %d", sm.activeSlab.version)
	}

	// 1. Write a record that ends EXACTLY at the 2MB boundary.
	// Data starts at 64KB. Zone 0 capacity is 2MB - 64KB = 2,031,616 bytes.
	// Record overhead is 10 bytes (HeaderSize).
	// So payload should be 2,031,616 - 10 = 2,031,606 bytes.
	payloadSize := int(ZoneSize - SlabV2DataStart - int64(HeaderSize))
	payload := make([]byte, payloadSize)
	rand.Read(payload)

	// Use DisableCompression to ensure exact sizing
	ptr1, err := sm.AppendWithOptions(nil, payload, AppendOptions{DisableCompression: true})
	if err != nil {
		t.Fatal(err)
	}

	if err := sm.Sync(); err != nil {
		t.Fatal(err)
	}

	if sm.activeSlab.Size() != ZoneSize {
		t.Fatalf("expected size to be exactly 2MB, got %d", sm.activeSlab.Size())
	}

	// 2. Write a small record. It should trigger Zone 1 header insertion.
	smallPayload := []byte("boundary-test")
	ptr2, err := sm.AppendWithOptions(nil, smallPayload, AppendOptions{DisableCompression: true})
	if err != nil {
		t.Fatal(err)
	}

	// Expected offset: ZoneSize (2MB) + ZoneHeaderSize (64B) + CRC (4B)
	expectedOffset := uint64(ZoneSize + int64(ZoneHeaderSize) + 4)
	if ptr2.Offset != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, ptr2.Offset)
	}

	// 3. Verify reads
	val1, err := sm.Read(ptr1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val1, payload) {
		t.Fatal("val1 mismatch")
	}

	val2, err := sm.Read(ptr2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val2, smallPayload) {
		t.Fatal("val2 mismatch")
	}
}

func TestSlabV2_MaxRecord(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionNone,
		},
		OmitSlabKeys: true,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Force V2 by providing a mock profile and rotating.
	sm.compressionTrainer.AcceptProfile(&compression.ActiveProfile{
		Dict: make([]byte, 32768),
		K:    1,
	})
	if err := sm.rotateLocked(); err != nil {
		t.Fatal(err)
	}

	if sm.activeSlab.version != Version2 {
		t.Fatalf("expected V2 slab after rotation, got %d", sm.activeSlab.version)
	}

	// Max record size in V2 is ZoneSize - ZoneHeaderSize.
	// But it also must fit in the current zone.
	// If we write a record that is larger than current zone space but <= MaxV2Record,
	// it should move to the next zone.

	maxPayload := int(ZoneSize - int64(ZoneHeaderSize) - int64(HeaderSize))
	payload := make([]byte, maxPayload)
	rand.Read(payload)

	// This should move to Zone 1 because it won't fit in Zone 0 (which has 2MB - 64KB space).
	// Use DisableCompression to test absolute limit.
	ptr, err := sm.AppendWithOptions(nil, payload, AppendOptions{DisableCompression: true})
	if err != nil {
		t.Fatal(err)
	}

	// Should be at start of Zone 1. Offset = 2MB + ZoneHeader (64) + CRC (4)
	expectedOffset := uint64(ZoneSize + int64(ZoneHeaderSize) + 4)
	if ptr.Offset != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, ptr.Offset)
	}

	val, err := sm.Read(ptr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, payload) {
		t.Fatal("payload mismatch")
	}

	// Attempting a record that is too large for a single V2 zone (>= 2MB - 64B) should fail.
	// We use ZoneSize + 1 to be absolutely sure it's rejected by V2 boundary logic.
	tooLarge := make([]byte, ZoneSize+1)
	_, err = sm.AppendWithOptions(nil, tooLarge, AppendOptions{DisableCompression: true})
	if err != ErrRecordTooLarge {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
}

func TestSlabV2_MixedVersionReopen(t *testing.T) {
	dir := t.TempDir()

	// 1. Create a V1 slab
	optsV1 := Options{Compression: CompressionOptions{Kind: CompressionNone}}
	sm1, err := NewSlabManagerWithOptions(dir, optsV1)
	if err != nil {
		t.Fatal(err)
	}
	val1 := []byte("v1-data")
	ptr1, err := sm1.Append(nil, val1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("V1 record: ptr1.Offset=%d", ptr1.Offset)
	sm1.Close()

	// 2. Open and Rotate to V2 (mocking profile readiness)
	sm2, err := NewSlabManagerWithOptions(dir, optsV1)
	if err != nil {
		t.Fatal(err)
	}

	// Manual rotate
	if err := sm2.rotateLocked(); err != nil {
		t.Fatal(err)
	}

	// Force it to V2 on disk so reopen sees it
	dict := []byte("test-dict-pattern-repeated-for-zstd-validation-starts-with-magic-hopefully")
	header := make([]byte, SlabV2DataStart)
	copy(header[0:8], MagicV2)
	header[8] = Version2
	copy(header[FileHeaderSizeV2:], dict)

	slab1Path := sm2.GetSlabPath(1)
	f, err := os.OpenFile(slab1Path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(header, 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Sync()
	f.Close()

	if err := sm2.TruncateActiveSlab(uint64(SlabV2DataStart)); err != nil {
		t.Fatal(err)
	}

	// Update in-memory state of sm2.activeSlab (which is slab 1)
	sm2.activeSlab.version = Version2
	sm2.activeSlab.globalDict = dict

	val2 := []byte("v2-data-compressed-with-dict")
	ptr2, err := sm2.Append(nil, val2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("V2 record: ptr2.Offset=%d, slabSize=%d", ptr2.Offset, sm2.activeSlab.Size())

	// Record ID should be 1
	if ptr2.FileID != 1 {
		t.Fatalf("expected fileID 1, got %d", ptr2.FileID)
	}

	sm2.Close()

	// 3. Reopen and verify both can be read
	sm3, err := NewSlabManagerWithOptions(dir, optsV1)
	if err != nil {
		t.Fatal(err)
	}
	defer sm3.Close()

	r1, err := sm3.Read(ptr1)
	if err != nil {
		t.Fatalf("read v1 failed: %v", err)
	}
	if !bytes.Equal(r1, val1) {
		t.Fatalf("v1 mismatch: got %q, want %q", r1, val1)
	}

	r2, err := sm3.Read(ptr2)
	if err != nil {
		t.Fatalf("read v2 failed: %v", err)
	}
	if !bytes.Equal(r2, val2) {
		t.Fatalf("v2 mismatch: got %q, want %q", r2, val2)
	}

	// Verify slab 1 is indeed V2
	if sm3.slabs[1].version != Version2 {
		t.Fatal("slab 1 should be V2")
	}
}

func TestSlabV2_GroupedBoundary(t *testing.T) {
	dir := t.TempDir()
	// Enable grouping
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
		OmitSlabKeys: true,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Manually force V2
	dict := make([]byte, GlobalDictSize)
	header := make([]byte, SlabV2DataStart)
	copy(header[0:8], MagicV2)
	header[8] = Version2
	if _, err := sm.activeSlab.File.WriteAt(header, 0); err != nil {
		t.Fatal(err)
	}
	if err := sm.TruncateActiveSlab(uint64(SlabV2DataStart)); err != nil {
		t.Fatal(err)
	}

	sm.activeSlab.version = Version2
	sm.activeSlab.globalDict = dict
	sm.activeSlab.globalDecs = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}

	// Set groupK > 1
	sm.currentProfile.Store(&compression.ActiveProfile{K: 4})

	// Write almost 2MB to push near boundary.
	bulkPayload := make([]byte, 100*1024)
	for i := 0; i < 19; i++ {
		_, err := sm.AppendWithOptions(nil, bulkPayload, AppendOptions{DisableCompression: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Size before grouped AppendMany: %d", sm.activeSlab.Size())

	// Now write a grouped batch that should cross 2MB.
	keys := [][]byte{nil, nil, nil, nil}
	vals := [][]byte{
		[]byte("grouped-1"),
		[]byte("grouped-2"),
		[]byte("grouped-3"),
		[]byte("grouped-4"),
	}

	ptrs, err := sm.AppendMany(keys, vals)
	if err != nil {
		t.Fatal(err)
	}

	if len(ptrs) != 4 {
		t.Fatalf("expected 4 ptrs, got %d", len(ptrs))
	}
	t.Logf("Grouped record offset: %d", ptrs[0].Offset)

	// Verify all can be read
	for i, ptr := range ptrs {
		got, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Fatalf("mismatch %d: got %q, want %q", i, got, vals[i])
		}
	}
}
