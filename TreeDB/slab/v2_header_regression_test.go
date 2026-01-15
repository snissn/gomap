package slab

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestV2EmptyDictDecoderFallback(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		CompressionAdaptiveTrainBytes: -1,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions: %v", err)
	}

	key := []byte("s/k:ibc/facks/ports/transfer/channels/channel-2/sequences/42")
	val := bytes.Repeat([]byte("redundant_data_"), 12)

	ptr, err := sm.Append(key, val)
	if err != nil {
		_ = sm.Close()
		t.Fatalf("Append: %v", err)
	}
	if !page.ValuePtrIsCompressed(ptr) {
		_ = sm.Close()
		t.Fatalf("expected compressed pointer")
	}
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sm2, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions reopen: %v", err)
	}
	defer sm2.Close()

	got, err := sm2.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch")
	}
}

func TestRepairTail_V2PreservesHeader(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		CompressionAdaptiveTrainBytes: -1,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	defer sm.Close()

	key1 := []byte("key_with_some_length_1")
	key2 := []byte("key_with_some_length_2")
	val := bytes.Repeat([]byte("redundant_data_"), 10)

	ptr1, err := sm.Append(key1, val)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if _, err := sm.Append(key2, val); err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := sm.Sync(); err != nil {
		t.Fatal(err)
	}

	before := sm.activeSlab.Size()
	if before <= SlabV2DataStart+HeaderSize {
		t.Fatalf("unexpected slab size %d", before)
	}
	if err := sm.TruncateActiveSlab(uint64(before - 5)); err != nil {
		t.Fatalf("TruncateActiveSlab: %v", err)
	}

	tail, err := sm.RepairActiveSlabTail()
	if err != nil {
		t.Fatalf("RepairActiveSlabTail: %v", err)
	}
	if tail < SlabV2DataStart {
		t.Fatalf("tail truncated below v2 header: %d", tail)
	}

	got, err := sm.Read(ptr1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch")
	}
}

func TestDecodeRecordForCompactor_V2FullCompressed(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		CompressionAdaptiveTrainBytes: -1,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	defer sm.Close()

	key := []byte("s/k:ibc/facks/ports/transfer/channels/channel-2/sequences/91042")
	val := bytes.Repeat([]byte("redundant_data_"), 20)

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !page.ValuePtrIsFullCompressed(ptr) {
		t.Fatalf("expected full compressed pointer")
	}
	if err := sm.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	raw, err := sm.activeSlab.read(int64(ptr.Offset), true, false)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}

	decodedKey, decodedVal, err := sm.DecodeRecordForCompactor(ptr, raw)
	if err != nil {
		t.Fatalf("DecodeRecordForCompactor: %v", err)
	}
	if !bytes.Equal(decodedKey, key) {
		t.Fatalf("key mismatch")
	}
	if !bytes.Equal(decodedVal, val) {
		t.Fatalf("value mismatch")
	}
}
