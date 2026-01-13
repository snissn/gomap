package slab

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
)

func fillToZone1(t *testing.T, sm *SlabManager, key, value []byte) {
	t.Helper()
	for sm.ActiveSlabTail() < ZoneSize {
		if _, err := sm.AppendWithOptions(key, value, AppendOptions{
			DisableCompression: true,
			SkipTraining:       true,
			SkipMetrics:        true,
		}); err != nil {
			t.Fatalf("fill to zone1: %v", err)
		}
	}
	if sm.ActiveSlabTail() < ZoneSize {
		t.Fatalf("expected to reach zone1 boundary, tail=%d", sm.ActiveSlabTail())
	}
}

func TestSlabV2_GlobalFallbackNoDict(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManagerWithOptions(dir, Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 1,
		},
		CompressionAdaptiveTrainBytes: -1,
	})
	if err != nil {
		t.Fatalf("new slab manager: %v", err)
	}
	defer func() { _ = sm.Close() }()

	fillToZone1(t, sm, []byte("k"), bytes.Repeat([]byte("f"), 64*1024))

	value := bytes.Repeat([]byte("z"), 32*1024)
	ptr, err := sm.Append([]byte("compressed"), value)
	if err != nil {
		t.Fatalf("append compressed: %v", err)
	}
	if ptr.Offset < ZoneSize {
		t.Fatalf("expected zone1 offset, got %d", ptr.Offset)
	}
	if !page.ValuePtrIsCompressed(ptr) {
		t.Fatalf("expected compressed value pointer")
	}

	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("read mismatch: got %d bytes", len(got))
	}
}

func TestSlabV2_LocalDictPadding(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManagerWithOptions(dir, Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 1,
		},
		CompressionAdaptiveTrainBytes: -1,
	})
	if err != nil {
		t.Fatalf("new slab manager: %v", err)
	}
	defer func() { _ = sm.Close() }()

	samples := make([][]byte, 8)
	for i := range samples {
		samples[i] = bytes.Repeat([]byte{byte('a' + i)}, 2048)
	}
	history := bytes.Join(samples, nil)
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  history,
	})
	if err != nil {
		t.Fatalf("build dict: %v", err)
	}
	if len(dict) >= GlobalDictSize {
		t.Fatalf("expected short dict, got %d bytes", len(dict))
	}

	sm.ForceAcceptProfileForTesting(&ActiveCompressionProfile{
		Dict:      dict,
		DictBytes: len(dict),
		K:         1,
	})

	fillToZone1(t, sm, []byte("k"), bytes.Repeat([]byte("f"), 64*1024))

	value := bytes.Repeat([]byte("payload"), 32*1024)
	ptr, err := sm.Append([]byte("compressed"), value)
	if err != nil {
		t.Fatalf("append compressed: %v", err)
	}
	if ptr.Offset < ZoneSize {
		t.Fatalf("expected zone1 offset, got %d", ptr.Offset)
	}
	if !page.ValuePtrIsCompressed(ptr) {
		t.Fatalf("expected compressed value pointer")
	}

	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("read mismatch: got %d bytes", len(got))
	}
}
