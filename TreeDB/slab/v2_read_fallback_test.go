package slab

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/klauspost/compress/dict"
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

func fillToZone(t *testing.T, sm *SlabManager, zoneID int64, key, value []byte) {
	t.Helper()
	target := uint64(zoneID) * ZoneSize
	for sm.ActiveSlabTail() < target {
		if _, err := sm.AppendWithOptions(key, value, AppendOptions{
			DisableCompression: true,
			SkipTraining:       true,
			SkipMetrics:        true,
		}); err != nil {
			t.Fatalf("fill to zone %d: %v", zoneID, err)
		}
	}
}

func readZoneHeader(t *testing.T, s *SlabFile, zoneID int64) ZoneHeader {
	t.Helper()
	var headerBuf [ZoneHeaderSize]byte
	if err := s.readRaw(zoneID*ZoneSize, headerBuf[:]); err != nil {
		t.Fatalf("read zone header: %v", err)
	}
	var zh ZoneHeader
	if err := zh.Unmarshal(headerBuf[:]); err != nil {
		t.Fatalf("unmarshal zone header: %v", err)
	}
	return zh
}

func makeTestSamples(count, size int) [][]byte {
	samples := make([][]byte, count)
	for i := range samples {
		pattern := []byte(fmt.Sprintf("sample-%02d-", i))
		repeats := size / len(pattern)
		if repeats == 0 {
			repeats = 1
		}
		buf := bytes.Repeat(pattern, repeats)
		if len(buf) < size {
			buf = append(buf, bytes.Repeat(pattern, size-len(buf))...)
		}
		samples[i] = buf[:size]
	}
	return samples
}

func buildTestDict(t *testing.T, id uint32, samples [][]byte) []byte {
	t.Helper()
	dictBytes, err := dict.BuildZstdDict(samples, dict.Options{
		MaxDictSize: 32 << 10,
		HashBytes:   6,
		ZstdDictID:  id,
		ZstdLevel:   zstd.SpeedDefault,
	})
	if err != nil {
		t.Fatalf("build dict: %v", err)
	}
	if len(dictBytes) == 0 {
		t.Fatalf("empty dict")
	}
	return dictBytes
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

	samples := makeTestSamples(8, 2048)
	dict := buildTestDict(t, 1, samples)
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

func TestSlabV2_UseRefDictionary(t *testing.T) {
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

	samples := makeTestSamples(16, 4096)
	dict := buildTestDict(t, 2, samples)

	profile := &ActiveCompressionProfile{
		Dict:      dict,
		DictBytes: len(dict),
		K:         1,
	}
	sm.ForceAcceptProfileForTesting(profile)

	fillToZone(t, sm, 1, []byte("k"), bytes.Repeat([]byte("f"), 64*1024))
	if _, err := sm.Append([]byte("zone1"), bytes.Repeat([]byte("payload"), 8*1024)); err != nil {
		t.Fatalf("append zone1: %v", err)
	}

	fillToZone(t, sm, 2, []byte("k2"), bytes.Repeat([]byte("f"), 64*1024))
	ptr, err := sm.Append([]byte("zone2"), bytes.Repeat([]byte("payload"), 8*1024))
	if err != nil {
		t.Fatalf("append zone2: %v", err)
	}

	sm.mu.RLock()
	s := sm.activeSlab
	sm.mu.RUnlock()

	zh := readZoneHeader(t, s, 2)
	if zh.DictType != ZoneDictRef {
		t.Fatalf("expected ZoneDictRef, got %d", zh.DictType)
	}
	if zh.DictLength != 1 {
		t.Fatalf("expected ref zone 1, got %d", zh.DictLength)
	}

	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("read zone2: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("read zone2 empty value")
	}
}

func TestSlabV2_DictSliceMmap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap-backed dict slices are not supported on Windows")
	}
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

	dict := buildTestDict(t, 3, makeTestSamples(8, 2048))
	sm.ForceAcceptProfileForTesting(&ActiveCompressionProfile{
		Dict:      dict,
		DictBytes: len(dict),
		K:         1,
	})

	fillToZone(t, sm, 1, []byte("k"), bytes.Repeat([]byte("f"), 64*1024))
	if _, err := sm.Append([]byte("zone1"), bytes.Repeat([]byte("payload"), 8*1024)); err != nil {
		t.Fatalf("append zone1: %v", err)
	}

	sm.mu.RLock()
	s := sm.activeSlab
	sm.mu.RUnlock()

	s.remapToFileSize()
	dictOffset := int64(ZoneSize) + ZoneHeaderSize
	if _, ok := s.dictSlice(dictOffset, GlobalDictSize); !ok {
		t.Fatalf("expected mmap-backed dict slice")
	}
}
