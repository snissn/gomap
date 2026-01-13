package slab

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
)

func TestSlabV2_RotationAndDictionary(t *testing.T) {
	dir := t.TempDir()
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

	// 1. Load a real ZSTD dictionary for testing.
	// We use an existing one from the codebase.
	dictPath := "../../tmp/dict-32k.zdict"
	realDict, err := os.ReadFile(dictPath)
	if err != nil {
		t.Skipf("skipping test as real dictionary not found: %v", err)
	}

	profile := &ActiveCompressionProfile{
		Dict:     realDict,
		DictHash: xxhash.Sum64(realDict),
		K:        1,
	}

	// 2. Manually create a new V2 slab file.
	newID := sm.ActiveSlabID() + 1
	newSlabPath := filepath.Join(dir, fmt.Sprintf("data-%04d.slab", newID))
	v2SlabFile, err := os.OpenFile(newSlabPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer v2SlabFile.Close()

	// Write V2 header and global dictionary.
	header := make([]byte, SlabV2DataStart)
	copy(header[0:8], MagicV2)
	header[8] = Version2
	// Write the real dict at the correct offset (32KB).
	// Note: SlabV2DataStart is 64KB. Header is 32KB. Dict is 32KB.
	copy(header[FileHeaderSizeV2:], realDict)
	if _, err := v2SlabFile.WriteAt(header, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := v2SlabFile.Seek(SlabV2DataStart, 0); err != nil {
		t.Fatal(err)
	}

	// Create SlabFile struct for V2.
	newSlab := newSlabFile(newSlabPath, newID, v2SlabFile, SlabV2DataStart, false, Version2, realDict, &sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(realDict))
			if err != nil {
				panic(err)
			}
			return dec
		},
	})

	// 3. Update SlabManager to use this new V2 slab and its compression config.
	sm.mu.Lock()
	sm.slabs[newSlab.ID] = newSlab // Add to slab map
	sm.activeSlab = newSlab        // Set as active
	sm.currentProfile = profile    // Set profile

	// Enable ZSTD compression WITH the dictionary.
	sm.activeCompression.kind = CompressionZSTD
	sm.activeCompression.level = zstd.EncoderLevel(sm.compression.level)
	sm.activeCompression.zstdEncs = &sync.Pool{
		New: func() any {
			enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(realDict), zstd.WithEncoderLevel(sm.activeCompression.level))
			if err != nil {
				panic(err)
			}
			return enc
		},
	}
	sm.activeCompression.zstdDecs = &sync.Pool{
		New: func() any {
			dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(realDict))
			if err != nil {
				panic(err)
			}
			return dec
		},
	}
	sm.mu.Unlock()

	// 4. Write data WITH compression.
	// Use some data that should compress well with a dictionary (repetitive).
	val := bytes.Repeat([]byte("hello-v2-dictionary-compression-is-cool-and-efficient "), 100)
	ptr, err := sm.Append(nil, val)
	if err != nil {
		t.Fatal(err)
	}

	// 5. Read data and verify.
	readVal, err := sm.Read(ptr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readVal, val) {
		t.Fatalf("read mismatch")
	}

	// 6. Verify file on disk (V2 magic and dict presence).
	path := newSlab.Path
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	magic := make([]byte, 8)
	if _, err := f.ReadAt(magic, 0); err != nil {
		t.Fatal(err)
	}
	if string(magic) != MagicV2 {
		t.Fatalf("expected magic %q, got %q", MagicV2, magic)
	}

	readDict := make([]byte, len(realDict))
	if _, err := f.ReadAt(readDict, FileHeaderSizeV2); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readDict, realDict) {
		t.Fatalf("read dict mismatch")
	}
}

func TestSlabV2_ZoneHeaders(t *testing.T) {
	// This test verifies that Zone Headers are written at 2MB boundaries.
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionNone, // Use None to ensure size is predictable
		},
		OmitSlabKeys: true,
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Manually force V2 on active slab for testing (simulating a rotation that happened)
	sm.activeSlab.version = Version2
	sm.activeSlab.Size = SlabV2DataStart
	sm.activeSlab.writeOffset = SlabV2DataStart
	// No dict needed for this test, just boundary checks.

	// Write almost 2MB.
	// Zone 0 starts at 64KB. Zone 1 boundary is at 2MB.
	// We need to write ~1.9MB.
	chunkSize := 100 * 1024
	iterations := (2*1024*1024 - 64*1024) / chunkSize

	// Create incompressible data just in case, though we used CompressionNone
	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 255)
	}

	for i := 0; i < iterations; i++ {
		_, err := sm.Append(nil, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Current size should be near 2MB.
	t.Logf("Size before crossing: %d", sm.activeSlab.Size)

	// Write one more record that crosses the 2MB boundary.
	_, err = sm.Append(nil, data)
	if err != nil {
		t.Fatal(err)
	}

	if sm.activeSlab.Size <= 2*1024*1024+int64(ZoneHeaderSize) {
		t.Fatalf("expected size to be beyond 2MB + header, got %d", sm.activeSlab.Size)
	}

	// Verify Zone Header exists at 2MB.
	path := sm.GetSlabPath(sm.activeSlab.ID)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	headerBuf := make([]byte, ZoneHeaderSize)
	if _, err := f.ReadAt(headerBuf, 2*1024*1024); err != nil {
		t.Fatal(err)
	}

	var zh ZoneHeader
	if err := zh.Unmarshal(headerBuf); err != nil {
		t.Fatal(err)
	}
	if zh.Magic != ZoneHeaderMagic {
		t.Fatalf("expected zone header magic %x, got %x", ZoneHeaderMagic, zh.Magic)
	}
	if zh.DictType != ZoneDictGlobal {
		t.Fatalf("expected USE_GLOBAL, got %d", zh.DictType)
	}
}
