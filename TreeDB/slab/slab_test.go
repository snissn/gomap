package slab

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func waitForMmap(t *testing.T, s *SlabFile) []byte {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := s.mmapData.Load().([]byte)
		if len(data) > 0 {
			return data
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("expected slab mmap to be available")
	return nil
}

func TestSlabRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	key := []byte("testkey")
	val := []byte("testvalue")

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	readVal, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readVal) != string(val) {
		t.Errorf("Value mismatch. Got %s, want %s", readVal, val)
	}
}

func TestSlabRotation(t *testing.T) {
	// Override MaxSlabSize
	originalMax := MaxSlabSize
	defer func() { MaxSlabSize = originalMax }()

	// Set small limit: enough for 1 record but not 2?
	// Header(10) + Key(1) + Val(1) = 12 bytes.
	// Account for the v2 data start.
	MaxSlabSize = slabV2DataStart + 20

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	// Record 1: 10 + 1 + 1 = 12 bytes. Offset 0.
	ptr1, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}
	if ptr1.FileID != 0 {
		t.Errorf("Expected FileID 0, got %d", ptr1.FileID)
	}

	// Record 2: 12 bytes.
	// Total 24 bytes > 20. Should trigger rotation.
	ptr2, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}
	if ptr2.FileID != 1 {
		t.Errorf("Expected FileID 1 for second record, got %d", ptr2.FileID)
	}

	// Verify files exist
	if _, err := os.Stat(filepath.Join(dir, "data-0000.slab")); err != nil {
		t.Error("data-0000.slab missing")
	}
	if _, err := os.Stat(filepath.Join(dir, "data-0001.slab")); err != nil {
		t.Error("data-0001.slab missing")
	}

	// Read back both
	val1, err := sm.Read(ptr1)
	if err != nil {
		t.Errorf("Read 1 failed: %v", err)
	}
	if string(val1) != "v" {
		t.Errorf("Value 1 mismatch")
	}

	val2, err := sm.Read(ptr2)
	if err != nil {
		t.Errorf("Read 2 failed: %v", err)
	}
	if string(val2) != "v" {
		t.Errorf("Value 2 mismatch")
	}
}

func TestOpenSlabSyncDirFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	orig := syncDirFn
	t.Cleanup(func() { syncDirFn = orig })
	syncDirFn = func(string) error { return errors.New("syncdir fail") }
	if _, err := OpenSlab(path, 0); err == nil {
		t.Fatalf("expected syncDir failure")
	}
}

func TestDataCorruption(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}

	key := []byte("key")
	val := []byte("val")
	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	realStart := int64(ptr.Offset) - 4
	if realStart < 0 {
		t.Fatalf("invalid slab offset: %d", ptr.Offset)
	}

	sm.Close() // Close to modify file

	// Corrupt file
	path := filepath.Join(dir, "data-0000.slab")
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	// Corrupt the CRC for the record we just wrote.
	if _, err := f.WriteAt([]byte{0x00}, realStart); err != nil {
		t.Fatalf("Corrupt failed: %v", err)
	}
	f.Close()

	// Reopen
	sm2, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer sm2.Close()

	_, err = sm2.Read(ptr)
	if err != ErrChecksumMismatch {
		t.Errorf("Expected ErrChecksumMismatch, got %v", err)
	}
}

func TestSlabRead_RejectsOversizedHeader(t *testing.T) {
	originalMax := MaxRecordSize
	MaxRecordSize = 64
	defer func() { MaxRecordSize = originalMax }()

	dir := t.TempDir()
	path := filepath.Join(dir, "data-0000.slab")
	s, err := OpenSlab(path, 0)
	if err != nil {
		t.Fatalf("OpenSlab failed: %v", err)
	}
	defer s.Close()

	var header [HeaderSize]byte
	binary.LittleEndian.PutUint16(header[4:6], 8)
	binary.LittleEndian.PutUint32(header[6:10], 80)

	if _, err := s.File.WriteAt(header[:], int64(slabV2DataStart)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	readOffset := int64(slabV2DataStart) + 4
	if _, err := s.Read(readOffset, false); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
}

func TestSlabRead_UsesMmapWhenAvailable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	ptr, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	val, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("Value mismatch: got %q", string(val))
	}

	s := sm.slabs[ptr.FileID]
	mmapData := waitForMmap(t, s)

	// Re-read to ensure we hit the mmap path (async remap may have used pread).
	val, err = sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	base := uintptr(unsafe.Pointer(&mmapData[0]))
	end := base + uintptr(len(mmapData))
	p := uintptr(unsafe.Pointer(&val[0]))
	if p < base || p >= end {
		t.Fatalf("expected Read() value to be backed by mmap (ptr=%#x not in [%#x,%#x))", p, base, end)
	}
}

func TestSlabRead_RemapsWhenOutOfMappedRange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	ptr1, err := sm.Append([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}

	val1, err := sm.Read(ptr1) // triggers initial mmap at current file size
	if err != nil {
		t.Fatalf("Read 1 failed: %v", err)
	}
	if string(val1) != "v1" {
		t.Fatalf("Value 1 mismatch: got %q", string(val1))
	}

	s := sm.slabs[ptr1.FileID]
	mmapData := waitForMmap(t, s)
	mmapLen := len(mmapData)

	ptr2, err := sm.Append([]byte("k2"), []byte("v2")) // grows the file beyond the existing mapping
	if err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}

	// First read may fall back to pread and schedule a remap.
	val2, err := sm.Read(ptr2)
	if err != nil {
		t.Fatalf("Read 2 failed: %v", err)
	}
	if string(val2) != "v2" {
		t.Fatalf("Value 2 mismatch: got %q", string(val2))
	}

	// Wait for the mapping to grow.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		newData, _ := s.mmapData.Load().([]byte)
		if len(newData) > mmapLen {
			mmapData = newData
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if len(mmapData) <= mmapLen {
		t.Fatalf("expected mmap length to grow (got %d, was %d)", len(mmapData), mmapLen)
	}

	// Re-read to ensure we hit the new mmap mapping.
	val2, err = sm.Read(ptr2)
	if err != nil {
		t.Fatalf("Read 2 failed: %v", err)
	}

	base := uintptr(unsafe.Pointer(&mmapData[0]))
	end := base + uintptr(len(mmapData))
	p := uintptr(unsafe.Pointer(&val2[0]))
	if p < base || p >= end {
		t.Fatalf("expected Read() value to be backed by NEW mmap (ptr=%#x not in [%#x,%#x))", p, base, end)
	}
}

func TestSlabRemap_CapsDeadMappings(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	originalMax := MaxDeadMappings
	MaxDeadMappings = 1
	defer func() { MaxDeadMappings = originalMax }()

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	ptr1, err := sm.Append([]byte("k1"), []byte("v1"))
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}

	_, err = sm.Read(ptr1)
	if err != nil {
		t.Fatalf("Read 1 failed: %v", err)
	}

	s := sm.slabs[ptr1.FileID]
	s.remapToFileSize()
	data1, _ := s.mmapData.Load().([]byte)
	if len(data1) == 0 {
		t.Fatalf("expected initial mmap")
	}

	ptr2, err := sm.Append([]byte("k2"), []byte("v2v2v2v2"))
	if err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}
	_, err = sm.Read(ptr2)
	if err != nil {
		t.Fatalf("Read 2 failed: %v", err)
	}
	s.remapToFileSize()
	data2, _ := s.mmapData.Load().([]byte)
	if len(s.deadMappings) != 1 {
		t.Fatalf("expected 1 dead mapping, got %d", len(s.deadMappings))
	}
	if len(data2) <= len(data1) {
		t.Fatalf("expected mmap to grow after first remap")
	}

	ptr3, err := sm.Append([]byte("k3"), []byte("v3v3v3v3v3v3"))
	if err != nil {
		t.Fatalf("Append 3 failed: %v", err)
	}
	_, err = sm.Read(ptr3)
	if err != nil {
		t.Fatalf("Read 3 failed: %v", err)
	}
	s.remapToFileSize()
	data3, _ := s.mmapData.Load().([]byte)
	if len(s.deadMappings) != 1 {
		t.Fatalf("expected dead mappings capped at 1, got %d", len(s.deadMappings))
	}
	if len(data3) != len(data2) {
		t.Fatalf("expected remap suppressed once cap reached")
	}
}
