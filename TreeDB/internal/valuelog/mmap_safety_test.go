package valuelog

import (
	"bytes"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	templ "github.com/snissn/gomap/TreeDB/template"
)

func ptrInMapping(view, mapping []byte) bool {
	if len(view) == 0 || len(mapping) == 0 {
		return false
	}
	vp := uintptr(unsafe.Pointer(&view[0]))
	mp := uintptr(unsafe.Pointer(&mapping[0]))
	end := mp + uintptr(len(mapping))
	return vp >= mp && vp < end
}

// TestMmapSafety_ZeroCopy_Remap verifies that holding a zero-copy view of an
// older mmap remains safe even after the File remaps due to growth.
//
// Without retaining dead mappings, this test would crash with SIGSEGV.
func TestMmapSafety_ZeroCopy_Remap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	old := enableCurrentWritableMmap
	enableCurrentWritableMmap = true
	defer func() { enableCurrentWritableMmap = old }()

	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := uint32(123)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	val1 := bytes.Repeat([]byte("v1"), 1024) // 2KB
	ptr1, err := w.Append(0, nil, 1, val1)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	view1, err := f.ReadUnsafe(ptr1, false)
	if err != nil {
		t.Fatalf("ReadUnsafe 1: %v", err)
	}
	if !bytes.Equal(view1, val1) {
		t.Fatalf("view1 mismatch")
	}
	oldMap, _ := f.mmapData.Load().([]byte)
	if !ptrInMapping(view1, oldMap) {
		t.Fatalf("expected view1 to reference mmap (zero-copy)")
	}

	// Grow the file enough to force a remap on the next read.
	blob := bytes.Repeat([]byte("X"), 1024*1024) // 1MB
	for i := 0; i < 10; i++ {
		if _, err := w.Append(0, nil, uint64(2+i), blob); err != nil {
			t.Fatalf("Append grow %d: %v", i, err)
		}
	}
	val2 := []byte("v2")
	ptr2, err := w.Append(0, nil, 999, val2)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	view2, err := f.ReadUnsafe(ptr2, false) // forces remap
	if err != nil {
		t.Fatalf("ReadUnsafe 2: %v", err)
	}
	if string(view2) != "v2" {
		t.Fatalf("view2 mismatch")
	}
	if len(f.deadMappings) == 0 {
		t.Fatalf("expected at least one dead mapping after remap")
	}

	// Access view1 again (should not crash).
	if !bytes.Equal(view1, val1) {
		t.Fatalf("view1 corrupted after remap")
	}
}

// TestMmapSafety_Concurrent_Remap stresses the race where one goroutine holds
// a view while another triggers remaps.
func TestMmapSafety_Concurrent_Remap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	old := enableCurrentWritableMmap
	enableCurrentWritableMmap = true
	defer func() { enableCurrentWritableMmap = old }()

	dir := t.TempDir()
	path := filepath.Join(dir, "value-l0-000001.log")
	fileID := uint32(123)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	val := bytes.Repeat([]byte("S"), 1024)
	ptr, err := w.Append(0, nil, 1, val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			v, err := f.ReadUnsafe(ptr, false)
			if err != nil {
				t.Errorf("ReadUnsafe: %v", err)
				return
			}
			if len(v) != 1024 || v[0] != 'S' || v[1023] != 'S' {
				t.Errorf("corrupt read")
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		blob := make([]byte, 1024*1024)
		for i := 0; i < 10; i++ {
			p, err := w.Append(0, nil, uint64(2+i), blob)
			if err != nil {
				t.Errorf("Append grow %d: %v", i, err)
				return
			}
			if err := w.Flush(); err != nil {
				t.Errorf("Flush grow %d: %v", i, err)
				return
			}
			// Force a read of new data to trigger remap.
			if _, err := f.ReadUnsafe(p, false); err != nil {
				t.Errorf("ReadUnsafe grow %d: %v", i, err)
				return
			}
		}
		close(done)
	}()

	wg.Wait()
}

func TestCurrentWritableMmapTargetMapsAheadWithinLeafSegment(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}
	prevCurrent := enableCurrentWritableMmap
	prevLeaf := enableCurrentLeafWritableMmap
	enableCurrentWritableMmap = false
	enableCurrentLeafWritableMmap = true
	withMaxDeadMappings(t, 1)
	withCurrentWritableMmapTargetBytes(t, 64<<10)
	defer func() {
		enableCurrentWritableMmap = prevCurrent
		enableCurrentLeafWritableMmap = prevLeaf
	}()

	dir := t.TempDir()
	fileID := mustEncodeFileID(t, ReservedLeafLogLaneID, 1)
	path := filepath.Join(dir, "value-l255-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	first := bytes.Repeat([]byte("a"), 1024)
	ptr, err := w.Append(0, nil, 1, first)
	if err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush first: %v", err)
	}

	f, err := openFile(path, fileID, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	f.currentWritable.Store(true)
	defer func() { _ = f.Close() }()

	got, err := f.ReadUnsafe(ptr, false)
	if err != nil {
		t.Fatalf("ReadUnsafe first: %v", err)
	}
	if !bytes.Equal(got, first) {
		t.Fatalf("first read mismatch")
	}
	data, _ := f.mmapData.Load().([]byte)
	if gotLen := len(data); gotLen < 64<<10 {
		t.Fatalf("mapped length=%d, want at least target", gotLen)
	}

	for i := 0; i < 20; i++ {
		value := bytes.Repeat([]byte{byte('b' + i%20)}, 1024)
		ptr, err := w.Append(0, nil, uint64(2+i), value)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("Flush %d: %v", i, err)
		}
		got, err := f.ReadUnsafe(ptr, false)
		if err != nil {
			t.Fatalf("ReadUnsafe %d: %v", i, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("read %d mismatch", i)
		}
	}

	if dead := f.deadMappingsCount.Load(); dead != 0 {
		t.Fatalf("mapped-ahead current leaf should not churn dead mappings within target, got=%d", dead)
	}
	if fallbacks := f.mmapReadFallbackReadAt.Load(); fallbacks != 0 {
		t.Fatalf("mapped-ahead current leaf should avoid ReadAt fallback, got=%d", fallbacks)
	}
}
