package slab

import (
	"bytes"
	"os"
	"testing"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSlab_OmitKeys(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		OmitSlabKeys: true,
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	key := []byte("test_key")
	val := []byte("test_value")

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !page.ValuePtrIsOmittedKey(ptr) {
		t.Errorf("Expected OmittedKey flag to be set")
	}

	// Verify Read works
	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("val mismatch: got %s, want %s", got, val)
	}

	// Check slab file content - key should NOT be present
	matches, _ := os.ReadDir(dir)
	var slabPath string
	for _, f := range matches {
		if f.Name() == "data-0000.slab" {
			slabPath = dir + "/" + f.Name()
		}
	}
	if slabPath == "" {
		t.Fatal("Slab file not found")
	}

	content, err := os.ReadFile(slabPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if bytes.Contains(content, key) {
		t.Errorf("Key should NOT be in the slab file when OmitSlabKeys is true")
	}
	if !bytes.Contains(content, val) {
		t.Errorf("Value should be in the slab file")
	}

	// Test AppendMany
	keys := [][]byte{[]byte("k1"), []byte("k2")}
	vals := [][]byte{[]byte("v1"), []byte("v2")}
	ptrs, err := sm.AppendMany(keys, vals)
	if err != nil {
		t.Fatalf("AppendMany: %v", err)
	}

	for i, p := range ptrs {
		if !page.ValuePtrIsOmittedKey(p) {
			t.Errorf("Expected OmittedKey flag for AppendMany[%d]", i)
		}
		got, err := sm.Read(p)
		if err != nil {
			t.Errorf("Read AppendMany[%d]: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Errorf("val mismatch AppendMany[%d]", i)
		}
	}
}
