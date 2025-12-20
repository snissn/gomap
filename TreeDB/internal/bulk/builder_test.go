package bulk

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// MockAllocator
type MockAllocator struct {
	p    *pager.Pager
	fail bool
}

func (m *MockAllocator) Alloc(hint uint64) (uint64, error) {
	if m.fail {
		return 0, errors.New("mock allocation failed")
	}
	return m.p.Alloc(1)
}

// MockIterator
type MockIterator struct {
	keys [][]byte
	idx  int
}

func (m *MockIterator) Valid() bool         { return m.idx < len(m.keys) }
func (m *MockIterator) Next()               { m.idx++ }
func (m *MockIterator) UnsafeKey() []byte   { return m.keys[m.idx] }
func (m *MockIterator) UnsafeValue() []byte { return []byte("val:" + string(m.keys[m.idx])) }
func (m *MockIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}
func (m *MockIterator) Seek(key []byte)             {}
func (m *MockIterator) Key() []byte                 { return m.UnsafeKey() }
func (m *MockIterator) Value() []byte               { return m.UnsafeValue() }
func (m *MockIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], m.UnsafeKey()...) }
func (m *MockIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], m.UnsafeValue()...) }
func (m *MockIterator) IsDeleted() bool             { return false }
func (m *MockIterator) Error() error                { return nil }
func (m *MockIterator) Close() error                { return nil }
func (m *MockIterator) Domain() (start, end []byte) { return nil, nil }

func TestBuild(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}

	// Create 1000 sorted keys
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	iter := &MockIterator{keys: keys}

	rootID, err := Build(iter, alloc, p)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify using Tree
	tr := tree.New(p, nil, rootID)
	for i := 0; i < 1000; i++ {
		k := keys[i]
		v, err := tr.Get(k)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", k, err)
		}
		expected := []byte("val:" + string(k))
		if !bytes.Equal(v, expected) {
			t.Errorf("Value mismatch for %s. Got %s, want %s", k, v, expected)
		}
	}
}