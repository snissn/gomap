package caching

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// MockBackend implements BackendDB
type MockBackend struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMockBackend() *MockBackend {
	return &MockBackend{data: make(map[string][]byte)}
}

func (m *MockBackend) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *MockBackend) Set(key, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = val
}

func (m *MockBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	m.mu.RLock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	return &MockIterator{backend: m, keys: keys, idx: -1}, nil
}

type MockIterator struct {
	backend *MockBackend
	keys    []string
	idx     int
}

func (it *MockIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.keys)
}

func (it *MockIterator) Next() {
	it.idx++
}

func (it *MockIterator) Seek(key []byte) {
	it.idx = sort.SearchStrings(it.keys, string(key))
	// If not found, sort.Search returns insertion point.
	// If exact match or greater, that's what we want.
	if it.idx == len(it.keys) {
		// eof
	}
}

func (it *MockIterator) UnsafeKey() []byte {
	return []byte(it.keys[it.idx])
}

func (it *MockIterator) UnsafeValue() []byte {
	it.backend.mu.RLock()
	defer it.backend.mu.RUnlock()
	return it.backend.data[it.keys[it.idx]]
}

func (it *MockIterator) IsDeleted() bool          { return false }
func (it *MockIterator) Error() error             { return nil }
func (it *MockIterator) Close() error             { return nil }
func (it *MockIterator) Domain() ([]byte, []byte) { return nil, nil }
func (it *MockIterator) Key() []byte              { return it.UnsafeKey() }
func (it *MockIterator) Value() []byte            { return it.UnsafeValue() }

func (m *MockBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *MockBackend) Print() error             { return nil }
func (m *MockBackend) Stats() map[string]string { return nil }

// NewBatch returns a struct that satisfies BatchInterface
func (m *MockBackend) NewBatch() BatchInterface {
	return &MockBatch{mb: m}
}

type MockBatch struct {
	mb *MockBackend
}

func (b *MockBatch) Set(key, value []byte) error {
	b.mb.Set(key, value)
	return nil
}
func (b *MockBatch) Delete(key []byte) error {
	b.mb.mu.Lock()
	delete(b.mb.data, string(key))
	b.mb.mu.Unlock()
	return nil
}
func (b *MockBatch) SetOps(ops map[string]batch.Entry) error {
	b.mb.mu.Lock()
	defer b.mb.mu.Unlock()
	for k, op := range ops {
		if op.Type == batch.OpDelete {
			delete(b.mb.data, k)
		} else {
			b.mb.data[k] = op.Value
		}
	}
	return nil
}

func (b *MockBatch) Write() error              { return nil }
func (b *MockBatch) WriteSync() error          { return nil }
func (b *MockBatch) Close() error              { return nil }
func (b *MockBatch) GetByteSize() (int, error) { return 0, nil }

func (m *MockBackend) Close() error { return nil }

func TestCachingDB_WriteAndFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Threshold 1 byte to trigger flush
	db, err := Open(dir, backend, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write 10 keys (should fit in memtable or trigger flush)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := db.SetSync(k, v); err != nil {
			t.Fatalf("SetSync: %v", err)
		}
	}

	// Verify visibility (Get)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		val, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if string(val) != fmt.Sprintf("v%d", i) {
			t.Errorf("Get %s: got %q", k, val)
		}
	}

	// Close to flush everything
	db.Close()

	// Verify backend received data
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		if string(backend.data[k]) != fmt.Sprintf("v%d", i) {
			t.Errorf("Backend missing %s", k)
		}
	}
}
