package caching

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap-gemini/TreeDB/internal/merging"
)

// MockBackend implements BackendDB
type MockBackend struct {
	data map[string][]byte
}

func NewMockBackend() *MockBackend {
	return &MockBackend{data: make(map[string][]byte)}
}

func (m *MockBackend) Get(key []byte) ([]byte, error) {
	val, ok := m.data[string(key)]
	if !ok {
		return nil, nil
	}
	return val, nil
}

func (m *MockBackend) Set(key, val []byte) {
	m.data[string(key)] = val
}

func (m *MockBackend) Iterator(start, end []byte) (merging.Iterator, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *MockBackend) ReverseIterator(start, end []byte) (merging.Iterator, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *MockBackend) Print() error { return nil }
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
	delete(b.mb.data, string(key))
	return nil
}
func (b *MockBatch) WriteSync() error { return nil }
func (b *MockBatch) Close() error { return nil }

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
