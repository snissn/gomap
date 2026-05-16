package iterator

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type mockUnsafeIterator struct {
	key   []byte
	value []byte
	err   error
}

func (m *mockUnsafeIterator) Valid() bool         { return true }
func (m *mockUnsafeIterator) Next()               {}
func (m *mockUnsafeIterator) Seek(_ []byte)       {}
func (m *mockUnsafeIterator) UnsafeKey() []byte   { return m.key }
func (m *mockUnsafeIterator) UnsafeValue() []byte { return m.value }
func (m *mockUnsafeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.value, page.ValuePtr{}, 0
}
func (m *mockUnsafeIterator) Key() []byte                 { return m.key }
func (m *mockUnsafeIterator) Value() []byte               { return m.value }
func (m *mockUnsafeIterator) KeyCopy(dst []byte) []byte   { return append(dst, m.key...) }
func (m *mockUnsafeIterator) ValueCopy(dst []byte) []byte { return append(dst, m.value...) }
func (m *mockUnsafeIterator) IsDeleted() bool             { return false }
func (m *mockUnsafeIterator) Error() error                { return m.err }
func (m *mockUnsafeIterator) Close() error                { return nil }
func (m *mockUnsafeIterator) Domain() ([]byte, []byte)    { return nil, nil }

var _ UnsafeIterator = (*mockUnsafeIterator)(nil)

func TestUnsafeIteratorCopyHelpers(t *testing.T) {
	it := &mockUnsafeIterator{key: []byte("k"), value: []byte("v")}

	key := it.KeyCopy([]byte("pre-"))
	if string(key) != "pre-k" {
		t.Fatalf("KeyCopy()=%q, want %q", string(key), "pre-k")
	}

	val := it.ValueCopy([]byte("pre-"))
	if string(val) != "pre-v" {
		t.Fatalf("ValueCopy()=%q, want %q", string(val), "pre-v")
	}
}
