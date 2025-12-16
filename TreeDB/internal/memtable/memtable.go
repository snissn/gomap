package memtable

import (
	"bytes"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/skiplist"
	"github.com/snissn/gomap/TreeDB/page"
)

type Memtable struct {
	sl *skiplist.SkipList
	mu sync.RWMutex
}

// New creates a new Memtable.
// We start with a reasonable capacity to avoid initial reallocations.
func New() *Memtable {
	return &Memtable{
		sl: skiplist.New(4 * 1024 * 1024), // 4MB initial capacity
	}
}

func (m *Memtable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, value)
}

func (m *Memtable) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sl.PutWithCallback(key, value, cb)
}

func (m *Memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Delete(key)
}

func (m *Memtable) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sl.DeleteWithCallback(key, cb)
}

// SetSteal - SkipList copies data, so Steal is same as Set
func (m *Memtable) SetSteal(key, value []byte) {
	m.Set(key, value)
}

// DeleteSteal - SkipList copies data, so Steal is same as Delete
func (m *Memtable) DeleteSteal(key []byte) {
	m.Delete(key)
}

func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(key)
}

// Size returns the total memory usage (arena size).
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Size()
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Count()
}

// Iterator wrapper
type Iterator struct {
	iter *skiplist.Iterator
	end  []byte
}

func (m *Memtable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	it := m.sl.NewIterator(start, end)
	return &Iterator{iter: it, end: end}
}

func (it *Iterator) Seek(key []byte) {
	it.iter.Seek(key)
	it.checkEnd()
}

func (it *Iterator) Next() {
	it.iter.Next()
	it.checkEnd()
}

func (it *Iterator) checkEnd() {
	if it.iter.Valid() && it.end != nil {
		if bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
			// Invalidate
			// skipList iterator doesn't have "Invalidate" method public?
			// We can just rely on wrapper Valid().
			// But wrapper Valid() calls iter.Valid().
			// We need state in wrapper.
			// Actually, let's just check in Valid().
		}
	}
}

func (it *Iterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	if it.end != nil && bytes.Compare(it.iter.UnsafeKey(), it.end) >= 0 {
		return false
	}
	return true
}

func (it *Iterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *Iterator) UnsafeValue() []byte {
	return it.iter.UnsafeValue()
}

func (it *Iterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.iter.UnsafeEntry()
}

func (it *Iterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *Iterator) Key() []byte {
	return it.iter.Key()
}

func (it *Iterator) Value() []byte {
	return it.iter.Value()
}

func (it *Iterator) Close() error {
	return it.iter.Close()
}

func (it *Iterator) Error() error {
	return it.iter.Error()
}

func (it *Iterator) Domain() (start, end []byte) {
	return nil, it.end
}
