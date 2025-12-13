package memtable

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"treedb/internal/iterator" // Import iterator interface
)

type Item struct {
	Key       []byte
	Value     []byte
	IsDeleted bool
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.Key, than.(*Item).Key) < 0
}

type Memtable struct {
	tree *btree.BTree
	size int64
	mu   sync.RWMutex
}

func New() *Memtable {
	return &Memtable{
		tree: btree.New(32),
	}
}

func (m *Memtable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)

	item := &Item{Key: k, Value: v, IsDeleted: false}
	old := m.tree.ReplaceOrInsert(item)
	
	added := int64(len(k) + len(v))
	if old != nil {
		oldItem := old.(*Item)
		m.size -= int64(len(oldItem.Key) + len(oldItem.Value))
	}
	m.size += added
}

func (m *Memtable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := append([]byte(nil), key...)
	item := &Item{Key: k, IsDeleted: true}
	old := m.tree.ReplaceOrInsert(item)
	
	added := int64(len(k))
	if old != nil {
		oldItem := old.(*Item)
		m.size -= int64(len(oldItem.Key) + len(oldItem.Value))
	}
	m.size += added
}

func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.tree.Get(&Item{Key: key})
	if item == nil {
		return nil, false, false
	}
	i := item.(*Item)
	return i.Value, i.IsDeleted, true
}

func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}

// Iterator iterates over a snapshot of the memtable.
type Iterator struct {
	tree *btree.BTree
	curr *Item
	valid bool
}

// NewIterator returns an iterator.UnsafeIterator.
func (m *Memtable) NewIterator() iterator.UnsafeIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// COW Clone for iteration safety without locking
	return &Iterator{tree: m.tree.Clone()}
}

// Seek positions the iterator to the first key >= target.
func (it *Iterator) Seek(key []byte) {
	it.valid = false
	it.tree.AscendGreaterOrEqual(&Item{Key: key}, func(i btree.Item) bool {
		it.curr = i.(*Item)
		it.valid = true
		return false // Stop after 1
	})
}

// Next advances the iterator.
func (it *Iterator) Next() {
	if !it.valid {
		return // Do nothing if already invalid
	}
	start := it.curr
	it.valid = false // Assume invalid until next is found
	
	// Find the entry strictly greater than start.
	// AscendGreaterOrEqual with 'start' will yield start itself first.
	// We need to skip it and take the next.
	skipCurrent := true
	it.tree.AscendGreaterOrEqual(start, func(i btree.Item) bool {
		if skipCurrent {
			skipCurrent = false
			return true // Continue to next item
		}
		it.curr = i.(*Item)
		it.valid = true
		return false // Stop after finding the next one
	})
}

// Valid returns true if the iterator is currently pointing to a valid item.
func (it *Iterator) Valid() bool {
	return it.valid
}

// UnsafeKey returns a view (no copy) of the current key.
func (it *Iterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.curr.Key
}

// UnsafeValue returns a view (no copy) of the current value.
func (it *Iterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.curr.Value
}

// IsDeleted returns true if the current item is a tombstone.
func (it *Iterator) IsDeleted() bool {
	if !it.valid {
		return false
	}
	return it.curr.IsDeleted
}

// Key returns a copy of the current key.
func (it *Iterator) Key() []byte {
	return append([]byte(nil), it.UnsafeKey()...)
}

// Value returns a copy of the current value.
func (it *Iterator) Value() []byte {
	return append([]byte(nil), it.UnsafeValue()...)
}

// Error returns nil.
func (it *Iterator) Error() error {
	return nil
}

// Close closes the iterator.
func (it *Iterator) Close() error {
	// No resources to close for in-memory btree iterator
	return nil
}

func (it *Iterator) Domain() (start, end []byte) {
	return nil, nil
}