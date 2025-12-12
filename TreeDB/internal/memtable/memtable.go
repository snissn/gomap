package memtable

import (
	"bytes"
	"sync"

	"github.com/google/btree"
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
	
	// Estimate size change
	// Note: We copy key/value to ensure immutability if caller reuses buffer?
	// The caller (CachingDB) usually handles copies or passes safe slices.
	// cosmos-db contract usually implies Set(k,v) copies are needed if user modifies them later.
	// For now assume unsafe, we should copy.
	// But let's defer copy to caller for performance, or do it here.
	// Doing it here is safer.
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
	// Return copy? Or direct?
	// Direct is faster, but unsafe if Memtable mutates (which it shouldn't for this item).
	// But ReplaceOrInsert replaces the item pointer.
	// So the old item structure is not mutated, it's replaced. Safe.
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

func (m *Memtable) NewIterator() *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// COW Clone for iteration safety without locking
	return &Iterator{tree: m.tree.Clone()}
}

func (it *Iterator) Seek(key []byte) {
	it.valid = false
	it.tree.AscendGreaterOrEqual(&Item{Key: key}, func(i btree.Item) bool {
		it.curr = i.(*Item)
		it.valid = true
		return false // Stop
	})
}

func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	start := it.curr
	it.valid = false
	
	it.tree.AscendGreaterOrEqual(start, func(i btree.Item) bool {
		item := i.(*Item)
		if item == start { // Pointer equality check might fail if Clone copies items?
			// google/btree Clone shares items (they are pointers).
			// But let's be safe with Key comparison.
			// Actually, if we use pointer comparison and it works, great.
			// If not, key comparison.
			return true // Skip current
		}
		it.curr = item
		it.valid = true
		return false // Stop
	})
}

func (it *Iterator) Valid() bool {
	return it.valid
}

func (it *Iterator) Item() *Item {
	if !it.valid {
		return nil
	}
	return it.curr
}
