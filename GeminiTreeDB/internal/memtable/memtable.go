package memtable

import (
	"bytes"
	"sort"
	"sync"
	"unsafe"

	"github.com/google/btree"
	"github.com/snissn/gomap-gemini/TreeDB/internal/iterator" // Import iterator interface
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
	idx  map[string]*Item
	size int64
	mu   sync.RWMutex
}

func New() *Memtable {
	return &Memtable{
		tree: btree.New(32),
		idx:  make(map[string]*Item),
	}
}

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
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
	m.idx[bytesToStringNoCopy(k)] = item
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
	m.idx[bytesToStringNoCopy(k)] = item
}

func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item := m.idx[bytesToStringNoCopy(key)]
	if item == nil {
		return nil, false, false
	}
	return item.Value, item.IsDeleted, true
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
	items []*Item
	idx   int
	valid bool
}

// NewIterator returns an iterator.UnsafeIterator.
func (m *Memtable) NewIterator() iterator.UnsafeIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := make([]*Item, 0, m.tree.Len())
	m.tree.Ascend(func(i btree.Item) bool {
		items = append(items, i.(*Item))
		return true
	})
	return &Iterator{items: items}
}

// Seek positions the iterator to the first key >= target.
func (it *Iterator) Seek(key []byte) {
	it.valid = false
	if len(it.items) == 0 {
		it.idx = 0
		return
	}
	if key == nil {
		it.idx = 0
		it.valid = true
		return
	}
	it.idx = sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].Key, key) >= 0
	})
	it.valid = it.idx >= 0 && it.idx < len(it.items)
}

// Next advances the iterator.
func (it *Iterator) Next() {
	if !it.valid {
		return // Do nothing if already invalid
	}

	it.idx++
	it.valid = it.idx >= 0 && it.idx < len(it.items)
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
	return it.items[it.idx].Key
}

// UnsafeValue returns a view (no copy) of the current value.
func (it *Iterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.items[it.idx].Value
}

// IsDeleted returns true if the current item is a tombstone.
func (it *Iterator) IsDeleted() bool {
	if !it.valid {
		return false
	}
	return it.items[it.idx].IsDeleted
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
	it.items = nil
	return nil
}

func (it *Iterator) Domain() (start, end []byte) {
	return nil, nil
}
