package memtable

import (
	"bytes"
	"fmt"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/skiplist"
	"github.com/snissn/gomap/TreeDB/page"
)

type Mode uint8

const (
	ModeSkiplist Mode = iota
	ModeHashSorted
	ModeBTree
)

func ModeFromString(mode string) (Mode, error) {
	switch mode {
	case "", "skiplist":
		return ModeSkiplist, nil
	case "hash_sorted":
		return ModeHashSorted, nil
	case "btree":
		return ModeBTree, nil
	default:
		return ModeSkiplist, fmt.Errorf("unknown memtable mode %q", mode)
	}
}

func (m Mode) String() string {
	switch m {
	case ModeSkiplist:
		return "skiplist"
	case ModeHashSorted:
		return "hash_sorted"
	case ModeBTree:
		return "btree"
	default:
		return "skiplist"
	}
}

type Table interface {
	Set(key, value []byte)
	PutWithCallback(key, value []byte, cb func(k, v []byte) error) error
	Delete(key []byte)
	DeleteWithCallback(key []byte, cb func(k, v []byte) error) error
	SetSteal(key, value []byte)
	DeleteSteal(key []byte)
	Get(key []byte) ([]byte, bool, bool)
	Size() int64
	Len() int
	// NewIterator may hold a read lock until Close; callers should avoid
	// iterating over mutable memtables on hot write paths.
	NewIterator(start, end []byte) iterator.UnsafeIterator
	Freeze()
}

// SortedBatchApplier is an optional fast path for applying a strictly-increasing
// batch under a single memtable lock.
//
// Callers should only use this when they know the entries are already in
// increasing key order.
type SortedBatchApplier interface {
	ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte))
}

// CompressedPutter is an optional fast path for inserting compressed values
// directly into the memtable arena.
type CompressedPutter interface {
	PutCompressedWithCallback(key, src []byte, flags uint8, maxSz int, compressor func(src, dst []byte) []byte, cb func(k, v []byte) error) error
}

type Memtable struct {
	sl *skiplist.SkipList
	mu sync.RWMutex
}

const defaultMemtableCapacity = 64 * 1024
const maxInitialMemtableCapacity = 256 << 20

// New creates a new Memtable (skiplist).
func New() *Memtable {
	return NewWithCapacity(defaultMemtableCapacity)
}

// NewWithCapacity creates a new Memtable with the requested arena capacity.
// A non-positive capacity uses a small default to keep rotations cheap.
func NewWithCapacity(capacity int) *Memtable {
	if capacity <= 0 {
		capacity = defaultMemtableCapacity
	}
	if capacity > maxInitialMemtableCapacity {
		capacity = maxInitialMemtableCapacity
	}
	return &Memtable{
		sl: skiplist.New(capacity),
	}
}

func NewWithCapacityMode(capacity int, mode Mode) (Table, error) {
	return NewWithCapacityModeAndIndexer(capacity, mode, nil)
}

func NewWithCapacityModeAndIndexer(capacity int, mode Mode, indexer *HashSortedIndexer) (Table, error) {
	switch mode {
	case ModeSkiplist:
		return NewWithCapacity(capacity), nil
	case ModeHashSorted:
		return NewHashSortedWithIndexer(indexer), nil
	case ModeBTree:
		return NewBTree(), nil
	default:
		return nil, fmt.Errorf("unknown memtable mode %q", mode.String())
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

// PutCompressedWithCallback inserts a key and compresses src into the skiplist arena.
// This is an unsafe/experimental path; reads may return compressed bytes.
func (m *Memtable) PutCompressedWithCallback(key, src []byte, flags uint8, maxSz int, compressor func(src, dst []byte) []byte, cb func(k, v []byte) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sl.PutCompressed(key, src, flags, maxSz, compressor, cb)
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

func (m *Memtable) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Fast path: if the batch keys are strictly increasing (guaranteed by the
	// caller when using SortedBatchApplier) and start after the current max key,
	// we can append without per-op comparisons/traversal.
	if len(entries) > 0 {
		last := m.sl.LastKey()
		if last == nil || bytes.Compare(entries[0].Key, last) > 0 {
			for _, op := range entries {
				if op.Type == batchpkg.OpDelete {
					m.sl.AppendDelete(op.Key)
				} else {
					m.sl.Append(op.Key, op.Value)
				}
				if onKey != nil {
					onKey(op.Key)
				}
			}
			return
		}
	}

	for _, op := range entries {
		if op.Type == batchpkg.OpDelete {
			m.sl.Delete(op.Key)
		} else {
			m.sl.Put(op.Key, op.Value)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
}

// SetSteal - SkipList copies data, so Steal is same as Set.
func (m *Memtable) SetSteal(key, value []byte) {
	m.Set(key, value)
}

// DeleteSteal - SkipList copies data, so Steal is same as Delete.
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

func (m *Memtable) Freeze() {}

// Reset clears the memtable while retaining its arena capacity.
func (m *Memtable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Reset()
}

// Iterator wrapper
type Iterator struct {
	iter *skiplist.Iterator
	end  []byte
	mu   *sync.RWMutex
}

func (m *Memtable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	it := m.sl.NewIterator(start, end)
	return &Iterator{iter: it, end: end, mu: &m.mu}
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

func (it *Iterator) KeyCopy(dst []byte) []byte {
	k := it.iter.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *Iterator) ValueCopy(dst []byte) []byte {
	v := it.iter.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *Iterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	if it.iter == nil {
		return nil
	}
	err := it.iter.Close()
	it.iter = nil
	return err
}

func (it *Iterator) Error() error {
	return it.iter.Error()
}

func (it *Iterator) Domain() (start, end []byte) {
	return nil, it.end
}
