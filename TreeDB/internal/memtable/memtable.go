package memtable

import (
	"bytes"
	"fmt"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/skiplist"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type Mode uint8

const (
	ModeSkiplist Mode = iota
	ModeHashSorted
	ModeBTree
	ModeAppendOnly
)

func ModeFromString(mode string) (Mode, error) {
	switch mode {
	case "", "skiplist":
		return ModeSkiplist, nil
	case "hash_sorted":
		return ModeHashSorted, nil
	case "btree":
		return ModeBTree, nil
	case "append_only":
		return ModeAppendOnly, nil
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
	case ModeAppendOnly:
		return "append_only"
	default:
		return "skiplist"
	}
}

type Table interface {
	Set(key, value []byte)
	// SetEntry stores a value with explicit flags and optional value pointer.
	// When flags include node.FlagPointer, value may be nil and ptr must be set;
	// if value is non-nil it is stored alongside the pointer bytes.
	SetEntry(key, value []byte, ptr page.ValuePtr, flags byte)
	PutWithCallback(key, value []byte, cb func(k, v []byte) error) error
	Delete(key []byte)
	DeleteWithCallback(key []byte, cb func(k, v []byte) error) error
	SetSteal(key, value []byte)
	// SetEntrySteal is like SetEntry but allows stealing the provided value slice.
	SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte)
	DeleteSteal(key []byte)
	Get(key []byte) ([]byte, bool, bool)
	// GetEntry returns the raw entry, including pointer and flags, if present.
	GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool)
	Size() int64
	Len() int
	// NewIterator may hold a read lock until Close; callers should avoid
	// iterating over mutable memtables on hot write paths.
	NewIterator(start, end []byte) iterator.UnsafeIterator
	// NewReverseIterator returns a reverse iterator over [start, end).
	// Like NewIterator, it may hold a read lock until Close.
	NewReverseIterator(start, end []byte) iterator.UnsafeIterator
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

// SortedBatchBorrowValueApplier is an optional fast path for applying a
// strictly-increasing batch under a single memtable lock while borrowing value
// slices from the caller. Keys must still be copied into memtable-owned
// storage.
//
// Callers should only use this when they know the entries are already in
// increasing key order and the borrowed values will remain immutable for the
// memtable lifetime.
type SortedBatchBorrowValueApplier interface {
	ApplyBorrowValueSortedBatch(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte))
}

// ValueBorrower marks memtables that can safely retain caller-owned value
// slices while still copying keys into their own storage.
//
// Callers must keep the underlying value storage alive until the memtable is
// retired or reset.
type ValueBorrower interface {
	SetEntryBorrowValue(key, value []byte, ptr page.ValuePtr, flags byte)
}

// StableUnsafeIteratorTable marks memtable implementations whose
// iterator.UnsafeIterator key/value views (from UnsafeKey/UnsafeValue/UnsafeEntry)
// are backed by storage that outlives the iterator itself.
//
// Implementations returning true MUST ensure these views stay valid and
// immutable across Next/Seek and remain valid after Close, at least until the
// underlying memtable is reset or garbage-collected.
//
// Callers such as buildOpRuns rely on this stronger contract to materialize
// immutable flush runs without per-entry defensive copies.
type StableUnsafeIteratorTable interface {
	StableUnsafeIteratorSlices() bool
}

// OperationMix describes the operation mix currently retained by a memtable.
// It is a performance-planning hint, not a correctness contract: implementations
// may count superseded append-log entries when their iterators later collapse
// multiple writes to the latest value for a key.
type OperationMix struct {
	Entries int
	Deletes int
}

// OperationMixProvider marks memtables that can report their write-buffer
// operation mix without forcing an iterator materialization pass.
type OperationMixProvider interface {
	OperationMix() OperationMix
}

// TrustedSortedBatchApplier is an optional fast path for callers that already
// guarantee strictly increasing keys (for example, stream-qualified batch
// writes partitioned by shard while preserving source order).
type TrustedSortedBatchApplier interface {
	ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte))
}

// TrustedSortedBatchBorrowValueApplier is an optional fast path for callers
// that already guarantee strictly increasing keys and immutable borrowed value
// slices.
type TrustedSortedBatchBorrowValueApplier interface {
	ApplyBorrowValueSortedBatchTrusted(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte))
}

// TrustedSortedBatchIndexApplier is an optional fast path for callers that
// already guarantee strictly increasing keys and can provide a sorted index
// list into a shared batch entry slice. This avoids materializing per-shard
// []batch.Entry views when the memtable can consume the original batch slice
// directly.
type TrustedSortedBatchIndexApplier interface {
	ApplyStealSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, onKey func(key []byte))
}

// TrustedSortedBatchBorrowValueIndexApplier is like
// TrustedSortedBatchBorrowValueApplier but consumes a sorted index list into a
// shared batch entry slice.
type TrustedSortedBatchBorrowValueIndexApplier interface {
	ApplyBorrowValueSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte))
}

type Memtable struct {
	sl *skiplist.SkipList
	mu sync.RWMutex
}

func (*Memtable) StableUnsafeIteratorSlices() bool { return true }

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
		return NewHashSortedWithCapacityAndIndexer(capacity, indexer), nil
	case ModeBTree:
		return NewBTree(), nil
	case ModeAppendOnly:
		return NewAppendOnlyWithCapacity(capacity), nil
	default:
		return nil, fmt.Errorf("unknown memtable mode %q", mode.String())
	}
}

func (m *Memtable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.Put(key, value)
}

func (m *Memtable) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sl.PutEntry(key, value, ptr, flags)
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
				} else if op.IsPtr {
					if len(op.Value) > 0 {
						buf := make([]byte, page.ValuePtrSize+len(op.Value))
						op.ValuePtr.Encode(buf[:page.ValuePtrSize])
						copy(buf[page.ValuePtrSize:], op.Value)
						_ = m.sl.AppendWithCallback(op.Key, buf, node.FlagPointer, nil)
					} else {
						var buf [page.ValuePtrSize]byte
						op.ValuePtr.Encode(buf[:])
						_ = m.sl.AppendWithCallback(op.Key, buf[:], node.FlagPointer, nil)
					}
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
		} else if op.IsPtr {
			m.sl.PutEntry(op.Key, op.Value, op.ValuePtr, node.FlagPointer)
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

// SetEntrySteal - SkipList copies data, so Steal is same as SetEntry.
func (m *Memtable) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntry(key, value, ptr, flags)
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

func (m *Memtable) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.GetEntry(key)
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

func (m *Memtable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	it := m.sl.NewReverseIterator(start, end)
	return &ReverseIterator{iter: it, start: start, end: end, mu: &m.mu}
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

type ReverseIterator struct {
	iter  *skiplist.ReverseIterator
	start []byte
	end   []byte
	mu    *sync.RWMutex
}

func (it *ReverseIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *ReverseIterator) Next() {
	it.iter.Next()
}

func (it *ReverseIterator) Valid() bool {
	if !it.iter.Valid() {
		return false
	}
	k := it.iter.UnsafeKey()
	if it.end != nil && bytes.Compare(k, it.end) >= 0 {
		return false
	}
	if it.start != nil && bytes.Compare(k, it.start) < 0 {
		return false
	}
	return true
}

func (it *ReverseIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *ReverseIterator) UnsafeValue() []byte {
	return it.iter.UnsafeValue()
}

func (it *ReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.iter.UnsafeEntry()
}

func (it *ReverseIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *ReverseIterator) Key() []byte {
	return it.iter.Key()
}

func (it *ReverseIterator) Value() []byte {
	return it.iter.Value()
}

func (it *ReverseIterator) KeyCopy(dst []byte) []byte {
	k := it.iter.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *ReverseIterator) ValueCopy(dst []byte) []byte {
	v := it.iter.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *ReverseIterator) Close() error {
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

func (it *ReverseIterator) Error() error {
	return it.iter.Error()
}

func (it *ReverseIterator) Domain() (start, end []byte) {
	return it.start, it.end
}
