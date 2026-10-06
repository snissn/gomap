package caching

import (
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrCOWUnsupported denotes a surface outside the explicit COW cache contract.
// Public dispatch must reject these surfaces before accepting their effects.
var ErrCOWUnsupported = errors.New("operation unsupported by cow_btree cache")

// cowTable adapts an already pinned immutable root to existing read/merge code.
// Its containing cut owns the root reference; iterators acquire independent pins.
// This adapter must never be installed as a legacy mutable shard.
type cowTable struct {
	shard     int
	root      *memtable.COWRoot
	size      int64
	resources *cowGenerationResources
}

var _ memtable.Table = (*cowTable)(nil)
var _ memtable.RevisionTable = (*cowTable)(nil)
var _ memtable.SuccessorTable = (*cowTable)(nil)
var _ iterator.UnsafeIterator = (*cowIterator)(nil)
var _ iterator.RevisionUnsafeIterator = (*cowIterator)(nil)

func cowBytes(s string) []byte {
	if s == "" {
		return nil
	}
	// Existing unsafe iterator/table accessors promise read-only borrowed bytes.
	// Safe public accessors copy them while a cut or cursor still owns the root.
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func (*cowTable) Set([]byte, []byte) { panic("mutation of immutable COW read adapter") }
func (*cowTable) SetEntry([]byte, []byte, page.ValuePtr, byte) {
	panic("mutation of immutable COW read adapter")
}
func (*cowTable) SetEntryWithRevision([]byte, []byte, page.ValuePtr, byte, page.EntryRevision) {
	panic("mutation of immutable COW read adapter")
}
func (*cowTable) PutWithCallback([]byte, []byte, func([]byte, []byte) error) error {
	return ErrCOWUnsupported
}
func (*cowTable) Delete([]byte) { panic("mutation of immutable COW read adapter") }
func (*cowTable) DeleteWithCallback([]byte, func([]byte, []byte) error) error {
	return ErrCOWUnsupported
}
func (*cowTable) SetSteal([]byte, []byte) { panic("mutation of immutable COW read adapter") }
func (*cowTable) SetEntrySteal([]byte, []byte, page.ValuePtr, byte) {
	panic("mutation of immutable COW read adapter")
}
func (*cowTable) DeleteSteal([]byte) { panic("mutation of immutable COW read adapter") }
func (*cowTable) Freeze()            {}

func (t *cowTable) Size() int64 { return t.size }
func (t *cowTable) Len() int    { return t.root.Len() }
func (t *cowTable) Get(key []byte) ([]byte, bool, bool) {
	v, _, flags, found := t.GetEntry(key)
	return v, flags&node.FlagTombstone != 0, found
}
func (t *cowTable) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	v, ptr, flags, _, found := t.GetEntryWithRevision(key)
	return v, ptr, flags, found
}
func (t *cowTable) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	r, found := t.root.Get(key)
	return cowBytes(r.Value), r.Ptr, r.Flags, r.Revision, found
}
func (t *cowTable) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	r, found := t.root.SeekGE(start, end)
	return cowBytes(r.Key), cowBytes(r.Value), r.Ptr, r.Flags, r.Revision, found
}
func (t *cowTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	// Reserve the wrapper and owned domain bounds before allocating them. C1
	// independently charges the cursor, tree traversal stack and cursor end bound.
	extra := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowIterator{}))) +
		memtable.COWAllocationCharge(uint64(len(start))) + memtable.COWAllocationCharge(uint64(len(end)))
	c, err := t.root.CursorWithExtraBytes(start, end, extra)
	if err != nil {
		return refusedCOWIterator(err)
	}
	it := &cowIterator{cursor: c, start: append([]byte(nil), start...), end: append([]byte(nil), end...)}
	if start != nil && it.start == nil {
		it.start = []byte{}
	}
	if end != nil && it.end == nil {
		it.end = []byte{}
	}
	it.load()
	return it
}
func (*cowTable) NewReverseIterator(_, _ []byte) iterator.UnsafeIterator {
	return &cowUnsupportedIterator
}

type cowIterator struct {
	mu         sync.Mutex
	cursor     *memtable.COWCursor
	record     memtable.COWRecord
	valid      bool
	err        error
	start, end []byte
}

// load is called only before publication of the iterator or under mu.
func (it *cowIterator) load() {
	it.record, it.valid, it.err = it.cursor.Record()
}
func (it *cowIterator) Valid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.valid
}
func (it *cowIterator) Next() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.cursor == nil || it.err != nil {
		return
	}
	if err := it.cursor.Next(); err != nil {
		it.err, it.valid = err, false
		return
	}
	it.load()
}
func (it *cowIterator) Seek(key []byte) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.cursor == nil || it.err != nil {
		return
	}
	if err := it.cursor.Seek(key); err != nil {
		it.err, it.valid = err, false
		return
	}
	it.load()
}
func (it *cowIterator) UnsafeKey() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()
	return cowBytes(it.record.Key)
}
func (it *cowIterator) UnsafeValue() []byte {
	v, _, _ := it.UnsafeEntry()
	return v
}
func (it *cowIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	v, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return v, ptr, flags
}
func (it *cowIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return cowBytes(it.record.Value), it.record.Ptr, it.record.Flags, it.record.Revision
}
func (it *cowIterator) Key() []byte   { return it.UnsafeKey() }
func (it *cowIterator) Value() []byte { return it.UnsafeValue() }
func (it *cowIterator) KeyCopy(dst []byte) []byte {
	it.mu.Lock()
	defer it.mu.Unlock()
	return append(dst[:0], it.record.Key...)
}
func (it *cowIterator) ValueCopy(dst []byte) []byte {
	it.mu.Lock()
	defer it.mu.Unlock()
	return append(dst[:0], it.record.Value...)
}
func (it *cowIterator) IsDeleted() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.record.Flags&node.FlagTombstone != 0
}
func (it *cowIterator) Error() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}
func (it *cowIterator) Domain() ([]byte, []byte) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.start, it.end
}
func (it *cowIterator) Close() error {
	it.mu.Lock()
	c := it.cursor
	it.cursor, it.valid = nil, false
	it.record = memtable.COWRecord{}
	it.mu.Unlock()
	// Releasing a cursor may retire file resources. Never call it under the
	// wrapper lock or any publication/admission lock.
	if c != nil {
		c.Close()
	}
	return nil
}

// Refusal paths return immutable static carriers. They neither allocate after
// denied admission nor mutate shared state when callers move or close them.
type cowErrorIterator struct{ err error }

var (
	cowCapacityIterator    = cowErrorIterator{memtable.ErrCOWCapacity}
	cowClosedIterator      = cowErrorIterator{memtable.ErrCOWClosed}
	cowUnsupportedIterator = cowErrorIterator{ErrCOWUnsupported}
)

func refusedCOWIterator(err error) iterator.UnsafeIterator {
	if err == memtable.ErrCOWCapacity {
		return &cowCapacityIterator
	}
	if err == memtable.ErrCOWClosed {
		return &cowClosedIterator
	}
	panic("unexpected COW cursor admission error")
}
func (*cowErrorIterator) Valid() bool                                { return false }
func (*cowErrorIterator) Next()                                      {}
func (*cowErrorIterator) Seek([]byte)                                {}
func (*cowErrorIterator) UnsafeKey() []byte                          { return nil }
func (*cowErrorIterator) UnsafeValue() []byte                        { return nil }
func (*cowErrorIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) { return nil, page.ValuePtr{}, 0 }
func (*cowErrorIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return nil, page.ValuePtr{}, 0, 0
}
func (*cowErrorIterator) Key() []byte                 { return nil }
func (*cowErrorIterator) Value() []byte               { return nil }
func (*cowErrorIterator) KeyCopy(dst []byte) []byte   { return dst[:0] }
func (*cowErrorIterator) ValueCopy(dst []byte) []byte { return dst[:0] }
func (*cowErrorIterator) IsDeleted() bool             { return false }
func (it *cowErrorIterator) Error() error             { return it.err }
func (*cowErrorIterator) Domain() ([]byte, []byte)    { return nil, nil }
func (*cowErrorIterator) Close() error                { return nil }
