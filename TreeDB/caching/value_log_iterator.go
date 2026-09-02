package caching

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type valueLogIterator struct {
	iter iterator.UnsafeIterator
	read func(key []byte, ptr page.ValuePtr) ([]byte, error)

	cached       bool
	cachedValue  []byte
	cachedPtr    page.ValuePtr
	cachedHasPtr bool
	keyScratch   []byte
	err          error
}

func newValueLogIterator(iter iterator.UnsafeIterator, read func(key []byte, ptr page.ValuePtr) ([]byte, error)) iterator.UnsafeIterator {
	if iter == nil || read == nil {
		return iter
	}
	return &valueLogIterator{
		iter: iter,
		read: read,
	}
}

func (it *valueLogIterator) Valid() bool {
	if it == nil {
		return false
	}
	return it.iter.Valid()
}

func (it *valueLogIterator) Next() {
	it.iter.Next()
	it.invalidate()
}

func (it *valueLogIterator) Seek(key []byte) {
	it.iter.Seek(key)
	it.invalidate()
}

func (it *valueLogIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *valueLogIterator) UnsafeValue() []byte {
	it.loadValue()
	return it.cachedValue
}

func (it *valueLogIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *valueLogIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	revision := page.LegacyEntryRevision
	if revIter, ok := it.iter.(iterator.RevisionUnsafeIterator); ok {
		_, _, _, revision = revIter.UnsafeEntryWithRevision()
	}
	if it.iter.IsDeleted() {
		return nil, page.ValuePtr{}, node.FlagTombstone, revision
	}
	it.loadValue()
	if it.cachedHasPtr {
		// Value-log pointers are materialized here; report inline semantics.
		return it.cachedValue, page.ValuePtr{}, node.FlagInline, revision
	}
	return it.cachedValue, page.ValuePtr{}, node.FlagInline, revision
}

func (it *valueLogIterator) Key() []byte {
	return it.iter.Key()
}

func (it *valueLogIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *valueLogIterator) KeyCopy(dst []byte) []byte {
	return it.iter.KeyCopy(dst)
}

func (it *valueLogIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.Value()...)
}

func (it *valueLogIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *valueLogIterator) Error() error {
	iterErr := it.iter.Error()
	if iterErr == nil {
		return it.err
	}
	if it.err == nil {
		return iterErr
	}
	return errors.Join(iterErr, it.err)
}

func (it *valueLogIterator) Close() error {
	closeErr := it.iter.Close()
	if closeErr == nil {
		return it.err
	}
	if it.err == nil {
		return closeErr
	}
	return errors.Join(closeErr, it.err)
}

func (it *valueLogIterator) Domain() (start, end []byte) {
	return it.iter.Domain()
}

func (it *valueLogIterator) invalidate() {
	it.cached = false
	it.cachedValue = nil
	it.cachedPtr = page.ValuePtr{}
	it.cachedHasPtr = false
}

func (it *valueLogIterator) loadValue() {
	if it.cached {
		return
	}
	it.cached = true
	val, ptr, flags := it.iter.UnsafeEntry()
	it.cachedValue = val
	if it.iter.IsDeleted() || it.read == nil {
		return
	}
	if flags&node.FlagPointer == 0 {
		return
	}
	it.cachedHasPtr = true
	it.cachedPtr = ptr
	unsafeKey := it.iter.UnsafeKey()
	// Keep a stable key view for the read callback without per-entry allocation.
	it.keyScratch = append(it.keyScratch[:0], unsafeKey...)
	val, err := it.read(it.keyScratch, ptr)
	if err != nil {
		it.err = err
		return
	}
	it.cachedValue = val
}
