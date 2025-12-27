package caching

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type valueLogIterator struct {
	iter iterator.UnsafeIterator
	ptrs *largePtrMap
	read func(page.ValuePtr) ([]byte, error)

	cached       bool
	cachedValue  []byte
	cachedPtr    page.ValuePtr
	cachedHasPtr bool
	err          error
}

func newValueLogIterator(iter iterator.UnsafeIterator, ptrs *largePtrMap, read func(page.ValuePtr) ([]byte, error)) iterator.UnsafeIterator {
	if iter == nil || ptrs == nil || read == nil {
		return iter
	}
	return &valueLogIterator{
		iter: iter,
		ptrs: ptrs,
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
	if it.iter.IsDeleted() {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	it.loadValue()
	if it.cachedHasPtr {
		return it.cachedValue, it.cachedPtr, node.FlagPointer
	}
	return it.cachedValue, page.ValuePtr{}, node.FlagInline
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
	it.cachedValue = it.iter.UnsafeValue()
	if it.iter.IsDeleted() || it.ptrs == nil || it.read == nil {
		return
	}
	if len(it.cachedValue) > 0 {
		return
	}
	key := bytesToStringNoCopy(it.iter.UnsafeKey())
	ptr, ok := it.ptrs.GetString(key)
	if !ok {
		return
	}
	it.cachedHasPtr = true
	it.cachedPtr = ptr
	val, err := it.read(ptr)
	if err != nil {
		it.err = err
		return
	}
	it.cachedValue = val
}
