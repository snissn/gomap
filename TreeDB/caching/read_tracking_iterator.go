package caching

import (
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/merging"
)

type readTrackingIterator struct {
	inner     merging.Iterator
	reclaimer *memtableReclaimer
	closed    atomic.Bool
}

func (it *readTrackingIterator) Next()         { it.inner.Next() }
func (it *readTrackingIterator) Valid() bool   { return it.inner.Valid() }
func (it *readTrackingIterator) Key() []byte   { return it.inner.Key() }
func (it *readTrackingIterator) Value() []byte { return it.inner.Value() }
func (it *readTrackingIterator) KeyCopy(dst []byte) []byte {
	return it.inner.KeyCopy(dst)
}
func (it *readTrackingIterator) ValueCopy(dst []byte) []byte {
	return it.inner.ValueCopy(dst)
}
func (it *readTrackingIterator) Error() error { return it.inner.Error() }
func (it *readTrackingIterator) Domain() (start, end []byte) {
	return it.inner.Domain()
}

func (it *readTrackingIterator) Close() error {
	if it.reclaimer != nil && !it.closed.Swap(true) {
		it.reclaimer.readExit()
	}
	return it.inner.Close()
}
