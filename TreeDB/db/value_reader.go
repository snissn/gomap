package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type unsafeAppendReader interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

type unsafeAppendBatchReader interface {
	ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error)
}

type unsafeToReader interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

type readChecksumCapability interface {
	ReadChecksumEnabled() bool
}

// valueReader resolves value-log pointers for tree lookups/iterators.
type valueReader struct {
	vlogs         tree.SlabReader
	leafPageCache *leafPageReadCache
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return newValueReader(state.ValueLogSet)
}

func newValueReader(vlogs tree.SlabReader) valueReader {
	return valueReader{vlogs: vlogs}
}

func (r *valueReader) reconfigure(vlogs tree.SlabReader, leafPageCache *leafPageReadCache) {
	if r == nil {
		return
	}
	r.vlogs = vlogs
	r.leafPageCache = leafPageCache
}

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if r.vlogs == nil {
		return nil, errors.New("treedb: missing value-log reader")
	}
	return r.vlogs.Read(ptr)
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if r.vlogs == nil {
		return nil, errors.New("treedb: missing value-log reader")
	}
	return r.vlogs.ReadUnsafe(ptr)
}

func (r valueReader) ReadChecksumEnabled() bool {
	if r.vlogs == nil {
		return true
	}
	if cap, ok := r.vlogs.(readChecksumCapability); ok {
		return cap.ReadChecksumEnabled()
	}
	return true
}

func (r valueReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	if r.vlogs == nil {
		return nil, false, errors.New("treedb: missing value-log reader")
	}
	if toer, ok := r.vlogs.(unsafeToReader); ok {
		return toer.ReadUnsafeTo(ptr, dst)
	}
	val, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, false, err
	}
	return val, false, nil
}

func (r valueReader) ReadLeafLogPageUnsafeTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, error) {
	if r.vlogs == nil {
		return nil, false, errors.New("treedb: missing value-log reader")
	}
	if r.leafPageCache != nil && cap(dst) >= page.PageSize {
		if val, usedDst, ok := r.leafPageCache.getTo(ptr, dst); ok {
			return val, usedDst, nil
		}
	}
	val, usedDst, err := r.ReadUnsafeTo(ptr.ValuePtr(), dst)
	if err != nil {
		return nil, false, err
	}
	if r.leafPageCache != nil && cap(dst) >= page.PageSize && len(val) == page.PageSize {
		r.leafPageCache.storeReadMiss(ptr, val)
	}
	return val, usedDst, nil
}

func (r valueReader) ReadLeafLogPageUnsafeView(ptr page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, bool, error) {
	if r.vlogs == nil {
		return nil, nil, false, errors.New("treedb: missing value-log reader")
	}
	if r.leafPageCache == nil {
		return nil, nil, false, nil
	}
	data, release, ok := r.leafPageCache.getViewLocked(ptr)
	return data, release, ok, nil
}

func (r valueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if r.vlogs == nil {
		return nil, errors.New("treedb: missing value-log reader")
	}
	if appender, ok := r.vlogs.(unsafeAppendReader); ok {
		return appender.ReadUnsafeAppend(ptr, dst)
	}
	val, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return append(dst[:0], val...), nil
}

func (r valueReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if r.vlogs == nil {
		return nil, errors.New("treedb: missing value-log reader")
	}
	if appender, ok := r.vlogs.(unsafeAppendBatchReader); ok {
		return appender.ReadUnsafeAppendBatch(ptrs, dst)
	}
	for i := range ptrs {
		val, err := r.vlogs.ReadUnsafe(ptrs[i])
		if err != nil {
			return nil, err
		}
		dst[i] = append(dst[i][:0], val...)
	}
	return dst, nil
}
