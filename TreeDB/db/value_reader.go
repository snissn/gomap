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

type uncheckedLeafPageBatchScratch struct {
	valuePtrs   []page.ValuePtr
	missDst     [][]byte
	missIndexes []int
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
	val, usedDst, _, err := r.ReadLeafLogPageUnsafeToWithState(ptr, dst)
	return val, usedDst, err
}

func (r valueReader) ReadLeafLogPageUnsafeToWithState(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, tree.LeafLogPageReadState, error) {
	if r.vlogs == nil {
		return nil, false, tree.LeafLogPageReadState{}, errors.New("treedb: missing value-log reader")
	}
	if r.leafPageCache != nil && cap(dst) >= page.PageSize {
		if val, usedDst, state, ok := r.leafPageCache.getToWithState(ptr, dst); ok {
			return val, usedDst, tree.LeafLogPageReadState{RecordChecksumVerified: state.RecordChecksumVerified, CacheEntryPresent: state.CacheEntryPresent, PageChecksumVerified: state.PageChecksumVerified}, nil
		}
	}
	val, usedDst, err := r.ReadUnsafeTo(ptr.ValuePtr(), dst)
	if err != nil {
		return nil, false, tree.LeafLogPageReadState{}, err
	}
	recordChecksumVerified := r.ReadChecksumEnabled()
	cacheEntryPresent := false
	if r.leafPageCache != nil && cap(dst) >= page.PageSize && len(val) == page.PageSize {
		cacheEntryPresent = r.leafPageCache.storeReadMiss(ptr, val, recordChecksumVerified)
	}
	return val, usedDst, tree.LeafLogPageReadState{RecordChecksumVerified: recordChecksumVerified, CacheEntryPresent: cacheEntryPresent}, nil
}

// readLeafLogPagesUncheckedBatch resolves a one-pass run of outer leaves while
// preserving leaf-page cache hits. It is deliberately limited to unchecked
// readers: verified reads retain the per-page checksum state handled by
// ReadLeafLogPageUnsafeToWithState.
func (r valueReader) readLeafLogPagesUncheckedBatch(ptrs []page.LeafLogPtr, dst [][]byte, scratch *uncheckedLeafPageBatchScratch) ([][]byte, error) {
	if r.vlogs == nil {
		return nil, errors.New("treedb: missing value-log reader")
	}
	if r.ReadChecksumEnabled() {
		return nil, errors.New("treedb: unchecked outer-leaf batch used with checksum verification enabled")
	}
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	if scratch == nil {
		scratch = &uncheckedLeafPageBatchScratch{}
	}
	scratch.valuePtrs = scratch.valuePtrs[:0]
	scratch.missDst = scratch.missDst[:0]
	scratch.missIndexes = scratch.missIndexes[:0]
	for i, ptr := range ptrs {
		if r.leafPageCache != nil && cap(dst[i]) >= page.PageSize {
			if val, _, _, ok := r.leafPageCache.getToWithState(ptr, dst[i][:0]); ok {
				dst[i] = val
				continue
			}
		}
		scratch.valuePtrs = append(scratch.valuePtrs, ptr.ValuePtr())
		scratch.missDst = append(scratch.missDst, dst[i][:0])
		scratch.missIndexes = append(scratch.missIndexes, i)
	}
	if len(scratch.valuePtrs) == 0 {
		return dst, nil
	}
	values, err := r.ReadUnsafeAppendBatch(scratch.valuePtrs, scratch.missDst)
	if err != nil {
		return nil, err
	}
	for i, val := range values {
		index := scratch.missIndexes[i]
		dst[index] = val
		if r.leafPageCache != nil && len(val) == page.PageSize {
			r.leafPageCache.storeReadMiss(ptrs[index], val, false)
		}
	}
	return dst, nil
}

func (r valueReader) ReadLeafLogPageUnsafeView(ptr page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, bool, error) {
	data, lease, ok, _, err := r.ReadLeafLogPageUnsafeViewWithState(ptr)
	return data, lease, ok, err
}

func (r valueReader) ReadLeafLogPageUnsafeViewWithState(ptr page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, bool, tree.LeafLogPageReadState, error) {
	if r.vlogs == nil {
		return nil, nil, false, tree.LeafLogPageReadState{}, errors.New("treedb: missing value-log reader")
	}
	if r.leafPageCache == nil {
		return nil, nil, false, tree.LeafLogPageReadState{}, nil
	}
	data, release, state, ok := r.leafPageCache.getViewLockedWithState(ptr)
	return data, release, ok, tree.LeafLogPageReadState{RecordChecksumVerified: state.RecordChecksumVerified, CacheEntryPresent: state.CacheEntryPresent, PageChecksumVerified: state.PageChecksumVerified}, nil
}

func (r valueReader) MarkLeafLogPageChecksumVerified(ptr page.LeafLogPtr) {
	if r.leafPageCache == nil {
		return
	}
	r.leafPageCache.markPageChecksumVerified(ptr)
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
