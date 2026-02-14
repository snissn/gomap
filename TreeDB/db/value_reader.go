package db

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	vlogs         tree.SlabReader
	outerLeafMode string
	cache         *outerLeafBlockCache
}

type unsafeAppendReader interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

type unsafeAppendBatchReader interface {
	ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error)
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return valueReader{vlogs: state.ValueLogSet}
}

func (r valueReader) decodeValue(ptr page.ValuePtr, raw []byte) ([]byte, error) {
	if !outerleaf.ModeEnabled(r.outerLeafMode) {
		return raw, nil
	}
	block, err := r.outerLeafBlock(ptr, raw)
	if err != nil {
		return nil, err
	}
	val, err := block.FirstValue()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r valueReader) decodeValueForKey(ptr page.ValuePtr, key, raw []byte) ([]byte, error) {
	decoded, found, err := r.decodeValueForKeyFound(ptr, key, raw)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
	}
	return decoded, nil
}

func (r valueReader) decodeValueForKeyFound(ptr page.ValuePtr, key, raw []byte) ([]byte, bool, error) {
	if !outerleaf.ModeEnabled(r.outerLeafMode) {
		return raw, true, nil
	}
	block, err := r.outerLeafBlock(ptr, raw)
	if err != nil {
		return nil, false, err
	}
	val, found, err := block.ValueForKey(key)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return val, true, nil
}

func (r valueReader) ReadUnsafeFenceForKey(ptr page.ValuePtr, key []byte) ([]byte, bool, error) {
	if strings.TrimSpace(r.outerLeafMode) != outerleaf.ModeV2FencePtr {
		return nil, false, nil
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		return block.ValueForKey(key)
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, false, err
	}
	return r.decodeValueForKeyFound(ptr, key, raw)
}

func (r valueReader) ReadUnsafeFenceBlock(ptr page.ValuePtr) ([]tree.FenceBlockEntry, bool, error) {
	if strings.TrimSpace(r.outerLeafMode) != outerleaf.ModeV2FencePtr {
		return nil, false, nil
	}
	block := r.cachedOuterLeafBlock(ptr)
	if block == nil {
		raw, err := r.readRawUnsafe(ptr)
		if err != nil {
			return nil, true, err
		}
		decoded, decErr := r.outerLeafBlock(ptr, raw)
		if decErr != nil {
			return nil, true, decErr
		}
		block = decoded
	}
	decoded, err := block.Entries(nil)
	if err != nil {
		return nil, true, err
	}
	entries := make([]tree.FenceBlockEntry, len(decoded))
	for i := range decoded {
		entries[i] = tree.FenceBlockEntry{Key: decoded[i].Key, Value: decoded[i].Value}
	}
	return entries, true, nil
}

func (r valueReader) ReadUnsafeFenceBlockKeys(ptr page.ValuePtr) ([][]byte, bool, error) {
	if strings.TrimSpace(r.outerLeafMode) != outerleaf.ModeV2FencePtr {
		return nil, false, nil
	}
	block := r.cachedOuterLeafBlock(ptr)
	if block == nil {
		raw, err := r.readRawUnsafe(ptr)
		if err != nil {
			return nil, true, err
		}
		decoded, decErr := r.outerLeafBlock(ptr, raw)
		if decErr != nil {
			return nil, true, decErr
		}
		block = decoded
	}
	keys, err := block.Keys(nil)
	if err != nil {
		return nil, true, err
	}
	return keys, true, nil
}

func (r valueReader) readRaw(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.Read(ptr)
}

func (r valueReader) readRawUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.ReadUnsafe(ptr)
}

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		return block.FirstValue()
	}
	raw, err := r.readRaw(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValue(ptr, raw)
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		return block.FirstValue()
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValue(ptr, raw)
}

func (r valueReader) ReadForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		decoded, found, err := block.ValueForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return decoded, nil
	}
	raw, err := r.readRaw(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValueForKey(ptr, key, raw)
}

func (r valueReader) ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		decoded, found, err := block.ValueForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		return decoded, nil
	}
	raw, err := r.readRawUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return r.decodeValueForKey(ptr, key, raw)
}

func (r valueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	return r.ReadUnsafeAppendForKey(ptr, nil, dst)
}

func (r valueReader) ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	if block := r.cachedOuterLeafBlock(ptr); block != nil {
		decoded, found, err := block.ValueForKey(key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("value reader: outer-leaf key lookup miss ptr=%+v key=%x", ptr, key)
		}
		if len(decoded) == 0 {
			return dst[:0], nil
		}
		return append(dst[:0], decoded...), nil
	}
	if app, ok := r.vlogs.(unsafeAppendReader); ok {
		raw, err := app.ReadUnsafeAppend(ptr, dst[:0])
		if err != nil {
			return nil, err
		}
		decoded, err := r.decodeValueForKey(ptr, key, raw)
		if err != nil {
			return nil, err
		}
		if len(decoded) == 0 {
			return dst[:0], nil
		}
		return append(dst[:0], decoded...), nil
	}
	val, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	decoded, err := r.decodeValueForKey(ptr, key, val)
	if err != nil {
		return nil, err
	}
	dst = append(dst[:0], decoded...)
	return dst, nil
}

func (r valueReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if app, ok := r.vlogs.(unsafeAppendBatchReader); ok {
		out, err := app.ReadUnsafeAppendBatch(ptrs, dst)
		if err != nil {
			return nil, err
		}
		for i := range out {
			decoded, decErr := r.decodeValue(ptrs[i], out[i])
			if decErr != nil {
				return nil, decErr
			}
			out[i] = decoded
		}
		return out, nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		var err error
		dst[i], err = r.ReadUnsafeAppend(ptr, dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (r valueReader) ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error) {
	if len(ptrs) != len(keys) {
		return nil, fmt.Errorf("value reader: ptr/key batch mismatch %d/%d", len(ptrs), len(keys))
	}
	if len(ptrs) == 0 {
		return dst[:0], nil
	}
	if app, ok := r.vlogs.(unsafeAppendBatchReader); ok {
		out, err := app.ReadUnsafeAppendBatch(ptrs, dst)
		if err != nil {
			return nil, err
		}
		for i := range out {
			decoded, decErr := r.decodeValueForKey(ptrs[i], keys[i], out[i])
			if decErr != nil {
				return nil, decErr
			}
			out[i] = decoded
		}
		return out, nil
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		var err error
		dst[i], err = r.ReadUnsafeAppendForKey(ptrs[i], keys[i], dst[i][:0])
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (r valueReader) outerLeafBlock(ptr page.ValuePtr, raw []byte) (*outerleaf.DecodedBlock, error) {
	if r.cache == nil {
		return outerleaf.DecodeBlock(raw, nil)
	}
	key := newOuterLeafBlockKey(ptr)
	if block := r.cache.get(key); block != nil {
		return block, nil
	}
	block, err := outerleaf.DecodeBlock(raw, nil)
	if err != nil {
		return nil, err
	}
	r.cache.put(key, block)
	return block, nil
}

func (r valueReader) cachedOuterLeafBlock(ptr page.ValuePtr) *outerleaf.DecodedBlock {
	if !outerleaf.ModeEnabled(r.outerLeafMode) || r.cache == nil {
		return nil
	}
	return r.cache.get(newOuterLeafBlockKey(ptr))
}
