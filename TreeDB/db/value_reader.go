package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/largevalue"
	"github.com/snissn/gomap/TreeDB/internal/leafblock"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	vlogs                  tree.SlabReader
	skipLeafBlockChecksums bool
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return newValueReader(state.ValueLogSet, false)
}

func newValueReader(vlogs tree.SlabReader, skipLeafBlockChecksums bool) valueReader {
	return valueReader{
		vlogs:                  vlogs,
		skipLeafBlockChecksums: skipLeafBlockChecksums,
	}
}

func (r *valueReader) reconfigure(vlogs tree.SlabReader, skipLeafBlockChecksums bool, _ any, _ any) {
	if r == nil {
		return
	}
	r.vlogs = vlogs
	r.skipLeafBlockChecksums = skipLeafBlockChecksums
}

func (r *valueReader) clearForPoolReuse() {
	if r == nil {
		return
	}
	r.vlogs = nil
	r.skipLeafBlockChecksums = false
}

func (r *valueReader) releaseDecodeContext() {
	// No pooled decode context in the exact lookup reader.
}

func (r valueReader) KeyAwareEnabled() bool { return true }

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	raw, err := r.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if r.vlogs == nil {
		return nil, fmt.Errorf("value reader: nil backing reader")
	}
	raw, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	if !leafblock.HasMagic(raw) {
		return raw, nil
	}
	return r.decodeLeafBlockEntry(ptr, nil, raw)
}

func (r valueReader) ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	raw, err := r.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	return append(dst, raw...), nil
}

func (r valueReader) ReadUnsafeAppendBatch(ptrs []page.ValuePtr, dst [][]byte) ([][]byte, error) {
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		val, err := r.ReadUnsafeAppend(ptrs[i], dst[i][:0])
		if err != nil {
			return nil, err
		}
		dst[i] = val
	}
	return dst, nil
}

func (r valueReader) ReadUnsafeForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	if r.vlogs == nil {
		return nil, fmt.Errorf("value reader: nil backing reader")
	}
	raw, err := r.vlogs.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	if !leafblock.HasMagic(raw) {
		return raw, nil
	}
	return r.decodeLeafBlockEntry(ptr, key, raw)
}

func (r valueReader) ReadForKey(ptr page.ValuePtr, key []byte) ([]byte, error) {
	raw, err := r.ReadUnsafeForKey(ptr, key)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func (r valueReader) ReadUnsafeAppendForKey(ptr page.ValuePtr, key []byte, dst []byte) ([]byte, error) {
	val, err := r.ReadUnsafeForKey(ptr, key)
	if err != nil {
		return nil, err
	}
	return append(dst, val...), nil
}

func (r valueReader) ReadUnsafeAppendBatchForKeys(ptrs []page.ValuePtr, keys [][]byte, dst [][]byte) ([][]byte, error) {
	if len(ptrs) != len(keys) {
		return nil, fmt.Errorf("value reader: ptr/key length mismatch %d/%d", len(ptrs), len(keys))
	}
	if cap(dst) < len(ptrs) {
		dst = make([][]byte, len(ptrs))
	} else {
		dst = dst[:len(ptrs)]
	}
	for i := range ptrs {
		val, err := r.ReadUnsafeAppendForKey(ptrs[i], keys[i], dst[i][:0])
		if err != nil {
			return nil, err
		}
		dst[i] = val
	}
	return dst, nil
}

func (r valueReader) decodeLeafBlockEntry(ptr page.ValuePtr, key []byte, raw []byte) ([]byte, error) {
	verify := !r.skipLeafBlockChecksums
	entry, ok, found, _, err := leafblock.DecodeEntryForKeyWithVerify(raw, key, nil, verify)
	if err != nil {
		return nil, err
	}
	if !ok {
		return raw, nil
	}
	if !found {
		return nil, fmt.Errorf("value reader: leafblock key miss ptr=%+v", ptr)
	}
	return r.resolveLookup(entry, 0)
}

func (r valueReader) resolveLookup(entry leafblock.LookupResult, depth int) ([]byte, error) {
	if depth > 8 {
		return nil, fmt.Errorf("value reader: blobref recursion too deep")
	}
	switch entry.Kind {
	case leafblock.EntryKindInline:
		return entry.Value, nil
	case leafblock.EntryKindBlobRef:
		if r.vlogs == nil {
			return nil, fmt.Errorf("value reader: nil backing reader")
		}
		raw, err := r.vlogs.ReadUnsafe(entry.BlobPtr)
		if err != nil {
			return nil, err
		}
		if manifest, ok, err := largevalue.DecodeManifest(raw); err != nil {
			return nil, err
		} else if ok {
			out := make([]byte, 0, int(manifest.TotalLen))
			for i := range manifest.Chunks {
				chunkRaw, err := r.vlogs.ReadUnsafe(manifest.Chunks[i])
				if err != nil {
					return nil, err
				}
				out = append(out, chunkRaw...)
			}
			return out, nil
		}
		if !leafblock.HasMagic(raw) {
			return raw, nil
		}
		// Nested leafblock: treat as singleton lookup.
		nested, ok, found, _, err := leafblock.DecodeEntryForKeyWithVerify(raw, nil, nil, !r.skipLeafBlockChecksums)
		if err != nil {
			return nil, err
		}
		if !ok || !found {
			return nil, fmt.Errorf("value reader: invalid nested leafblock blobref")
		}
		return r.resolveLookup(nested, depth+1)
	default:
		return nil, fmt.Errorf("value reader: unsupported leafblock kind %d", entry.Kind)
	}
}
