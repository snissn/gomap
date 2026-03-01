package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/largevalue"
	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	vlogs                  tree.SlabReader
	skipOuterLeafChecksums bool
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return newValueReader(state.ValueLogSet, false)
}

func newValueReader(vlogs tree.SlabReader, skipOuterLeafChecksums bool) valueReader {
	return valueReader{
		vlogs:                  vlogs,
		skipOuterLeafChecksums: skipOuterLeafChecksums,
	}
}

func (r *valueReader) reconfigure(vlogs tree.SlabReader, skipOuterLeafChecksums bool, _ any, _ any) {
	if r == nil {
		return
	}
	r.vlogs = vlogs
	r.skipOuterLeafChecksums = skipOuterLeafChecksums
}

func (r *valueReader) clearForPoolReuse() {
	if r == nil {
		return
	}
	r.vlogs = nil
	r.skipOuterLeafChecksums = false
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
	if !outerleaf.HasMagic(raw) {
		return raw, nil
	}
	return r.decodeOuterLeafEntry(ptr, nil, raw)
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
	if !outerleaf.HasMagic(raw) {
		return raw, nil
	}
	return r.decodeOuterLeafEntry(ptr, key, raw)
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

func (r valueReader) decodeOuterLeafEntry(ptr page.ValuePtr, key []byte, raw []byte) ([]byte, error) {
	verify := !r.skipOuterLeafChecksums
	entry, ok, found, _, err := outerleaf.DecodeEntryForKeyWithVerify(raw, key, nil, verify)
	if err != nil {
		return nil, err
	}
	if !ok {
		return raw, nil
	}
	if !found {
		return nil, fmt.Errorf("value reader: outerleaf key miss ptr=%+v", ptr)
	}
	return r.resolveLookup(entry, 0)
}

func (r valueReader) resolveLookup(entry outerleaf.LookupResult, depth int) ([]byte, error) {
	if depth > 8 {
		return nil, fmt.Errorf("value reader: blobref recursion too deep")
	}
	switch entry.Kind {
	case outerleaf.EntryKindInline:
		return entry.Value, nil
	case outerleaf.EntryKindBlobRef:
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
		if !outerleaf.HasMagic(raw) {
			return raw, nil
		}
		// Nested outerleaf: treat as singleton lookup.
		nested, ok, found, _, err := outerleaf.DecodeEntryForKeyWithVerify(raw, nil, nil, !r.skipOuterLeafChecksums)
		if err != nil {
			return nil, err
		}
		if !ok || !found {
			return nil, fmt.Errorf("value reader: invalid nested outerleaf blobref")
		}
		return r.resolveLookup(nested, depth+1)
	default:
		return nil, fmt.Errorf("value reader: unsupported outerleaf kind %d", entry.Kind)
	}
}
