package tree

import (
	"bytes"
	"encoding/binary"
	"sort"

	"treedb/internal/page"
)

// We duplicate minimal slotted-page parsing helpers here because internal/page
// does not expose directory offsets.
const slotHeaderSize = 4

func heapTop(body []byte) uint16 {
	if len(body) < slotHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint16(body[0:2])
}

func dirStart(body []byte, count int) int {
	return len(body) - count*2
}

func dirEntry(body []byte, count int, i int) (uint16, error) {
	if i < 0 || i >= count {
		return 0, page.ErrPageCorrupt
	}
	start := dirStart(body, count)
	offPos := start + i*2
	if offPos+2 > len(body) {
		return 0, page.ErrPageCorrupt
	}
	return binary.LittleEndian.Uint16(body[offPos : offPos+2]), nil
}

type leafKV struct {
	key   []byte
	entry LeafEntry
}

func parseLeafEntries(buf []byte) ([]leafKV, error) {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return nil, err
	}
	if h.Flags != page.PageTypeLeaf {
		return nil, page.ErrWrongPageType
	}
	count := int(h.Count)
	out := make([]leafKV, 0, count)
	for i := 0; i < count; i++ {
		off, err := dirEntry(body, count, i)
		if err != nil {
			return nil, err
		}
		k, flags, inline, ptr, _, err := decodeLeafEntry(body, off)
		if err != nil {
			return nil, err
		}
		rec := leafKV{
			key: append([]byte(nil), k...),
			entry: LeafEntry{
				Flags:       flags,
				InlineValue: append([]byte(nil), inline...),
				Ptr:         ptr,
			},
		}
		out = append(out, rec)
	}
	return out, nil
}

type internalKV struct {
	key   []byte
	child page.PageID
}

func parseInternalEntries(buf []byte) ([]internalKV, error) {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return nil, err
	}
	if h.Flags != page.PageTypeInternal {
		return nil, page.ErrWrongPageType
	}
	count := int(h.Count)
	if count == 0 {
		return nil, nil
	}
	out := make([]internalKV, count)
	for i := 0; i < count; i++ {
		off, err := dirEntry(body, count, i)
		if err != nil {
			return nil, err
		}
		var next uint16
		if i+1 < count {
			next, err = dirEntry(body, count, i+1)
			if err != nil {
				return nil, err
			}
		} else {
			next = heapTop(body)
		}
		if next < off || int(next) > len(body) {
			return nil, page.ErrPageCorrupt
		}
		entryLen := int(next - off)
		child, key, _, err := decodeInternalEntry(body, off, entryLen)
		if err != nil {
			return nil, err
		}
		out[i] = internalKV{key: append([]byte(nil), key...), child: child}
	}
	return out, nil
}

func decodeLeafEntry(body []byte, off uint16) ([]byte, page.LeafFlags, []byte, page.ValuePtr, int, error) {
	if int(off)+7 > len(body) {
		return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
	}
	src := body[off:]
	keyLen := int(binary.LittleEndian.Uint16(src[0:2]))
	valLen := int(binary.LittleEndian.Uint32(src[2:6]))
	flags := page.LeafFlags(src[6])
	base := 7 + keyLen
	if base > len(src) {
		return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
	}
	key := src[7 : 7+keyLen]
	pos := base
	switch flags {
	case page.LeafFlagInline:
		if pos+valLen > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, page.ValuePtr{}, base + valLen, nil
	case page.LeafFlagPointer:
		if pos+page.ValuePtrSize > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		ptr, err := page.DecodeValuePtrLE(src[pos : pos+page.ValuePtrSize])
		if err != nil {
			return nil, 0, nil, page.ValuePtr{}, 0, err
		}
		return key, flags, nil, ptr, base + page.ValuePtrSize, nil
	case page.LeafFlagTombstone:
		return key, flags, nil, page.ValuePtr{}, base, nil
	default:
		if pos+valLen > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, page.ValuePtr{}, base + valLen, nil
	}
}

func decodeInternalEntry(body []byte, off uint16, entryLen int) (page.PageID, []byte, int, error) {
	if entryLen < 8 || int(off)+entryLen > len(body) {
		return 0, nil, 0, page.ErrPageCorrupt
	}
	src := body[off:]
	child := page.PageID(binary.LittleEndian.Uint64(src[0:8]))
	key := src[8:entryLen]
	return child, key, entryLen, nil
}

func findLeafIndex(entries []leafKV, key []byte) (int, bool) {
	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})
	if i < len(entries) && bytes.Equal(entries[i].key, key) {
		return i, true
	}
	return i, false
}

func findChildIndex(entries []internalKV, key []byte) int {
	if len(entries) == 0 {
		return 0
	}
	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) > 0
	})
	if i == 0 {
		return 0
	}
	return i - 1
}

