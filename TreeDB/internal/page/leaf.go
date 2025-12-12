package page

import (
	"encoding/binary"
	"fmt"
)

// LeafFlags describes how a leaf entry stores its value.
type LeafFlags uint8

const (
	LeafFlagInline    LeafFlags = 0x00
	LeafFlagPointer   LeafFlags = 0x01
	LeafFlagTombstone LeafFlags = 0x02
)

// LeafPage wraps a page buffer as a slotted leaf node.
type LeafPage struct {
	buf  []byte
	h    *Header
	body []byte
}

// InitLeafPage initializes buf as an empty leaf page.
func InitLeafPage(buf []byte, pid PageID) (*LeafPage, error) {
	h, body, err := initSlottedPage(buf, pid, PageTypeLeaf)
	if err != nil {
		return nil, err
	}
	return &LeafPage{buf: buf, h: h, body: body}, nil
}

// OpenLeafPage interprets buf as a leaf page.
func OpenLeafPage(buf []byte) (*LeafPage, error) {
	h, body, err := SplitPage(buf)
	if err != nil {
		return nil, err
	}
	if h.Flags != PageTypeLeaf {
		return nil, ErrWrongPageType
	}
	if len(body) < slotHeaderSize {
		return nil, ErrPageCorrupt
	}
	return &LeafPage{buf: buf, h: h, body: body}, nil
}

// Count returns the number of directory entries.
func (p *LeafPage) Count() int {
	return int(p.h.Count)
}

// FreeSpace returns bytes of contiguous free space.
func (p *LeafPage) FreeSpace() int {
	return freeSpace(p.body, p.Count())
}

// Search performs a binary search for key.
func (p *LeafPage) Search(key []byte) (int, bool, error) {
	return binarySearchOffsets(p.body, p.Count(), key, p.keyAt)
}

// Set inserts or updates a key in this leaf. For Pointer entries, inlineValue is ignored.
func (p *LeafPage) Set(key []byte, flags LeafFlags, inlineValue []byte, ptr ValuePtr) (bool, error) {
	if len(key) > 0xffff {
		return false, fmt.Errorf("leaf: key too large")
	}
	idx, found, err := p.Search(key)
	if err != nil {
		return false, err
	}

	newSize := leafEntrySize(len(key), flags, len(inlineValue))
	count := p.Count()

	if found {
		oldOff, _ := dirEntry(p.body, count, idx)
		_, _, _, _, oldSize, err := decodeLeafEntry(p.body, oldOff)
		if err != nil {
			return false, err
		}
		if newSize <= oldSize {
			if _, err := encodeLeafEntry(p.body, oldOff, key, flags, inlineValue, ptr); err != nil {
				return false, err
			}
			return false, nil
		}

		need := newSize
		if freeSpace(p.body, count) < need {
			if err := p.Defragment(); err != nil {
				return false, err
			}
		}
		if freeSpace(p.body, count) < need {
			return false, ErrPageFull
		}
		newOff := heapTop(p.body)
		if _, err := encodeLeafEntry(p.body, newOff, key, flags, inlineValue, ptr); err != nil {
			return false, err
		}
		setHeapTop(p.body, newOff+uint16(newSize))
		if err := setDirEntry(p.body, count, idx, newOff); err != nil {
			return false, err
		}
		return false, nil
	}

	need := newSize + 2 // entry + directory slot
	if freeSpace(p.body, count) < need {
		if err := p.Defragment(); err != nil {
			return false, err
		}
	}
	if freeSpace(p.body, count) < need {
		return false, ErrPageFull
	}
	off := heapTop(p.body)
	if _, err := encodeLeafEntry(p.body, off, key, flags, inlineValue, ptr); err != nil {
		return false, err
	}
	setHeapTop(p.body, off+uint16(newSize))
	if err := insertDirEntry(p.body, count, idx, off); err != nil {
		return false, err
	}
	p.h.Count++
	return true, nil
}

// Delete marks key as tombstoned. Returns true if key was present.
func (p *LeafPage) Delete(key []byte) (bool, error) {
	idx, found, err := p.Search(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	count := p.Count()
	off, _ := dirEntry(p.body, count, idx)
	_, _, _, _, oldSize, err := decodeLeafEntry(p.body, off)
	if err != nil {
		return false, err
	}
	tombSize := leafEntrySize(len(key), LeafFlagTombstone, 0)
	if tombSize > oldSize {
		return false, ErrPageCorrupt
	}
	_, err = encodeLeafEntry(p.body, off, key, LeafFlagTombstone, nil, ValuePtr{})
	return true, err
}

// KeyAt returns the key for directory index i.
func (p *LeafPage) KeyAt(i int) ([]byte, error) {
	off, err := dirEntry(p.body, p.Count(), i)
	if err != nil {
		return nil, err
	}
	k, err := p.keyAt(off)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), k...), nil
}

// FlagsAt returns the flags for directory index i.
func (p *LeafPage) FlagsAt(i int) (LeafFlags, error) {
	off, err := dirEntry(p.body, p.Count(), i)
	if err != nil {
		return 0, err
	}
	_, flags, _, _, _, err := decodeLeafEntry(p.body, off)
	if err != nil {
		return 0, err
	}
	return flags, nil
}

// EntryAt returns the decoded leaf entry at directory index i.
// Returned key and inline value are copies.
func (p *LeafPage) EntryAt(i int) ([]byte, LeafFlags, []byte, ValuePtr, error) {
	off, err := dirEntry(p.body, p.Count(), i)
	if err != nil {
		return nil, 0, nil, ValuePtr{}, err
	}
	key, flags, val, ptr, _, err := decodeLeafEntry(p.body, off)
	if err != nil {
		return nil, 0, nil, ValuePtr{}, err
	}
	keyOut := append([]byte(nil), key...)
	var valOut []byte
	if val != nil {
		valOut = append([]byte(nil), val...)
	}
	return keyOut, flags, valOut, ptr, nil
}

// Defragment compacts the heap and updates directory offsets.
func (p *LeafPage) Defragment() error {
	count := p.Count()
	if count == 0 {
		setHeapTop(p.body, slotHeaderSize)
		return nil
	}
	tmp := make([]byte, len(p.body))
	copy(tmp[:slotHeaderSize], p.body[:slotHeaderSize])

	newTop := uint16(slotHeaderSize)
	for i := 0; i < count; i++ {
		off, err := dirEntry(p.body, count, i)
		if err != nil {
			return err
		}
		_, _, _, _, sz, err := decodeLeafEntry(p.body, off)
		if err != nil {
			return err
		}
		copy(tmp[newTop:newTop+uint16(sz)], p.body[off:off+uint16(sz)])
		if err := setDirEntry(p.body, count, i, newTop); err != nil {
			return err
		}
		newTop += uint16(sz)
	}

	// Replace heap region.
	copy(p.body[:newTop], tmp[:newTop])
	for i := newTop; i < uint16(dirStart(p.body, count)); i++ {
		p.body[i] = 0
	}
	setHeapTop(p.body, newTop)
	return nil
}

func (p *LeafPage) keyAt(off uint16) ([]byte, error) {
	key, _, _, _, _, err := decodeLeafEntry(p.body, off)
	return key, err
}

func leafEntrySize(keyLen int, flags LeafFlags, valLen int) int {
	base := 2 + 4 + 1 + keyLen
	switch flags {
	case LeafFlagInline:
		return base + valLen
	case LeafFlagPointer:
		return base + ValuePtrSize
	case LeafFlagTombstone:
		return base
	default:
		return base
	}
}

func encodeLeafEntry(body []byte, off uint16, key []byte, flags LeafFlags, val []byte, ptr ValuePtr) (int, error) {
	size := leafEntrySize(len(key), flags, len(val))
	if int(off)+size > len(body) {
		return 0, ErrPageCorrupt
	}
	dst := body[off:]
	binary.LittleEndian.PutUint16(dst[0:2], uint16(len(key)))
	switch flags {
	case LeafFlagInline:
		binary.LittleEndian.PutUint32(dst[2:6], uint32(len(val)))
	case LeafFlagPointer:
		binary.LittleEndian.PutUint32(dst[2:6], 0)
	case LeafFlagTombstone:
		binary.LittleEndian.PutUint32(dst[2:6], 0)
	default:
		binary.LittleEndian.PutUint32(dst[2:6], uint32(len(val)))
	}
	dst[6] = byte(flags)
	copy(dst[7:7+len(key)], key)
	pos := 7 + len(key)
	switch flags {
	case LeafFlagInline:
		copy(dst[pos:pos+len(val)], val)
	case LeafFlagPointer:
		if err := ptr.EncodeLE(dst[pos : pos+ValuePtrSize]); err != nil {
			return 0, err
		}
	case LeafFlagTombstone:
		// no value bytes
	}
	return size, nil
}

func decodeLeafEntry(body []byte, off uint16) ([]byte, LeafFlags, []byte, ValuePtr, int, error) {
	if int(off)+7 > len(body) {
		return nil, 0, nil, ValuePtr{}, 0, ErrPageCorrupt
	}
	src := body[off:]
	keyLen := int(binary.LittleEndian.Uint16(src[0:2]))
	valLen := int(binary.LittleEndian.Uint32(src[2:6]))
	flags := LeafFlags(src[6])
	base := 7 + keyLen
	if base > len(src) {
		return nil, 0, nil, ValuePtr{}, 0, ErrPageCorrupt
	}
	key := src[7 : 7+keyLen]
	pos := base

	switch flags {
	case LeafFlagInline:
		if pos+valLen > len(src) {
			return nil, 0, nil, ValuePtr{}, 0, ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, ValuePtr{}, base + valLen, nil
	case LeafFlagPointer:
		if pos+ValuePtrSize > len(src) {
			return nil, 0, nil, ValuePtr{}, 0, ErrPageCorrupt
		}
		ptr, err := DecodeValuePtrLE(src[pos : pos+ValuePtrSize])
		if err != nil {
			return nil, 0, nil, ValuePtr{}, 0, err
		}
		return key, flags, nil, ptr, base + ValuePtrSize, nil
	case LeafFlagTombstone:
		return key, flags, nil, ValuePtr{}, base, nil
	default:
		if pos+valLen > len(src) {
			return nil, 0, nil, ValuePtr{}, 0, ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, ValuePtr{}, base + valLen, nil
	}
}
