package page

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// InternalPage wraps a page buffer as a slotted internal node.
type InternalPage struct {
	buf  []byte
	h    *Header
	body []byte
}

// InitInternalPage initializes buf as an empty internal page.
func InitInternalPage(buf []byte, pid PageID) (*InternalPage, error) {
	h, body, err := initSlottedPage(buf, pid, PageTypeInternal)
	if err != nil {
		return nil, err
	}
	return &InternalPage{buf: buf, h: h, body: body}, nil
}

// OpenInternalPage interprets buf as an internal page.
func OpenInternalPage(buf []byte) (*InternalPage, error) {
	h, body, err := SplitPage(buf)
	if err != nil {
		return nil, err
	}
	if h.Flags != PageTypeInternal {
		return nil, ErrWrongPageType
	}
	if len(body) < slotHeaderSize {
		return nil, ErrPageCorrupt
	}
	return &InternalPage{buf: buf, h: h, body: body}, nil
}

// Count returns number of directory entries.
func (p *InternalPage) Count() int {
	return int(p.h.Count)
}

// FreeSpace returns bytes of contiguous free space.
func (p *InternalPage) FreeSpace() int {
	return freeSpace(p.body, p.Count())
}

// Search performs a binary search for key.
func (p *InternalPage) Search(key []byte) (int, bool, error) {
	count := p.Count()
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		k, err := p.keyAtIndex(mid)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(k, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < count {
		k, err := p.keyAtIndex(lo)
		if err != nil {
			return 0, false, err
		}
		if bytes.Equal(k, key) {
			return lo, true, nil
		}
	}
	return lo, false, nil
}

// Set inserts a new child pointer or updates an existing one.
// Returns true if inserted.
func (p *InternalPage) Set(key []byte, child PageID) (bool, error) {
	if len(key) > 0xffff {
		return false, fmt.Errorf("internal: key too large")
	}
	idx, found, err := p.Search(key)
	if err != nil {
		return false, err
	}
	count := p.Count()
	if found {
		off, _ := dirEntry(p.body, count, idx)
		if int(off)+8 > len(p.body) {
			return false, ErrPageCorrupt
		}
		binary.LittleEndian.PutUint64(p.body[off:off+8], uint64(child))
		return false, nil
	}

	newSize := 8 + len(key)
	need := newSize + 2
	if freeSpace(p.body, count) < need {
		// Internal pages are kept packed; defrag is a rebuild.
		if err := p.Defragment(); err != nil {
			return false, err
		}
	}
	if freeSpace(p.body, count) < need {
		return false, ErrPageFull
	}

	entries, err := p.entries()
	if err != nil {
		return false, err
	}
	entries = append(entries, internalEntry{})
	copy(entries[idx+1:], entries[idx:])
	entries[idx] = internalEntry{key: append([]byte(nil), key...), child: child}
	if err := p.rebuild(entries); err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes key from this internal page.
// Returns true if present.
func (p *InternalPage) Delete(key []byte) (bool, error) {
	idx, found, err := p.Search(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	entries, err := p.entries()
	if err != nil {
		return false, err
	}
	entries = append(entries[:idx], entries[idx+1:]...)
	if err := p.rebuild(entries); err != nil {
		return false, err
	}
	return true, nil
}

// EntryAt returns the decoded internal entry at directory index i.
// Returned key is a copy.
func (p *InternalPage) EntryAt(i int) ([]byte, PageID, error) {
	count := p.Count()
	if i < 0 || i >= count {
		return nil, 0, ErrPageCorrupt
	}
	off, err := dirEntry(p.body, count, i)
	if err != nil {
		return nil, 0, err
	}
	child, key, _, err := decodeInternalEntry(p.body, off, p.entryLen(i))
	if err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), key...), child, nil
}

// ChildAt returns the child pointer at directory index i.
func (p *InternalPage) ChildAt(i int) (PageID, error) {
	count := p.Count()
	if i < 0 || i >= count {
		return 0, ErrPageCorrupt
	}
	off, err := dirEntry(p.body, count, i)
	if err != nil {
		return 0, err
	}
	if int(off)+8 > len(p.body) {
		return 0, ErrPageCorrupt
	}
	return PageID(binary.LittleEndian.Uint64(p.body[off : off+8])), nil
}

// Defragment rebuilds the heap in directory order.
func (p *InternalPage) Defragment() error {
	entries, err := p.entries()
	if err != nil {
		return err
	}
	return p.rebuild(entries)
}

type internalEntry struct {
	key   []byte
	child PageID
}

func (p *InternalPage) entries() ([]internalEntry, error) {
	count := p.Count()
	if count == 0 {
		return nil, nil
	}
	entries := make([]internalEntry, count)
	for i := 0; i < count; i++ {
		off, err := dirEntry(p.body, count, i)
		if err != nil {
			return nil, err
		}
		child, key, _, err := decodeInternalEntry(p.body, off, p.entryLen(i))
		if err != nil {
			return nil, err
		}
		entries[i] = internalEntry{key: append([]byte(nil), key...), child: child}
	}
	return entries, nil
}

// entryLen returns the length of directory entry i, assuming the heap is packed
// in directory order (offsets ascending).
func (p *InternalPage) entryLen(i int) int {
	count := p.Count()
	off, _ := dirEntry(p.body, count, i)
	var next uint16
	if i+1 < count {
		next, _ = dirEntry(p.body, count, i+1)
	} else {
		next = heapTop(p.body)
	}
	return int(next - off)
}

func (p *InternalPage) rebuild(entries []internalEntry) error {
	count := len(entries)
	p.h.Count = uint16(count)
	for i := range p.body {
		p.body[i] = 0
	}
	setHeapTop(p.body, slotHeaderSize)

	heapOff := uint16(slotHeaderSize)
	for i, e := range entries {
		need := 8 + len(e.key)
		if int(heapOff)+need > dirStart(p.body, count) {
			return ErrPageFull
		}
		encodeInternalEntry(p.body, heapOff, e.child, e.key)
		if err := setDirEntry(p.body, count, i, heapOff); err != nil {
			return err
		}
		heapOff += uint16(need)
	}
	setHeapTop(p.body, heapOff)
	return nil
}

func (p *InternalPage) keyAt(off uint16) ([]byte, error) {
	// Find directory index for off to derive length.
	count := p.Count()
	for i := 0; i < count; i++ {
		dOff, _ := dirEntry(p.body, count, i)
		if dOff == off {
			_, key, _, err := decodeInternalEntry(p.body, off, p.entryLen(i))
			return key, err
		}
	}
	return nil, ErrPageCorrupt
}

func (p *InternalPage) keyAtIndex(i int) ([]byte, error) {
	count := p.Count()
	if i < 0 || i >= count {
		return nil, ErrPageCorrupt
	}
	off, err := dirEntry(p.body, count, i)
	if err != nil {
		return nil, err
	}
	_, key, _, err := decodeInternalEntry(p.body, off, p.entryLen(i))
	return key, err
}

func encodeInternalEntry(body []byte, off uint16, child PageID, key []byte) {
	dst := body[off:]
	binary.LittleEndian.PutUint64(dst[0:8], uint64(child))
	copy(dst[8:8+len(key)], key)
}

func decodeInternalEntry(body []byte, off uint16, entryLen int) (PageID, []byte, int, error) {
	if entryLen < 8 || int(off)+entryLen > len(body) {
		return 0, nil, 0, ErrPageCorrupt
	}
	src := body[off:]
	child := PageID(binary.LittleEndian.Uint64(src[0:8]))
	key := src[8:entryLen]
	return child, key, entryLen, nil
}

// DecodeInternalEntryView decodes an internal entry and returns a key slice that
// aliases body. Callers must not retain the returned slice after unpinning the
// underlying page buffer.
func DecodeInternalEntryView(body []byte, off uint16, entryLen int) (PageID, []byte, int, error) {
	return decodeInternalEntry(body, off, entryLen)
}

// CompareChild is a helper for tests and later tree code.
func (p *InternalPage) CompareChild(i int, wantKey []byte, wantChild PageID) error {
	key, err := p.KeyAt(i)
	if err != nil {
		return err
	}
	if !bytes.Equal(key, wantKey) {
		return fmt.Errorf("internal: key mismatch")
	}
	off, _ := dirEntry(p.body, p.Count(), i)
	child := PageID(binary.LittleEndian.Uint64(p.body[off : off+8]))
	if child != wantChild {
		return fmt.Errorf("internal: child mismatch")
	}
	return nil
}

// KeyAt returns the key for directory index i.
func (p *InternalPage) KeyAt(i int) ([]byte, error) {
	count := p.Count()
	off, err := dirEntry(p.body, count, i)
	if err != nil {
		return nil, err
	}
	_, key, _, err := decodeInternalEntry(p.body, off, p.entryLen(i))
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), key...), nil
}
