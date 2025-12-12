package pager

import (
	"encoding/binary"

	"treedb/internal/page"
)

const freelistHeaderExtra = 8 // NextPageID uint64 after standard header.

func freelistCapacity() int {
	return (page.PageSize - page.HeaderSize - freelistHeaderExtra) / 8
}

type freelistPage struct {
	next  page.PageID
	ids   []page.PageID
	pageID page.PageID
}

func openFreelistPage(buf []byte) (*page.Header, []byte, page.PageID, int, error) {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if h.Flags != page.PageTypeFreelist {
		return nil, nil, 0, 0, ErrFileCorrupt
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		return nil, nil, 0, 0, err
	}
	if len(body) < freelistHeaderExtra {
		return nil, nil, 0, 0, ErrFileCorrupt
	}
	count := int(h.Count)
	if count < 0 || count > freelistCapacity() {
		return nil, nil, 0, 0, ErrFileCorrupt
	}
	next := page.PageID(binary.LittleEndian.Uint64(body[0:8]))
	return h, body, next, count, nil
}

// initFreelistPage initializes buf as a freelist page with the provided next pointer.
// Caller must ensure buf is zeroed or accept that this will zero the entire body.
func initFreelistPage(pid, next page.PageID, buf []byte) error {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	h.PageID = pid
	h.Flags = page.PageTypeFreelist
	h.Count = 0
	if len(body) < freelistHeaderExtra {
		return ErrFileCorrupt
	}
	binary.LittleEndian.PutUint64(body[0:8], uint64(next))
	for i := freelistHeaderExtra; i < len(body); i++ {
		body[i] = 0
	}
	h.SetBodyCRC(body)
	return nil
}

// popFreelistID removes and returns the last page ID from buf.
// If the page contains no IDs, ok=false and next is returned.
func popFreelistID(buf []byte) (alloc page.PageID, next page.PageID, ok bool, err error) {
	h, body, next, count, err := openFreelistPage(buf)
	if err != nil {
		return 0, 0, false, err
	}
	if count == 0 {
		return 0, next, false, nil
	}
	off := freelistHeaderExtra + (count-1)*8
	if off+8 > len(body) {
		return 0, 0, false, ErrFileCorrupt
	}
	alloc = page.PageID(binary.LittleEndian.Uint64(body[off : off+8]))
	binary.LittleEndian.PutUint64(body[off:off+8], 0)
	h.Count = uint16(count - 1)
	h.SetBodyCRC(body)
	return alloc, next, true, nil
}

// appendFreelistIDs appends as many IDs as will fit into buf and returns the count appended.
// If the page is full, it returns 0, nil error.
func appendFreelistIDs(buf []byte, ids []page.PageID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	h, body, _, count, err := openFreelistPage(buf)
	if err != nil {
		return 0, err
	}
	capacity := freelistCapacity()
	if count == capacity {
		return 0, nil
	}
	space := capacity - count
	n := space
	if n > len(ids) {
		n = len(ids)
	}
	off := freelistHeaderExtra + count*8
	if off+8*n > len(body) {
		return 0, ErrFileCorrupt
	}
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(body[off+i*8:off+(i+1)*8], uint64(ids[i]))
	}
	h.Count = uint16(count + n)
	h.SetBodyCRC(body)
	return n, nil
}

func decodeFreelistPage(pid page.PageID, buf []byte) (freelistPage, error) {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return freelistPage{}, err
	}
	if h.Flags != page.PageTypeFreelist {
		return freelistPage{}, ErrFileCorrupt
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		return freelistPage{}, err
	}
	if len(body) < freelistHeaderExtra {
		return freelistPage{}, ErrFileCorrupt
	}
	next := page.PageID(binary.LittleEndian.Uint64(body[0:8]))
	count := int(h.Count)
	if count < 0 || count > freelistCapacity() {
		return freelistPage{}, ErrFileCorrupt
	}
	ids := make([]page.PageID, count)
	off := 8
	for i := 0; i < count; i++ {
		ids[i] = page.PageID(binary.LittleEndian.Uint64(body[off : off+8]))
		off += 8
	}
	return freelistPage{next: next, ids: ids, pageID: pid}, nil
}

func encodeFreelistPage(p freelistPage, buf []byte) error {
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	h.PageID = p.pageID
	h.Flags = page.PageTypeFreelist
	h.Count = uint16(len(p.ids))
	if len(p.ids) > freelistCapacity() {
		return ErrFileCorrupt
	}
	binary.LittleEndian.PutUint64(body[0:8], uint64(p.next))
	off := 8
	for _, id := range p.ids {
		binary.LittleEndian.PutUint64(body[off:off+8], uint64(id))
		off += 8
	}
	// Zero remaining slots for determinism.
	for off+8 <= len(body) {
		binary.LittleEndian.PutUint64(body[off:off+8], 0)
		off += 8
	}
	h.SetBodyCRC(body)
	return nil
}
