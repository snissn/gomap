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

