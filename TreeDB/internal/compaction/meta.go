package compaction

import (
	"encoding/binary"
	"fmt"

	"treedb/internal/page"
	"treedb/internal/pager"
)

// encodeMetaPage builds a meta page buffer for pid and meta.
// This mirrors the root package commit path.
func encodeMetaPage(pid page.PageID, m pager.Meta) ([]byte, error) {
	buf := make([]byte, page.PageSize)
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return nil, err
	}
	h.PageID = pid
	h.Flags = page.PageTypeMeta
	h.Count = 0
	if len(body) < 60 {
		return nil, fmt.Errorf("compaction: meta body too small")
	}
	binary.LittleEndian.PutUint64(body[0:8], m.CommitSeq)
	binary.LittleEndian.PutUint64(body[8:16], uint64(m.UserRootPageID))
	binary.LittleEndian.PutUint64(body[16:24], uint64(m.SystemRootPageID))
	binary.LittleEndian.PutUint64(body[24:32], uint64(m.FreelistHeadID))
	binary.LittleEndian.PutUint64(body[32:40], m.TotalPages)
	binary.LittleEndian.PutUint32(body[40:44], m.ActiveSlabID)
	binary.LittleEndian.PutUint64(body[44:52], m.ActiveSlabTail)
	binary.LittleEndian.PutUint64(body[52:60], m.LastCommitHeight)
	for i := 60; i < len(body); i++ {
		body[i] = 0
	}
	h.SetBodyCRC(body)
	return buf, nil
}

