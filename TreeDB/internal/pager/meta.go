package pager

import (
	"encoding/binary"

	"treedb/internal/page"
)

// Meta mirrors the on-disk superblock body fields.
// The checksum is stored in the page header.
type Meta struct {
	CommitSeq        uint64
	UserRootPageID   page.PageID
	SystemRootPageID page.PageID
	FreelistHeadID   page.PageID
	TotalPages       uint64
	ActiveSlabID     uint32
	ActiveSlabTail   uint64
	LastCommitHeight uint64
}

const metaBodySize = 8 + 8 + 8 + 8 + 8 + 4 + 8 + 8

func encodeMetaBody(m Meta, body []byte) {
	// body must be at least metaBodySize.
	binary.LittleEndian.PutUint64(body[0:8], m.CommitSeq)
	binary.LittleEndian.PutUint64(body[8:16], uint64(m.UserRootPageID))
	binary.LittleEndian.PutUint64(body[16:24], uint64(m.SystemRootPageID))
	binary.LittleEndian.PutUint64(body[24:32], uint64(m.FreelistHeadID))
	binary.LittleEndian.PutUint64(body[32:40], m.TotalPages)
	binary.LittleEndian.PutUint32(body[40:44], m.ActiveSlabID)
	binary.LittleEndian.PutUint64(body[44:52], m.ActiveSlabTail)
	binary.LittleEndian.PutUint64(body[52:60], m.LastCommitHeight)
}

func decodeMetaBody(body []byte) Meta {
	return Meta{
		CommitSeq:        binary.LittleEndian.Uint64(body[0:8]),
		UserRootPageID:   page.PageID(binary.LittleEndian.Uint64(body[8:16])),
		SystemRootPageID: page.PageID(binary.LittleEndian.Uint64(body[16:24])),
		FreelistHeadID:   page.PageID(binary.LittleEndian.Uint64(body[24:32])),
		TotalPages:       binary.LittleEndian.Uint64(body[32:40]),
		ActiveSlabID:     binary.LittleEndian.Uint32(body[40:44]),
		ActiveSlabTail:   binary.LittleEndian.Uint64(body[44:52]),
		LastCommitHeight: binary.LittleEndian.Uint64(body[52:60]),
	}
}

