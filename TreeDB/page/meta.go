package page

import "encoding/binary"

// MetaPageBody represents the body of the Superblock.
type MetaPageBody struct {
	CommitSeq        uint64
	UserRootPageID   uint64
	SystemRootPageID uint64
	FreelistHeadID   uint64
	TotalPages       uint64
	ActiveSlabID     uint32
	ActiveSlabTail   uint64
	LastCommitHeight uint64
	NextValueID      uint64
	LastSeq          uint64
}

const MetaPageBodySize = 76

// EncodeMetaBody encodes the MetaPageBody into the provided buffer.
func (m *MetaPageBody) Encode(buf []byte) {
	_ = buf[MetaPageBodySize-1]
	binary.LittleEndian.PutUint64(buf[0:8], m.CommitSeq)
	binary.LittleEndian.PutUint64(buf[8:16], m.UserRootPageID)
	binary.LittleEndian.PutUint64(buf[16:24], m.SystemRootPageID)
	binary.LittleEndian.PutUint64(buf[24:32], m.FreelistHeadID)
	binary.LittleEndian.PutUint64(buf[32:40], m.TotalPages)
	binary.LittleEndian.PutUint32(buf[40:44], m.ActiveSlabID)
	binary.LittleEndian.PutUint64(buf[44:52], m.ActiveSlabTail)
	binary.LittleEndian.PutUint64(buf[52:60], m.LastCommitHeight)
	binary.LittleEndian.PutUint64(buf[60:68], m.NextValueID)
	binary.LittleEndian.PutUint64(buf[68:76], m.LastSeq)
}

// DecodeMetaBody decodes the MetaPageBody from the provided buffer.
func DecodeMetaBody(buf []byte) MetaPageBody {
	_ = buf[MetaPageBodySize-1]
	return MetaPageBody{
		CommitSeq:        binary.LittleEndian.Uint64(buf[0:8]),
		UserRootPageID:   binary.LittleEndian.Uint64(buf[8:16]),
		SystemRootPageID: binary.LittleEndian.Uint64(buf[16:24]),
		FreelistHeadID:   binary.LittleEndian.Uint64(buf[24:32]),
		TotalPages:       binary.LittleEndian.Uint64(buf[32:40]),
		ActiveSlabID:     binary.LittleEndian.Uint32(buf[40:44]),
		ActiveSlabTail:   binary.LittleEndian.Uint64(buf[44:52]),
		LastCommitHeight: binary.LittleEndian.Uint64(buf[52:60]),
		NextValueID:      binary.LittleEndian.Uint64(buf[60:68]),
		LastSeq:          binary.LittleEndian.Uint64(buf[68:76]),
	}
}
