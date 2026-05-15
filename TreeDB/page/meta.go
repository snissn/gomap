package page

import (
	"bytes"
	"encoding/binary"
)

// MetaPageBody represents the body of the Superblock.
type MetaPageBody struct {
	CommitSeq         uint64
	UserRootPageID    uint64
	SystemRootPageID  uint64
	FreelistHeadID    uint64
	TotalPages        uint64
	ActiveSlabID      uint32
	ActiveSlabTail    uint64
	LastCommitHeight  uint64
	AppliedCommandLSN uint64
}

const (
	MetaPageBodySizeLegacy       = 60
	MetaPageBodySizeCommandWALV1 = 76
	MetaPageBodySize             = MetaPageBodySizeCommandWALV1
)

var metaCommandWALV1Magic = [8]byte{'T', 'M', 'E', 'T', 'A', 'W', '1', 0}

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
	copy(buf[60:68], metaCommandWALV1Magic[:])
	binary.LittleEndian.PutUint64(buf[68:76], m.AppliedCommandLSN)
}

// DecodeMetaBody decodes the legacy-safe MetaPageBody fields from the provided
// buffer. The command-WAL AppliedCommandLSN extension requires an explicit
// in-page marker and is decoded by DecodeMetaBodyCommandWALV1.
func DecodeMetaBody(buf []byte) MetaPageBody {
	_ = buf[MetaPageBodySizeLegacy-1]
	return MetaPageBody{
		CommitSeq:        binary.LittleEndian.Uint64(buf[0:8]),
		UserRootPageID:   binary.LittleEndian.Uint64(buf[8:16]),
		SystemRootPageID: binary.LittleEndian.Uint64(buf[16:24]),
		FreelistHeadID:   binary.LittleEndian.Uint64(buf[24:32]),
		TotalPages:       binary.LittleEndian.Uint64(buf[32:40]),
		ActiveSlabID:     binary.LittleEndian.Uint32(buf[40:44]),
		ActiveSlabTail:   binary.LittleEndian.Uint64(buf[44:52]),
		LastCommitHeight: binary.LittleEndian.Uint64(buf[52:60]),
	}
}

// DecodeMetaBodyCommandWALV1 decodes the command-WAL V1 meta extension only
// when the in-page V1 marker is present. Legacy pages and unmarked buffers
// decode as AppliedCommandLSN=0.
func DecodeMetaBodyCommandWALV1(buf []byte) MetaPageBody {
	m := DecodeMetaBody(buf)
	if len(buf) >= MetaPageBodySizeCommandWALV1 && bytes.Equal(buf[60:68], metaCommandWALV1Magic[:]) {
		m.AppliedCommandLSN = binary.LittleEndian.Uint64(buf[68:76])
	}
	return m
}
