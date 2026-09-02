package valuelog

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	laneBits = 8
	seqBits  = 31 - laneBits

	maxLaneID     = (1 << laneBits) - 1
	maxSegmentSeq = (1 << seqBits) - 1
	// ReservedLeafLogLaneID is the dedicated lane used for outer-leaf storage.
	ReservedLeafLogLaneID = maxLaneID
)

var ErrSegmentIDRange = errors.New("valuelog: segment id out of range")

func EncodeSegmentID(lane uint32, seq uint32) (uint32, error) {
	if lane > maxLaneID || seq > maxSegmentSeq {
		return 0, ErrSegmentIDRange
	}
	return (lane << seqBits) | seq, nil
}

func DecodeSegmentID(id uint32) (lane uint32, seq uint32) {
	lane = id >> seqBits
	seq = id & maxSegmentSeq
	return lane, seq
}

func EncodeFileID(lane uint32, seq uint32) (uint32, error) {
	seg, err := EncodeSegmentID(lane, seq)
	if err != nil {
		return 0, err
	}
	return page.ValueLogFileID(seg), nil
}

func DecodeFileID(fileID uint32) (lane uint32, seq uint32) {
	seg := page.ValueLogSegmentID(fileID)
	return DecodeSegmentID(seg)
}
