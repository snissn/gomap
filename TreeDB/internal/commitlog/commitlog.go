package commitlog

import "errors"

const (
	Version = 1

	OpSetRID    = byte(0)
	OpSetInline = byte(1)
	OpDelete    = byte(2)

	segmentHeaderSize = 8
	batchHeaderSize   = 1 + 4
	recordHeaderSize  = 1 + 2 + 4 + 8
)

var (
	ErrCorrupt        = errors.New("commitlog: corrupt record")
	ErrRecordTooLarge = errors.New("commitlog: record too large")
)

type Record struct {
	Op    byte
	Key   []byte
	Value []byte
	RID   uint64
}

type Options struct {
	// MaxSegmentSize bounds the total commitlog segment payload size (bytes).
	// 0 uses the default limit; values < 0 disable the cap.
	MaxSegmentSize int64
}
