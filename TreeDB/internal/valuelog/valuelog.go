package valuelog

import "errors"

const (
	Version = 1

	HeaderSize = 4 + 1 + 1 + 2 + 8 + 4
)

var (
	ErrCorrupt        = errors.New("valuelog: corrupt record")
	ErrRecordTooLarge = errors.New("valuelog: record too large")
)

type Record struct {
	RID   uint64
	Value []byte
}
