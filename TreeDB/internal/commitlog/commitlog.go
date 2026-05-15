package commitlog

import "errors"

const (
	Version = 1

	OpSetRID    = byte(0)
	OpSetInline = byte(1)
	OpDelete    = byte(2)

	segmentHeaderSize = 8
	batchHeaderSize   = 1 + 4
	recordHeaderSize  = 1 + 2 + 4 + 8 + 8
)

const (
	segmentFlagCompressed uint32 = 1 << 31
	segmentLenMask        uint32 = ^segmentFlagCompressed
)

var (
	ErrCorrupt                           = errors.New("commitlog: corrupt record")
	ErrRecordTooLarge                    = errors.New("commitlog: record too large")
	ErrMixedBatchSeq                     = errors.New("commitlog: mixed batch sequence")
	ErrCommandWALTerminalTail            = errors.New("commitlog: command wal terminal incomplete tail")
	ErrCommandWALLegacyPayload           = errors.New("commitlog: legacy raw payload in command wal")
	ErrCommandWALUnsupportedVersion      = errors.New("commitlog: command wal unsupported version")
	ErrCommandWALUnsupportedKind         = errors.New("commitlog: command wal unsupported kind")
	ErrCommandWALUnsupportedCriticalFlag = errors.New("commitlog: command wal unsupported critical flag")
	ErrCommandWALPayloadDigestMismatch   = errors.New("commitlog: command wal payload digest mismatch")
	ErrCommandWALDuplicateLSN            = errors.New("commitlog: command wal duplicate lsn")
	ErrJournalOwnerExists                = errors.New("commitlog: journal owner already exists")
)

type Record struct {
	Op    byte
	Key   []byte
	Value []byte
	RID   uint64
	Seq   uint64
}

type Options struct {
	// MaxSegmentSize bounds the total commitlog segment payload size (bytes).
	// 0 uses the default limit; values < 0 disable the cap.
	MaxSegmentSize int64

	// Compress enables best-effort zstd compression for commitlog segments.
	// Segments are only stored compressed when the compressed payload (plus a
	// small header) is smaller than the raw payload, so compression never causes
	// size amplification. Small segments are left uncompressed to avoid adding
	// hot-path CPU overhead for minimal disk savings.
	Compress bool
}
