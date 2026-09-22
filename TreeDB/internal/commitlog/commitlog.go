package commitlog

import (
	"errors"
	"fmt"
)

const (
	Version = 1
	// zeroInlineBatchVersion is a compact batch payload for all-zero inline
	// values. Readers normalize it to ordinary OpSetInline records.
	zeroInlineBatchVersion = 2
	// recordRevisionBatchVersion extends the v1 record header with an optional
	// per-record entry revision. A zero revision means "use the batch Seq".
	recordRevisionBatchVersion = 3

	OpSetRID    = byte(0)
	OpSetInline = byte(1)
	OpDelete    = byte(2)
	// OpSetInlineZero stores an inline zero-filled value without carrying the
	// value bytes. Readers normalize it back to OpSetInline.
	OpSetInlineZero = byte(3)

	segmentHeaderSize = 8
	batchHeaderSize   = 1 + 4
	recordHeaderSize  = 1 + 2 + 4 + 8 + 8
	// recordRevisionHeaderSize appends Revision after the v1 Seq field.
	recordRevisionHeaderSize = recordHeaderSize + 8

	zeroInlineBatchHeaderSize  = 1 + 4 + 8 + 4
	zeroInlineRecordHeaderSize = 2
)

const (
	segmentFlagCompressed uint32 = 1 << 31
	segmentLenMask        uint32 = ^segmentFlagCompressed
)

var (
	ErrCorrupt                                 = errors.New("commitlog: corrupt record")
	ErrRecordTooLarge                          = errors.New("commitlog: record too large")
	ErrMixedBatchSeq                           = errors.New("commitlog: mixed batch sequence")
	ErrCommandWALTerminalTail                  = errors.New("commitlog: command wal terminal incomplete tail")
	ErrCommandWALLegacyPayload                 = errors.New("commitlog: legacy raw payload in command wal")
	ErrCommandWALUnsupportedVersion            = errors.New("commitlog: command wal unsupported version")
	ErrCommandWALV1RebuildRequired             = fmt.Errorf("%w: command WAL V1 requires rebuilding this pre-alpha database for V2", ErrCommandWALUnsupportedVersion)
	ErrCommandWALV2CompressedRecordUnsupported = errors.New("commitlog: command wal V2 compressed segment record unsupported")
	ErrCommandWALV2TailIdentityUnavailable     = errors.New("commitlog: command wal V2 terminal tail identity unavailable")
	ErrCommandWALUnsupportedKind               = errors.New("commitlog: command wal unsupported kind")
	ErrCommandWALUnsupportedCriticalFlag       = errors.New("commitlog: command wal unsupported critical flag")
	ErrCommandWALUnsupportedExternalRef        = errors.New("commitlog: command wal external refs are reserved until V2 activation")
	ErrCommandWALDuplicateLSN                  = errors.New("commitlog: command wal duplicate lsn")
	ErrCommandWALStaleSegment                  = errors.New("commitlog: command wal stale segment")
	ErrCommandWALSegmentSeqExhausted           = errors.New("commitlog: command wal segment sequence exhausted")
	ErrJournalOwnerExists                      = errors.New("commitlog: journal owner already exists")
)

func isBatchPayloadVersion(version byte) bool {
	return version == Version || version == zeroInlineBatchVersion || version == recordRevisionBatchVersion
}

type Record struct {
	Op       byte
	Key      []byte
	Value    []byte
	RID      uint64
	Seq      uint64
	Revision uint64
}

type Options struct {
	// MaxSegmentSize bounds the total commitlog segment payload size (bytes).
	// 0 uses the default limit; values < 0 disable the cap.
	MaxSegmentSize int64

	// BufferSize controls the buffered writer size. Values <= 0 use the
	// commitlog default.
	BufferSize int

	// DeferredCommandBufferSize caps the internal command-frame buffer used by
	// trusted public command appends. Values <= 0 disable deferred command-frame
	// finalization.
	DeferredCommandBufferSize int

	// DeferredCommandBufferRetainSize bounds command-frame buffer capacity kept
	// after flush. Values <= 0 retain the full allocated capacity.
	DeferredCommandBufferRetainSize int

	// Compress enables best-effort zstd compression for commitlog segments.
	// Segments are only stored compressed when the compressed payload (plus a
	// small header) is smaller than the raw payload, so compression never causes
	// size amplification. Small segments are left uncompressed to avoid adding
	// hot-path CPU overhead for minimal disk savings.
	Compress bool
}
