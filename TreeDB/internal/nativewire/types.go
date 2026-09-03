package nativewire

import (
	"errors"
	"fmt"
)

const (
	Magic             = "TDB1"
	ProtocolMajorV1   = uint16(1)
	ProtocolMinorV0   = uint16(0)
	FrameHeaderLenV1  = uint16(40)
	MaxFrameHeaderLen = FrameHeaderLenV1
)

type FrameType uint16

const (
	FrameHello FrameType = iota + 1
	FrameHelloOK
	FrameRequest
	FrameResponse
	FrameData
	FrameError
	FrameCancel
	FramePing
	FramePong
	FrameGoaway
)

const (
	frameRequiredFlagsMask = uint32(0x0000ffff)
)

type SectionID uint64

const (
	SectionCommandHeader     SectionID = 1
	SectionError             SectionID = 2
	SectionCapabilitySet     SectionID = 3
	SectionDeadline          SectionID = 4
	SectionTraceContext      SectionID = 5
	SectionAckPolicy         SectionID = 6
	SectionConsistencyPolicy SectionID = 7
	SectionIdempotencyKey    SectionID = 8
	SectionChecksum          SectionID = 9
	SectionCompression       SectionID = 10
	SectionResponseMeta      SectionID = 11
	SectionCursorMeta        SectionID = 12
)

const (
	CommandSpecificSectionStart SectionID = 100
	SectionFlagCritical         uint64    = 1
	knownSectionFlags                     = SectionFlagCritical
)

const (
	SectionCollectionRef          SectionID = 100
	SectionDocumentFormat         SectionID = 101
	SectionDocumentIDs            SectionID = 102
	SectionDocuments              SectionID = 103
	SectionTemplateRecords        SectionID = 104
	SectionExpectedCatalogVersion SectionID = 105
	SectionReplacementMode        SectionID = 106
	SectionCollectionMeta         SectionID = 107
	SectionIndexDefinition        SectionID = 108
	SectionIndexName              SectionID = 109
	SectionCollectionHandle       SectionID = 110
	SectionIndexValue             SectionID = 111
	SectionIndexLowerBound        SectionID = 112
	SectionIndexUpperBound        SectionID = 113
	SectionCursorRef              SectionID = 114
	SectionCursorLimits           SectionID = 115
	SectionPresenceBitmap         SectionID = 116
	SectionTruncated              SectionID = 117
	SectionUpdateFieldNames       SectionID = 121
	SectionUpdateFieldValues      SectionID = 122
	SectionVectorSearchRequest    SectionID = 123
	SectionVectorFastOptions      SectionID = 124
	SectionVectorPinOptions       SectionID = 125
	SectionVectorSearchResponse   SectionID = 126
	SectionVectorFastEvidence     SectionID = 127
	SectionVectorStatus           SectionID = 128
	SectionDenseSearchRequest     SectionID = 129
	SectionDenseSearchResponse    SectionID = 130
)

type CommandID uint64

const (
	CommandCreateCollection CommandID = 10
	CommandListCollections  CommandID = 11
	CommandCreateIndex      CommandID = 12
	CommandListIndexes      CommandID = 13
	CommandDropIndex        CommandID = 14
	CommandOpenCollection   CommandID = 15
	CommandCloseCollection  CommandID = 16
	CommandDropCollection   CommandID = 17

	CommandInsertBatch     CommandID = 30
	CommandReplaceBatch    CommandID = 31
	CommandDeleteBatch     CommandID = 32
	CommandFlushCollection CommandID = 33
	CommandFlushAll        CommandID = 34
	CommandCheckpoint      CommandID = 35
	CommandUpdateBSONSet   CommandID = 36

	CommandGetMany     CommandID = 50
	CommandIndexLookup CommandID = 51
	CommandIndexRange  CommandID = 52
	CommandOpenScan    CommandID = 53
	CommandCursorNext  CommandID = 54
	CommandCursorClose CommandID = 55
	CommandExplain     CommandID = 56
	CommandStats       CommandID = 57

	CommandVectorStatus              CommandID = 58
	CommandVectorSearchStrict        CommandID = 59
	CommandVectorSearchFast          CommandID = 60
	CommandVectorPinSearchSnapshot   CommandID = 61
	CommandVectorSearchPinned        CommandID = 62
	CommandVectorClosePinnedSnapshot CommandID = 63
	CommandDenseVectorSearch         CommandID = 64
)

type DocumentFormat uint64

const (
	DocumentFormatDefault    DocumentFormat = 0
	DocumentFormatJSON       DocumentFormat = 1
	DocumentFormatBSON       DocumentFormat = 2
	DocumentFormatTemplateV1 DocumentFormat = 3
)

type AckPolicy uint64

const (
	AckVisible       AckPolicy = 1
	AckFlushed       AckPolicy = 2
	AckSynced        AckPolicy = 3
	AckRaftCommitted AckPolicy = 4
)

type ConsistencyPolicy uint64

const (
	ConsistencyLocalStale   ConsistencyPolicy = 1
	ConsistencyLeaderRead   ConsistencyPolicy = 2
	ConsistencyLinearizable ConsistencyPolicy = 3
	ConsistencyLeaseRead    ConsistencyPolicy = 4
)

type ErrorCode uint64

const (
	ErrMalformedFrame ErrorCode = iota + 1
	ErrUnsupportedVersion
	ErrUnsupportedFeature
	ErrAuthRequired
	ErrPermissionDenied
	ErrInvalidCommand
	ErrCollectionNotFound
	ErrIndexNotFound
	ErrDuplicateDocumentID
	ErrDocumentExists
	ErrUniqueIndexConflict
	ErrCatalogVersionMismatch
	ErrReadOnly
	ErrTimeout
	ErrCanceled
	ErrResourceExhausted
	ErrInternal
	ErrDurabilityUnavailable
	ErrConsistencyUnavailable
	ErrCursorNotFound
	ErrCatalogChanged
	ErrIdempotencyConflict
	ErrCommitAmbiguous
	errCodeSentinel
)

// MaxErrorCode is the highest native-wire error code currently assigned by
// this protocol version.
const MaxErrorCode = errCodeSentinel - 1

type ProtocolError struct {
	Code   ErrorCode
	Reason string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "nativewire: <nil>"
	}
	if e.Reason == "" {
		return fmt.Sprintf("nativewire: error code %d", e.Code)
	}
	return fmt.Sprintf("nativewire: error code %d: %s", e.Code, e.Reason)
}

func protocolError(code ErrorCode, format string, args ...any) error {
	return &ProtocolError{Code: code, Reason: fmt.Sprintf(format, args...)}
}

func ErrorCodeOf(err error) (ErrorCode, bool) {
	if err == nil {
		return 0, false
	}
	var e *ProtocolError
	if errors.As(err, &e) {
		return e.Code, true
	}
	return 0, false
}
