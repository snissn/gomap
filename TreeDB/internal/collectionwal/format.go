package collectionwal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
)

const (
	segmentFormatVersion = uint16(1)
	frameFormatVersion   = uint16(1)
	minReaderVersionV1   = uint16(1)

	SegmentHeaderLen          = 64
	FrameHeaderLen            = 96
	CommitTrailerLen          = 16
	TransactionFixedHeaderLen = 288
	SectionTableEntryLen      = 32
)

var (
	segmentMagic        = [8]byte{'T', 'D', 'B', 'C', 'W', 'A', 'L', 0x01}
	frameMagic          = [8]byte{'T', 'D', 'B', 'C', 'W', 'T', 'X', 0x01}
	commitTrailerMagic  = [8]byte{'T', 'D', 'B', 'C', 'W', 'C', 'M', 0x01}
	payloadMagic        = [8]byte{'T', 'D', 'B', 'C', 'W', 'P', '1', 0x01}
	collectionWALCRC32C = crc32.MakeTable(crc32.Castagnoli)
)

const (
	fileClassCollectionWAL = uint32(1)
)

type RecordType uint16

const (
	RecordTypeTransaction     RecordType = 1
	RecordTypeSegmentMetadata RecordType = 2
	RecordTypeCleanupRecord   RecordType = 3
)

const (
	FrameFlagCompressed            uint32 = 1 << 0
	FrameFlagCommitTrailerRequired uint32 = 1 << 1
)

type CompressionCodec uint16

const (
	CompressionNone CompressionCodec = 0
	CompressionZstd CompressionCodec = 1
)

const (
	SectionTableVersionV1 = uint16(1)
)

type SectionType uint16

const (
	SectionTypeRootDeltaTable      SectionType = 1
	SectionTypeSideRefTable        SectionType = 2
	SectionTypeSystemDeltaTemplate SectionType = 3
	SectionTypeDescriptorOps       SectionType = 4
	SectionTypeStats               SectionType = 5
	SectionTypeUnknownPreserved    SectionType = 6
)

const (
	SectionFlagCritical       uint32 = 1 << 0
	SectionFlagReplayCritical uint32 = 1 << 1
)

const RequiredFeatureCollectionWALV1 uint32 = 1 << 0

type SegmentHeader struct {
	Lane        uint32
	SegmentSeq  uint64
	FirstWALLSN uint64
	LastWALLSN  uint64
}

type FrameHeader struct {
	RecordType             RecordType
	FrameFlags             uint32
	CompressionCodec       CompressionCodec
	SectionTableVersion    uint16
	WALLSN                 uint64
	CollectionUID          [CollectionUIDBytes]byte
	CollectionSeq          uint64
	StoredPayloadLen       uint64
	RawPayloadLen          uint64
	StoredPayloadCRC32C    uint32
	ReplayDigestCRC32C     uint32
	RequiredFeatureBitsLow uint32
}

type Section struct {
	Type    SectionType
	Version uint16
	Flags   uint32
	Data    []byte
}

func (s Section) Critical() bool {
	return s.Flags&SectionFlagCritical != 0
}

func (s Section) ReplayCritical() bool {
	return s.Flags&SectionFlagReplayCritical != 0 || sectionTypeReplayCritical(s.Type)
}

type Transaction struct {
	WALLSN                   uint64
	CollectionUID            [CollectionUIDBytes]byte
	CollectionGeneration     uint64
	CollectionSeq            uint64
	DependsOnCollectionSeq   uint64
	CatalogEpoch             uint64
	SchemaEpoch              uint64
	SchemaVersion            uint64
	BaseCommitSeq            uint64
	BaseSystemRootID         uint64
	BaseCatalogDigest        [32]byte
	CatalogDigest            [32]byte
	LogicalCatalogDigest     [32]byte
	LocalReplayCatalogDigest [32]byte
	MutationClass            uint32
	RootDeltaCount           uint32
	SideRefCount             uint32
	DescriptorOpCount        uint32
	TransactionFlags         uint32
	Sections                 []Section
}

type FrameOutcome string

const (
	OutcomeCompleteValid          FrameOutcome = "CompleteValid"
	OutcomeCompleteCorrupt        FrameOutcome = "CompleteCorrupt"
	OutcomeTerminalIncompleteTail FrameOutcome = "TerminalIncompleteTail"
	OutcomeNonTerminalShortRead   FrameOutcome = "NonTerminalShortRead"
	OutcomeUnsupportedVersion     FrameOutcome = "UnsupportedVersion"
	OutcomeUnsupportedSkippable   FrameOutcome = "UnsupportedSkippableRecord"
	OutcomeMissingSegment         FrameOutcome = "MissingSegment"
	OutcomeDuplicateWALLSN        FrameOutcome = "DuplicateWALLSN"
	OutcomeDuplicateCollectionSeq FrameOutcome = "DuplicateCollectionSeq"
	OutcomeMaliciousLength        FrameOutcome = "MaliciousLength"
	OutcomeMixedVersionSegment    FrameOutcome = "MixedVersionSegment"
)

type FrameScanResult struct {
	Outcome     FrameOutcome
	Offset      int64
	Length      int64
	Header      FrameHeader
	Transaction Transaction
	Err         error
}

func EncodeSegmentHeader(h SegmentHeader) []byte {
	buf := make([]byte, SegmentHeaderLen)
	copy(buf[0:8], segmentMagic[:])
	binary.LittleEndian.PutUint16(buf[8:10], SegmentHeaderLen)
	binary.LittleEndian.PutUint16(buf[10:12], segmentFormatVersion)
	binary.LittleEndian.PutUint16(buf[12:14], minReaderVersionV1)
	// SegmentFlags at [14:16] are currently zero.
	binary.LittleEndian.PutUint32(buf[20:24], fileClassCollectionWAL)
	binary.LittleEndian.PutUint32(buf[24:28], h.Lane)
	binary.LittleEndian.PutUint64(buf[28:36], h.SegmentSeq)
	binary.LittleEndian.PutUint64(buf[36:44], h.FirstWALLSN)
	binary.LittleEndian.PutUint64(buf[44:52], h.LastWALLSN)
	binary.LittleEndian.PutUint32(buf[16:20], crc32cWithZero(buf, 16, 20))
	return buf
}

func DecodeSegmentHeader(buf []byte) (SegmentHeader, error) {
	var h SegmentHeader
	if len(buf) < SegmentHeaderLen {
		return h, fmt.Errorf("%w: segment header short read", ErrCollectionWALTerminalTail)
	}
	if !bytes.Equal(buf[0:8], segmentMagic[:]) {
		return h, fmt.Errorf("%w: bad segment magic", ErrCollectionWALCorruptMiddle)
	}
	if got := binary.LittleEndian.Uint16(buf[8:10]); got != SegmentHeaderLen {
		return h, fmt.Errorf("%w: segment header len %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if got := binary.LittleEndian.Uint16(buf[10:12]); got != segmentFormatVersion {
		return h, fmt.Errorf("%w: segment format version %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if got := binary.LittleEndian.Uint16(buf[12:14]); got > minReaderVersionV1 {
		return h, fmt.Errorf("%w: segment min reader version %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if flags := binary.LittleEndian.Uint16(buf[14:16]); flags != 0 {
		return h, fmt.Errorf("%w: nonzero segment flags %d", ErrCollectionWALUnsupportedVersion, flags)
	}
	if got, want := binary.LittleEndian.Uint32(buf[16:20]), crc32cWithZero(buf[:SegmentHeaderLen], 16, 20); got != want {
		return h, fmt.Errorf("%w: segment header crc got %08x want %08x", ErrCollectionWALBadChecksum, got, want)
	}
	if got := binary.LittleEndian.Uint32(buf[20:24]); got != fileClassCollectionWAL {
		return h, fmt.Errorf("%w: segment file class %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if !allZero(buf[52:64]) {
		return h, fmt.Errorf("%w: nonzero segment header reserved bytes", ErrCollectionWALUnsupportedVersion)
	}
	h.Lane = binary.LittleEndian.Uint32(buf[24:28])
	h.SegmentSeq = binary.LittleEndian.Uint64(buf[28:36])
	h.FirstWALLSN = binary.LittleEndian.Uint64(buf[36:44])
	h.LastWALLSN = binary.LittleEndian.Uint64(buf[44:52])
	return h, nil
}

func EncodeTransactionPayload(txn Transaction) ([]byte, error) {
	if err := validateTransactionForEncode(txn); err != nil {
		return nil, err
	}
	sectionCount := len(txn.Sections)
	sectionTableLen := sectionCount * SectionTableEntryLen
	if sectionCount > math.MaxUint32 || sectionTableLen/SectionTableEntryLen != sectionCount {
		return nil, fmt.Errorf("%w: section table overflow", ErrCollectionWALResourceLimit)
	}
	payloadLen := TransactionFixedHeaderLen + sectionTableLen
	for _, section := range txn.Sections {
		if len(section.Data) > math.MaxInt-payloadLen {
			return nil, fmt.Errorf("%w: payload length overflow", ErrCollectionWALResourceLimit)
		}
		payloadLen += len(section.Data)
	}
	if err := ValidateEncodedTransactionSize(uint64(payloadLen)); err != nil {
		return nil, err
	}

	buf := make([]byte, payloadLen)
	copy(buf[0:8], payloadMagic[:])
	binary.LittleEndian.PutUint16(buf[8:10], segmentFormatVersion)
	binary.LittleEndian.PutUint16(buf[10:12], TransactionFixedHeaderLen)
	binary.LittleEndian.PutUint32(buf[12:16], txn.TransactionFlags)
	copy(buf[16:32], txn.CollectionUID[:])
	binary.LittleEndian.PutUint64(buf[32:40], txn.CollectionGeneration)
	binary.LittleEndian.PutUint64(buf[40:48], txn.CollectionSeq)
	binary.LittleEndian.PutUint64(buf[48:56], txn.DependsOnCollectionSeq)
	binary.LittleEndian.PutUint64(buf[56:64], txn.CatalogEpoch)
	binary.LittleEndian.PutUint64(buf[64:72], txn.SchemaEpoch)
	binary.LittleEndian.PutUint64(buf[72:80], txn.SchemaVersion)
	binary.LittleEndian.PutUint64(buf[80:88], txn.BaseCommitSeq)
	binary.LittleEndian.PutUint64(buf[88:96], txn.BaseSystemRootID)
	copy(buf[96:128], txn.BaseCatalogDigest[:])
	copy(buf[128:160], txn.CatalogDigest[:])
	copy(buf[160:192], txn.LogicalCatalogDigest[:])
	copy(buf[192:224], txn.LocalReplayCatalogDigest[:])
	binary.LittleEndian.PutUint32(buf[224:228], txn.MutationClass)
	binary.LittleEndian.PutUint32(buf[228:232], txn.RootDeltaCount)
	binary.LittleEndian.PutUint32(buf[232:236], txn.SideRefCount)
	binary.LittleEndian.PutUint32(buf[236:240], txn.DescriptorOpCount)
	binary.LittleEndian.PutUint32(buf[240:244], uint32(sectionCount))
	binary.LittleEndian.PutUint32(buf[244:248], crc32cWithZero(buf[:TransactionFixedHeaderLen], 244, 248))

	dataOffset := TransactionFixedHeaderLen + sectionTableLen
	for i, section := range txn.Sections {
		entryOffset := TransactionFixedHeaderLen + i*SectionTableEntryLen
		binary.LittleEndian.PutUint16(buf[entryOffset:entryOffset+2], uint16(section.Type))
		binary.LittleEndian.PutUint16(buf[entryOffset+2:entryOffset+4], section.Version)
		binary.LittleEndian.PutUint32(buf[entryOffset+4:entryOffset+8], section.Flags)
		binary.LittleEndian.PutUint64(buf[entryOffset+8:entryOffset+16], uint64(dataOffset))
		binary.LittleEndian.PutUint64(buf[entryOffset+16:entryOffset+24], uint64(len(section.Data)))
		binary.LittleEndian.PutUint32(buf[entryOffset+24:entryOffset+28], crc32c(section.Data))
		copy(buf[dataOffset:dataOffset+len(section.Data)], section.Data)
		dataOffset += len(section.Data)
	}
	return buf, nil
}

func DecodeTransactionPayload(payload []byte) (Transaction, error) {
	return decodeTransactionPayload(payload, 0)
}

func EncodeTransactionFrame(txn Transaction) ([]byte, error) {
	payload, err := EncodeTransactionPayload(txn)
	if err != nil {
		return nil, err
	}
	if err := ValidateFramePayloadSize(uint64(len(payload))); err != nil {
		return nil, err
	}
	replayDigestCRC, err := ReplayDigestCRC32C(txn)
	if err != nil {
		return nil, err
	}

	header := make([]byte, FrameHeaderLen)
	copy(header[0:8], frameMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], FrameHeaderLen)
	binary.LittleEndian.PutUint16(header[10:12], frameFormatVersion)
	binary.LittleEndian.PutUint16(header[12:14], minReaderVersionV1)
	binary.LittleEndian.PutUint16(header[14:16], uint16(RecordTypeTransaction))
	binary.LittleEndian.PutUint32(header[16:20], FrameFlagCommitTrailerRequired)
	binary.LittleEndian.PutUint16(header[20:22], uint16(CompressionNone))
	binary.LittleEndian.PutUint16(header[22:24], SectionTableVersionV1)
	binary.LittleEndian.PutUint64(header[24:32], txn.WALLSN)
	copy(header[32:48], txn.CollectionUID[:])
	binary.LittleEndian.PutUint64(header[48:56], txn.CollectionSeq)
	binary.LittleEndian.PutUint64(header[56:64], uint64(len(payload)))
	binary.LittleEndian.PutUint64(header[64:72], uint64(len(payload)))
	binary.LittleEndian.PutUint32(header[76:80], crc32c(payload))
	binary.LittleEndian.PutUint32(header[80:84], replayDigestCRC)
	binary.LittleEndian.PutUint32(header[84:88], RequiredFeatureCollectionWALV1)
	binary.LittleEndian.PutUint32(header[72:76], crc32cWithZero(header, 72, 76))

	trailer := make([]byte, CommitTrailerLen)
	copy(trailer[0:8], commitTrailerMagic[:])
	binary.LittleEndian.PutUint32(trailer[8:12], CommitTrailerLen)
	wholeCRC := crc32.New(collectionWALCRC32C)
	_, _ = wholeCRC.Write(header)
	_, _ = wholeCRC.Write(payload)
	_, _ = wholeCRC.Write(trailer[:12])
	binary.LittleEndian.PutUint32(trailer[12:16], wholeCRC.Sum32())

	frame := make([]byte, 0, len(header)+len(payload)+len(trailer))
	frame = append(frame, header...)
	frame = append(frame, payload...)
	frame = append(frame, trailer...)
	return frame, nil
}

func DecodeTransactionFrame(frame []byte) (Transaction, error) {
	seenWALLSN := make(map[uint64]struct{})
	seenCollectionSeq := make(map[collectionSeqKey]struct{})
	result, next := scanFrame(frame, 0, true, seenWALLSN, seenCollectionSeq)
	if result.Outcome != OutcomeCompleteValid {
		if result.Err != nil {
			return Transaction{}, result.Err
		}
		return Transaction{}, fmt.Errorf("%w: frame outcome %s", ErrCollectionWALCorruptMiddle, result.Outcome)
	}
	if next != len(frame) {
		return Transaction{}, fmt.Errorf("%w: trailing bytes after transaction frame", ErrCollectionWALCorruptMiddle)
	}
	return result.Transaction, nil
}

func ScanSegment(segment []byte, terminalActiveSegment bool) (SegmentHeader, []FrameScanResult, error) {
	header, err := DecodeSegmentHeader(segment)
	if err != nil {
		return SegmentHeader{}, nil, err
	}
	var results []FrameScanResult
	seenWALLSN := make(map[uint64]struct{})
	seenCollectionSeq := make(map[collectionSeqKey]struct{})
	for offset := SegmentHeaderLen; offset < len(segment); {
		result, next := scanFrame(segment, offset, terminalActiveSegment, seenWALLSN, seenCollectionSeq)
		results = append(results, result)
		if result.Outcome != OutcomeCompleteValid && result.Outcome != OutcomeUnsupportedSkippable {
			return header, results, nil
		}
		if next <= offset {
			return header, results, fmt.Errorf("%w: scanner made no progress", ErrCollectionWALCorruptMiddle)
		}
		offset = next
	}
	return header, results, nil
}

func ReplayDigest(txn Transaction) ([32]byte, error) {
	if err := validateTransactionForReplayDigest(txn); err != nil {
		return [32]byte{}, err
	}
	var b []byte
	b = appendUint64(b, txn.WALLSN)
	b = append(b, txn.CollectionUID[:]...)
	b = appendUint64(b, txn.CollectionGeneration)
	b = appendUint64(b, txn.CollectionSeq)
	b = appendUint64(b, txn.DependsOnCollectionSeq)
	b = appendUint64(b, txn.CatalogEpoch)
	b = appendUint64(b, txn.SchemaEpoch)
	b = appendUint64(b, txn.SchemaVersion)
	b = appendUint64(b, txn.BaseCommitSeq)
	b = appendUint64(b, txn.BaseSystemRootID)
	b = append(b, txn.BaseCatalogDigest[:]...)
	b = append(b, txn.CatalogDigest[:]...)
	b = append(b, txn.LogicalCatalogDigest[:]...)
	b = append(b, txn.LocalReplayCatalogDigest[:]...)
	b = appendUint32(b, txn.MutationClass)
	b = appendUint32(b, txn.RootDeltaCount)
	b = appendUint32(b, txn.SideRefCount)
	b = appendUint32(b, txn.DescriptorOpCount)
	for _, section := range txn.Sections {
		if !section.ReplayCritical() {
			continue
		}
		b = appendUint16(b, uint16(section.Type))
		b = appendUint16(b, section.Version)
		b = appendUint32(b, section.Flags)
		b = appendUint64(b, uint64(len(section.Data)))
		b = append(b, section.Data...)
	}
	return sha256.Sum256(b), nil
}

func ReplayDigestCRC32C(txn Transaction) (uint32, error) {
	digest, err := ReplayDigest(txn)
	if err != nil {
		return 0, err
	}
	return crc32c(digest[:]), nil
}

func decodeTransactionPayload(payload []byte, wallsn uint64) (Transaction, error) {
	var txn Transaction
	if err := ValidateEncodedTransactionSize(uint64(len(payload))); err != nil {
		return txn, err
	}
	if len(payload) < TransactionFixedHeaderLen {
		return txn, fmt.Errorf("%w: transaction payload shorter than fixed header", ErrCollectionWALCorruptMiddle)
	}
	if !bytes.Equal(payload[0:8], payloadMagic[:]) {
		return txn, fmt.Errorf("%w: bad transaction payload magic", ErrCollectionWALCorruptMiddle)
	}
	if got := binary.LittleEndian.Uint16(payload[8:10]); got != segmentFormatVersion {
		return txn, fmt.Errorf("%w: transaction version %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if got := binary.LittleEndian.Uint16(payload[10:12]); got != TransactionFixedHeaderLen {
		return txn, fmt.Errorf("%w: transaction fixed header len %d", ErrCollectionWALUnsupportedVersion, got)
	}
	if got, want := binary.LittleEndian.Uint32(payload[244:248]), crc32cWithZero(payload[:TransactionFixedHeaderLen], 244, 248); got != want {
		return txn, fmt.Errorf("%w: transaction fixed header crc got %08x want %08x", ErrCollectionWALBadChecksum, got, want)
	}
	if !allZero(payload[248:288]) {
		return txn, fmt.Errorf("%w: nonzero transaction reserved bytes", ErrCollectionWALUnsupportedVersion)
	}

	txn.WALLSN = wallsn
	copy(txn.CollectionUID[:], payload[16:32])
	txn.CollectionGeneration = binary.LittleEndian.Uint64(payload[32:40])
	txn.CollectionSeq = binary.LittleEndian.Uint64(payload[40:48])
	txn.DependsOnCollectionSeq = binary.LittleEndian.Uint64(payload[48:56])
	txn.CatalogEpoch = binary.LittleEndian.Uint64(payload[56:64])
	txn.SchemaEpoch = binary.LittleEndian.Uint64(payload[64:72])
	txn.SchemaVersion = binary.LittleEndian.Uint64(payload[72:80])
	txn.BaseCommitSeq = binary.LittleEndian.Uint64(payload[80:88])
	txn.BaseSystemRootID = binary.LittleEndian.Uint64(payload[88:96])
	copy(txn.BaseCatalogDigest[:], payload[96:128])
	copy(txn.CatalogDigest[:], payload[128:160])
	copy(txn.LogicalCatalogDigest[:], payload[160:192])
	copy(txn.LocalReplayCatalogDigest[:], payload[192:224])
	txn.MutationClass = binary.LittleEndian.Uint32(payload[224:228])
	txn.RootDeltaCount = binary.LittleEndian.Uint32(payload[228:232])
	txn.SideRefCount = binary.LittleEndian.Uint32(payload[232:236])
	txn.DescriptorOpCount = binary.LittleEndian.Uint32(payload[236:240])
	txn.TransactionFlags = binary.LittleEndian.Uint32(payload[12:16])
	sectionCount := binary.LittleEndian.Uint32(payload[240:244])
	sectionTableLen := uint64(sectionCount) * SectionTableEntryLen
	dataStart := uint64(TransactionFixedHeaderLen) + sectionTableLen
	if dataStart > uint64(len(payload)) || sectionTableLen/SectionTableEntryLen != uint64(sectionCount) {
		return txn, fmt.Errorf("%w: section table exceeds payload", ErrCollectionWALResourceLimit)
	}
	txn.Sections = make([]Section, 0, sectionCount)
	ranges := make([]sectionRange, 0, sectionCount)
	for i := uint32(0); i < sectionCount; i++ {
		entryOffset := TransactionFixedHeaderLen + int(i)*SectionTableEntryLen
		section := Section{
			Type:    SectionType(binary.LittleEndian.Uint16(payload[entryOffset : entryOffset+2])),
			Version: binary.LittleEndian.Uint16(payload[entryOffset+2 : entryOffset+4]),
			Flags:   binary.LittleEndian.Uint32(payload[entryOffset+4 : entryOffset+8]),
		}
		offset := binary.LittleEndian.Uint64(payload[entryOffset+8 : entryOffset+16])
		length := binary.LittleEndian.Uint64(payload[entryOffset+16 : entryOffset+24])
		wantCRC := binary.LittleEndian.Uint32(payload[entryOffset+24 : entryOffset+28])
		if !allZero(payload[entryOffset+28 : entryOffset+32]) {
			return txn, fmt.Errorf("%w: nonzero section reserved bytes", ErrCollectionWALUnsupportedVersion)
		}
		if err := validateSectionMetadata(section); err != nil {
			return txn, err
		}
		if offset < dataStart || length > uint64(len(payload)) || offset > uint64(len(payload))-length {
			return txn, fmt.Errorf("%w: section %d range [%d,%d) outside payload", ErrCollectionWALCorruptMiddle, i, offset, offset+length)
		}
		sectionBytes := payload[offset : offset+length]
		if got := crc32c(sectionBytes); got != wantCRC {
			return txn, fmt.Errorf("%w: section %d crc got %08x want %08x", ErrCollectionWALBadChecksum, i, got, wantCRC)
		}
		section.Data = append([]byte(nil), sectionBytes...)
		txn.Sections = append(txn.Sections, section)
		ranges = append(ranges, sectionRange{start: offset, end: offset + length})
	}
	if err := validateSectionRanges(ranges); err != nil {
		return txn, err
	}
	if err := validateDecodedTransaction(txn, uint64(len(payload))); err != nil {
		return txn, err
	}
	return txn, nil
}

func scanFrame(data []byte, offset int, terminalActiveSegment bool, seenWALLSN map[uint64]struct{}, seenCollectionSeq map[collectionSeqKey]struct{}) (FrameScanResult, int) {
	result := FrameScanResult{Offset: int64(offset)}
	remaining := len(data) - offset
	if remaining < FrameHeaderLen {
		result.Length = int64(remaining)
		result.Outcome, result.Err = shortReadOutcome(terminalActiveSegment, "frame header short read")
		return result, len(data)
	}
	headerBytes := data[offset : offset+FrameHeaderLen]
	if !bytes.Equal(headerBytes[0:8], frameMagic[:]) {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeCompleteCorrupt
		result.Err = fmt.Errorf("%w: bad frame magic", ErrCollectionWALCorruptMiddle)
		return result, len(data)
	}
	if got := binary.LittleEndian.Uint16(headerBytes[8:10]); got != FrameHeaderLen {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeUnsupportedVersion
		result.Err = fmt.Errorf("%w: frame header len %d", ErrCollectionWALUnsupportedVersion, got)
		return result, len(data)
	}
	if got := binary.LittleEndian.Uint16(headerBytes[10:12]); got != frameFormatVersion {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeUnsupportedVersion
		result.Err = fmt.Errorf("%w: frame format version %d", ErrCollectionWALUnsupportedVersion, got)
		return result, len(data)
	}
	if got := binary.LittleEndian.Uint16(headerBytes[12:14]); got > minReaderVersionV1 {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeUnsupportedVersion
		result.Err = fmt.Errorf("%w: frame min reader version %d", ErrCollectionWALUnsupportedVersion, got)
		return result, len(data)
	}
	header, err := parseFrameHeader(headerBytes)
	result.Header = header
	if err != nil {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = outcomeForFrameError(err)
		result.Err = err
		return result, len(data)
	}
	storedLen := header.StoredPayloadLen
	rawLen := header.RawPayloadLen
	if err := ValidateFramePayloadSize(storedLen); err != nil {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeMaliciousLength
		result.Err = err
		return result, len(data)
	}
	if err := ValidateEncodedTransactionSize(rawLen); err != nil {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeMaliciousLength
		result.Err = err
		return result, len(data)
	}
	if storedLen > uint64(math.MaxInt-FrameHeaderLen-CommitTrailerLen) {
		result.Length = int64(FrameHeaderLen)
		result.Outcome = OutcomeMaliciousLength
		result.Err = fmt.Errorf("%w: frame length overflows int", ErrCollectionWALResourceLimit)
		return result, len(data)
	}
	frameLen := FrameHeaderLen + int(storedLen) + CommitTrailerLen
	result.Length = int64(frameLen)
	if remaining < frameLen {
		result.Length = int64(remaining)
		result.Outcome, result.Err = shortReadOutcome(terminalActiveSegment, "frame payload or commit trailer short read")
		return result, len(data)
	}
	payload := data[offset+FrameHeaderLen : offset+FrameHeaderLen+int(storedLen)]
	trailer := data[offset+FrameHeaderLen+int(storedLen) : offset+frameLen]
	if got := crc32c(payload); got != header.StoredPayloadCRC32C {
		result.Outcome = OutcomeCompleteCorrupt
		result.Err = fmt.Errorf("%w: payload crc got %08x want %08x", ErrCollectionWALBadChecksum, got, header.StoredPayloadCRC32C)
		return result, offset + frameLen
	}
	if err := validateCommitTrailer(headerBytes, payload, trailer); err != nil {
		result.Outcome = OutcomeCompleteCorrupt
		result.Err = err
		return result, offset + frameLen
	}
	if header.RecordType != RecordTypeTransaction {
		switch header.RecordType {
		case RecordTypeSegmentMetadata, RecordTypeCleanupRecord:
		default:
			result.Outcome = OutcomeUnsupportedVersion
			result.Err = fmt.Errorf("%w: unknown record type %d", ErrCollectionWALUnsupportedVersion, header.RecordType)
			return result, offset + frameLen
		}
		result.Outcome = OutcomeUnsupportedSkippable
		result.Err = fmt.Errorf("%w: unsupported skippable record type %d", ErrCollectionWALUnsupportedVersion, header.RecordType)
		return result, offset + frameLen
	}
	if header.RequiredFeatureBitsLow&RequiredFeatureCollectionWALV1 == 0 {
		result.Outcome = OutcomeUnsupportedVersion
		result.Err = fmt.Errorf("%w: transaction frame missing collection_wal_v1 feature bit", ErrCollectionWALUnsupportedVersion)
		return result, offset + frameLen
	}
	if header.CompressionCodec != CompressionNone || header.FrameFlags&FrameFlagCompressed != 0 || header.StoredPayloadLen != header.RawPayloadLen {
		result.Outcome = OutcomeUnsupportedVersion
		result.Err = fmt.Errorf("%w: compressed collection WAL frame unsupported in PR1-min", ErrCollectionWALUnsupportedVersion)
		return result, offset + frameLen
	}
	txn, err := decodeTransactionPayload(payload, header.WALLSN)
	if err != nil {
		result.Outcome = outcomeForFrameError(err)
		result.Err = err
		return result, offset + frameLen
	}
	if txn.CollectionUID != header.CollectionUID || txn.CollectionSeq != header.CollectionSeq {
		result.Outcome = OutcomeCompleteCorrupt
		result.Err = fmt.Errorf("%w: frame/payload collection identity mismatch", ErrCollectionWALIdentityMismatch)
		return result, offset + frameLen
	}
	gotReplayCRC, err := ReplayDigestCRC32C(txn)
	if err != nil {
		result.Outcome = outcomeForFrameError(err)
		result.Err = err
		return result, offset + frameLen
	}
	if gotReplayCRC != header.ReplayDigestCRC32C {
		result.Outcome = OutcomeCompleteCorrupt
		result.Err = fmt.Errorf("%w: replay digest crc got %08x want %08x", ErrCollectionWALBadChecksum, gotReplayCRC, header.ReplayDigestCRC32C)
		return result, offset + frameLen
	}
	if _, ok := seenWALLSN[header.WALLSN]; ok {
		result.Outcome = OutcomeDuplicateWALLSN
		result.Err = fmt.Errorf("%w: duplicate WALLSN %d", ErrCollectionWALSequenceGap, header.WALLSN)
		return result, offset + frameLen
	}
	seqKey := collectionSeqKey{uid: header.CollectionUID, seq: header.CollectionSeq}
	if _, ok := seenCollectionSeq[seqKey]; ok {
		result.Outcome = OutcomeDuplicateCollectionSeq
		result.Err = fmt.Errorf("%w: duplicate collection sequence %d", ErrCollectionWALSequenceGap, header.CollectionSeq)
		return result, offset + frameLen
	}
	seenWALLSN[header.WALLSN] = struct{}{}
	seenCollectionSeq[seqKey] = struct{}{}
	result.Outcome = OutcomeCompleteValid
	result.Transaction = txn
	return result, offset + frameLen
}

func parseFrameHeader(header []byte) (FrameHeader, error) {
	var h FrameHeader
	gotHeaderCRC := binary.LittleEndian.Uint32(header[72:76])
	wantHeaderCRC := crc32cWithZero(header, 72, 76)
	if gotHeaderCRC != wantHeaderCRC {
		return h, fmt.Errorf("%w: frame header crc got %08x want %08x", ErrCollectionWALBadChecksum, gotHeaderCRC, wantHeaderCRC)
	}
	if !allZero(header[88:96]) {
		return h, fmt.Errorf("%w: nonzero frame reserved bytes", ErrCollectionWALUnsupportedVersion)
	}
	h.RecordType = RecordType(binary.LittleEndian.Uint16(header[14:16]))
	h.FrameFlags = binary.LittleEndian.Uint32(header[16:20])
	h.CompressionCodec = CompressionCodec(binary.LittleEndian.Uint16(header[20:22]))
	h.SectionTableVersion = binary.LittleEndian.Uint16(header[22:24])
	h.WALLSN = binary.LittleEndian.Uint64(header[24:32])
	copy(h.CollectionUID[:], header[32:48])
	h.CollectionSeq = binary.LittleEndian.Uint64(header[48:56])
	h.StoredPayloadLen = binary.LittleEndian.Uint64(header[56:64])
	h.RawPayloadLen = binary.LittleEndian.Uint64(header[64:72])
	h.StoredPayloadCRC32C = binary.LittleEndian.Uint32(header[76:80])
	h.ReplayDigestCRC32C = binary.LittleEndian.Uint32(header[80:84])
	h.RequiredFeatureBitsLow = binary.LittleEndian.Uint32(header[84:88])
	if h.FrameFlags&^uint32(FrameFlagCompressed|FrameFlagCommitTrailerRequired) != 0 {
		return h, fmt.Errorf("%w: unknown frame flags %08x", ErrCollectionWALUnsupportedVersion, h.FrameFlags)
	}
	if h.FrameFlags&FrameFlagCommitTrailerRequired == 0 {
		return h, fmt.Errorf("%w: frame missing required commit trailer flag", ErrCollectionWALUnsupportedVersion)
	}
	if h.SectionTableVersion != SectionTableVersionV1 {
		return h, fmt.Errorf("%w: section table version %d", ErrCollectionWALUnsupportedVersion, h.SectionTableVersion)
	}
	if h.RequiredFeatureBitsLow&^RequiredFeatureCollectionWALV1 != 0 {
		return h, fmt.Errorf("%w: unknown required feature bits %08x", ErrCollectionWALUnsupportedVersion, h.RequiredFeatureBitsLow)
	}
	return h, nil
}

func validateCommitTrailer(header, payload, trailer []byte) error {
	if len(trailer) != CommitTrailerLen {
		return fmt.Errorf("%w: commit trailer length %d", ErrCollectionWALCorruptMiddle, len(trailer))
	}
	if !bytes.Equal(trailer[0:8], commitTrailerMagic[:]) {
		return fmt.Errorf("%w: bad commit trailer magic", ErrCollectionWALCorruptMiddle)
	}
	if got := binary.LittleEndian.Uint32(trailer[8:12]); got != CommitTrailerLen {
		return fmt.Errorf("%w: commit trailer len %d", ErrCollectionWALUnsupportedVersion, got)
	}
	crc := crc32.New(collectionWALCRC32C)
	_, _ = crc.Write(header)
	_, _ = crc.Write(payload)
	_, _ = crc.Write(trailer[:12])
	if got, want := binary.LittleEndian.Uint32(trailer[12:16]), crc.Sum32(); got != want {
		return fmt.Errorf("%w: whole frame crc got %08x want %08x", ErrCollectionWALBadChecksum, got, want)
	}
	return nil
}

func validateTransactionForEncode(txn Transaction) error {
	if txn.WALLSN == 0 {
		return fmt.Errorf("%w: WALLSN is required", ErrCollectionWALIdentityMismatch)
	}
	if err := validateDecodedTransaction(txn, 0); err != nil {
		return err
	}
	for _, section := range txn.Sections {
		if err := validateSectionMetadata(section); err != nil {
			return err
		}
	}
	return nil
}

func validateTransactionForReplayDigest(txn Transaction) error {
	if txn.WALLSN == 0 {
		return fmt.Errorf("%w: WALLSN is required for replay digest", ErrCollectionWALIdentityMismatch)
	}
	if txn.CollectionUID == ([CollectionUIDBytes]byte{}) || txn.CollectionSeq == 0 {
		return fmt.Errorf("%w: collection identity is required for replay digest", ErrCollectionWALIdentityMismatch)
	}
	return nil
}

func validateDecodedTransaction(txn Transaction, encodedLen uint64) error {
	if encodedLen > 0 {
		if err := ValidateEncodedTransactionSize(encodedLen); err != nil {
			return err
		}
	}
	if txn.CollectionUID == ([CollectionUIDBytes]byte{}) {
		return fmt.Errorf("%w: collection UID is required", ErrCollectionWALIdentityMismatch)
	}
	if txn.TransactionFlags != 0 {
		return fmt.Errorf("%w: unknown transaction flags %08x", ErrCollectionWALUnsupportedVersion, txn.TransactionFlags)
	}
	if txn.CollectionGeneration == 0 {
		return fmt.Errorf("%w: collection generation is required", ErrCollectionWALIdentityMismatch)
	}
	if txn.CollectionSeq == 0 {
		return fmt.Errorf("%w: collection sequence is required", ErrCollectionWALSequenceGap)
	}
	if txn.DependsOnCollectionSeq+1 != txn.CollectionSeq {
		return fmt.Errorf("%w: depends_on_collection_seq=%d collection_seq=%d", ErrCollectionWALSequenceGap, txn.DependsOnCollectionSeq, txn.CollectionSeq)
	}
	if txn.RootDeltaCount > MaxRootDeltasPerTransaction {
		return fmt.Errorf("%w: root delta count %d exceeds %d", ErrCollectionWALResourceLimit, txn.RootDeltaCount, MaxRootDeltasPerTransaction)
	}
	if txn.SideRefCount > MaxSideRefsPerTransaction {
		return fmt.Errorf("%w: side ref count %d exceeds %d", ErrCollectionWALResourceLimit, txn.SideRefCount, MaxSideRefsPerTransaction)
	}
	if txn.DescriptorOpCount > MaxDescriptorOpsPerTransaction {
		return fmt.Errorf("%w: descriptor op count %d exceeds %d", ErrCollectionWALResourceLimit, txn.DescriptorOpCount, MaxDescriptorOpsPerTransaction)
	}
	var rootDeltaBytes uint64
	for _, section := range txn.Sections {
		if section.Type == SectionTypeRootDeltaTable {
			rootDeltaBytes += uint64(len(section.Data))
			if uint64(len(section.Data)) > MaxInlineRootDeltaBytesPerRoot {
				return fmt.Errorf("%w: root delta section bytes %d exceeds per-root inline cap %d", ErrCollectionWALResourceLimit, len(section.Data), MaxInlineRootDeltaBytesPerRoot)
			}
		}
	}
	if rootDeltaBytes > MaxInlineRootDeltaBytesPerTransaction {
		return fmt.Errorf("%w: root delta bytes %d exceeds per-transaction inline cap %d", ErrCollectionWALResourceLimit, rootDeltaBytes, MaxInlineRootDeltaBytesPerTransaction)
	}
	return nil
}

func validateSectionMetadata(section Section) error {
	if section.Version != SectionTableVersionV1 {
		return fmt.Errorf("%w: section type %d version %d", ErrCollectionWALUnsupportedVersion, section.Type, section.Version)
	}
	if section.Flags&^uint32(SectionFlagCritical|SectionFlagReplayCritical) != 0 {
		return fmt.Errorf("%w: section type %d unknown flags %08x", ErrCollectionWALUnsupportedVersion, section.Type, section.Flags)
	}
	if !knownSectionType(section.Type) && (section.Critical() || section.ReplayCritical()) {
		return fmt.Errorf("%w: unknown critical section type %d", ErrCollectionWALUnsupportedVersion, section.Type)
	}
	return nil
}

func validateSectionRanges(ranges []sectionRange) error {
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].end < ranges[j].end
		}
		return ranges[i].start < ranges[j].start
	})
	for i := 1; i < len(ranges); i++ {
		if ranges[i].start < ranges[i-1].end {
			return fmt.Errorf("%w: overlapping section ranges", ErrCollectionWALCorruptMiddle)
		}
	}
	return nil
}

func sectionTypeReplayCritical(t SectionType) bool {
	switch t {
	case SectionTypeRootDeltaTable, SectionTypeSideRefTable, SectionTypeSystemDeltaTemplate, SectionTypeDescriptorOps:
		return true
	default:
		return false
	}
}

func knownSectionType(t SectionType) bool {
	switch t {
	case SectionTypeRootDeltaTable,
		SectionTypeSideRefTable,
		SectionTypeSystemDeltaTemplate,
		SectionTypeDescriptorOps,
		SectionTypeStats,
		SectionTypeUnknownPreserved:
		return true
	default:
		return false
	}
}

func outcomeForFrameError(err error) FrameOutcome {
	switch {
	case errors.Is(err, ErrCollectionWALResourceLimit):
		return OutcomeMaliciousLength
	case errors.Is(err, ErrCollectionWALUnsupportedVersion):
		return OutcomeUnsupportedVersion
	case errors.Is(err, ErrCollectionWALTerminalTail):
		return OutcomeTerminalIncompleteTail
	default:
		return OutcomeCompleteCorrupt
	}
}

func shortReadOutcome(terminal bool, msg string) (FrameOutcome, error) {
	if terminal {
		return OutcomeTerminalIncompleteTail, fmt.Errorf("%w: %s", ErrCollectionWALTerminalTail, msg)
	}
	return OutcomeNonTerminalShortRead, fmt.Errorf("%w: %s", ErrCollectionWALCorruptMiddle, msg)
}

func crc32c(b []byte) uint32 {
	return crc32.Checksum(b, collectionWALCRC32C)
}

func crc32cWithZero(buf []byte, start, end int) uint32 {
	crc := crc32.New(collectionWALCRC32C)
	_, _ = crc.Write(buf[:start])
	_, _ = crc.Write(make([]byte, end-start))
	_, _ = crc.Write(buf[end:])
	return crc.Sum32()
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func appendUint16(dst []byte, v uint16) []byte {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return append(dst, buf[:]...)
}

func appendUint32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func appendUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

type sectionRange struct {
	start uint64
	end   uint64
}

type collectionSeqKey struct {
	uid [CollectionUIDBytes]byte
	seq uint64
}
