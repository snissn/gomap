package valuelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var compactLeafPagePayloadMagic = [8]byte{0x8a, 'L', 'F', 'P', 'G', 0x01, 0x91, 0x3c}
var compactLeafPagePayloadCRCTable = crc32.MakeTable(crc32.Castagnoli)
var compactLeafPagePayloadChecksumZeros [1024]byte

const compactLeafPagePayloadHeaderSize = len(compactLeafPagePayloadMagic) + 4
const compactLeafPagePayloadDirName = "leaf_vlog"

func HasCompactLeafLogPayload(payload []byte) bool {
	return len(payload) >= compactLeafPagePayloadHeaderSize &&
		bytes.Equal(payload[:len(compactLeafPagePayloadMagic)], compactLeafPagePayloadMagic[:])
}

func compactLeafPageLivePayloadBounds(leafPage []byte) (prefixLen, suffixStart, suffixLen int, ok bool, err error) {
	if len(leafPage) != page.PageSize {
		return 0, 0, 0, false, nil
	}
	prefixLen, suffixLen, err = node.LeafPageLiveBounds(leafPage)
	if err != nil {
		return 0, 0, 0, false, nil
	}
	compactLen := compactLeafPagePayloadLen(prefixLen, suffixLen)
	if compactLen >= len(leafPage) {
		return 0, 0, 0, false, nil
	}
	suffixStart = len(leafPage) - suffixLen
	return prefixLen, suffixStart, suffixLen, true, nil
}

func compactLeafPagePayloadLen(prefixLen, suffixLen int) int {
	return compactLeafPagePayloadHeaderSize + prefixLen + suffixLen
}

func compactLeafCanonicalChecksum(leafPage []byte, prefixLen, suffixStart, suffixLen int) uint32 {
	if len(leafPage) < page.PageHeaderSize {
		return 0
	}
	checksumEnd := page.PageChecksumOffset + page.PageChecksumSize
	sum := crc32.Update(0, compactLeafPagePayloadCRCTable, leafPage[:page.PageChecksumOffset])
	sum = crc32.Update(sum, compactLeafPagePayloadCRCTable, compactLeafPagePayloadChecksumZeros[:page.PageChecksumSize])
	if prefixLen > checksumEnd {
		sum = crc32.Update(sum, compactLeafPagePayloadCRCTable, leafPage[checksumEnd:prefixLen])
	}
	gapLen := page.PageSize - prefixLen - suffixLen
	for gapLen > 0 {
		n := len(compactLeafPagePayloadChecksumZeros)
		if n > gapLen {
			n = gapLen
		}
		sum = crc32.Update(sum, compactLeafPagePayloadCRCTable, compactLeafPagePayloadChecksumZeros[:n])
		gapLen -= n
	}
	if suffixLen > 0 {
		sum = crc32.Update(sum, compactLeafPagePayloadCRCTable, leafPage[suffixStart:suffixStart+suffixLen])
	}
	return sum
}

// CompactLeafLogPayloadLen reports the payload length that
// MaybeCompactLeafLogPayload would produce without materializing the payload.
// When compaction is not beneficial, it returns len(leafPage) and compacted=false.
func CompactLeafLogPayloadLen(leafPage []byte) (payloadLen int, compacted bool, err error) {
	prefixLen, _, suffixLen, ok, err := compactLeafPageLivePayloadBounds(leafPage)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return len(leafPage), false, nil
	}
	return compactLeafPagePayloadLen(prefixLen, suffixLen), true, nil
}

func encodeCompactLeafLogPayload(dst, leafPage []byte, prefixLen, suffixStart, suffixLen int) {
	copy(dst[:len(compactLeafPagePayloadMagic)], compactLeafPagePayloadMagic[:])
	binary.LittleEndian.PutUint16(dst[len(compactLeafPagePayloadMagic):len(compactLeafPagePayloadMagic)+2], uint16(prefixLen))
	binary.LittleEndian.PutUint16(dst[len(compactLeafPagePayloadMagic)+2:compactLeafPagePayloadHeaderSize], uint16(suffixLen))

	payload := dst[compactLeafPagePayloadHeaderSize:]
	sum := compactLeafCanonicalChecksum(leafPage, prefixLen, suffixStart, suffixLen)

	checksumEnd := page.PageChecksumOffset + page.PageChecksumSize
	copy(payload[:page.PageChecksumOffset], leafPage[:page.PageChecksumOffset])
	binary.LittleEndian.PutUint32(payload[page.PageChecksumOffset:checksumEnd], sum)
	if prefixLen > checksumEnd {
		copy(payload[checksumEnd:prefixLen], leafPage[checksumEnd:prefixLen])
	}
	copy(payload[prefixLen:], leafPage[suffixStart:])
}

func MaybeCompactLeafLogPayload(leafPage []byte) ([]byte, bool, error) {
	prefixLen, suffixStart, suffixLen, ok, err := compactLeafPageLivePayloadBounds(leafPage)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return leafPage, false, nil
	}
	payload := make([]byte, compactLeafPagePayloadLen(prefixLen, suffixLen))
	encodeCompactLeafLogPayload(payload, leafPage, prefixLen, suffixStart, suffixLen)
	return payload, true, nil
}

// PrepareCompactLeafPageRecord builds the raw K=1 value-log record used for an
// outer leaf page. It lets callers do the page compaction, copy and CRC work
// before taking the writer mutex; the returned length is ready to store in a
// ValuePtr.
func PrepareCompactLeafPageRecord(dst []byte, rid uint64, leafPage []byte) ([]byte, FrameStats, bool, uint32, error) {
	if rid == 0 {
		return nil, FrameStats{}, false, 0, errors.New("valuelog: missing rid")
	}
	prefixLen, suffixStart, suffixLen, compacted, err := compactLeafPageLivePayloadBounds(leafPage)
	if err != nil {
		return nil, FrameStats{}, false, 0, err
	}
	payloadLen := len(leafPage)
	if compacted {
		payloadLen = compactLeafPagePayloadLen(prefixLen, suffixLen)
	}
	bodyLen := uint32(FrameHeaderSize + 8 + 8 + payloadLen)
	if recordSizeExceedsMax(bodyLen) {
		return nil, FrameStats{}, false, 0, ErrRecordTooLarge
	}
	recordLen := HeaderSize + int(bodyLen)
	if cap(dst) < recordLen {
		dst = make([]byte, recordLen)
	} else {
		dst = dst[:recordLen]
	}

	dst[4] = Version
	dst[5] = recordFlagGrouped
	dst[6] = 0
	dst[7] = 0
	binary.LittleEndian.PutUint64(dst[8:16], 0)
	binary.LittleEndian.PutUint32(dst[16:20], bodyLen)

	off := HeaderSize
	dst[off] = FrameVersion
	dst[off+1] = 0
	dst[off+2] = 1
	dst[off+3] = 0
	binary.LittleEndian.PutUint64(dst[off+4:off+12], 0)
	off += FrameHeaderSize

	binary.LittleEndian.PutUint64(dst[off:off+8], rid)
	off += 8
	binary.LittleEndian.PutUint32(dst[off:off+4], 0)
	binary.LittleEndian.PutUint32(dst[off+4:off+8], uint32(payloadLen))
	off += 8

	if compacted {
		encodeCompactLeafLogPayload(dst[off:off+payloadLen], leafPage, prefixLen, suffixStart, suffixLen)
	} else {
		copy(dst[off:], leafPage)
	}
	sum := crc.ChecksumParts(dst[4:HeaderSize], dst[HeaderSize:])
	binary.LittleEndian.PutUint32(dst[0:4], sum)

	recordLenHint := uint32(headerWithoutCRC) + bodyLen
	if recordLenHint > page.ValuePtrGroupedMaxRecordLen {
		recordLenHint = 0
	}
	return dst, FrameStats{Records: 1, RawPayloadBytes: payloadLen, StoredPayloadBytes: payloadLen, Kept: false}, compacted, page.ValuePtrMarkGrouped(recordLenHint, 0), nil
}

func compactLeafLogPayloadBounds(payload []byte) (prefixLen, suffixLen int, decoded bool, err error) {
	if !HasCompactLeafLogPayload(payload) {
		return 0, 0, false, nil
	}
	prefixLen = int(binary.LittleEndian.Uint16(payload[len(compactLeafPagePayloadMagic) : len(compactLeafPagePayloadMagic)+2]))
	suffixLen = int(binary.LittleEndian.Uint16(payload[len(compactLeafPagePayloadMagic)+2 : compactLeafPagePayloadHeaderSize]))
	if prefixLen < node.NodeHeaderSize || prefixLen+suffixLen > page.PageSize {
		return 0, 0, true, ErrCorrupt
	}
	if compactLeafPagePayloadHeaderSize+prefixLen+suffixLen != len(payload) {
		return 0, 0, true, ErrCorrupt
	}
	return prefixLen, suffixLen, true, nil
}

func decodeCompactLeafLogPayloadTo(payload, dst []byte) ([]byte, bool, bool, error) {
	prefixLen, suffixLen, decoded, err := compactLeafLogPayloadBounds(payload)
	if err != nil || !decoded {
		if err != nil {
			return nil, false, decoded, err
		}
		return payload, false, false, nil
	}
	out := dst
	usedDst := false
	if cap(out) >= page.PageSize {
		out = out[:page.PageSize]
		usedDst = true
	} else {
		out = make([]byte, page.PageSize)
	}
	decodeCompactLeafLogPayloadInto(out, payload, prefixLen, suffixLen)
	return out, usedDst, true, nil
}

func decodeCompactLeafLogPayloadInto(out, payload []byte, prefixLen, suffixLen int) {
	src := payload[compactLeafPagePayloadHeaderSize:]
	copy(out[:prefixLen], src[:prefixLen])
	if suffixLen > 0 {
		copy(out[page.PageSize-suffixLen:], src[prefixLen:prefixLen+suffixLen])
	}
	clear(out[prefixLen : page.PageSize-suffixLen])
}

func appendCompactLeafLogPayload(dst, payload []byte) ([]byte, error) {
	prefixLen, suffixLen, decoded, err := compactLeafLogPayloadBounds(payload)
	if err != nil {
		return nil, err
	}
	if !decoded {
		if len(payload) == 0 {
			return dst, nil
		}
		if cap(dst) >= len(dst)+len(payload) && sliceAliasesBytes(dst[:cap(dst)], payload) {
			return dst[:len(dst)+len(payload)], nil
		}
		return append(dst, payload...), nil
	}

	var expanded [page.PageSize]byte
	copy(expanded[:prefixLen], payload[compactLeafPagePayloadHeaderSize:compactLeafPagePayloadHeaderSize+prefixLen])
	copy(expanded[page.PageSize-suffixLen:], payload[compactLeafPagePayloadHeaderSize+prefixLen:])
	return append(dst, expanded[:]...), nil
}

func allowsCompactLeafLogPayload(fileID uint32, path string) bool {
	if !isLeafLogFileID(fileID) || path == "" {
		return false
	}
	return filepath.Base(filepath.Dir(path)) == compactLeafPagePayloadDirName
}

func maybeDecodeLeafLogPayloadTo(fileID uint32, path string, payload, dst []byte) ([]byte, bool, bool, error) {
	if !allowsCompactLeafLogPayload(fileID, path) {
		return payload, false, false, nil
	}
	out, usedDst, decoded, err := decodeCompactLeafLogPayloadTo(payload, dst)
	if err != nil {
		return nil, false, decoded, err
	}
	if decoded {
		return out, usedDst, true, nil
	}
	return payload, false, false, nil
}

func appendMaybeDecodeLeafLogPayload(fileID uint32, path string, dst, payload []byte) ([]byte, error) {
	if !allowsCompactLeafLogPayload(fileID, path) {
		if len(payload) == 0 {
			return dst, nil
		}
		if cap(dst) >= len(dst)+len(payload) && sliceAliasesBytes(dst[:cap(dst)], payload) {
			return dst[:len(dst)+len(payload)], nil
		}
		return append(dst, payload...), nil
	}
	return appendCompactLeafLogPayload(dst, payload)
}
