package valuelog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"path/filepath"

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
	if cap(dst) >= page.PageSize && sliceAliasesBytes(dst[:cap(dst)], payload) {
		payload = append([]byte(nil), payload...)
	}
	out := dst
	usedDst := false
	if cap(out) >= page.PageSize {
		out = out[:page.PageSize]
		clear(out)
		usedDst = true
	} else {
		out = make([]byte, page.PageSize)
	}
	copy(out[:prefixLen], payload[compactLeafPagePayloadHeaderSize:compactLeafPagePayloadHeaderSize+prefixLen])
	copy(out[page.PageSize-suffixLen:], payload[compactLeafPagePayloadHeaderSize+prefixLen:])
	return out, usedDst, true, nil
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
