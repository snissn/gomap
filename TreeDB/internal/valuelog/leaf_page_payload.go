package valuelog

import (
	"bytes"
	"encoding/binary"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var compactLeafPagePayloadMagic = [8]byte{0x8a, 'L', 'F', 'P', 'G', 0x01, 0x91, 0x3c}

const compactLeafPagePayloadHeaderSize = len(compactLeafPagePayloadMagic) + 4
const compactLeafPagePayloadDirName = "leaf_vlog"

func HasCompactLeafLogPayload(payload []byte) bool {
	return len(payload) >= compactLeafPagePayloadHeaderSize &&
		bytes.Equal(payload[:len(compactLeafPagePayloadMagic)], compactLeafPagePayloadMagic[:])
}

func MaybeCompactLeafLogPayload(leafPage []byte) ([]byte, bool, error) {
	if len(leafPage) != page.PageSize {
		return leafPage, false, nil
	}
	prefixLen, suffixLen, err := node.LeafPageLiveBounds(leafPage)
	if err != nil {
		return leafPage, false, nil
	}
	compactLen := compactLeafPagePayloadHeaderSize + prefixLen + suffixLen
	if compactLen >= len(leafPage) {
		return leafPage, false, nil
	}

	suffixStart := len(leafPage) - suffixLen
	var canonical [page.PageSize]byte
	copy(canonical[:prefixLen], leafPage[:prefixLen])
	copy(canonical[suffixStart:], leafPage[suffixStart:])
	page.UpdateChecksum(canonical[:])

	payload := make([]byte, compactLen)
	copy(payload[:len(compactLeafPagePayloadMagic)], compactLeafPagePayloadMagic[:])
	binary.LittleEndian.PutUint16(payload[len(compactLeafPagePayloadMagic):len(compactLeafPagePayloadMagic)+2], uint16(prefixLen))
	binary.LittleEndian.PutUint16(payload[len(compactLeafPagePayloadMagic)+2:compactLeafPagePayloadHeaderSize], uint16(suffixLen))
	copy(payload[compactLeafPagePayloadHeaderSize:compactLeafPagePayloadHeaderSize+prefixLen], canonical[:prefixLen])
	copy(payload[compactLeafPagePayloadHeaderSize+prefixLen:], canonical[suffixStart:])
	return payload, true, nil
}

func decodeCompactLeafLogPayloadTo(payload, dst []byte) ([]byte, bool, bool, error) {
	if !HasCompactLeafLogPayload(payload) {
		return payload, false, false, nil
	}
	prefixLen := int(binary.LittleEndian.Uint16(payload[len(compactLeafPagePayloadMagic) : len(compactLeafPagePayloadMagic)+2]))
	suffixLen := int(binary.LittleEndian.Uint16(payload[len(compactLeafPagePayloadMagic)+2 : compactLeafPagePayloadHeaderSize]))
	if prefixLen < node.NodeHeaderSize || prefixLen+suffixLen > page.PageSize {
		return nil, false, true, ErrCorrupt
	}
	if compactLeafPagePayloadHeaderSize+prefixLen+suffixLen != len(payload) {
		return nil, false, true, ErrCorrupt
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
	out, _, decoded, err := decodeCompactLeafLogPayloadTo(payload, nil)
	if err != nil {
		return nil, err
	}
	if decoded {
		return append(dst, out...), nil
	}
	if len(payload) == 0 {
		return dst, nil
	}
	if cap(dst) >= len(dst)+len(payload) && sliceAliasesBytes(dst[:cap(dst)], payload) {
		return dst[:len(dst)+len(payload)], nil
	}
	return append(dst, payload...), nil
}
