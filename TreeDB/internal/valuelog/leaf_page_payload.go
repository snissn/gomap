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
	payload := make([]byte, compactLen)
	copy(payload[:len(compactLeafPagePayloadMagic)], compactLeafPagePayloadMagic[:])
	binary.LittleEndian.PutUint16(payload[len(compactLeafPagePayloadMagic):len(compactLeafPagePayloadMagic)+2], uint16(prefixLen))
	binary.LittleEndian.PutUint16(payload[len(compactLeafPagePayloadMagic)+2:compactLeafPagePayloadHeaderSize], uint16(suffixLen))
	prefix := payload[compactLeafPagePayloadHeaderSize : compactLeafPagePayloadHeaderSize+prefixLen]
	suffix := payload[compactLeafPagePayloadHeaderSize+prefixLen:]
	copy(prefix, leafPage[:prefixLen])
	copy(suffix, leafPage[suffixStart:])
	sum := page.CalculateChecksumWithZeroGap(prefix, page.PageSize-prefixLen-suffixLen, suffix)
	binary.LittleEndian.PutUint32(prefix[8:12], sum)
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
	return maybeDecodeLeafLogPayloadAllowed(allowsCompactLeafLogPayload(fileID, path), payload, dst)
}

func maybeDecodeLeafLogPayloadAllowed(allowCompact bool, payload, dst []byte) ([]byte, bool, bool, error) {
	if !allowCompact {
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
	return appendMaybeDecodeLeafLogPayloadAllowed(allowsCompactLeafLogPayload(fileID, path), dst, payload)
}

func appendMaybeDecodeLeafLogPayloadAllowed(allowCompact bool, dst, payload []byte) ([]byte, error) {
	if !allowCompact {
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
