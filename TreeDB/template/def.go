package template

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/zeebo/xxh3"
)

const (
	templateDefVerAnchor = 1
	templateDefVerMask   = 2
)

var (
	ErrCorruptTemplateDef = errors.New("template: corrupt template def")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeTemplateDef serializes anchors into the TemplateDefBytes format.
func EncodeTemplateDef(def TemplateDef, cfg Config) ([]byte, error) {
	cfg = NormalizeConfig(cfg)
	if def.Kind == 0 || def.Kind == TemplateAnchors {
		if len(def.Anchors) == 0 {
			return nil, ErrCorruptTemplateDef
		}
		if len(def.Anchors) > cfg.MaxAnchorsPerTemplate {
			return nil, ErrCorruptTemplateDef
		}
		total := 0
		for _, a := range def.Anchors {
			if len(a) < cfg.MinAnchorLen || len(a) > cfg.MaxAnchorLen {
				return nil, ErrCorruptTemplateDef
			}
			total += len(a)
			if total > cfg.MaxAnchorBytesTotal {
				return nil, ErrCorruptTemplateDef
			}
		}
		size := 1 + uvarintLen(uint64(len(def.Anchors)))
		for _, a := range def.Anchors {
			size += uvarintLen(uint64(len(a))) + len(a)
		}
		size += 4
		buf := make([]byte, size)
		buf[0] = templateDefVerAnchor
		off := 1
		off += binary.PutUvarint(buf[off:], uint64(len(def.Anchors)))
		for _, a := range def.Anchors {
			off += binary.PutUvarint(buf[off:], uint64(len(a)))
			copy(buf[off:], a)
			off += len(a)
		}
		crc := crc32.Checksum(buf[:off], crcTable)
		binary.LittleEndian.PutUint32(buf[off:], crc)
		off += 4
		if off != len(buf) {
			return nil, ErrCorruptTemplateDef
		}
		return buf, nil
	}
	if def.Kind != TemplateMask {
		return nil, ErrCorruptTemplateDef
	}
	if len(def.Base) == 0 {
		return nil, ErrCorruptTemplateDef
	}
	maskLen := (len(def.Base) + 7) / 8
	if len(def.Mask) != maskLen {
		return nil, ErrCorruptTemplateDef
	}
	size := 1 + 1 + uvarintLen(uint64(len(def.Base))) + uvarintLen(uint64(len(def.Mask)))
	size += len(def.Mask) + len(def.Base)
	size += 4
	buf := make([]byte, size)
	buf[0] = templateDefVerMask
	buf[1] = byte(def.Kind)
	off := 2
	off += binary.PutUvarint(buf[off:], uint64(len(def.Base)))
	off += binary.PutUvarint(buf[off:], uint64(len(def.Mask)))
	copy(buf[off:], def.Mask)
	off += len(def.Mask)
	copy(buf[off:], def.Base)
	off += len(def.Base)
	crc := crc32.Checksum(buf[:off], crcTable)
	binary.LittleEndian.PutUint32(buf[off:], crc)
	off += 4
	if off != len(buf) {
		return nil, ErrCorruptTemplateDef
	}
	return buf, nil
}

// DecodeTemplateDef parses TemplateDefBytes.
func DecodeTemplateDef(buf []byte) (TemplateDef, error) {
	if len(buf) < 1+1+4 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	if len(buf) < 5 {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	payloadLen := len(buf) - 4
	crcWant := binary.LittleEndian.Uint32(buf[payloadLen:])
	crcGot := crc32.Checksum(buf[:payloadLen], crcTable)
	if crcGot != crcWant {
		return TemplateDef{}, ErrCorruptTemplateDef
	}
	switch buf[0] {
	case templateDefVerAnchor:
		off := 1
		anchorCount64, n := binary.Uvarint(buf[off:payloadLen])
		if n <= 0 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		off += n
		if anchorCount64 == 0 || anchorCount64 > 1<<20 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		anchorCount := int(anchorCount64)
		anchors := make([][]byte, anchorCount)
		for i := 0; i < anchorCount; i++ {
			if off >= payloadLen {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			anchorLen64, n := binary.Uvarint(buf[off:payloadLen])
			if n <= 0 {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			off += n
			if anchorLen64 == 0 || anchorLen64 > uint64(payloadLen-off) {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			anchorLen := int(anchorLen64)
			anchors[i] = buf[off : off+anchorLen]
			off += anchorLen
		}
		if off != payloadLen {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		return TemplateDef{Kind: TemplateAnchors, Anchors: anchors}, nil
	case templateDefVerMask:
		if payloadLen < 2 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		kind := TemplateKind(buf[1])
		if kind != TemplateMask {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		off := 2
		baseLen64, n := binary.Uvarint(buf[off:payloadLen])
		if n <= 0 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		off += n
		maskLen64, n := binary.Uvarint(buf[off:payloadLen])
		if n <= 0 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		off += n
		if baseLen64 == 0 || baseLen64 > uint64(payloadLen-off) {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		maskLen := int(maskLen64)
		if maskLen <= 0 || maskLen > payloadLen-off {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		if off+maskLen > payloadLen {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		mask := buf[off : off+maskLen]
		off += maskLen
		baseLen := int(baseLen64)
		if off+baseLen != payloadLen {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		base := buf[off : off+baseLen]
		varPositions := make([]int, 0, baseLen)
		for i := 0; i < baseLen; i++ {
			if mask[i/8]&(1<<uint(i%8)) != 0 {
				varPositions = append(varPositions, i)
			}
		}
		return TemplateDef{Kind: TemplateMask, Mask: mask, Base: base, VarPositions: varPositions}, nil
	default:
		return TemplateDef{}, ErrCorruptTemplateDef
	}
}

// TemplateID computes a deterministic ID for TemplateDefBytes.
func TemplateID(defBytes []byte, salt byte) uint64 {
	if salt == 0 {
		return xxh3.Hash(defBytes)
	}
	buf := make([]byte, len(defBytes)+1)
	copy(buf, defBytes)
	buf[len(defBytes)] = salt
	return xxh3.Hash(buf)
}
