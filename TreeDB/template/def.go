package template

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/zeebo/xxh3"
)

const (
	templateDefVerAnchor = 1
	templateDefVerMaskV2 = 2
	templateDefVerMask   = 3
)

var (
	ErrCorruptTemplateDef = errors.New("template: corrupt template def")
)

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
		checksum := crc.Checksum(buf[:off])
		binary.LittleEndian.PutUint32(buf[off:], checksum)
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
		if len(def.Mask) == 0 && len(def.VarPositions) > 0 {
			def.Mask = buildMaskFromPositions(def.VarPositions, len(def.Base))
		}
		if len(def.Mask) != maskLen {
			return nil, ErrCorruptTemplateDef
		}
	}
	if len(def.VarPositions) == 0 {
		def.VarPositions = buildVarPositions(def.Mask, len(def.Base))
	}
	for i, pos := range def.VarPositions {
		if int(pos) >= len(def.Base) {
			return nil, ErrCorruptTemplateDef
		}
		if i > 0 && def.VarPositions[i-1] >= pos {
			return nil, ErrCorruptTemplateDef
		}
	}
	varCount := len(def.VarPositions)
	size := 1 + 1 + uvarintLen(uint64(len(def.Base))) + uvarintLen(uint64(len(def.Mask))) + uvarintLen(uint64(varCount))
	size += len(def.Mask) + len(def.Base)
	for _, pos := range def.VarPositions {
		size += uvarintLen(uint64(pos))
	}
	size += 4
	buf := make([]byte, size)
	buf[0] = templateDefVerMask
	buf[1] = byte(def.Kind)
	off := 2
	off += binary.PutUvarint(buf[off:], uint64(len(def.Base)))
	off += binary.PutUvarint(buf[off:], uint64(len(def.Mask)))
	off += binary.PutUvarint(buf[off:], uint64(varCount))
	copy(buf[off:], def.Mask)
	off += len(def.Mask)
	for _, pos := range def.VarPositions {
		off += binary.PutUvarint(buf[off:], uint64(pos))
	}
	copy(buf[off:], def.Base)
	off += len(def.Base)
	checksum := crc.Checksum(buf[:off])
	binary.LittleEndian.PutUint32(buf[off:], checksum)
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
	crcGot := crc.Checksum(buf[:payloadLen])
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
		varCount64, n := binary.Uvarint(buf[off:payloadLen])
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
		varCount := int(varCount64)
		if varCount < 0 {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		varPositions := make([]uint16, varCount)
		for i := 0; i < varCount; i++ {
			if off >= payloadLen {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			pos64, n := binary.Uvarint(buf[off:payloadLen])
			if n <= 0 {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			off += n
			if pos64 > 1<<16-1 {
				return TemplateDef{}, ErrCorruptTemplateDef
			}
			varPositions[i] = uint16(pos64)
		}
		baseLen := int(baseLen64)
		if off+baseLen != payloadLen {
			return TemplateDef{}, ErrCorruptTemplateDef
		}
		base := buf[off : off+baseLen]
		if err := validateVarPositions(base, mask, varPositions); err != nil {
			return TemplateDef{}, err
		}
		if len(varPositions) == 0 {
			varPositions = buildVarPositions(mask, len(base))
		}
		constPositions := buildConstPositions(mask, len(base))
		varSpans, constSpans := buildMaskSpans(mask, len(base))
		return TemplateDef{
			Kind:           TemplateMask,
			Mask:           mask,
			Base:           base,
			VarPositions:   varPositions,
			ConstPositions: constPositions,
			maskVarSpans:   varSpans,
			maskConstSpans: constSpans,
		}, nil
	case templateDefVerMaskV2:
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
		varPositions := buildVarPositions(mask, len(base))
		constPositions := buildConstPositions(mask, len(base))
		varSpans, constSpans := buildMaskSpans(mask, len(base))
		return TemplateDef{
			Kind:           TemplateMask,
			Mask:           mask,
			Base:           base,
			VarPositions:   varPositions,
			ConstPositions: constPositions,
			maskVarSpans:   varSpans,
			maskConstSpans: constSpans,
		}, nil
	default:
		return TemplateDef{}, ErrCorruptTemplateDef
	}
}

func buildVarPositions(mask []byte, total int) []uint16 {
	if len(mask) == 0 || total <= 0 {
		return nil
	}
	out := make([]uint16, 0)
	for i := 0; i < total; i++ {
		if mask[i/8]&(1<<uint(i%8)) != 0 {
			out = append(out, uint16(i))
		}
	}
	return out
}

func buildConstPositions(mask []byte, total int) []uint16 {
	if len(mask) == 0 || total <= 0 {
		return nil
	}
	out := make([]uint16, 0)
	for i := 0; i < total; i++ {
		if mask[i/8]&(1<<uint(i%8)) == 0 {
			out = append(out, uint16(i))
		}
	}
	return out
}

func buildMaskFromPositions(positions []uint16, total int) []byte {
	if total <= 0 {
		return nil
	}
	maskLen := (total + 7) / 8
	mask := make([]byte, maskLen)
	for _, pos := range positions {
		if int(pos) >= total {
			continue
		}
		mask[pos/8] |= 1 << uint(pos%8)
	}
	return mask
}

func validateVarPositions(base []byte, mask []byte, positions []uint16) error {
	if len(base) == 0 {
		return ErrCorruptTemplateDef
	}
	if len(mask) == 0 {
		return ErrCorruptTemplateDef
	}
	prev := uint16(0)
	for i, pos := range positions {
		if int(pos) >= len(base) {
			return ErrCorruptTemplateDef
		}
		if i > 0 && pos <= prev {
			return ErrCorruptTemplateDef
		}
		if mask[pos/8]&(1<<uint(pos%8)) == 0 {
			return ErrCorruptTemplateDef
		}
		prev = pos
	}
	return nil
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
