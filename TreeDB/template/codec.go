package template

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

const (
	magic0        = 'T'
	magic1        = 'M'
	payloadVer    = 1
	flagEncoded   = 1 << 0
	flagMask      = 1 << 1
	flagMaskFull  = 1 << 2
	payloadHeader = 4
)

var (
	ErrCorrupt         = errors.New("template: corrupt payload")
	ErrMissingTemplate = errors.New("template: missing template")
)

// IsEncodedPayload reports whether payload looks like a template-encoded value.
func IsEncodedPayload(payload []byte) bool {
	return len(payload) >= payloadHeader &&
		payload[0] == magic0 &&
		payload[1] == magic1 &&
		payload[2] == payloadVer &&
		payload[3]&flagEncoded != 0
}

// EncodePayload builds a TemplateValue payload from template ID and gaps.
func EncodePayload(templateID uint64, gaps [][]byte) ([]byte, error) {
	gapCount := len(gaps)
	if gapCount == 0 {
		return nil, ErrCorrupt
	}
	payloadLen := payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(gapCount))
	for _, g := range gaps {
		payloadLen += uvarintLen(uint64(len(g))) + len(g)
	}
	out := make([]byte, payloadLen)
	out[0] = magic0
	out[1] = magic1
	out[2] = payloadVer
	out[3] = flagEncoded
	off := payloadHeader
	out = out[:payloadLen]
	off += binary.PutUvarint(out[off:], templateID)
	off += binary.PutUvarint(out[off:], uint64(gapCount))
	for _, g := range gaps {
		off += binary.PutUvarint(out[off:], uint64(len(g)))
		copy(out[off:], g)
		off += len(g)
	}
	if off != payloadLen {
		return nil, ErrCorrupt
	}
	return out, nil
}

// EncodeMaskPayload builds a TemplateValue payload for mask templates.
func EncodeMaskPayload(templateID uint64, mask []byte, vars []byte) ([]byte, error) {
	payloadLen := payloadHeader + uvarintLen(templateID) + len(mask) + len(vars)
	out := make([]byte, payloadLen)
	out[0] = magic0
	out[1] = magic1
	out[2] = payloadVer
	out[3] = flagEncoded | flagMask | flagMaskFull
	off := payloadHeader
	off += binary.PutUvarint(out[off:], templateID)
	copy(out[off:], mask)
	off += len(mask)
	copy(out[off:], vars)
	off += len(vars)
	if off != payloadLen {
		return nil, ErrCorrupt
	}
	return out, nil
}

// DecodePayload decodes a TemplateValue payload using the provided lookup.
// If payload is not template-encoded, it is returned as-is.
func DecodePayload(payload []byte, lookup func(id uint64) ([]byte, error), opts DecodeOptions) ([]byte, error) {
	if len(payload) == 0 || !IsEncodedPayload(payload) {
		return payload, nil
	}
	if lookup == nil {
		return nil, ErrMissingTemplate
	}
	isMask := payload[3]&flagMask != 0
	isFull := payload[3]&flagMaskFull != 0
	off := payloadHeader
	id, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return nil, ErrCorrupt
	}
	off += n
	if isMask {
		defBytes, err := lookup(id)
		if err != nil {
			return nil, err
		}
		def, err := DecodeTemplateDef(defBytes)
		if err != nil {
			return nil, err
		}
		if def.Kind != TemplateMask {
			return nil, ErrCorrupt
		}
		decodedLen := len(def.Base)
		if opts.MaxDecodedBytes > 0 && decodedLen > opts.MaxDecodedBytes {
			return nil, ErrCorrupt
		}
		if isFull {
			maskLen := (decodedLen + 7) / 8
			if off+maskLen > len(payload) {
				return nil, ErrCorrupt
			}
			mask := payload[off : off+maskLen]
			off += maskLen
			varBytes := payload[off:]
			diffCount := 0
			for _, b := range mask {
				diffCount += bits.OnesCount8(b)
			}
			if diffCount != len(varBytes) {
				return nil, ErrCorrupt
			}
			out := make([]byte, decodedLen)
			copy(out, def.Base)
			idx := 0
			for i := 0; i < decodedLen; i++ {
				if mask[i/8]&(1<<uint(i%8)) != 0 {
					out[i] = varBytes[idx]
					idx++
				}
			}
			if idx != len(varBytes) {
				return nil, ErrCorrupt
			}
			return out, nil
		}
		if len(def.Mask) == 0 {
			return nil, ErrCorrupt
		}
		varCount := 0
		for _, b := range def.Mask {
			varCount += bits.OnesCount8(b)
		}
		if off+varCount != len(payload) {
			return nil, ErrCorrupt
		}
		varBytes := payload[off:]
		out := make([]byte, decodedLen)
		copy(out, def.Base)
		idx := 0
		for i := 0; i < decodedLen; i++ {
			if def.Mask[i/8]&(1<<uint(i%8)) != 0 {
				if idx >= len(varBytes) {
					return nil, ErrCorrupt
				}
				out[i] = varBytes[idx]
				idx++
			}
		}
		if idx != len(varBytes) {
			return nil, ErrCorrupt
		}
		return out, nil
	}
	gapCount64, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return nil, ErrCorrupt
	}
	off += n
	if gapCount64 == 0 {
		return nil, ErrCorrupt
	}
	if opts.MaxGaps > 0 && gapCount64 > uint64(opts.MaxGaps) {
		return nil, ErrCorrupt
	}
	gapCount := int(gapCount64)
	gaps := make([][]byte, gapCount)
	totalGapBytes := 0
	for i := 0; i < gapCount; i++ {
		gapLen64, n := binary.Uvarint(payload[off:])
		if n <= 0 {
			return nil, ErrCorrupt
		}
		off += n
		if gapLen64 > uint64(len(payload)-off) {
			return nil, ErrCorrupt
		}
		gapLen := int(gapLen64)
		gaps[i] = payload[off : off+gapLen]
		off += gapLen
		totalGapBytes += gapLen
	}
	if off != len(payload) {
		return nil, ErrCorrupt
	}
	defBytes, err := lookup(id)
	if err != nil {
		return nil, err
	}
	def, err := DecodeTemplateDef(defBytes)
	if err != nil {
		return nil, err
	}
	if def.Kind != TemplateAnchors {
		return nil, ErrCorrupt
	}
	if len(def.Anchors)+1 != gapCount {
		return nil, ErrCorrupt
	}
	anchorBytes := 0
	for _, a := range def.Anchors {
		anchorBytes += len(a)
	}
	decodedLen64 := int64(totalGapBytes) + int64(anchorBytes)
	if decodedLen64 < 0 {
		return nil, ErrCorrupt
	}
	if opts.MaxDecodedBytes > 0 && decodedLen64 > int64(opts.MaxDecodedBytes) {
		return nil, ErrCorrupt
	}
	out := make([]byte, 0, int(decodedLen64))
	for i, g := range gaps {
		out = append(out, g...)
		if i < len(def.Anchors) {
			out = append(out, def.Anchors[i]...)
		}
	}
	return out, nil
}
