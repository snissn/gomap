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

// EncodedPayloadTemplateID returns the immutable template generation selected
// by an encoded payload without resolving or decoding its definition.
func EncodedPayloadTemplateID(payload []byte) (uint64, error) {
	if !IsEncodedPayload(payload) {
		return 0, ErrCorrupt
	}
	templateID, n := binary.Uvarint(payload[payloadHeader:])
	if n <= 0 || templateID == 0 {
		return 0, ErrCorrupt
	}
	return templateID, nil
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

func grow(dst []byte, n int) []byte {
	if n <= 0 {
		return dst
	}
	oldLen := len(dst)
	newLen := oldLen + n
	if newLen < 0 {
		return dst[:0]
	}
	if cap(dst) < newLen {
		newCap := cap(dst) * 2
		if newCap < newLen {
			newCap = newLen
		}
		tmp := make([]byte, oldLen, newCap)
		copy(tmp, dst)
		dst = tmp
	}
	return dst[:newLen]
}

// DecodePayloadAppend decodes a TemplateValue payload and appends the decoded
// bytes to dst.
//
// If payload is not template-encoded, it is appended as-is.
func DecodePayloadAppend(dst, payload []byte, lookup func(id uint64) (TemplateDef, error), opts DecodeOptions) ([]byte, error) {
	if len(payload) == 0 {
		return dst, nil
	}
	if !IsEncodedPayload(payload) {
		oldLen := len(dst)
		dst = grow(dst, len(payload))
		copy(dst[oldLen:], payload)
		return dst, nil
	}
	if lookup == nil {
		return dst, ErrMissingTemplate
	}

	baseLen := len(dst)
	fail := func(err error) ([]byte, error) {
		if baseLen <= len(dst) {
			dst = dst[:baseLen]
		}
		return dst, err
	}

	isMask := payload[3]&flagMask != 0
	isFull := payload[3]&flagMaskFull != 0

	off := payloadHeader
	id, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return fail(ErrCorrupt)
	}
	off += n

	def, err := lookup(id)
	if err != nil {
		return fail(err)
	}

	if isMask {
		if def.Kind != TemplateMask {
			return fail(ErrCorrupt)
		}
		decodedLen := len(def.Base)
		if opts.MaxDecodedBytes > 0 && decodedLen > opts.MaxDecodedBytes {
			return fail(ErrCorrupt)
		}
		outStart := len(dst)
		dst = grow(dst, decodedLen)
		out := dst[outStart : outStart+decodedLen]
		copy(out, def.Base)

		if isFull {
			maskLen := (decodedLen + 7) / 8
			if off+maskLen > len(payload) {
				return fail(ErrCorrupt)
			}
			mask := payload[off : off+maskLen]
			off += maskLen
			varBytes := payload[off:]
			diffCount := 0
			for _, b := range mask {
				diffCount += bits.OnesCount8(b)
			}
			if diffCount != len(varBytes) {
				return fail(ErrCorrupt)
			}
			idx := 0
			for i := 0; i < decodedLen; i++ {
				if mask[i/8]&(1<<uint(i%8)) != 0 {
					out[i] = varBytes[idx]
					idx++
				}
			}
			if idx != len(varBytes) {
				return fail(ErrCorrupt)
			}
			return dst, nil
		}

		if len(def.Mask) == 0 {
			return fail(ErrCorrupt)
		}
		varCount := len(def.VarPositions)
		if varCount == 0 {
			for _, b := range def.Mask {
				varCount += bits.OnesCount8(b)
			}
		}
		if off+varCount != len(payload) {
			return fail(ErrCorrupt)
		}
		varBytes := payload[off:]
		if len(def.VarPositions) > 0 {
			if len(def.VarPositions) != len(varBytes) {
				return fail(ErrCorrupt)
			}
			for i, pos := range def.VarPositions {
				if int(pos) >= len(out) {
					return fail(ErrCorrupt)
				}
				out[pos] = varBytes[i]
			}
			return dst, nil
		}

		idx := 0
		for i := 0; i < decodedLen; i++ {
			if def.Mask[i/8]&(1<<uint(i%8)) != 0 {
				if idx >= len(varBytes) {
					return fail(ErrCorrupt)
				}
				out[i] = varBytes[idx]
				idx++
			}
		}
		if idx != len(varBytes) {
			return fail(ErrCorrupt)
		}
		return dst, nil
	}

	anchors := def.Anchors
	if def.Kind != TemplateAnchors && def.Kind != 0 {
		return fail(ErrCorrupt)
	}

	gapCount64, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return fail(ErrCorrupt)
	}
	off += n
	if gapCount64 == 0 {
		return fail(ErrCorrupt)
	}
	if opts.MaxGaps > 0 && gapCount64 > uint64(opts.MaxGaps) {
		return fail(ErrCorrupt)
	}
	gapCount := int(gapCount64)
	if len(anchors)+1 != gapCount {
		return fail(ErrCorrupt)
	}

	decodedBytes := 0
	maxDecoded := opts.MaxDecodedBytes
	if maxDecoded < 0 {
		maxDecoded = 0
	}
	for i := 0; i < gapCount; i++ {
		gapLen64, n := binary.Uvarint(payload[off:])
		if n <= 0 {
			return fail(ErrCorrupt)
		}
		off += n
		if gapLen64 > uint64(len(payload)-off) {
			return fail(ErrCorrupt)
		}
		gapLen := int(gapLen64)
		if maxDecoded > 0 && decodedBytes+gapLen > maxDecoded {
			return fail(ErrCorrupt)
		}
		oldLen := len(dst)
		dst = grow(dst, gapLen)
		copy(dst[oldLen:], payload[off:off+gapLen])
		off += gapLen
		decodedBytes += gapLen

		if i < len(anchors) {
			a := anchors[i]
			if maxDecoded > 0 && decodedBytes+len(a) > maxDecoded {
				return fail(ErrCorrupt)
			}
			oldLen = len(dst)
			dst = grow(dst, len(a))
			copy(dst[oldLen:], a)
			decodedBytes += len(a)
		}
	}
	if off != len(payload) {
		return fail(ErrCorrupt)
	}
	return dst, nil
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
	dst, err := DecodePayloadAppend(nil, payload, func(id uint64) (TemplateDef, error) {
		defBytes, err := lookup(id)
		if err != nil {
			return TemplateDef{}, err
		}
		return DecodeTemplateDef(defBytes)
	}, opts)
	return dst, err
}
