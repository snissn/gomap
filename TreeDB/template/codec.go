package template

import (
	"encoding/binary"
	"errors"
)

const (
	magic0        = 'T'
	magic1        = 'M'
	payloadVer    = 1
	flagEncoded   = 1 << 0
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

// DecodePayload decodes a TemplateValue payload using the provided lookup.
// If payload is not template-encoded, it is returned as-is.
func DecodePayload(payload []byte, lookup func(id uint64) ([]byte, error), opts DecodeOptions) ([]byte, error) {
	if len(payload) == 0 || !IsEncodedPayload(payload) {
		return payload, nil
	}
	if lookup == nil {
		return nil, ErrMissingTemplate
	}
	off := payloadHeader
	id, n := binary.Uvarint(payload[off:])
	if n <= 0 {
		return nil, ErrCorrupt
	}
	off += n
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
