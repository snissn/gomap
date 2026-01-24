package template

import (
	"bytes"
	"encoding/binary"
)

func minEncodedLen(rawLen int, gapCount int, anchorBytes int, templateID uint64) int {
	// header + templateID + gapCount + per-gap length varints + raw minus anchors
	if rawLen < anchorBytes {
		return rawLen
	}
	base := payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(gapCount))
	base += gapCount // minimum 1 byte per gap length
	return base + (rawLen - anchorBytes)
}

func encodedLen(gaps [][]byte, templateID uint64) int {
	gapCount := len(gaps)
	if gapCount == 0 {
		return 0
	}
	length := payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(gapCount))
	for _, g := range gaps {
		length += uvarintLen(uint64(len(g))) + len(g)
	}
	return length
}

func matchTemplate(value []byte, anchors [][]byte, templateID uint64, cfg Config) (gaps [][]byte, encLen int, reason string, matched bool) {
	rawLen := len(value)
	gapCount := len(anchors) + 1
	if cfg.MaxGaps > 0 && gapCount > cfg.MaxGaps {
		return nil, 0, reasonKeepBounds, false
	}
	anchorBytes := 0
	for _, a := range anchors {
		anchorBytes += len(a)
	}
	minEnc := minEncodedLen(rawLen, gapCount, anchorBytes, templateID)
	if rawLen-minEnc < cfg.MinSavingsBytes {
		return nil, 0, reasonMatchExpectedSavings, false
	}
	if cfg.MaxDecodedBytes > 0 && rawLen > cfg.MaxDecodedBytes {
		return nil, 0, reasonKeepBounds, false
	}

	gaps = make([][]byte, 0, gapCount)
	pos := 0
	ops := 0
	for _, a := range anchors {
		if cfg.MaxAnchorSearchOps > 0 && ops >= cfg.MaxAnchorSearchOps {
			return nil, 0, reasonMatchOpsCap, false
		}
		idx := bytes.Index(value[pos:], a)
		ops++
		if idx < 0 {
			return nil, 0, reasonMatchMissingAnchor, false
		}
		start := pos + idx
		if start < pos {
			return nil, 0, reasonMatchOverlap, false
		}
		gaps = append(gaps, value[pos:start])
		pos = start + len(a)
		if pos > len(value) {
			return nil, 0, reasonMatchOverlap, false
		}
	}
	gaps = append(gaps, value[pos:])
	encLen = encodedLen(gaps, templateID)
	if rawLen-encLen < cfg.MinSavingsBytes {
		return gaps, encLen, reasonKeepNoSavings, true
	}
	if cfg.MaxDecodedBytes > 0 && rawLen > cfg.MaxDecodedBytes {
		return gaps, encLen, reasonKeepBounds, true
	}
	return gaps, encLen, "", true
}

func matchMaskTemplate(value []byte, def TemplateDef, templateID uint64, cfg Config) (payload []byte, encLen int, reason string, matched bool) {
	if def.Kind != TemplateMask || len(def.Base) == 0 {
		return nil, 0, reasonMatchMissingAnchor, false
	}
	if len(value) != len(def.Base) {
		return nil, 0, reasonMatchMissingAnchor, false
	}
	if cfg.MaxDecodedBytes > 0 && len(value) > cfg.MaxDecodedBytes {
		return nil, 0, reasonKeepBounds, false
	}
	maskLen := len(def.Mask)
	if maskLen == 0 {
		maskLen = (len(def.Base) + 7) / 8
	}
	maxLen := payloadHeader + uvarintLen(templateID) + maskLen + len(value)
	out := make([]byte, maxLen)
	out[0] = magic0
	out[1] = magic1
	out[2] = payloadVer
	out[3] = flagEncoded | flagMask
	off := payloadHeader
	off += binary.PutUvarint(out[off:], templateID)
	maskOff := off
	diffOff := maskOff + maskLen
	idx := 0
	for i := 0; i < len(def.Base); i++ {
		if value[i] == def.Base[i] {
			continue
		}
		out[maskOff+i/8] |= 1 << uint(i%8)
		out[diffOff+idx] = value[i]
		idx++
	}
	encLen = diffOff + idx
	if len(value)-encLen < cfg.MinSavingsBytes {
		return nil, encLen, reasonMatchExpectedSavings, false
	}
	payload = out[:encLen]
	return payload, encLen, "", true
}
