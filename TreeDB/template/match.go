package template

import (
	"bytes"
	"encoding/binary"
	"math/bits"
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
	rawLen := len(value)
	maskLenFull := (len(def.Base) + 7) / 8
	varMask := def.Mask
	if len(varMask) != maskLenFull {
		varMask = nil
	}
	if varMask == nil {
		maxLen := payloadHeader + uvarintLen(templateID) + maskLenFull + rawLen
		full := make([]byte, maxLen)
		full[0] = magic0
		full[1] = magic1
		full[2] = payloadVer
		full[3] = flagEncoded | flagMask | flagMaskFull
		off := payloadHeader
		off += binary.PutUvarint(full[off:], templateID)
		maskOff := off
		diffOff := maskOff + maskLenFull
		idx := 0
		for i := 0; i < len(def.Base); i++ {
			if value[i] == def.Base[i] {
				continue
			}
			full[maskOff+i/8] |= 1 << uint(i%8)
			full[diffOff+idx] = value[i]
			idx++
		}
		fullLen := diffOff + idx
		if rawLen-fullLen < cfg.MinSavingsBytes {
			return nil, fullLen, reasonMatchExpectedSavings, false
		}
		payload = full[:fullLen]
		encLen = fullLen
		return payload, encLen, "", true
	}

	varCount := 0
	if len(def.VarPositions) > 0 {
		varCount = len(def.VarPositions)
	} else {
		for _, b := range varMask {
			varCount += bits.OnesCount8(b)
		}
	}
	if len(def.VarPositions) > 0 && len(def.ConstPositions) > 0 {
		constMismatch := false
		for _, pos := range def.ConstPositions {
			if value[pos] != def.Base[pos] {
				constMismatch = true
				break
			}
		}
		varBytes := make([]byte, varCount)
		diffCount := 0
		for i, pos := range def.VarPositions {
			b := value[pos]
			varBytes[i] = b
			if b != def.Base[pos] {
				diffCount++
			}
		}
		if !constMismatch {
			sparseLen := payloadHeader + uvarintLen(templateID) + varCount
			fullLen := payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
			useSparse := sparseLen <= fullLen
			bestLen := fullLen
			if useSparse {
				bestLen = sparseLen
			}
			if rawLen-bestLen < cfg.MinSavingsBytes {
				return nil, bestLen, reasonMatchExpectedSavings, false
			}
			if useSparse {
				out := make([]byte, sparseLen)
				out[0] = magic0
				out[1] = magic1
				out[2] = payloadVer
				out[3] = flagEncoded | flagMask
				off := payloadHeader
				off += binary.PutUvarint(out[off:], templateID)
				copy(out[off:], varBytes)
				payload = out
				encLen = sparseLen
				return payload, encLen, "", true
			}
			full := make([]byte, fullLen)
			full[0] = magic0
			full[1] = magic1
			full[2] = payloadVer
			full[3] = flagEncoded | flagMask | flagMaskFull
			off := payloadHeader
			off += binary.PutUvarint(full[off:], templateID)
			maskOff := off
			diffOff := maskOff + maskLenFull
			idx := 0
			for _, pos := range def.VarPositions {
				if value[pos] == def.Base[pos] {
					continue
				}
				full[maskOff+int(pos)/8] |= 1 << uint(pos%8)
				full[diffOff+idx] = value[pos]
				idx++
			}
			payload = full
			encLen = fullLen
			return payload, encLen, "", true
		}
	}
	varBytes := make([]byte, varCount)
	varIdx := 0
	diffCount := 0
	constMismatch := false
	for i := 0; i < len(def.Base); i++ {
		if varMask[i/8]&(1<<uint(i%8)) != 0 {
			varBytes[varIdx] = value[i]
			varIdx++
		}
		if value[i] != def.Base[i] {
			diffCount++
			if varMask[i/8]&(1<<uint(i%8)) == 0 {
				constMismatch = true
			}
		}
	}
	fullLen := payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
	sparseLen := payloadHeader + uvarintLen(templateID) + varCount
	useSparse := !constMismatch && sparseLen <= fullLen
	bestLen := fullLen
	if useSparse {
		bestLen = sparseLen
	}
	if rawLen-bestLen < cfg.MinSavingsBytes {
		return nil, bestLen, reasonMatchExpectedSavings, false
	}
	if useSparse {
		out := make([]byte, sparseLen)
		out[0] = magic0
		out[1] = magic1
		out[2] = payloadVer
		out[3] = flagEncoded | flagMask
		off := payloadHeader
		off += binary.PutUvarint(out[off:], templateID)
		copy(out[off:], varBytes)
		payload = out
		encLen = sparseLen
		return payload, encLen, "", true
	}
	full := make([]byte, fullLen)
	full[0] = magic0
	full[1] = magic1
	full[2] = payloadVer
	full[3] = flagEncoded | flagMask | flagMaskFull
	off := payloadHeader
	off += binary.PutUvarint(full[off:], templateID)
	maskOff := off
	diffOff := maskOff + maskLenFull
	idx := 0
	for i := 0; i < len(def.Base); i++ {
		if value[i] == def.Base[i] {
			continue
		}
		full[maskOff+i/8] |= 1 << uint(i%8)
		full[diffOff+idx] = value[i]
		idx++
	}
	payload = full
	encLen = fullLen
	return payload, encLen, "", true
}
