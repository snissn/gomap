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

// matchAndEncodeAnchor matches value against anchors in a single pass and
// encodes directly into the output payload with zero intermediate [][]byte
// slice allocation.
func matchAndEncodeAnchor(value []byte, def TemplateDef, templateID uint64, cfg Config, minSavings int) (payload []byte, encLen int, matched bool) {
	anchors := def.Anchors
	rawLen := len(value)
	gapCount := len(anchors) + 1
	if cfg.MaxGaps > 0 && gapCount > cfg.MaxGaps {
		return nil, 0, false
	}
	if cfg.MaxDecodedBytes > 0 && rawLen > cfg.MaxDecodedBytes {
		return nil, 0, false
	}
	anchorBytes := def.anchorBytes
	if anchorBytes == 0 {
		for _, a := range anchors {
			anchorBytes += len(a)
		}
	}
	if minSavings < cfg.MinSavingsBytes {
		minSavings = cfg.MinSavingsBytes
	}
	minEnc := minEncodedLen(rawLen, gapCount, anchorBytes, templateID)
	if rawLen-minEnc < minSavings {
		return nil, 0, false
	}
	var stackSpans [64]span
	var spans []span
	if gapCount <= len(stackSpans) {
		spans = stackSpans[:gapCount]
	} else {
		spans = make([]span, gapCount)
	}
	encLen = payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(gapCount))
	pos := 0
	ops := 0
	for i, a := range anchors {
		if cfg.MaxAnchorSearchOps > 0 && ops >= cfg.MaxAnchorSearchOps {
			return nil, 0, false
		}
		idx := bytes.Index(value[pos:], a)
		ops++
		if idx < 0 {
			return nil, 0, false
		}
		start := pos + idx
		gapLen := start - pos
		spans[i] = span{start: pos, end: start}
		encLen += uvarintLen(uint64(gapLen)) + gapLen
		pos = start + len(a)
	}
	tailLen := rawLen - pos
	spans[gapCount-1] = span{start: pos, end: rawLen}
	encLen += uvarintLen(uint64(tailLen)) + tailLen
	if rawLen-encLen < minSavings {
		return nil, encLen, true
	}
	out := make([]byte, encLen)
	out[0] = magic0
	out[1] = magic1
	out[2] = payloadVer
	out[3] = flagEncoded
	off := payloadHeader
	off += binary.PutUvarint(out[off:], templateID)
	off += binary.PutUvarint(out[off:], uint64(gapCount))
	for _, sp := range spans {
		g := value[sp.start:sp.end]
		off += binary.PutUvarint(out[off:], uint64(len(g)))
		copy(out[off:], g)
		off += len(g)
	}
	return out, encLen, true
}

func matchTemplateLen(value []byte, anchors [][]byte, templateID uint64, cfg Config) (encLen int, reason string, matched bool) {
	rawLen := len(value)
	gapCount := len(anchors) + 1
	if cfg.MaxGaps > 0 && gapCount > cfg.MaxGaps {
		return 0, reasonKeepBounds, false
	}
	anchorBytes := 0
	for _, a := range anchors {
		anchorBytes += len(a)
	}
	minEnc := minEncodedLen(rawLen, gapCount, anchorBytes, templateID)
	if rawLen-minEnc < cfg.MinSavingsBytes {
		return 0, reasonMatchExpectedSavings, false
	}
	if cfg.MaxDecodedBytes > 0 && rawLen > cfg.MaxDecodedBytes {
		return 0, reasonKeepBounds, false
	}

	encLen = payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(gapCount))
	pos := 0
	ops := 0
	for _, a := range anchors {
		if cfg.MaxAnchorSearchOps > 0 && ops >= cfg.MaxAnchorSearchOps {
			return 0, reasonMatchOpsCap, false
		}
		idx := bytes.Index(value[pos:], a)
		ops++
		if idx < 0 {
			return 0, reasonMatchMissingAnchor, false
		}
		start := pos + idx
		if start < pos {
			return 0, reasonMatchOverlap, false
		}
		gapLen := start - pos
		encLen += uvarintLen(uint64(gapLen)) + gapLen
		pos = start + len(a)
		if pos > len(value) {
			return 0, reasonMatchOverlap, false
		}
	}
	gapLen := len(value) - pos
	encLen += uvarintLen(uint64(gapLen)) + gapLen
	if rawLen-encLen < cfg.MinSavingsBytes {
		return encLen, reasonKeepNoSavings, true
	}
	if cfg.MaxDecodedBytes > 0 && rawLen > cfg.MaxDecodedBytes {
		return encLen, reasonKeepBounds, true
	}
	return encLen, "", true
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

// matchAndEncodeMask matches value against a mask template and encodes it in a
// single pass, using SIMD bytes.Equal on constant spans, direct copy on
// variable spans when sparse is guaranteed optimal (varCount <= maskLenFull),
// and word-at-a-time uint64 XOR scanning on full-mask fallback.
func matchAndEncodeMask(value []byte, def TemplateDef, templateID uint64, cfg Config, minSavings int) (payload []byte, encLen int, sparse bool, matched bool) {
	if def.Kind != TemplateMask || len(def.Base) == 0 || len(value) != len(def.Base) {
		return nil, 0, false, false
	}
	if cfg.MaxDecodedBytes > 0 && len(value) > cfg.MaxDecodedBytes {
		return nil, 0, false, false
	}
	rawLen := len(value)
	idLen := uvarintLen(templateID)
	maskLenFull := (rawLen + 7) / 8
	varMask := def.Mask
	if len(varMask) != maskLenFull {
		varMask = nil
	}
	if minSavings < cfg.MinSavingsBytes {
		minSavings = cfg.MinSavingsBytes
	}

	if varMask != nil && (len(def.maskConstSpans) > 0 || len(def.ConstPositions) > 0) {
		constMismatch := false
		if len(def.maskConstSpans) > 0 {
			for _, s := range def.maskConstSpans {
				if !bytes.Equal(value[s.start:s.end], def.Base[s.start:s.end]) {
					constMismatch = true
					break
				}
			}
		} else {
			for _, pos := range def.ConstPositions {
				if value[pos] != def.Base[pos] {
					constMismatch = true
					break
				}
			}
		}
		if !constMismatch {
			varCount := len(def.VarPositions)
			if varCount == 0 {
				for _, b := range varMask {
					varCount += bits.OnesCount8(b)
				}
			}
			sparseLen := payloadHeader + idLen + varCount
			// When varCount <= maskLenFull, sparseLen <= fullLen = hdr + maskLenFull + diffCount
			// ALWAYS holds regardless of diffCount — skip scanning VarPositions entirely!
			if varCount <= maskLenFull {
				if rawLen-sparseLen < cfg.MinSavingsBytes {
					return nil, sparseLen, true, false
				}
				if rawLen-sparseLen < minSavings {
					return nil, sparseLen, true, true
				}
				out := make([]byte, sparseLen)
				out[0] = magic0
				out[1] = magic1
				out[2] = payloadVer
				out[3] = flagEncoded | flagMask
				off := payloadHeader
				off += binary.PutUvarint(out[off:], templateID)
				if len(def.maskVarSpans) > 0 {
					for _, s := range def.maskVarSpans {
						off += copy(out[off:], value[s.start:s.end])
					}
				} else if len(def.VarPositions) > 0 {
					for i, pos := range def.VarPositions {
						out[off+i] = value[pos]
					}
				} else {
					idx := 0
					for i := 0; i < rawLen; i++ {
						if varMask[i/8]&(1<<uint(i%8)) != 0 {
							out[off+idx] = value[i]
							idx++
						}
					}
				}
				return out, sparseLen, true, true
			}

			// varCount > maskLenFull: count diffs over VarPositions to pick sparse vs full.
			diffCount := 0
			for _, pos := range def.VarPositions {
				if value[pos] != def.Base[pos] {
					diffCount++
				}
			}
			fullLen := payloadHeader + idLen + maskLenFull + diffCount
			if sparseLen <= fullLen {
				if rawLen-sparseLen < cfg.MinSavingsBytes {
					return nil, sparseLen, true, false
				}
				if rawLen-sparseLen < minSavings {
					return nil, sparseLen, true, true
				}
				out := make([]byte, sparseLen)
				out[0] = magic0
				out[1] = magic1
				out[2] = payloadVer
				out[3] = flagEncoded | flagMask
				off := payloadHeader
				off += binary.PutUvarint(out[off:], templateID)
				if len(def.maskVarSpans) > 0 {
					for _, s := range def.maskVarSpans {
						off += copy(out[off:], value[s.start:s.end])
					}
				} else {
					for i, pos := range def.VarPositions {
						out[off+i] = value[pos]
					}
				}
				return out, sparseLen, true, true
			}
			if rawLen-fullLen < cfg.MinSavingsBytes {
				return nil, fullLen, false, false
			}
			if rawLen-fullLen < minSavings {
				return nil, fullLen, false, true
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
			return full, fullLen, false, true
		}
	}

	// Single-pass word-at-a-time uint64 XOR scan for full-mask encoding
	// (const-byte drift or no precomputed const spans).
	var stackMask [256]byte
	var maskBuf []byte
	if maskLenFull <= len(stackMask) {
		maskBuf = stackMask[:maskLenFull]
		clear(maskBuf)
	} else {
		maskBuf = make([]byte, maskLenFull)
	}
	var stackDiff [512]byte
	diffBuf := stackDiff[:0]
	base := def.Base
	nWords := rawLen / 8
	for wi := 0; wi < nWords; wi++ {
		off := wi * 8
		xv := binary.LittleEndian.Uint64(value[off:off+8]) ^ binary.LittleEndian.Uint64(base[off:off+8])
		if xv == 0 {
			continue
		}
		var mb byte
		for xv != 0 {
			byteIdx := bits.TrailingZeros64(xv) >> 3
			mb |= 1 << uint(byteIdx)
			diffBuf = append(diffBuf, value[off+byteIdx])
			xv &^= uint64(0xff) << uint(byteIdx*8)
		}
		maskBuf[wi] = mb
	}
	for i := nWords * 8; i < rawLen; i++ {
		if value[i] != base[i] {
			maskBuf[i/8] |= 1 << uint(i%8)
			diffBuf = append(diffBuf, value[i])
		}
	}
	fullLen := payloadHeader + idLen + maskLenFull + len(diffBuf)
	if rawLen-fullLen < cfg.MinSavingsBytes {
		return nil, fullLen, false, false
	}
	if rawLen-fullLen < minSavings {
		return nil, fullLen, false, true
	}
	full := make([]byte, fullLen)
	full[0] = magic0
	full[1] = magic1
	full[2] = payloadVer
	full[3] = flagEncoded | flagMask | flagMaskFull
	off := payloadHeader
	off += binary.PutUvarint(full[off:], templateID)
	copy(full[off:off+maskLenFull], maskBuf)
	copy(full[off+maskLenFull:], diffBuf)
	return full, fullLen, false, true
}

func matchMaskTemplateLen(value []byte, def TemplateDef, templateID uint64, cfg Config) (encLen int, sparse bool, reason string, matched bool) {
	if def.Kind != TemplateMask || len(def.Base) == 0 {
		return 0, false, reasonMatchMissingAnchor, false
	}
	if len(value) != len(def.Base) {
		return 0, false, reasonMatchMissingAnchor, false
	}
	if cfg.MaxDecodedBytes > 0 && len(value) > cfg.MaxDecodedBytes {
		return 0, false, reasonKeepBounds, false
	}
	rawLen := len(value)
	maskLenFull := (len(def.Base) + 7) / 8
	varMask := def.Mask
	if len(varMask) != maskLenFull {
		varMask = nil
	}
	if varMask == nil {
		diffCount := 0
		for i := 0; i < len(def.Base); i++ {
			if value[i] != def.Base[i] {
				diffCount++
			}
		}
		encLen = payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
		if rawLen-encLen < cfg.MinSavingsBytes {
			return encLen, false, reasonMatchExpectedSavings, false
		}
		return encLen, false, "", true
	}

	varCount := 0
	if len(def.VarPositions) > 0 {
		varCount = len(def.VarPositions)
	} else {
		for _, b := range varMask {
			varCount += bits.OnesCount8(b)
		}
	}
	if len(def.VarPositions) > 0 && (len(def.maskConstSpans) > 0 || len(def.ConstPositions) > 0) {
		constMismatch := false
		if len(def.maskConstSpans) > 0 {
			for _, s := range def.maskConstSpans {
				if !bytes.Equal(value[s.start:s.end], def.Base[s.start:s.end]) {
					constMismatch = true
					break
				}
			}
		} else {
			for _, pos := range def.ConstPositions {
				if value[pos] != def.Base[pos] {
					constMismatch = true
					break
				}
			}
		}
		if !constMismatch {
			diffCount := 0
			for _, pos := range def.VarPositions {
				if value[pos] != def.Base[pos] {
					diffCount++
				}
			}
			sparseLen := payloadHeader + uvarintLen(templateID) + varCount
			fullLen := payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
			encLen = fullLen
			sparse = false
			if sparseLen <= fullLen {
				encLen = sparseLen
				sparse = true
			}
			if rawLen-encLen < cfg.MinSavingsBytes {
				return encLen, sparse, reasonMatchExpectedSavings, false
			}
			return encLen, sparse, "", true
		}
	}

	diffCount := 0
	constMismatch := false
	for i := 0; i < len(def.Base); i++ {
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
	encLen = fullLen
	sparse = false
	if useSparse {
		encLen = sparseLen
		sparse = true
	}
	if rawLen-encLen < cfg.MinSavingsBytes {
		return encLen, sparse, reasonMatchExpectedSavings, false
	}
	return encLen, sparse, "", true
}

func encodeMaskTemplate(value []byte, def TemplateDef, templateID uint64, useSparse bool) []byte {
	if def.Kind != TemplateMask || len(def.Base) == 0 {
		return nil
	}
	if len(value) != len(def.Base) {
		return nil
	}
	maskLenFull := (len(def.Base) + 7) / 8
	varMask := def.Mask
	if len(varMask) != maskLenFull {
		varMask = nil
	}
	if useSparse && varMask != nil {
		varCount := 0
		if len(def.VarPositions) > 0 {
			varCount = len(def.VarPositions)
		} else {
			for _, b := range varMask {
				varCount += bits.OnesCount8(b)
			}
		}
		payloadLen := payloadHeader + uvarintLen(templateID) + varCount
		out := make([]byte, payloadLen)
		out[0] = magic0
		out[1] = magic1
		out[2] = payloadVer
		out[3] = flagEncoded | flagMask
		off := payloadHeader
		off += binary.PutUvarint(out[off:], templateID)
		if len(def.maskVarSpans) > 0 {
			dstOff := off
			for _, s := range def.maskVarSpans {
				dstOff += copy(out[dstOff:], value[s.start:s.end])
			}
		} else if len(def.VarPositions) > 0 {
			for i, pos := range def.VarPositions {
				out[off+i] = value[pos]
			}
		} else {
			idx := 0
			for i := 0; i < len(value); i++ {
				if varMask[i/8]&(1<<uint(i%8)) == 0 {
					continue
				}
				out[off+idx] = value[i]
				idx++
			}
		}
		return out
	}
	if varMask != nil && len(def.VarPositions) > 0 && (len(def.maskConstSpans) > 0 || len(def.ConstPositions) > 0) {
		constMismatch := false
		if len(def.maskConstSpans) > 0 {
			for _, s := range def.maskConstSpans {
				if !bytes.Equal(value[s.start:s.end], def.Base[s.start:s.end]) {
					constMismatch = true
					break
				}
			}
		} else {
			for _, pos := range def.ConstPositions {
				if value[pos] != def.Base[pos] {
					constMismatch = true
					break
				}
			}
		}
		if !constMismatch {
			diffCount := 0
			for _, pos := range def.VarPositions {
				if value[pos] != def.Base[pos] {
					diffCount++
				}
			}
			fullLen := payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
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
			return full
		}
	}
	diffCount := 0
	for i := 0; i < len(def.Base); i++ {
		if value[i] != def.Base[i] {
			diffCount++
		}
	}
	fullLen := payloadHeader + uvarintLen(templateID) + maskLenFull + diffCount
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
	return full
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
