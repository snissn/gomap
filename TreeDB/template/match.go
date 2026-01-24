package template

import "bytes"

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

func maskMatches(base []byte, mask []byte, value []byte) bool {
	if len(value) != len(base) {
		return false
	}
	for i := 0; i < len(base); i++ {
		if mask[i/8]&(1<<uint(i%8)) != 0 {
			continue
		}
		if value[i] != base[i] {
			return false
		}
	}
	return true
}

func matchMaskTemplate(value []byte, def TemplateDef, templateID uint64, cfg Config) (vars []byte, encLen int, reason string, matched bool) {
	if def.Kind != TemplateMask || len(def.Base) == 0 {
		return nil, 0, reasonMatchMissingAnchor, false
	}
	if len(value) != len(def.Base) {
		return nil, 0, reasonMatchMissingAnchor, false
	}
	if cfg.MaxDecodedBytes > 0 && len(value) > cfg.MaxDecodedBytes {
		return nil, 0, reasonKeepBounds, false
	}
	if !maskMatches(def.Base, def.Mask, value) {
		return nil, 0, reasonMatchMissingAnchor, false
	}
	varCount := len(def.VarPositions)
	encLen = payloadHeader + uvarintLen(templateID) + uvarintLen(uint64(varCount)) + varCount
	if len(value)-encLen < cfg.MinSavingsBytes {
		return nil, encLen, reasonMatchExpectedSavings, false
	}
	vars = make([]byte, varCount)
	for i, pos := range def.VarPositions {
		vars[i] = value[pos]
	}
	if len(value)-encLen < cfg.MinSavingsBytes {
		return vars, encLen, reasonKeepNoSavings, true
	}
	return vars, encLen, "", true
}
