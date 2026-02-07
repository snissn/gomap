package caching

import (
	"math"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type vlogCompressionMode uint8

const (
	vlogCompressionDefault vlogCompressionMode = iota
	vlogCompressionOff
	vlogCompressionBlock
	vlogCompressionDict
	vlogCompressionAuto
)

type vlogAutoPolicy uint8

const (
	vlogAutoBalanced vlogAutoPolicy = iota
	vlogAutoThroughput
	vlogAutoSize
)

type vlogCompressionWriteMode uint8

const (
	vlogWriteOff vlogCompressionWriteMode = iota
	vlogWriteBlock
	vlogWriteDict
)

const (
	defaultVlogHoldBytes      = 64 << 20
	defaultVlogProbeBytes     = 8 << 20
	defaultVlogModeDwellBytes = 4 << 20
)

func normalizeVlogCompressionMode(v uint8) vlogCompressionMode {
	switch vlogCompressionMode(v) {
	case vlogCompressionDefault, vlogCompressionOff, vlogCompressionBlock, vlogCompressionDict, vlogCompressionAuto:
		return vlogCompressionMode(v)
	default:
		return vlogCompressionDefault
	}
}

func normalizeVlogAutoPolicy(v uint8) vlogAutoPolicy {
	switch vlogAutoPolicy(v) {
	case vlogAutoThroughput, vlogAutoBalanced, vlogAutoSize:
		return vlogAutoPolicy(v)
	default:
		return vlogAutoBalanced
	}
}

func normalizeVlogBlockCodec(v uint8) valuelog.BlockCodec {
	switch v {
	case 0:
		return valuelog.BlockCodecSnappy
	case 1:
		return valuelog.BlockCodecLZ4
	default:
		return valuelog.BlockCodecSnappy
	}
}

func autoDictWinThreshold(policy vlogAutoPolicy) float64 {
	switch policy {
	case vlogAutoThroughput:
		return 0.15
	case vlogAutoSize:
		return 0.05
	default:
		return 0.10
	}
}

type vlogCompressionSelector struct {
	mu sync.Mutex

	policy     vlogAutoPolicy
	holdBytes  uint64
	probeBytes uint64
	dwellBytes uint64

	currentMode vlogCompressionWriteMode
	modeBytes   uint64

	holdRemaining  uint64
	probeRemaining uint64

	incompressibleStreak uint8

	blockRatio   float64
	dictRatio    float64
	blockSamples uint64
	dictSamples  uint64
}

func newVlogCompressionSelector(policy vlogAutoPolicy, holdBytes, probeBytes uint64) *vlogCompressionSelector {
	if holdBytes == 0 {
		holdBytes = defaultVlogHoldBytes
	}
	if probeBytes == 0 {
		probeBytes = defaultVlogProbeBytes
	}
	if probeBytes > holdBytes {
		probeBytes = holdBytes
	}
	return &vlogCompressionSelector{
		policy:      policy,
		holdBytes:   holdBytes,
		probeBytes:  probeBytes,
		dwellBytes:  defaultVlogModeDwellBytes,
		currentMode: vlogWriteBlock,
		blockRatio:  1.0,
		dictRatio:   1.0,
	}
}

func (s *vlogCompressionSelector) choose(dictAvailable bool, rawPayloadBytes int) (vlogCompressionWriteMode, bool) {
	if s == nil {
		if dictAvailable {
			return vlogWriteDict, false
		}
		return vlogWriteBlock, false
	}
	if rawPayloadBytes <= 0 {
		return vlogWriteOff, false
	}
	rawBytes := uint64(rawPayloadBytes)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.holdRemaining > 0 {
		if rawBytes < s.holdRemaining {
			s.holdRemaining -= rawBytes
		} else {
			s.holdRemaining = 0
		}
		if s.holdRemaining > 0 {
			if s.probeBytes == 0 {
				return vlogWriteOff, false
			}
			if s.probeRemaining <= rawBytes {
				nextProbe := s.probeBytes
				if s.holdRemaining > 0 && nextProbe > s.holdRemaining {
					nextProbe = s.holdRemaining
				}
				s.probeRemaining = nextProbe
				mode := vlogWriteBlock
				if dictAvailable && s.preferredMode(dictAvailable) == vlogWriteDict {
					mode = vlogWriteDict
				}
				return mode, true
			}
			s.probeRemaining -= rawBytes
			return vlogWriteOff, false
		}
	}

	target := s.preferredMode(dictAvailable)
	if s.currentMode != target {
		if s.modeBytes < s.dwellBytes && s.modeBytes+rawBytes < s.dwellBytes {
			target = s.currentMode
		} else {
			s.currentMode = target
			s.modeBytes = 0
		}
	}
	s.modeBytes += rawBytes
	return target, false
}

func (s *vlogCompressionSelector) preferredMode(dictAvailable bool) vlogCompressionWriteMode {
	const incompressibleRatio = 0.98
	if s.blockSamples > 0 && s.blockRatio >= incompressibleRatio {
		if !dictAvailable || (s.dictSamples > 0 && s.dictRatio >= incompressibleRatio) {
			return vlogWriteOff
		}
	}
	if !dictAvailable {
		return vlogWriteBlock
	}
	if s.dictSamples == 0 {
		return vlogWriteBlock
	}
	if s.blockSamples == 0 {
		if s.dictRatio < incompressibleRatio {
			return vlogWriteDict
		}
		return vlogWriteBlock
	}
	if s.blockRatio-s.dictRatio >= autoDictWinThreshold(s.policy) {
		return vlogWriteDict
	}
	return vlogWriteBlock
}

func (s *vlogCompressionSelector) observe(mode vlogCompressionWriteMode, rawPayloadBytes, storedPayloadBytes int, probe bool) {
	if s == nil || rawPayloadBytes <= 0 {
		return
	}
	ratio := 1.0
	if storedPayloadBytes > 0 {
		ratio = float64(storedPayloadBytes) / float64(rawPayloadBytes)
	}
	if ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		ratio = 1.0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch mode {
	case vlogWriteBlock:
		s.blockRatio, s.blockSamples = ewmaRatio(s.blockRatio, s.blockSamples, ratio)
	case vlogWriteDict:
		s.dictRatio, s.dictSamples = ewmaRatio(s.dictRatio, s.dictSamples, ratio)
	default:
		return
	}

	const incompressibleRatio = 0.98
	if probe && ratio < incompressibleRatio {
		s.incompressibleStreak = 0
		s.holdRemaining = 0
		s.probeRemaining = s.probeBytes
		return
	}
	if ratio >= incompressibleRatio {
		if s.incompressibleStreak < 0xff {
			s.incompressibleStreak++
		}
		if s.incompressibleStreak >= 2 && s.holdBytes > 0 {
			s.holdRemaining = s.holdBytes
			nextProbe := s.probeBytes
			if nextProbe == 0 || nextProbe > s.holdRemaining {
				nextProbe = s.holdRemaining
			}
			s.probeRemaining = nextProbe
			s.currentMode = vlogWriteOff
			s.modeBytes = 0
		}
		return
	}
	s.incompressibleStreak = 0
}

func (s *vlogCompressionSelector) blockObservedRatio() float64 {
	if s == nil {
		return 1.0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blockSamples == 0 || s.blockRatio <= 0 || math.IsNaN(s.blockRatio) || math.IsInf(s.blockRatio, 0) {
		return 1.0
	}
	return s.blockRatio
}

func ewmaRatio(prev float64, samples uint64, sample float64) (float64, uint64) {
	if sample <= 0 || math.IsNaN(sample) || math.IsInf(sample, 0) {
		sample = 1.0
	}
	if samples == 0 || prev <= 0 || math.IsNaN(prev) || math.IsInf(prev, 0) {
		return sample, 1
	}
	const alpha = 0.2
	return prev*(1-alpha) + sample*alpha, samples + 1
}

func (db *DB) resolveVlogWriteMode(l *lane, dictID uint64, rawPayloadBytes int) (vlogCompressionWriteMode, bool) {
	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	switch mode {
	case vlogCompressionOff:
		return vlogWriteOff, false
	case vlogCompressionBlock:
		return vlogWriteBlock, false
	case vlogCompressionDict:
		if dictID != 0 {
			return vlogWriteDict, false
		}
		return vlogWriteOff, false
	case vlogCompressionDefault:
		if dictID != 0 {
			return vlogWriteDict, false
		}
		return vlogWriteOff, false
	default:
		if l == nil || l.vlogCompressionSelector == nil {
			if dictID != 0 {
				return vlogWriteDict, false
			}
			return vlogWriteBlock, false
		}
		return l.vlogCompressionSelector.choose(dictID != 0, rawPayloadBytes)
	}
}

func (db *DB) observeVlogWriteMode(l *lane, mode vlogCompressionWriteMode, rawPayloadBytes, storedPayloadBytes int, probe bool) {
	if db == nil || l == nil {
		return
	}
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) != vlogCompressionAuto {
		return
	}
	if l.vlogCompressionSelector == nil {
		return
	}
	l.vlogCompressionSelector.observe(mode, rawPayloadBytes, storedPayloadBytes, probe)
}

func (db *DB) chooseValueLogBlockWriteK(l *lane, records, rawPayloadBytes int) int {
	if records <= 1 {
		return 1
	}
	ratio := 1.0
	if l != nil && l.vlogCompressionSelector != nil {
		ratio = l.vlogCompressionSelector.blockObservedRatio()
	}
	k := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, db.valueLogBlockTargetBytes, ratio)
	if k < 1 {
		k = 1
	}
	if k > valuelog.MaxFrameK {
		k = valuelog.MaxFrameK
	}
	return k
}
