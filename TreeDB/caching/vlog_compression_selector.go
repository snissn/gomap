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

type vlogAutoCandidate uint8

const (
	vlogAutoCandidateOff vlogAutoCandidate = iota
	vlogAutoCandidateDict
	vlogAutoCandidateBlockSnappy
	vlogAutoCandidateBlockLZ4
)

const vlogAutoCandidateCount = int(vlogAutoCandidateBlockLZ4) + 1

const (
	defaultVlogHoldBytes      = 64 << 20
	defaultVlogProbeBytes     = 8 << 20
	defaultVlogExploreBytes   = 8 << 20
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

func normalizeSelectorBlockCodec(codec valuelog.BlockCodec) valuelog.BlockCodec {
	switch codec {
	case valuelog.BlockCodecLZ4:
		return valuelog.BlockCodecLZ4
	default:
		return valuelog.BlockCodecSnappy
	}
}

func autoSwitchMargin(policy vlogAutoPolicy) float64 {
	switch policy {
	case vlogAutoThroughput:
		return 0.08
	case vlogAutoSize:
		return 0.03
	default:
		return 0.05
	}
}

func autoSelectionMargin(policy vlogAutoPolicy) float64 {
	switch policy {
	case vlogAutoThroughput:
		return 0.01
	case vlogAutoSize:
		return 0.005
	default:
		return 0.01
	}
}

type vlogCandidateMetrics struct {
	ratio      float64
	throughput float64 // raw bytes/ns
	samples    uint64
}

type vlogCompressionSelectorStats struct {
	bytesByCandidate  [vlogAutoCandidateCount]uint64
	framesByCandidate [vlogAutoCandidateCount]uint64
	switches          [vlogAutoCandidateCount][vlogAutoCandidateCount]uint64
	probeAttempts     uint64
	probeSuccesses    uint64
	holdEnters        uint64
	holdExits         uint64
	bypassBytes       uint64
}

type vlogCompressionSelector struct {
	mu sync.Mutex

	policy       vlogAutoPolicy
	holdBytes    uint64
	probeBytes   uint64
	exploreBytes uint64
	dwellBytes   uint64

	seedCodec        valuelog.BlockCodec
	currentCandidate vlogAutoCandidate
	modeBytes        uint64

	holdRemaining    uint64
	probeRemaining   uint64
	exploreRemaining uint64

	incompressibleStreak uint8

	metrics [vlogAutoCandidateCount]vlogCandidateMetrics

	bytesByCandidate  [vlogAutoCandidateCount]uint64
	framesByCandidate [vlogAutoCandidateCount]uint64
	switches          [vlogAutoCandidateCount][vlogAutoCandidateCount]uint64
	probeAttempts     uint64
	probeSuccesses    uint64
	holdEnters        uint64
	holdExits         uint64
	bypassBytes       uint64
}

func (c vlogAutoCandidate) suffix() string {
	switch c {
	case vlogAutoCandidateOff:
		return "off"
	case vlogAutoCandidateDict:
		return "dict"
	case vlogAutoCandidateBlockSnappy:
		return "block_snappy"
	case vlogAutoCandidateBlockLZ4:
		return "block_lz4"
	default:
		return "unknown"
	}
}

func blockCandidateFromCodec(codec valuelog.BlockCodec) vlogAutoCandidate {
	switch normalizeSelectorBlockCodec(codec) {
	case valuelog.BlockCodecLZ4:
		return vlogAutoCandidateBlockLZ4
	default:
		return vlogAutoCandidateBlockSnappy
	}
}

func candidateFromWriteMode(mode vlogCompressionWriteMode, codec valuelog.BlockCodec) vlogAutoCandidate {
	switch mode {
	case vlogWriteDict:
		return vlogAutoCandidateDict
	case vlogWriteBlock:
		return blockCandidateFromCodec(codec)
	default:
		return vlogAutoCandidateOff
	}
}

func candidateWriteMode(c vlogAutoCandidate, seed valuelog.BlockCodec) (vlogCompressionWriteMode, valuelog.BlockCodec) {
	switch c {
	case vlogAutoCandidateDict:
		return vlogWriteDict, normalizeSelectorBlockCodec(seed)
	case vlogAutoCandidateBlockLZ4:
		return vlogWriteBlock, valuelog.BlockCodecLZ4
	case vlogAutoCandidateBlockSnappy:
		return vlogWriteBlock, valuelog.BlockCodecSnappy
	default:
		return vlogWriteOff, normalizeSelectorBlockCodec(seed)
	}
}

func normalizeMetricRatio(v float64) float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 1.0
	}
	if v < 0.01 {
		return 0.01
	}
	if v > 4.0 {
		return 4.0
	}
	return v
}

func normalizeMetricThroughput(v float64) float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 1.0
	}
	if v < 1e-9 {
		return 1e-9
	}
	return v
}

func scoreForPolicy(policy vlogAutoPolicy, ratio, throughputRel float64) float64 {
	ratio = normalizeMetricRatio(ratio)
	throughputRel = normalizeMetricThroughput(throughputRel)
	sizeScore := 1.0 / ratio
	switch policy {
	case vlogAutoThroughput:
		return throughputRel + 0.05*sizeScore
	case vlogAutoSize:
		return sizeScore + 0.10*throughputRel
	default:
		return 0.65*throughputRel + 0.35*sizeScore
	}
}

func (s *vlogCompressionSelector) metric(c vlogAutoCandidate) vlogCandidateMetrics {
	m := s.metrics[c]
	m.ratio = normalizeMetricRatio(m.ratio)
	m.throughput = normalizeMetricThroughput(m.throughput)
	if m.samples == 0 {
		switch c {
		case vlogAutoCandidateOff:
			m.samples = 1
			m.ratio = 1
			m.throughput = 1
		case vlogAutoCandidateDict:
			m.ratio = 1
			m.throughput = 0.9
		case vlogAutoCandidateBlockLZ4:
			m.ratio = 1
			m.throughput = 0.95
		default:
			m.ratio = 1
			m.throughput = 0.97
		}
	}
	return m
}

func (s *vlogCompressionSelector) offThroughput() float64 {
	return s.metric(vlogAutoCandidateOff).throughput
}

func (s *vlogCompressionSelector) candidateScore(c vlogAutoCandidate) float64 {
	m := s.metric(c)
	off := s.offThroughput()
	if off <= 0 {
		off = 1.0
	}
	return scoreForPolicy(s.policy, m.ratio, m.throughput/off)
}

func (s *vlogCompressionSelector) candidateLikelyBeneficial(c vlogAutoCandidate) bool {
	if c == vlogAutoCandidateOff {
		return true
	}
	m := s.metric(c)
	offThroughput := s.offThroughput()
	if offThroughput <= 0 {
		offThroughput = 1.0
	}
	if m.ratio <= 0.985 {
		return true
	}
	if m.throughput >= offThroughput*1.03 {
		return true
	}
	if m.ratio <= 0.995 && m.throughput >= offThroughput*0.99 {
		return true
	}
	return false
}

func (s *vlogCompressionSelector) availableCandidates(dictAvailable bool) []vlogAutoCandidate {
	out := make([]vlogAutoCandidate, 0, 4)
	out = append(out, vlogAutoCandidateOff, vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4)
	if dictAvailable {
		out = append(out, vlogAutoCandidateDict)
	}
	return out
}

func (s *vlogCompressionSelector) preferredCandidate(dictAvailable bool) vlogAutoCandidate {
	cands := s.availableCandidates(dictAvailable)
	best := vlogAutoCandidateOff
	bestScore := s.candidateScore(best)
	margin := autoSelectionMargin(s.policy)
	for _, c := range cands {
		if c == vlogAutoCandidateOff {
			continue
		}
		if !s.candidateLikelyBeneficial(c) {
			continue
		}
		score := s.candidateScore(c)
		if score > bestScore*(1+margin) {
			best = c
			bestScore = score
		}
	}
	return best
}

func (s *vlogCompressionSelector) preferredProbeCandidate(dictAvailable bool) vlogAutoCandidate {
	cands := s.availableCandidates(dictAvailable)
	best := vlogAutoCandidateOff
	bestScore := -1.0
	for _, c := range cands {
		if c == vlogAutoCandidateOff {
			continue
		}
		score := s.candidateScore(c)
		if score > bestScore {
			best = c
			bestScore = score
		}
	}
	if best == vlogAutoCandidateOff {
		return blockCandidateFromCodec(s.seedCodec)
	}
	return best
}

func (s *vlogCompressionSelector) preferredExplorationCandidate(dictAvailable bool) vlogAutoCandidate {
	cands := s.availableCandidates(dictAvailable)
	current := s.currentCandidate
	best := current
	bestSamples := uint64(^uint64(0))
	bestScore := -1.0
	found := false
	for _, c := range cands {
		if c == current {
			continue
		}
		m := s.metric(c)
		score := s.candidateScore(c)
		if !found || m.samples < bestSamples || (m.samples == bestSamples && score > bestScore) {
			best = c
			bestSamples = m.samples
			bestScore = score
			found = true
		}
	}
	if found {
		return best
	}
	return current
}

func (s *vlogCompressionSelector) maybeSwitch(target vlogAutoCandidate, rawBytes uint64, dictAvailable bool) vlogAutoCandidate {
	current := s.currentCandidate
	if current == vlogAutoCandidateDict && !dictAvailable {
		current = blockCandidateFromCodec(s.seedCodec)
		s.currentCandidate = current
		s.modeBytes = 0
	}
	if target == vlogAutoCandidateDict && !dictAvailable {
		target = blockCandidateFromCodec(s.seedCodec)
	}
	if current == target {
		s.modeBytes += rawBytes
		return current
	}
	if s.modeBytes < s.dwellBytes && s.modeBytes+rawBytes < s.dwellBytes {
		s.modeBytes += rawBytes
		return current
	}
	currentScore := s.candidateScore(current)
	targetScore := s.candidateScore(target)
	if targetScore <= currentScore {
		s.modeBytes += rawBytes
		return current
	}
	if currentScore > 0 && targetScore < currentScore*(1+autoSwitchMargin(s.policy)) {
		s.modeBytes += rawBytes
		return current
	}
	s.switches[current][target]++
	s.currentCandidate = target
	s.modeBytes = rawBytes
	return target
}

func (s *vlogCompressionSelector) clearHold() {
	if s.holdRemaining > 0 {
		s.holdExits++
	}
	s.holdRemaining = 0
	if s.probeBytes > 0 {
		s.probeRemaining = s.probeBytes
	} else {
		s.probeRemaining = 0
	}
	s.incompressibleStreak = 0
}

func newVlogCompressionSelectorWithSeed(policy vlogAutoPolicy, holdBytes, probeBytes uint64, seedCodec valuelog.BlockCodec) *vlogCompressionSelector {
	if holdBytes == 0 {
		holdBytes = defaultVlogHoldBytes
	}
	if probeBytes == 0 {
		probeBytes = defaultVlogProbeBytes
	}
	if probeBytes > holdBytes {
		probeBytes = holdBytes
	}
	seedCodec = normalizeSelectorBlockCodec(seedCodec)
	return &vlogCompressionSelector{
		policy:           policy,
		holdBytes:        holdBytes,
		probeBytes:       probeBytes,
		exploreBytes:     defaultVlogExploreBytes,
		dwellBytes:       defaultVlogModeDwellBytes,
		seedCodec:        seedCodec,
		currentCandidate: blockCandidateFromCodec(seedCodec),
		exploreRemaining: defaultVlogExploreBytes,
		metrics: [vlogAutoCandidateCount]vlogCandidateMetrics{
			vlogAutoCandidateOff:         {ratio: 1.0, throughput: 1.0, samples: 1},
			vlogAutoCandidateDict:        {ratio: 1.0, throughput: 0.90},
			vlogAutoCandidateBlockSnappy: {ratio: 1.0, throughput: 0.97},
			vlogAutoCandidateBlockLZ4:    {ratio: 1.0, throughput: 0.95},
		},
	}
}

func newVlogCompressionSelector(policy vlogAutoPolicy, holdBytes, probeBytes uint64) *vlogCompressionSelector {
	return newVlogCompressionSelectorWithSeed(policy, holdBytes, probeBytes, valuelog.BlockCodecSnappy)
}

func (s *vlogCompressionSelector) choose(dictAvailable bool, rawPayloadBytes int) (vlogCompressionWriteMode, valuelog.BlockCodec, bool) {
	if s == nil {
		if dictAvailable {
			return vlogWriteDict, valuelog.BlockCodecSnappy, false
		}
		return vlogWriteBlock, valuelog.BlockCodecSnappy, false
	}
	if rawPayloadBytes <= 0 {
		return vlogWriteOff, s.seedCodec, false
	}
	rawBytes := uint64(rawPayloadBytes)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.holdRemaining > 0 {
		if rawBytes < s.holdRemaining {
			s.holdRemaining -= rawBytes
		} else {
			s.holdRemaining = 0
			s.holdExits++
		}
		if s.holdRemaining > 0 {
			if s.probeBytes == 0 {
				s.bypassBytes += rawBytes
				return vlogWriteOff, s.seedCodec, false
			}
			if s.probeRemaining <= rawBytes {
				nextProbe := s.probeBytes
				if s.holdRemaining > 0 && nextProbe > s.holdRemaining {
					nextProbe = s.holdRemaining
				}
				s.probeRemaining = nextProbe
				candidate := s.preferredProbeCandidate(dictAvailable)
				s.probeAttempts++
				mode, codec := candidateWriteMode(candidate, s.seedCodec)
				return mode, codec, true
			}
			s.probeRemaining -= rawBytes
			s.bypassBytes += rawBytes
			return vlogWriteOff, s.seedCodec, false
		}
	}

	if s.exploreBytes > 0 {
		if s.exploreRemaining <= rawBytes {
			s.exploreRemaining = s.exploreBytes
			candidate := s.preferredExplorationCandidate(dictAvailable)
			if candidate != s.currentCandidate {
				s.probeAttempts++
				mode, codec := candidateWriteMode(candidate, s.seedCodec)
				return mode, codec, true
			}
		} else {
			s.exploreRemaining -= rawBytes
		}
	}

	target := s.preferredCandidate(dictAvailable)
	chosen := s.maybeSwitch(target, rawBytes, dictAvailable)
	mode, codec := candidateWriteMode(chosen, s.seedCodec)
	return mode, codec, false
}

func (s *vlogCompressionSelector) observe(mode vlogCompressionWriteMode, blockCodec valuelog.BlockCodec, rawPayloadBytes, storedPayloadBytes int, wallNs int64, probe bool) {
	if s == nil || rawPayloadBytes <= 0 {
		return
	}
	ratio := 1.0
	if storedPayloadBytes > 0 {
		ratio = float64(storedPayloadBytes) / float64(rawPayloadBytes)
	}
	ratio = normalizeMetricRatio(ratio)
	throughput := 1.0
	if wallNs > 0 {
		throughput = float64(rawPayloadBytes) / float64(wallNs)
	}
	throughput = normalizeMetricThroughput(throughput)
	candidate := candidateFromWriteMode(mode, blockCodec)

	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.metrics[candidate]
	oldSamples := m.samples
	ratioEWMA, ratioSamples := ewmaMetric(m.ratio, oldSamples, ratio)
	throughputEWMA, throughputSamples := ewmaMetric(m.throughput, oldSamples, throughput)
	m.ratio = ratioEWMA
	m.throughput = throughputEWMA
	if ratioSamples >= throughputSamples {
		m.samples = ratioSamples
	} else {
		m.samples = throughputSamples
	}
	s.metrics[candidate] = m
	s.bytesByCandidate[candidate] += uint64(rawPayloadBytes)
	s.framesByCandidate[candidate]++

	const incompressibleRatio = 0.98
	offThroughput := s.offThroughput()
	if offThroughput <= 0 {
		offThroughput = 1.0
	}
	if probe && candidate != vlogAutoCandidateOff {
		if s.candidateLikelyBeneficial(candidate) {
			s.probeSuccesses++
			s.clearHold()
		}
	}

	if candidate == vlogAutoCandidateOff {
		return
	}
	if ratio >= incompressibleRatio && throughput <= offThroughput*0.99 {
		if s.incompressibleStreak < 0xFF {
			s.incompressibleStreak++
		}
		if s.incompressibleStreak >= 2 && s.holdBytes > 0 && s.holdRemaining == 0 {
			s.holdEnters++
			s.holdRemaining = s.holdBytes
			nextProbe := s.probeBytes
			if nextProbe == 0 || nextProbe > s.holdRemaining {
				nextProbe = s.holdRemaining
			}
			s.probeRemaining = nextProbe
			s.currentCandidate = vlogAutoCandidateOff
			s.modeBytes = 0
		}
		return
	}
	s.incompressibleStreak = 0
}

func (s *vlogCompressionSelector) blockObservedRatio(codec valuelog.BlockCodec) float64 {
	if s == nil {
		return 1.0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := blockCandidateFromCodec(codec)
	m := s.metric(candidate)
	if m.samples > 0 {
		return m.ratio
	}
	// Fall back to whichever block codec has the stronger signal.
	snappy := s.metric(vlogAutoCandidateBlockSnappy)
	lz4 := s.metric(vlogAutoCandidateBlockLZ4)
	if snappy.samples == 0 && lz4.samples == 0 {
		return 1.0
	}
	if snappy.samples >= lz4.samples {
		return snappy.ratio
	}
	return lz4.ratio
}

func (s *vlogCompressionSelector) snapshot() vlogCompressionSelectorStats {
	out := vlogCompressionSelectorStats{}
	if s == nil {
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out.bytesByCandidate = s.bytesByCandidate
	out.framesByCandidate = s.framesByCandidate
	out.switches = s.switches
	out.probeAttempts = s.probeAttempts
	out.probeSuccesses = s.probeSuccesses
	out.holdEnters = s.holdEnters
	out.holdExits = s.holdExits
	out.bypassBytes = s.bypassBytes
	return out
}

func ewmaMetric(prev float64, samples uint64, sample float64) (float64, uint64) {
	if sample <= 0 || math.IsNaN(sample) || math.IsInf(sample, 0) {
		sample = 1.0
	}
	if samples == 0 || prev <= 0 || math.IsNaN(prev) || math.IsInf(prev, 0) {
		return sample, 1
	}
	const alpha = 0.2
	return prev*(1-alpha) + sample*alpha, samples + 1
}

func (db *DB) resolveVlogWriteMode(l *lane, dictID uint64, rawPayloadBytes int) (vlogCompressionWriteMode, valuelog.BlockCodec, bool) {
	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	switch mode {
	case vlogCompressionOff:
		return vlogWriteOff, db.valueLogBlockCodec, false
	case vlogCompressionBlock:
		return vlogWriteBlock, db.valueLogBlockCodec, false
	case vlogCompressionDict:
		if dictID != 0 {
			return vlogWriteDict, db.valueLogBlockCodec, false
		}
		return vlogWriteOff, db.valueLogBlockCodec, false
	case vlogCompressionDefault:
		if dictID != 0 {
			return vlogWriteDict, db.valueLogBlockCodec, false
		}
		return vlogWriteOff, db.valueLogBlockCodec, false
	default:
		if l == nil || l.vlogCompressionSelector == nil {
			if dictID != 0 {
				return vlogWriteDict, db.valueLogBlockCodec, false
			}
			return vlogWriteBlock, db.valueLogBlockCodec, false
		}
		return l.vlogCompressionSelector.choose(dictID != 0, rawPayloadBytes)
	}
}

func (db *DB) observeVlogWriteMode(l *lane, mode vlogCompressionWriteMode, blockCodec valuelog.BlockCodec, rawPayloadBytes, storedPayloadBytes int, probe bool, wallNs int64) {
	if db == nil || l == nil {
		return
	}
	if normalizeVlogCompressionMode(db.valueLogCompressionMode) != vlogCompressionAuto {
		return
	}
	if l.vlogCompressionSelector == nil {
		return
	}
	l.vlogCompressionSelector.observe(mode, blockCodec, rawPayloadBytes, storedPayloadBytes, wallNs, probe)
}

func (db *DB) chooseValueLogBlockWriteK(l *lane, records, rawPayloadBytes int, codec valuelog.BlockCodec) int {
	if records <= 1 {
		return 1
	}
	ratio := 1.0
	if l != nil && l.vlogCompressionSelector != nil {
		ratio = l.vlogCompressionSelector.blockObservedRatio(codec)
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
