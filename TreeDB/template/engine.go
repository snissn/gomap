package template

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

// Engine implements schema-blind template compression.
type Engine struct {
	cfg            Config
	stats          TemplateStats
	trainMu        sync.Mutex
	buckets        map[uint64]*bucket
	bucketSeq      uint64
	totalTemplates int
	trainSeq       atomic.Uint64
}

type bucket struct {
	key                uint64
	samples            []sample
	sampleBytes        int
	samplesSeen        int
	lastPublishSample  int
	templatesPublished int
	lastSeen           uint64
}

type sample struct {
	value []byte
}

type anchorCand struct {
	bytes     []byte
	hash      uint64
	positions []int
	median    int
	start     int
}

// NewEngine creates a template engine with normalized config.
func NewEngine(cfg Config) *Engine {
	cfg = NormalizeConfig(cfg)
	return &Engine{
		cfg:     cfg,
		buckets: make(map[uint64]*bucket),
	}
}

// StatsSnapshot returns a copy of current stats.
func (e *Engine) StatsSnapshot() map[string]string {
	if e == nil {
		return map[string]string{}
	}
	return e.stats.Snapshot()
}

// Encode attempts to template-encode value using templatedb candidates.
func (e *Engine) Encode(ctx context.Context, value []byte, store Store) ([]byte, bool) {
	if e == nil {
		return value, false
	}
	cfg := e.cfg
	if cfg.FingerprintK <= 0 || len(value) < cfg.FingerprintK {
		e.stats.addReason(reasonSkipSmall)
		e.observeTraining(value, store)
		return value, false
	}
	fps := RoutingFingerprints(value, cfg)
	if len(fps) == 0 {
		fps = Fingerprints(value, cfg)
	}
	if len(fps) == 0 {
		e.stats.addReason(reasonSkipNoFPs)
		e.observeTraining(value, store)
		return value, false
	}
	if store == nil {
		e.stats.addReason(reasonNoCandidates)
		e.observeTraining(value, store)
		return value, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.stats.Attempted.Add(1)
	maxFPReads := cfg.MaxFPReads
	if maxFPReads <= 0 || maxFPReads > len(fps) {
		maxFPReads = len(fps)
	}
	type candScore struct {
		id    uint64
		size  int
		score int
	}
	candMap := make(map[uint64]*candScore)
	for i := 0; i < maxFPReads; i++ {
		fp := fps[i]
		e.stats.CandidateFPReads.Add(1)
		cands, err := store.GetCandidates(ctx, fp, cfg.MaxCandidatesPerFP)
		if err != nil {
			e.stats.addReason(reasonFPLookupErr)
			continue
		}
		for _, c := range cands {
			cs := candMap[c.ID]
			if cs == nil {
				cs = &candScore{id: c.ID, size: c.Size}
				candMap[c.ID] = cs
			}
			cs.score++
			if c.Size > 0 && (cs.size == 0 || c.Size < cs.size) {
				cs.size = c.Size
			}
		}
	}
	if len(candMap) == 0 {
		e.stats.addReason(reasonNoCandidates)
		e.observeTraining(value, store)
		return value, false
	}
	candidates := make([]*candScore, 0, len(candMap))
	for _, c := range candMap {
		candidates = append(candidates, c)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if candidates[i].size != candidates[j].size {
			return candidates[i].size < candidates[j].size
		}
		return candidates[i].id < candidates[j].id
	})
	maxFetch := cfg.MaxTemplateFetch
	if maxFetch <= 0 || maxFetch > len(candidates) {
		maxFetch = len(candidates)
	}
	bestSavings := 0
	var bestPayload []byte
	matchedAny := false
	for i := 0; i < maxFetch; i++ {
		cand := candidates[i]
		e.stats.TemplateFetches.Add(1)
		defBytes, err := store.GetTemplateDef(ctx, cand.id)
		if err != nil {
			e.stats.addReason(reasonTemplateFetchErr)
			continue
		}
		def, err := DecodeTemplateDef(defBytes)
		if err != nil {
			e.stats.addReason(reasonTemplateFetchErr)
			continue
		}
		e.stats.CandidateTemplatesConsidered.Add(1)
		switch def.Kind {
		case 0, TemplateAnchors:
			gaps, encLen, reason, matched := matchTemplate(value, def.Anchors, cand.id, cfg)
			if reason != "" {
				e.stats.addReason(reason)
			}
			if !matched {
				continue
			}
			matchedAny = true
			savings := len(value) - encLen
			if savings <= bestSavings {
				continue
			}
			payload, err := EncodePayload(cand.id, gaps)
			if err != nil {
				continue
			}
			bestSavings = savings
			bestPayload = payload
		case TemplateMask:
			payload, encLen, reason, matched := matchMaskTemplate(value, def, cand.id, cfg)
			if reason != "" {
				e.stats.addReason(reason)
			}
			if !matched {
				continue
			}
			matchedAny = true
			savings := len(value) - encLen
			if savings <= bestSavings {
				continue
			}
			bestSavings = savings
			bestPayload = payload
		default:
			e.stats.addReason(reasonTemplateFetchErr)
			continue
		}
	}
	if matchedAny {
		e.stats.Matched.Add(1)
	}
	if bestSavings >= cfg.MinSavingsBytes && len(bestPayload) > 0 {
		e.stats.Kept.Add(1)
		e.stats.BytesSaved.Add(uint64(bestSavings))
		e.observeTraining(value, store)
		return bestPayload, true
	}
	if matchedAny && bestSavings < cfg.MinSavingsBytes {
		e.stats.addReason(reasonKeepNoSavings)
	}
	// Not kept.
	e.observeTraining(value, store)
	return value, false
}

func (e *Engine) observeTraining(value []byte, store Store) {
	if store == nil || e == nil {
		return
	}
	cfg := e.cfg
	if cfg.FingerprintK <= 0 || len(value) < cfg.FingerprintK {
		return
	}
	fps := BucketFingerprints(value, cfg)
	if len(fps) == 0 {
		fps = Fingerprints(value, cfg)
	}
	if len(fps) == 0 {
		return
	}
	seq := e.trainSeq.Add(1)
	if cfg.TrainSampleStride > 1 && seq%uint64(cfg.TrainSampleStride) != 0 {
		return
	}
	bucketKey := BucketKey(fps)
	if bucketKey == 0 {
		return
	}
	e.trainMu.Lock()
	defer e.trainMu.Unlock()
	b := e.buckets[bucketKey]
	if b == nil {
		if len(e.buckets) >= cfg.MaxBuckets {
			e.evictOldestBucket()
		}
		b = &bucket{key: bucketKey}
		e.buckets[bucketKey] = b
	}
	e.bucketSeq++
	b.lastSeen = e.bucketSeq
	// Add sample (copy).
	cp := append([]byte(nil), value...)
	b.samples = append(b.samples, sample{value: cp})
	b.sampleBytes += len(cp)
	b.samplesSeen++
	for (cfg.MaxValuesPerBucket > 0 && len(b.samples) > cfg.MaxValuesPerBucket) || (cfg.MaxBytesPerBucket > 0 && b.sampleBytes > cfg.MaxBytesPerBucket) {
		old := b.samples[0]
		b.samples = b.samples[1:]
		b.sampleBytes -= len(old.value)
	}
	if cfg.SynthesizeEverySamples <= 0 || b.samplesSeen%cfg.SynthesizeEverySamples != 0 {
		return
	}
	if cfg.CooldownValues > 0 && b.samplesSeen-b.lastPublishSample < cfg.CooldownValues {
		return
	}
	if cfg.MaxTemplatesPerBucket > 0 && b.templatesPublished >= cfg.MaxTemplatesPerBucket {
		return
	}
	if cfg.MaxTemplatesTotal > 0 && e.totalTemplates >= cfg.MaxTemplatesTotal {
		return
	}
	def, routeValue, activated, ok := synthesizeTemplate(b.samples, cfg)
	if !ok {
		return
	}
	if !activated {
		return
	}
	defBytes, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		return
	}
	routeFPs := RoutingFingerprints(routeValue, cfg)
	if len(routeFPs) == 0 {
		routeFPs = RouteFingerprints(def.Anchors, cfg)
	}
	if len(routeFPs) == 0 {
		return
	}
	if _, err := store.PutTemplateDef(context.Background(), defBytes, routeFPs); err != nil {
		return
	}
	b.lastPublishSample = b.samplesSeen
	b.templatesPublished++
	e.totalTemplates++
	e.stats.TemplatesPublished.Add(1)
}

func (e *Engine) evictOldestBucket() {
	var (
		oldestKey  uint64
		oldestSeen uint64
		set        bool
	)
	for k, b := range e.buckets {
		if b == nil {
			continue
		}
		if !set || b.lastSeen < oldestSeen || (b.lastSeen == oldestSeen && k < oldestKey) {
			oldestKey = k
			oldestSeen = b.lastSeen
			set = true
		}
	}
	if set {
		delete(e.buckets, oldestKey)
	}
}

// synthesizeTemplate builds a template from bucket samples.
// Returns def, activated, ok.
func synthesizeTemplate(samples []sample, cfg Config) (TemplateDef, []byte, bool, bool) {
	cfg = NormalizeConfig(cfg)
	if len(samples) == 0 {
		return TemplateDef{}, nil, false, false
	}
	if !cfg.DisableMaskTemplates {
		if def, routeValue, activated, ok := synthesizeMaskTemplate(samples, cfg); ok {
			return def, routeValue, activated, true
		}
	}
	k := cfg.FingerprintK
	if k <= 0 {
		return TemplateDef{}, nil, false, false
	}
	sampleLimit := len(samples)
	if cfg.MaxValuesScannedPerSynthesis > 0 && sampleLimit > cfg.MaxValuesScannedPerSynthesis {
		sampleLimit = cfg.MaxValuesScannedPerSynthesis
	}
	type cand struct {
		hash  uint64
		bytes []byte
		count int
	}
	counts := make(map[uint64]*cand)
	scanned := 0
	perSample := cfg.MaxAnchorScanPerSynthesis / sampleLimit
	if perSample < 1 {
		perSample = 1
	}
	for i := 0; i < sampleLimit && scanned < cfg.MaxAnchorScanPerSynthesis; i++ {
		val := samples[i].value
		if len(val) < k {
			continue
		}
		n := len(val) - k + 1
		if n <= 0 {
			continue
		}
		scanLimit := perSample
		if scanLimit > n {
			scanLimit = n
		}
		for j := 0; j < scanLimit && scanned < cfg.MaxAnchorScanPerSynthesis; j++ {
			off := 0
			if scanLimit > 1 {
				off = j * (n - 1) / (scanLimit - 1)
			}
			gram := val[off : off+k]
			h := xxh3.Hash(gram)
			c := counts[h]
			if c == nil {
				counts[h] = &cand{hash: h, bytes: append([]byte(nil), gram...), count: 1}
			} else if bytes.Equal(c.bytes, gram) {
				c.count++
			}
			scanned++
		}
	}
	if len(counts) == 0 {
		return TemplateDef{}, nil, false, false
	}
	anchors := make([]*anchorCand, 0, len(counts))
	for _, c := range counts {
		if c.count < cfg.MinAnchorFreq {
			continue
		}
		if len(c.bytes) != k {
			continue
		}
		if isAmbiguous(c.bytes, samples, sampleLimit, cfg.AmbiguityPct) {
			continue
		}
		positions := make([]int, sampleLimit)
		present := 0
		for i := 0; i < sampleLimit; i++ {
			pos := bytes.Index(samples[i].value, c.bytes)
			if pos < 0 {
				positions[i] = -1
				continue
			}
			if bytes.Index(samples[i].value[pos+1:], c.bytes) >= 0 {
				positions[i] = -1
				continue
			}
			positions[i] = pos
			present++
		}
		if float64(present)/float64(sampleLimit) < cfg.MinPresenceRatio {
			continue
		}
		posList := make([]int, 0, present)
		for _, p := range positions {
			if p >= 0 {
				posList = append(posList, p)
			}
		}
		sort.Ints(posList)
		median := posList[len(posList)/2]
		anchors = append(anchors, &anchorCand{bytes: c.bytes, hash: c.hash, positions: positions, median: median, start: median})
	}
	if len(anchors) == 0 {
		return TemplateDef{}, nil, false, false
	}
	// Extend anchors and compute starts.
	refIdx := selectReferenceSample(samples, sampleLimit)
	for _, a := range anchors {
		extendAnchor(a, samples, sampleLimit, refIdx, cfg)
	}
	// Order by start position.
	sort.Slice(anchors, func(i, j int) bool {
		if anchors[i].start != anchors[j].start {
			return anchors[i].start < anchors[j].start
		}
		return anchors[i].hash < anchors[j].hash
	})
	// Drop overlaps + caps.
	selected := make([][]byte, 0, len(anchors))
	totalBytes := 0
	prevEnd := -1
	for _, a := range anchors {
		if cfg.MaxAnchorsPerTemplate > 0 && len(selected) >= cfg.MaxAnchorsPerTemplate {
			break
		}
		if a.start < prevEnd {
			continue
		}
		if len(a.bytes) < cfg.MinAnchorLen || len(a.bytes) > cfg.MaxAnchorLen {
			continue
		}
		if cfg.MaxAnchorBytesTotal > 0 && totalBytes+len(a.bytes) > cfg.MaxAnchorBytesTotal {
			break
		}
		selected = append(selected, a.bytes)
		totalBytes += len(a.bytes)
		prevEnd = a.start + len(a.bytes)
	}
	if len(selected) == 0 {
		return TemplateDef{}, nil, false, false
	}
	def := TemplateDef{Kind: TemplateAnchors, Anchors: selected}
	// Quality gate + activation.
	hits, saved, meanRatio := simulateEncoding(def.Anchors, samples, sampleLimit, cfg)
	if hits == 0 {
		return TemplateDef{}, nil, false, false
	}
	meanSavings := int(saved) / hits
	if meanSavings < cfg.MinPublishSavingsBytes && meanRatio > cfg.MinPublishRatio {
		return TemplateDef{}, nil, false, false
	}
	activated := hits >= cfg.MinActivateHits || saved >= cfg.MinActivateSavedBytes
	return def, samples[refIdx].value, activated, true
}

func synthesizeMaskTemplate(samples []sample, cfg Config) (TemplateDef, []byte, bool, bool) {
	cfg = NormalizeConfig(cfg)
	if cfg.LengthBucketMinLen > 0 {
		if len(samples) == 0 {
			return TemplateDef{}, nil, false, false
		}
	}
	sampleLimit := len(samples)
	if cfg.MaskMaxValuesScanned > 0 && sampleLimit > cfg.MaskMaxValuesScanned {
		sampleLimit = cfg.MaskMaxValuesScanned
	}
	lengthCounts := make(map[int]int, sampleLimit)
	bestLen := 0
	bestCount := 0
	for i := 0; i < sampleLimit; i++ {
		l := len(samples[i].value)
		lengthCounts[l]++
		if lengthCounts[l] > bestCount || (lengthCounts[l] == bestCount && l > bestLen) {
			bestLen = l
			bestCount = lengthCounts[l]
		}
	}
	if bestLen == 0 || bestCount == 0 {
		return TemplateDef{}, nil, false, false
	}
	if cfg.LengthBucketMinLen > 0 && bestLen < cfg.LengthBucketMinLen {
		return TemplateDef{}, nil, false, false
	}
	if bestCount < cfg.MinAnchorFreq {
		return TemplateDef{}, nil, false, false
	}
	// Build per-position byte counts.
	counts := make([][256]int, bestLen)
	totalSamples := 0
	for i := 0; i < sampleLimit; i++ {
		val := samples[i].value
		if len(val) != bestLen {
			continue
		}
		for pos := 0; pos < bestLen; pos++ {
			counts[pos][val[pos]]++
		}
		totalSamples++
	}
	if totalSamples == 0 {
		return TemplateDef{}, nil, false, false
	}
	base := make([]byte, bestLen)
	constCount := 0
	for pos := 0; pos < bestLen; pos++ {
		maxCount := 0
		var maxByte byte
		for b, c := range counts[pos] {
			if c > maxCount {
				maxCount = c
				maxByte = byte(b)
			}
		}
		base[pos] = maxByte
		if float64(maxCount)/float64(totalSamples) >= cfg.MaskMinPresenceRatio {
			constCount++
		}
	}
	maskLen := (bestLen + 7) / 8
	minConst := cfg.MaskMinConstBytes
	if minConst < int(float64(bestLen)*cfg.MaskMinConstFrac) {
		minConst = int(float64(bestLen) * cfg.MaskMinConstFrac)
	}
	if constCount < minConst {
		return TemplateDef{}, nil, false, false
	}
	encHeader := payloadHeader + uvarintLen(1) + maskLen
	hits, saved, meanRatio := simulateMaskEncoding(base, samples, sampleLimit, encHeader)
	if hits == 0 {
		return TemplateDef{}, nil, false, false
	}
	meanSavings := int(saved) / hits
	if meanSavings < cfg.MinPublishSavingsBytes && meanRatio > cfg.MinPublishRatio {
		return TemplateDef{}, nil, false, false
	}
	activated := hits >= cfg.MinActivateHits || saved >= cfg.MinActivateSavedBytes
	refIdx := selectReferenceSample(samples, sampleLimit)
	refValue := samples[refIdx].value
	if len(refValue) != bestLen {
		for i := 0; i < sampleLimit; i++ {
			if len(samples[i].value) == bestLen {
				refValue = samples[i].value
				break
			}
		}
	}
	def := TemplateDef{Kind: TemplateMask, Base: base, Mask: make([]byte, maskLen)}
	return def, refValue, activated, true
}

func isAmbiguous(anchor []byte, samples []sample, limit int, maxPct float64) bool {
	if maxPct <= 0 {
		return false
	}
	ambiguous := 0
	for i := 0; i < limit; i++ {
		if bytes.Count(samples[i].value, anchor) != 1 {
			ambiguous++
		}
	}
	return float64(ambiguous)/float64(limit) > maxPct
}

func selectReferenceSample(samples []sample, limit int) int {
	best := 0
	bestHash := uint64(0)
	for i := 0; i < limit; i++ {
		h := xxh3.Hash(samples[i].value)
		if i == 0 || h < bestHash {
			best = i
			bestHash = h
		}
	}
	return best
}

func extendAnchor(a *anchorCand, samples []sample, limit int, refIdx int, cfg Config) {
	ref := samples[refIdx].value
	refPos := -1
	if refIdx < len(a.positions) {
		refPos = a.positions[refIdx]
	}
	if refPos < 0 {
		for i := 0; i < limit; i++ {
			if a.positions[i] >= 0 {
				refIdx = i
				ref = samples[i].value
				refPos = a.positions[i]
				break
			}
		}
	}
	if refPos < 0 {
		return
	}
	start := refPos
	end := refPos + len(a.bytes)
	for start > 0 && (end-start) < cfg.MaxAnchorLen {
		b := ref[start-1]
		matches := 0
		total := 0
		for i := 0; i < limit; i++ {
			pos := a.positions[i]
			if pos < 0 {
				continue
			}
			offset := refPos - start
			if pos-offset <= 0 {
				total++
				continue
			}
			total++
			if samples[i].value[pos-offset-1] == b {
				matches++
			}
		}
		if total == 0 || float64(matches)/float64(total) < cfg.MinPresenceRatio {
			break
		}
		start--
	}
	for end < len(ref) && (end-start) < cfg.MaxAnchorLen {
		b := ref[end]
		matches := 0
		total := 0
		for i := 0; i < limit; i++ {
			pos := a.positions[i]
			if pos < 0 {
				continue
			}
			off := pos + (end - refPos)
			if off >= len(samples[i].value) {
				total++
				continue
			}
			total++
			if samples[i].value[off] == b {
				matches++
			}
		}
		if total == 0 || float64(matches)/float64(total) < cfg.MinPresenceRatio {
			break
		}
		end++
	}
	a.bytes = append([]byte(nil), ref[start:end]...)
	leftExt := refPos - start
	a.start = a.median - leftExt
}

func simulateEncoding(anchors [][]byte, samples []sample, limit int, cfg Config) (hits int, saved int, meanRatio float64) {
	if limit == 0 {
		return 0, 0, 1
	}
	ratioSum := 0.0
	for i := 0; i < limit; i++ {
		gaps, encLen, _, matched := matchTemplate(samples[i].value, anchors, 1, cfg)
		if !matched {
			continue
		}
		if len(gaps) == 0 {
			continue
		}
		if encLen >= len(samples[i].value)-cfg.MinSavingsBytes {
			continue
		}
		hits++
		saved += len(samples[i].value) - encLen
		ratioSum += float64(encLen) / float64(len(samples[i].value))
	}
	if hits == 0 {
		return 0, 0, 1
	}
	meanRatio = ratioSum / float64(hits)
	return hits, saved, meanRatio
}

func simulateMaskEncoding(base []byte, samples []sample, limit int, encHeader int) (hits int, saved int, meanRatio float64) {
	if limit == 0 || len(base) == 0 {
		return 0, 0, 1
	}
	ratioSum := 0.0
	for i := 0; i < limit; i++ {
		val := samples[i].value
		if len(val) != len(base) {
			continue
		}
		diffCount := 0
		for j := 0; j < len(base); j++ {
			if val[j] != base[j] {
				diffCount++
			}
		}
		encLen := encHeader + diffCount
		if encLen >= len(val) {
			continue
		}
		hits++
		saved += len(val) - encLen
		ratioSum += float64(encLen) / float64(len(val))
	}
	if hits == 0 {
		return 0, 0, 1
	}
	meanRatio = ratioSum / float64(hits)
	return hits, saved, meanRatio
}
