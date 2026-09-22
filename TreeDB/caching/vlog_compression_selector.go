package caching

import (
	"bytes"
	"math"
	"strconv"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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

const vlogCompressionWriteModeCount = int(vlogWriteDict) + 1

type vlogAutoCandidate uint8

const (
	vlogAutoCandidateOff vlogAutoCandidate = iota
	vlogAutoCandidateDict
	vlogAutoCandidateBlockSnappy
	vlogAutoCandidateBlockLZ4
	vlogAutoCandidateBlockZSTD
)

const vlogAutoCandidateCount = int(vlogAutoCandidateBlockZSTD) + 1

const vlogBlockCodecCount = 3

const vlogBlockKBucketCount = 8

const vlogPayloadBucketCount = 10

var vlogBlockKBucketUpperBounds = [vlogBlockKBucketCount]int{1, 2, 4, 8, 16, 32, 64, valuelog.MaxFrameK}

var vlogPayloadBucketUpperBounds = [vlogPayloadBucketCount - 1]int{1024, 2048, 4096, 8192, 16384, 32768, 49152, 65536, 131072}

const (
	// Require a small amount of observed signal before overriding configured
	// block codec defaults for large no-dict payload streams.
	largePayloadBlockCodecMinSamples = 4
	largePayloadBlockCodecTieMargin  = 0.01
	// Large value-log payloads need a materially larger grouped-frame target than
	// the generic 4 KiB block target. Otherwise K collapses to 1 on 40+ KiB
	// records before the selector has any chance to observe cross-record wins.
	largePayloadBlockTargetMinPayloadBytes = 16 << 10
	largePayloadBlockTargetMultiplier      = 8
	largePayloadBlockBootstrapRatio        = 0.92
)

var (
	vlogAutoCandidatesNoDict   = [...]vlogAutoCandidate{vlogAutoCandidateOff, vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD}
	vlogAutoCandidatesWithDict = [...]vlogAutoCandidate{vlogAutoCandidateOff, vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD, vlogAutoCandidateDict}
	vlogSelectorBlockCodecs    = [...]valuelog.BlockCodec{valuelog.BlockCodecSnappy, valuelog.BlockCodecLZ4, valuelog.BlockCodecZSTD}
)

var (
	valueLogRetainedSemanticStreamV1BlockMagic     = []byte("crss1blk\x00")
	valueLogRetainedSemanticStreamV1BlockZSTDMagic = []byte("crss1zst\x00")
)

const (
	defaultVlogHoldBytes      = 64 << 20
	defaultVlogProbeBytes     = 8 << 20
	defaultVlogExploreBytes   = 8 << 20
	defaultVlogModeDwellBytes = 4 << 20
	// Throughput policy favors a stable, low-overhead block path for medium+
	// payloads to avoid per-write selector churn in hot write workloads.
	throughputAutoBlockMinPayloadBytes = 256
	// Force-pointer retained payloads are storage-critical. Keep medium+ values
	// on a stable block codec path and let the frame preparer fall back to raw
	// only when compressed bytes are not worth keeping.
	forcePointerAutoBlockMinPayloadBytes = 512
	// Template-v1 retained bodies in collection typed-storage runs are often
	// smaller than the generic forced-pointer threshold after column elision, but
	// they arrive as highly repetitive value-log batches. Treat those batches as
	// storage-first once the frame has enough total bytes to amortize block
	// compression.
	retainedStorageFirstMinUnitPayloadBytes  = 128
	retainedStorageFirstMinBatchPayloadBytes = 4 << 10
	// High-entropy forced-pointer streams may bypass compression in explicit
	// throughput policy. Balanced/size policy treats forced pointers as durable
	// storage-first payloads and keeps them on block compression so retained JSON
	// does not silently land as raw value-log frames.
	forcePointerAutoRawBypassMinPayloadBytes = 512
	forcePointerAutoRawBypassMaxPayloadBytes = 1024
	// Storage-first retained value-log streams should write larger grouped
	// compressed frames by policy, not by incidental selector history. JSONBench
	// retained payloads are JSON/template-v1-like and benefit from batching
	// enough rows for block codecs to exploit cross-record repetition.
	storageFirstRetainedValueLogBlockTargetCompressedBytes = 256 << 10
	// Larger grouped-frame targets reduce per-record compression overhead for
	// forced-pointer/storage-first streams.
	forcePointerBlockTargetCompressedBytes = storageFirstRetainedValueLogBlockTargetCompressedBytes
	forcePointerBlockBootstrapRatio        = 0.50
	// Live leaf-log frames are a read-path structure as well as a write/storage
	// structure. Keep grouping modest so cold point reads do not repeatedly decode
	// large multi-page frames when access has little locality.
	leafLogBlockMaxK = 8
	// Keep the live leaf-log on a larger block target than generic value-log
	// payloads. Leaf pages are appended in batches and benefit materially from
	// grouped compression; the K cap above still bounds cold point-read decode
	// amplification.
	leafLogBlockTargetCompressedBytes = 32 << 10
	// For large payloads, allow strong observed dict wins to override generic
	// throughput scoring so we do not lock highly-compressible streams into
	// block mode.
	largePayloadDictPreferMinPayloadBytes = 32 << 10
	largePayloadDictPreferMinSamples      = 4
	largePayloadDictPreferAbsRatioMax     = 0.15
	largePayloadDictPreferRelRatioMax     = 0.35
	largePayloadDictPreferHugeRelRatioMax = 0.20
	vlogAutoThroughputParityRatioGate     = 1.01
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
	case 2:
		return valuelog.BlockCodecZSTD
	default:
		return valuelog.BlockCodecSnappy
	}
}

func normalizeSelectorBlockCodec(codec valuelog.BlockCodec) valuelog.BlockCodec {
	switch codec {
	case valuelog.BlockCodecLZ4:
		return valuelog.BlockCodecLZ4
	case valuelog.BlockCodecZSTD:
		return valuelog.BlockCodecZSTD
	default:
		return valuelog.BlockCodecSnappy
	}
}

func vlogBlockCodecIndex(codec valuelog.BlockCodec) int {
	switch normalizeSelectorBlockCodec(codec) {
	case valuelog.BlockCodecLZ4:
		return 1
	case valuelog.BlockCodecZSTD:
		return 2
	default:
		return 0
	}
}

func vlogBlockCodecSuffix(idx int) string {
	switch idx {
	case 1:
		return "lz4"
	case 2:
		return "zstd"
	default:
		return "snappy"
	}
}

func vlogBlockKBucketIndex(k int) int {
	for i, upper := range vlogBlockKBucketUpperBounds {
		if k <= upper {
			return i
		}
	}
	return vlogBlockKBucketCount - 1
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

	metrics          [vlogAutoCandidateCount]vlogCandidateMetrics
	seededCandidates [vlogAutoCandidateCount]bool

	bytesByCandidate  [vlogAutoCandidateCount]uint64
	framesByCandidate [vlogAutoCandidateCount]uint64
	switches          [vlogAutoCandidateCount][vlogAutoCandidateCount]uint64
	probeAttempts     uint64
	probeSuccesses    uint64
	holdEnters        uint64
	holdExits         uint64
	bypassBytes       uint64
}

func (s *vlogCompressionSelector) seedDictCandidate(ratio float64) {
	if s == nil {
		return
	}
	ratio = normalizeMetricRatio(ratio)
	// Ignore neutral/negative hints; this seed is only for clear dict wins.
	if ratio >= 0.98 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	m := s.metrics[vlogAutoCandidateDict]
	if m.samples == 0 || ratio < normalizeMetricRatio(m.ratio) {
		m.ratio = ratio
	}
	// Keep throughput neutral to avoid biasing policy decisions toward "size at
	// all costs" in throughput mode.
	if m.throughput <= 0 {
		m.throughput = 1.0
	}
	if m.samples == 0 {
		m.samples = 1
	}
	s.metrics[vlogAutoCandidateDict] = m
	s.seededCandidates[vlogAutoCandidateDict] = true
}

func (s *vlogCompressionSelector) normalizeLargePayloadCandidate(c vlogAutoCandidate, unitPayloadBytes int) vlogAutoCandidate {
	if s == nil {
		return c
	}
	if s.policy == vlogAutoThroughput {
		return c
	}
	if unitPayloadBytes >= 1024 && c == vlogAutoCandidateBlockSnappy {
		best := c
		bestRatio := 1.0
		for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
			m := s.metric(candidate)
			if m.samples < 4 {
				continue
			}
			if m.ratio < bestRatio {
				best = candidate
				bestRatio = m.ratio
			}
		}
		// When a stronger block codec is already producing very small output, keep
		// large-payload streams on it to avoid ratio regressions from exploratory
		// snappy frames.
		if best != c && (bestRatio <= 0.02 || (unitPayloadBytes >= 512 && bestRatio <= 0.05)) {
			return best
		}
	}
	return c
}

func (s *vlogCompressionSelector) shouldPreferLargePayloadDict(dictAvailable bool, unitPayloadBytes int) bool {
	if s == nil || !dictAvailable || unitPayloadBytes < largePayloadDictPreferMinPayloadBytes {
		return false
	}
	dict := s.metric(vlogAutoCandidateDict)
	if dict.samples < largePayloadDictPreferMinSamples {
		return false
	}
	bestBlock := s.metric(vlogAutoCandidateBlockSnappy)
	for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
		block := s.metric(candidate)
		if block.samples > bestBlock.samples ||
			(block.samples == bestBlock.samples && (block.ratio < bestBlock.ratio || (block.ratio == bestBlock.ratio && block.throughput > bestBlock.throughput))) {
			bestBlock = block
		}
	}

	strongAbsolute := dict.ratio <= largePayloadDictPreferAbsRatioMax
	strongRelative := bestBlock.samples >= largePayloadDictPreferMinSamples &&
		dict.ratio <= bestBlock.ratio*largePayloadDictPreferRelRatioMax
	if !strongAbsolute && !strongRelative {
		return false
	}
	if bestBlock.samples < largePayloadDictPreferMinSamples {
		return strongAbsolute && throughputRatioAboveGate(dict.throughput, s.offThroughput(), vlogAutoThroughputParityRatioGate)
	}
	if !throughputRatioAboveGate(dict.throughput, bestBlock.throughput, vlogAutoThroughputParityRatioGate) {
		return false
	}

	switch s.policy {
	case vlogAutoThroughput:
		return dict.ratio <= bestBlock.ratio*largePayloadDictPreferHugeRelRatioMax || dict.throughput >= bestBlock.throughput*1.03
	case vlogAutoBalanced:
		return dict.ratio <= bestBlock.ratio*largePayloadDictPreferHugeRelRatioMax || dict.throughput >= bestBlock.throughput*1.03
	default:
		return true
	}
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
	case vlogAutoCandidateBlockZSTD:
		return "block_zstd"
	default:
		return "unknown"
	}
}

func blockCandidateFromCodec(codec valuelog.BlockCodec) vlogAutoCandidate {
	switch normalizeSelectorBlockCodec(codec) {
	case valuelog.BlockCodecLZ4:
		return vlogAutoCandidateBlockLZ4
	case valuelog.BlockCodecZSTD:
		return vlogAutoCandidateBlockZSTD
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
	case vlogAutoCandidateBlockZSTD:
		return vlogWriteBlock, valuelog.BlockCodecZSTD
	case vlogAutoCandidateBlockSnappy:
		return vlogWriteBlock, valuelog.BlockCodecSnappy
	default:
		return vlogWriteOff, normalizeSelectorBlockCodec(seed)
	}
}

func vlogWriteModeSuffix(mode vlogCompressionWriteMode) string {
	switch mode {
	case vlogWriteBlock:
		return "block"
	case vlogWriteDict:
		return "dict"
	default:
		return "off"
	}
}

type vlogPayloadKind uint8

const (
	vlogPayloadKindSingleValue vlogPayloadKind = iota
	vlogPayloadKindOuterLeaf
	vlogPayloadKindMixed
)

const vlogPayloadKindCount = int(vlogPayloadKindMixed) + 1

type vlogPayloadSplitKind uint8

const (
	vlogPayloadSplitSingleValue vlogPayloadSplitKind = iota
	vlogPayloadSplitOuterLeaf
)

const vlogPayloadSplitKindCount = int(vlogPayloadSplitOuterLeaf) + 1

type vlogOuterLeafCodecKind uint8

const (
	vlogOuterLeafCodecNone vlogOuterLeafCodecKind = iota
	vlogOuterLeafCodecSnappy
	vlogOuterLeafCodecLZ4
	vlogOuterLeafCodecLegacyPage
	vlogOuterLeafCodecUnknown
	vlogOuterLeafCodecMixed
)

const vlogOuterLeafCodecKindCount = int(vlogOuterLeafCodecMixed) + 1

func normalizeVlogPayloadKind(kind vlogPayloadKind) vlogPayloadKind {
	switch kind {
	case vlogPayloadKindSingleValue, vlogPayloadKindOuterLeaf, vlogPayloadKindMixed:
		return kind
	default:
		return vlogPayloadKindMixed
	}
}

func normalizeVlogOuterLeafCodecKind(kind vlogOuterLeafCodecKind) vlogOuterLeafCodecKind {
	switch kind {
	case vlogOuterLeafCodecNone,
		vlogOuterLeafCodecSnappy,
		vlogOuterLeafCodecLZ4,
		vlogOuterLeafCodecLegacyPage,
		vlogOuterLeafCodecUnknown,
		vlogOuterLeafCodecMixed:
		return kind
	default:
		return vlogOuterLeafCodecUnknown
	}
}

func vlogPayloadKindSuffix(kind vlogPayloadKind) string {
	switch normalizeVlogPayloadKind(kind) {
	case vlogPayloadKindSingleValue:
		return "single_value"
	case vlogPayloadKindOuterLeaf:
		return "outer_leaf"
	default:
		return "mixed"
	}
}

func normalizeVlogPayloadSplitKind(kind vlogPayloadSplitKind) vlogPayloadSplitKind {
	switch kind {
	case vlogPayloadSplitSingleValue, vlogPayloadSplitOuterLeaf:
		return kind
	default:
		return vlogPayloadSplitSingleValue
	}
}

func vlogPayloadSplitSuffix(kind vlogPayloadSplitKind) string {
	switch normalizeVlogPayloadSplitKind(kind) {
	case vlogPayloadSplitOuterLeaf:
		return "outer_leaf"
	default:
		return "single_value"
	}
}

func vlogOuterLeafCodecSuffix(kind vlogOuterLeafCodecKind) string {
	switch normalizeVlogOuterLeafCodecKind(kind) {
	case vlogOuterLeafCodecNone:
		return "none"
	case vlogOuterLeafCodecSnappy:
		return "snappy"
	case vlogOuterLeafCodecLZ4:
		return "lz4"
	case vlogOuterLeafCodecLegacyPage:
		return "legacy_page"
	case vlogOuterLeafCodecMixed:
		return "mixed"
	default:
		return "unknown"
	}
}

func vlogPayloadBucketIndex(unitPayloadBytes int) int {
	for i, upper := range vlogPayloadBucketUpperBounds {
		if unitPayloadBytes <= upper {
			return i
		}
	}
	return vlogPayloadBucketCount - 1
}

func vlogPayloadBucketSuffix(idx int) string {
	if idx < 0 {
		return "unknown"
	}
	if idx < len(vlogPayloadBucketUpperBounds) {
		return "le_" + strconv.Itoa(vlogPayloadBucketUpperBounds[idx])
	}
	if len(vlogPayloadBucketUpperBounds) == 0 {
		return "gt_0"
	}
	return "gt_" + strconv.Itoa(vlogPayloadBucketUpperBounds[len(vlogPayloadBucketUpperBounds)-1])
}

type vlogWriteModeSnapshot struct {
	RawBytes          [vlogCompressionWriteModeCount]uint64
	StoredBytes       [vlogCompressionWriteModeCount]uint64
	Frames            [vlogCompressionWriteModeCount]uint64
	BucketRawBytes    [vlogCompressionWriteModeCount][vlogPayloadBucketCount]uint64
	BucketStoredBytes [vlogCompressionWriteModeCount][vlogPayloadBucketCount]uint64
	BucketFrames      [vlogCompressionWriteModeCount][vlogPayloadBucketCount]uint64
}

type vlogPayloadKindSnapshot struct {
	RawBytes    [vlogPayloadKindCount]uint64
	StoredBytes [vlogPayloadKindCount]uint64
	Frames      [vlogPayloadKindCount]uint64
}

type vlogPayloadSplitSnapshot struct {
	RawBytes    [vlogPayloadSplitKindCount]uint64
	StoredBytes [vlogPayloadSplitKindCount]uint64
	Records     [vlogPayloadSplitKindCount]uint64
}

type vlogOuterLeafCodecSnapshot struct {
	RawBytes    [vlogOuterLeafCodecKindCount]uint64
	StoredBytes [vlogOuterLeafCodecKindCount]uint64
	Frames      [vlogOuterLeafCodecKindCount]uint64
}

func recordLaneVlogWriteObservation(l *lane, mode vlogCompressionWriteMode, rawPayloadBytes, unitPayloadBytes, storedPayloadBytes int) {
	if l == nil || rawPayloadBytes <= 0 {
		return
	}
	if int(mode) >= vlogCompressionWriteModeCount {
		mode = vlogWriteOff
	}
	if unitPayloadBytes <= 0 {
		unitPayloadBytes = rawPayloadBytes
	}
	if storedPayloadBytes <= 0 {
		storedPayloadBytes = rawPayloadBytes
	}
	bucket := vlogPayloadBucketIndex(unitPayloadBytes)
	l.vlogWriteModeRawBytes[mode].Add(uint64(rawPayloadBytes))
	l.vlogWriteModeStoredBytes[mode].Add(uint64(storedPayloadBytes))
	l.vlogWriteModeFrames[mode].Add(1)
	l.vlogWriteModeBucketRawBytes[mode][bucket].Add(uint64(rawPayloadBytes))
	l.vlogWriteModeBucketStoredBytes[mode][bucket].Add(uint64(storedPayloadBytes))
	l.vlogWriteModeBucketFrames[mode][bucket].Add(1)
}

func recordLaneVlogPayloadKindObservation(l *lane, kind vlogPayloadKind, rawPayloadBytes, storedPayloadBytes int) {
	if l == nil || rawPayloadBytes <= 0 {
		return
	}
	kind = normalizeVlogPayloadKind(kind)
	if storedPayloadBytes <= 0 {
		storedPayloadBytes = rawPayloadBytes
	}
	l.vlogPayloadKindRawBytes[kind].Add(uint64(rawPayloadBytes))
	l.vlogPayloadKindStoredBytes[kind].Add(uint64(storedPayloadBytes))
	l.vlogPayloadKindFrames[kind].Add(1)
}

func recordLaneVlogPayloadSplitObservation(l *lane, kind vlogPayloadSplitKind, rawPayloadBytes, storedPayloadBytes, records int) {
	if l == nil || rawPayloadBytes <= 0 {
		return
	}
	kind = normalizeVlogPayloadSplitKind(kind)
	if storedPayloadBytes <= 0 {
		storedPayloadBytes = rawPayloadBytes
	}
	if records <= 0 {
		records = 1
	}
	l.vlogPayloadSplitRawBytes[kind].Add(uint64(rawPayloadBytes))
	l.vlogPayloadSplitStoredBytes[kind].Add(uint64(storedPayloadBytes))
	l.vlogPayloadSplitRecords[kind].Add(uint64(records))
}

func recordLaneVlogOuterLeafCodecObservation(l *lane, kind vlogOuterLeafCodecKind, rawPayloadBytes, storedPayloadBytes int) {
	if l == nil || rawPayloadBytes <= 0 {
		return
	}
	kind = normalizeVlogOuterLeafCodecKind(kind)
	if storedPayloadBytes <= 0 {
		storedPayloadBytes = rawPayloadBytes
	}
	l.vlogOuterLeafCodecRawBytes[kind].Add(uint64(rawPayloadBytes))
	l.vlogOuterLeafCodecStoredBytes[kind].Add(uint64(storedPayloadBytes))
	l.vlogOuterLeafCodecFrames[kind].Add(1)
}

func snapshotLaneVlogWriteMode(l *lane) vlogWriteModeSnapshot {
	var out vlogWriteModeSnapshot
	if l == nil {
		return out
	}
	for mode := 0; mode < vlogCompressionWriteModeCount; mode++ {
		out.RawBytes[mode] = l.vlogWriteModeRawBytes[mode].Load()
		out.StoredBytes[mode] = l.vlogWriteModeStoredBytes[mode].Load()
		out.Frames[mode] = l.vlogWriteModeFrames[mode].Load()
		for bucket := 0; bucket < vlogPayloadBucketCount; bucket++ {
			out.BucketRawBytes[mode][bucket] = l.vlogWriteModeBucketRawBytes[mode][bucket].Load()
			out.BucketStoredBytes[mode][bucket] = l.vlogWriteModeBucketStoredBytes[mode][bucket].Load()
			out.BucketFrames[mode][bucket] = l.vlogWriteModeBucketFrames[mode][bucket].Load()
		}
	}
	return out
}

func snapshotLaneVlogPayloadKind(l *lane) vlogPayloadKindSnapshot {
	var out vlogPayloadKindSnapshot
	if l == nil {
		return out
	}
	for kind := 0; kind < vlogPayloadKindCount; kind++ {
		out.RawBytes[kind] = l.vlogPayloadKindRawBytes[kind].Load()
		out.StoredBytes[kind] = l.vlogPayloadKindStoredBytes[kind].Load()
		out.Frames[kind] = l.vlogPayloadKindFrames[kind].Load()
	}
	return out
}

func snapshotLaneVlogPayloadSplit(l *lane) vlogPayloadSplitSnapshot {
	var out vlogPayloadSplitSnapshot
	if l == nil {
		return out
	}
	for kind := 0; kind < vlogPayloadSplitKindCount; kind++ {
		out.RawBytes[kind] = l.vlogPayloadSplitRawBytes[kind].Load()
		out.StoredBytes[kind] = l.vlogPayloadSplitStoredBytes[kind].Load()
		out.Records[kind] = l.vlogPayloadSplitRecords[kind].Load()
	}
	return out
}

func snapshotLaneVlogOuterLeafCodec(l *lane) vlogOuterLeafCodecSnapshot {
	var out vlogOuterLeafCodecSnapshot
	if l == nil {
		return out
	}
	for kind := 0; kind < vlogOuterLeafCodecKindCount; kind++ {
		out.RawBytes[kind] = l.vlogOuterLeafCodecRawBytes[kind].Load()
		out.StoredBytes[kind] = l.vlogOuterLeafCodecStoredBytes[kind].Load()
		out.Frames[kind] = l.vlogOuterLeafCodecFrames[kind].Load()
	}
	return out
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
			m.ratio = 0.92
			m.throughput = 0.9
		case vlogAutoCandidateBlockLZ4:
			m.ratio = 0.92
			m.throughput = 0.95
		case vlogAutoCandidateBlockZSTD:
			m.ratio = 0.89
			m.throughput = 0.88
		default:
			m.ratio = 0.93
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

func throughputRatioAboveGate(candidate, baseline, minRatio float64) bool {
	return baseline > 0 && candidate/baseline > minRatio
}

func (s *vlogCompressionSelector) candidatePassesThroughputGate(c vlogAutoCandidate) bool {
	if c == vlogAutoCandidateOff {
		return true
	}
	m := s.metric(c)
	return throughputRatioAboveGate(m.throughput, s.offThroughput(), vlogAutoThroughputParityRatioGate)
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
	if !s.candidatePassesThroughputGate(c) {
		return false
	}
	// Strong size wins are beneficial only after the strict throughput parity gate passes.
	if m.ratio <= 0.90 {
		return true
	}
	switch s.policy {
	case vlogAutoThroughput:
		if m.throughput >= offThroughput*1.03 {
			return true
		}
		return m.ratio <= 0.985
	case vlogAutoSize:
		if m.ratio <= 0.98 {
			return true
		}
		return m.throughput >= offThroughput*1.02
	default:
		if m.ratio <= 0.95 {
			return true
		}
		if m.throughput >= offThroughput*1.03 {
			return true
		}
		if m.ratio <= 0.985 {
			return true
		}
		return m.ratio <= 0.995
	}
}

func (s *vlogCompressionSelector) availableCandidates(dictAvailable bool) []vlogAutoCandidate {
	if dictAvailable {
		return vlogAutoCandidatesWithDict[:]
	}
	return vlogAutoCandidatesNoDict[:]
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
	// When we have strong compression evidence, avoid getting stuck in off mode.
	if best == vlogAutoCandidateOff {
		forced := vlogAutoCandidateOff
		forcedScore := -1.0
		for _, c := range cands {
			if c == vlogAutoCandidateOff {
				continue
			}
			m := s.metric(c)
			if m.samples < 4 || m.ratio > 0.90 {
				continue
			}
			if !s.candidatePassesThroughputGate(c) {
				continue
			}
			score := s.candidateScore(c)
			if score > forcedScore {
				forced = c
				forcedScore = score
			}
		}
		if forced != vlogAutoCandidateOff {
			return forced
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
	currentMetric := s.metric(current)
	found := false
	for _, c := range cands {
		if c == current {
			continue
		}
		if c == vlogAutoCandidateOff {
			continue
		}
		if s.skipExplorationCandidate(current, currentMetric, c) {
			continue
		}
		m := s.metric(c)
		samples := m.samples
		if c == vlogAutoCandidateDict && s.seededCandidates[vlogAutoCandidateDict] {
			samples = 0
		}
		score := s.candidateScore(c)
		if !found || samples < bestSamples || (samples == bestSamples && score > bestScore) {
			best = c
			bestSamples = samples
			bestScore = score
			found = true
		}
	}
	if found {
		return best
	}
	return current
}

func isBlockCandidate(c vlogAutoCandidate) bool {
	return c == vlogAutoCandidateBlockSnappy || c == vlogAutoCandidateBlockLZ4 || c == vlogAutoCandidateBlockZSTD
}

func (s *vlogCompressionSelector) skipExplorationCandidate(current vlogAutoCandidate, currentMetric vlogCandidateMetrics, candidate vlogAutoCandidate) bool {
	candidateMetric := s.metric(candidate)

	// Once compression is clearly beneficial, avoid exploratory off probes that
	// inflate stored bytes in steady-state runs.
	if candidate == vlogAutoCandidateOff && current != vlogAutoCandidateOff {
		if currentMetric.samples >= 4 && currentMetric.ratio <= 0.90 {
			return true
		}
	}

	// Avoid repeatedly probing a clearly dominated alternative codec. This keeps
	// auto close to the best steady-state ratio on highly compressible streams.
	if isBlockCandidate(current) && isBlockCandidate(candidate) {
		if currentMetric.samples >= 4 && candidateMetric.samples >= 4 {
			switch s.policy {
			case vlogAutoThroughput:
				if candidateMetric.ratio > currentMetric.ratio*2.00 && candidateMetric.throughput < currentMetric.throughput*1.15 {
					return true
				}
			default:
				ratioWorse := candidateMetric.ratio > currentMetric.ratio*1.20
				throughputGain := candidateMetric.throughput >= currentMetric.throughput*1.10
				if ratioWorse && !throughputGain {
					return true
				}
			}
		}
	}
	return false
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
	initialExplore := uint64(defaultVlogExploreBytes)
	if initialExplore > 512<<10 {
		// Prime a first exploration decision quickly, then use the normal steady
		// exploration cadence for the rest of the stream.
		initialExplore = 512 << 10
	}
	return &vlogCompressionSelector{
		policy:           policy,
		holdBytes:        holdBytes,
		probeBytes:       probeBytes,
		exploreBytes:     defaultVlogExploreBytes,
		dwellBytes:       defaultVlogModeDwellBytes,
		seedCodec:        seedCodec,
		currentCandidate: blockCandidateFromCodec(seedCodec),
		exploreRemaining: initialExplore,
		metrics: [vlogAutoCandidateCount]vlogCandidateMetrics{
			vlogAutoCandidateOff:         {ratio: 1.0, throughput: 1.0, samples: 1},
			vlogAutoCandidateDict:        {ratio: 0.92, throughput: 0.90},
			vlogAutoCandidateBlockSnappy: {ratio: 0.93, throughput: 0.97},
			vlogAutoCandidateBlockLZ4:    {ratio: 0.92, throughput: 0.95},
			vlogAutoCandidateBlockZSTD:   {ratio: 0.89, throughput: 0.88},
		},
	}
}

func newVlogCompressionSelector(policy vlogAutoPolicy, holdBytes, probeBytes uint64) *vlogCompressionSelector {
	return newVlogCompressionSelectorWithSeed(policy, holdBytes, probeBytes, valuelog.BlockCodecSnappy)
}

func (s *vlogCompressionSelector) choose(dictAvailable bool, rawPayloadBytes, unitPayloadBytes int) (vlogCompressionWriteMode, valuelog.BlockCodec, bool) {
	if s == nil {
		if dictAvailable {
			return vlogWriteDict, valuelog.BlockCodecSnappy, false
		}
		return vlogWriteBlock, valuelog.BlockCodecSnappy, false
	}
	if rawPayloadBytes <= 0 {
		return vlogWriteOff, s.seedCodec, false
	}
	if unitPayloadBytes <= 0 {
		unitPayloadBytes = rawPayloadBytes
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
				if s.incompressibleStreak >= 3 && s.holdRemaining > 0 {
					// After repeated incompressible outcomes, probe sparsely while in
					// hold to keep overhead close to off-mode behavior.
					nextProbe = s.holdRemaining
				}
				if s.holdRemaining > 0 && nextProbe > s.holdRemaining {
					nextProbe = s.holdRemaining
				}
				s.probeRemaining = nextProbe
				candidate := s.preferredProbeCandidate(dictAvailable)
				if s.shouldPreferLargePayloadDict(dictAvailable, unitPayloadBytes) {
					candidate = vlogAutoCandidateDict
				}
				candidate = s.normalizeLargePayloadCandidate(candidate, unitPayloadBytes)
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
			nextInterval := s.exploreBytes
			if s.currentCandidate == vlogAutoCandidateOff && s.incompressibleStreak >= 2 {
				nextInterval = s.exploreBytes * 8
			} else if s.dwellBytes > 0 && s.currentCandidate != vlogAutoCandidateOff && s.modeBytes >= 4*s.dwellBytes {
				// Once a non-off winner has held through multiple dwell windows,
				// reduce exploration pressure to limit steady-state probe overhead.
				nextInterval = s.exploreBytes * 4
			}
			if nextInterval == 0 {
				nextInterval = s.exploreBytes
			}
			s.exploreRemaining = nextInterval
			if nextInterval == s.exploreBytes && !s.shouldSkipExploration(dictAvailable) {
				candidate := s.preferredExplorationCandidate(dictAvailable)
				if s.shouldPreferLargePayloadDict(dictAvailable, unitPayloadBytes) {
					candidate = vlogAutoCandidateDict
				}
				candidate = s.normalizeLargePayloadCandidate(candidate, unitPayloadBytes)
				if candidate != s.currentCandidate {
					s.probeAttempts++
					mode, codec := candidateWriteMode(candidate, s.seedCodec)
					return mode, codec, true
				}
			}
		} else {
			s.exploreRemaining -= rawBytes
		}
	}

	if s.shouldPreferLargePayloadDict(dictAvailable, unitPayloadBytes) {
		if s.currentCandidate != vlogAutoCandidateDict {
			s.switches[s.currentCandidate][vlogAutoCandidateDict]++
			s.currentCandidate = vlogAutoCandidateDict
			s.modeBytes = rawBytes
		} else {
			s.modeBytes += rawBytes
		}
		mode, codec := candidateWriteMode(vlogAutoCandidateDict, s.seedCodec)
		return mode, codec, false
	}

	target := s.preferredCandidate(dictAvailable)
	target = s.normalizeLargePayloadCandidate(target, unitPayloadBytes)
	chosen := s.maybeSwitch(target, rawBytes, dictAvailable)
	mode, codec := candidateWriteMode(chosen, s.seedCodec)
	return mode, codec, false
}

func (s *vlogCompressionSelector) shouldSkipExploration(dictAvailable bool) bool {
	current := s.currentCandidate
	if current == vlogAutoCandidateOff {
		return false
	}
	currentMetric := s.metric(current)
	if currentMetric.samples < 4 || currentMetric.ratio > 0.25 {
		return false
	}
	// Keep exploration on until every block codec has initial signal.
	if isBlockCandidate(current) {
		for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
			if s.metric(candidate).samples == 0 {
				return false
			}
		}
	}
	// If dict is available but still unsampled, allow occasional exploration so
	// auto can still discover dict wins.
	if dictAvailable && (s.metric(vlogAutoCandidateDict).samples == 0 || s.seededCandidates[vlogAutoCandidateDict]) {
		return false
	}
	cands := s.availableCandidates(dictAvailable)
	for _, c := range cands {
		if c == current {
			continue
		}
		if !s.skipExplorationCandidate(current, currentMetric, c) {
			return false
		}
	}
	return true
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
	s.seededCandidates[candidate] = false
	s.bytesByCandidate[candidate] += uint64(rawPayloadBytes)
	s.framesByCandidate[candidate]++

	const incompressibleRatio = 0.98
	offThroughput := s.offThroughput()
	if offThroughput <= 0 {
		offThroughput = 1.0
	}
	if probe && candidate != vlogAutoCandidateOff {
		updated := s.metric(candidate)
		if updated.ratio < 0.99 && s.candidateLikelyBeneficial(candidate) {
			s.probeSuccesses++
			s.clearHold()
			if candidate != s.currentCandidate {
				currentMetric := s.metric(s.currentCandidate)
				// Let successful probes steer steady-state mode quickly when the
				// probed candidate shows a clear size or throughput advantage.
				if updated.ratio <= currentMetric.ratio*0.90 || updated.throughput >= currentMetric.throughput*1.05 {
					s.switches[s.currentCandidate][candidate]++
					s.currentCandidate = candidate
					s.modeBytes = 0
				}
			}
		}
	}

	if candidate == vlogAutoCandidateOff {
		return
	}
	if ratio >= incompressibleRatio {
		if s.incompressibleStreak < 0xFF {
			s.incompressibleStreak++
		}
		if s.holdBytes > 0 {
			if s.incompressibleStreak >= 2 && s.holdRemaining == 0 {
				s.holdEnters++
				s.holdRemaining = s.holdBytes
				nextProbe := s.probeBytes
				if nextProbe == 0 || nextProbe > s.holdRemaining {
					nextProbe = s.holdRemaining
				}
				s.probeRemaining = nextProbe
				s.currentCandidate = vlogAutoCandidateOff
				s.modeBytes = 0
			} else if s.incompressibleStreak >= 3 && s.holdRemaining > 0 {
				// Persist hold for long incompressible spans and probe sparsely.
				s.holdRemaining = s.holdBytes
				if s.probeBytes > 0 {
					s.probeRemaining = s.holdRemaining
				}
			}
		}
		return
	}
	s.incompressibleStreak = 0
}

func (s *vlogCompressionSelector) blockObservedRatio(codec valuelog.BlockCodec) float64 {
	ratio, _ := s.blockObservedRatioWithSamples(codec)
	return ratio
}

func (s *vlogCompressionSelector) blockObservedRatioWithSamples(codec valuelog.BlockCodec) (float64, uint64) {
	if s == nil {
		return 1.0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := blockCandidateFromCodec(codec)
	m := s.metric(candidate)
	if m.samples > 0 {
		return m.ratio, m.samples
	}
	// Fall back to whichever block codec has the strongest signal.
	best := vlogCandidateMetrics{}
	bestSet := false
	for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
		block := s.metric(candidate)
		if block.samples == 0 {
			continue
		}
		if !bestSet || block.samples > best.samples ||
			(block.samples == best.samples && (block.ratio < best.ratio || (block.ratio == best.ratio && block.throughput > best.throughput))) {
			best = block
			bestSet = true
		}
	}
	if !bestSet {
		return 0.92, 0
	}
	return best.ratio, best.samples
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

func (s *vlogCompressionSelector) allowDictSampling(writeMode vlogCompressionWriteMode) bool {
	if s == nil {
		return writeMode != vlogWriteOff
	}
	if writeMode == vlogWriteDict {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentCandidate == vlogAutoCandidateOff && (s.holdRemaining > 0 || s.incompressibleStreak >= 2) {
		return false
	}
	blockSamples := uint64(0)
	for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
		blockSamples += s.metrics[candidate].samples
	}
	if blockSamples < 4 {
		return false
	}
	bestRatio := 1.0
	for _, candidate := range []vlogAutoCandidate{vlogAutoCandidateBlockSnappy, vlogAutoCandidateBlockLZ4, vlogAutoCandidateBlockZSTD} {
		block := s.metrics[candidate]
		if block.samples == 0 {
			continue
		}
		r := normalizeMetricRatio(block.ratio)
		if r < bestRatio {
			bestRatio = r
		}
	}
	return bestRatio <= 0.97
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

type vlogBlockKSnapshot struct {
	Count   [vlogBlockCodecCount]uint64
	Sum     [vlogBlockCodecCount]uint64
	Max     [vlogBlockCodecCount]uint64
	Buckets [vlogBlockCodecCount][vlogBlockKBucketCount]uint64
}

type vlogBlockRatioSnapshot struct {
	Ratio   [vlogBlockCodecCount]float64
	Samples [vlogBlockCodecCount]uint64
}

func observeLaneVlogBlockRatio(l *lane, codec valuelog.BlockCodec, rawPayloadBytes, storedPayloadBytes int) {
	if l == nil || rawPayloadBytes <= 0 {
		return
	}
	ratio := 1.0
	if storedPayloadBytes > 0 {
		ratio = float64(storedPayloadBytes) / float64(rawPayloadBytes)
	}
	ratio = normalizeMetricRatio(ratio)
	idx := vlogBlockCodecIndex(codec)
	oldSamples := l.vlogBlockRatioSamples[idx].Load()
	oldBits := l.vlogBlockRatioBits[idx].Load()
	oldRatio := 1.0
	if oldSamples > 0 && oldBits != 0 {
		oldRatio = normalizeMetricRatio(math.Float64frombits(oldBits))
	}
	nextRatio, _ := ewmaMetric(oldRatio, oldSamples, ratio)
	l.vlogBlockRatioBits[idx].Store(math.Float64bits(nextRatio))
	l.vlogBlockRatioSamples[idx].Add(1)
}

func laneVlogBlockObservedRatio(l *lane, codec valuelog.BlockCodec) float64 {
	if l == nil {
		return 1.0
	}
	idx := vlogBlockCodecIndex(codec)
	if l.vlogBlockRatioSamples[idx].Load() == 0 {
		return 1.0
	}
	bits := l.vlogBlockRatioBits[idx].Load()
	if bits == 0 {
		return 1.0
	}
	return normalizeMetricRatio(math.Float64frombits(bits))
}

func laneVlogBlockObservedRatioWithSamples(l *lane, codec valuelog.BlockCodec) (float64, uint64) {
	if l == nil {
		return 1.0, 0
	}
	idx := vlogBlockCodecIndex(codec)
	samples := l.vlogBlockRatioSamples[idx].Load()
	if samples == 0 {
		return 1.0, 0
	}
	bits := l.vlogBlockRatioBits[idx].Load()
	if bits == 0 {
		return 1.0, samples
	}
	return normalizeMetricRatio(math.Float64frombits(bits)), samples
}

func chooseLargePayloadNoDictBlockCodecObserved(l *lane, configured valuelog.BlockCodec) (valuelog.BlockCodec, bool) {
	configured = normalizeSelectorBlockCodec(configured)
	bestCodec := configured
	bestRatio := 1.0
	bestSamples := uint64(0)
	found := false
	for _, codec := range vlogSelectorBlockCodecs {
		ratio, samples := laneVlogBlockObservedRatioWithSamples(l, codec)
		if samples < largePayloadBlockCodecMinSamples {
			continue
		}
		if !found ||
			ratio+largePayloadBlockCodecTieMargin < bestRatio ||
			(math.Abs(ratio-bestRatio) <= largePayloadBlockCodecTieMargin && samples > bestSamples) {
			bestCodec = codec
			bestRatio = ratio
			bestSamples = samples
			found = true
		}
	}
	if found {
		return bestCodec, true
	}
	return configured, false
}

func chooseLargePayloadNoDictBlockCodec(l *lane, configured valuelog.BlockCodec) valuelog.BlockCodec {
	codec, _ := chooseLargePayloadNoDictBlockCodecObserved(l, configured)
	return codec
}

func retainedStorageFirstObservedBlockCodecReady(l *lane) bool {
	sampledCodecs := 0
	for _, codec := range vlogSelectorBlockCodecs {
		_, samples := laneVlogBlockObservedRatioWithSamples(l, codec)
		if samples < largePayloadBlockCodecMinSamples {
			continue
		}
		if codec == valuelog.BlockCodecZSTD {
			return true
		}
		sampledCodecs++
	}
	return sampledCodecs >= 2
}

func chooseRetainedStorageFirstBlockCodec(l *lane, configured valuelog.BlockCodec, mode vlogCompressionMode) valuelog.BlockCodec {
	configured = normalizeSelectorBlockCodec(configured)
	if mode != vlogCompressionDefault && mode != vlogCompressionAuto {
		return configured
	}
	if codec, ok := chooseLargePayloadNoDictBlockCodecObserved(l, configured); ok && retainedStorageFirstObservedBlockCodecReady(l) {
		return codec
	}
	return valuelog.BlockCodecZSTD
}

func (db *DB) storageFirstValueLogAuto(unitPayloadBytes int) bool {
	if db == nil {
		return false
	}
	if unitPayloadBytes < forcePointerAutoBlockMinPayloadBytes {
		return false
	}
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput {
		return false
	}
	if db.forceValueLogPointers {
		return true
	}
	return unitPayloadBytes > db.minValueLogInlineThreshold()
}

func (db *DB) retainedStorageFirstValueLogAuto(rawPayloadBytes, unitPayloadBytes int, records []valuelog.Record) bool {
	if db == nil || len(records) == 0 {
		return false
	}
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput {
		return false
	}
	if rawPayloadBytes <= 0 {
		for i := range records {
			rawPayloadBytes += len(records[i].Value)
		}
	}
	if unitPayloadBytes <= 0 {
		unitPayloadBytes = rawPayloadBytes / len(records)
	}
	if !valueLogRecordsLookRetainedJSONLike(records) {
		return false
	}
	if db.storageFirstValueLogAuto(unitPayloadBytes) {
		return true
	}
	if unitPayloadBytes >= forcePointerAutoBlockMinPayloadBytes {
		return true
	}
	return unitPayloadBytes >= retainedStorageFirstMinUnitPayloadBytes &&
		rawPayloadBytes >= retainedStorageFirstMinBatchPayloadBytes
}

func (db *DB) retainedStorageFirstValueLogAutoValue(value []byte) bool {
	if db == nil || !valueLogPayloadLooksRetainedJSONLike(value) {
		return false
	}
	if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput {
		return false
	}
	return db.storageFirstValueLogAuto(len(value)) || len(value) >= forcePointerAutoBlockMinPayloadBytes
}

func (db *DB) valueLogPayloadExceedsInlineThreshold(unitPayloadBytes int) bool {
	if db == nil {
		return false
	}
	if db.forceValueLogPointers {
		return true
	}
	return unitPayloadBytes > db.minValueLogInlineThreshold()
}

func (db *DB) minValueLogInlineThreshold() int {
	threshold := db.valueLogThreshold
	if threshold <= 0 {
		threshold = page.DefaultInlineThreshold
	}
	for i := range db.valueLogDomainThresholds {
		domainThreshold := db.valueLogDomainThresholds[i].InlineThreshold
		if domainThreshold >= 0 && domainThreshold < threshold {
			threshold = domainThreshold
		}
	}
	return threshold
}

func (db *DB) storageFirstMediumValueLogAuto(unitPayloadBytes int) bool {
	return db.storageFirstValueLogAuto(unitPayloadBytes) &&
		unitPayloadBytes < largePayloadBlockTargetMinPayloadBytes
}

func (db *DB) preferLeafPageBlockCodec(l *lane, unitPayloadBytes int, configured valuelog.BlockCodec) (valuelog.BlockCodec, bool) {
	if db == nil || l == nil || !db.indexOuterLeavesInValueLog {
		return configured, false
	}
	if !db.isLeafLogAppendLane(l) {
		return configured, false
	}
	switch normalizeVlogCompressionMode(db.valueLogCompressionMode) {
	case vlogCompressionDefault, vlogCompressionAuto:
		if normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput {
			return configured, false
		}
		return valuelog.BlockCodecLZ4, true
	default:
		return configured, false
	}
}

func recordLaneVlogBlockK(l *lane, codec valuelog.BlockCodec, k int) {
	if l == nil || k <= 0 {
		return
	}
	idx := vlogBlockCodecIndex(codec)
	ku := uint64(k)
	l.vlogBlockKCount[idx].Add(1)
	l.vlogBlockKSum[idx].Add(ku)
	for {
		cur := l.vlogBlockKMax[idx].Load()
		if ku <= cur {
			break
		}
		if l.vlogBlockKMax[idx].CompareAndSwap(cur, ku) {
			break
		}
	}
	b := vlogBlockKBucketIndex(k)
	l.vlogBlockKBuckets[idx][b].Add(1)
}

func snapshotLaneVlogBlockK(l *lane) vlogBlockKSnapshot {
	out := vlogBlockKSnapshot{}
	if l == nil {
		return out
	}
	for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
		out.Count[codecIdx] = l.vlogBlockKCount[codecIdx].Load()
		out.Sum[codecIdx] = l.vlogBlockKSum[codecIdx].Load()
		out.Max[codecIdx] = l.vlogBlockKMax[codecIdx].Load()
		for bucket := 0; bucket < vlogBlockKBucketCount; bucket++ {
			out.Buckets[codecIdx][bucket] = l.vlogBlockKBuckets[codecIdx][bucket].Load()
		}
	}
	return out
}

func snapshotLaneVlogBlockRatio(l *lane) vlogBlockRatioSnapshot {
	out := vlogBlockRatioSnapshot{}
	if l == nil {
		return out
	}
	for codecIdx := 0; codecIdx < vlogBlockCodecCount; codecIdx++ {
		samples := l.vlogBlockRatioSamples[codecIdx].Load()
		out.Samples[codecIdx] = samples
		if samples == 0 {
			continue
		}
		bits := l.vlogBlockRatioBits[codecIdx].Load()
		if bits == 0 {
			continue
		}
		out.Ratio[codecIdx] = normalizeMetricRatio(math.Float64frombits(bits))
	}
	return out
}

func (db *DB) vlogSelectorEnabled(mode vlogCompressionMode) bool {
	if db == nil {
		return false
	}
	switch mode {
	case vlogCompressionAuto:
		return true
	case vlogCompressionDict:
		// Keep explicit dict mode strict by default. Only enable selector-driven
		// dict-vs-block adaptation in aggressive autotune mode.
		return db.valueLogAutotuneOptions.Mode == valuelog.AutotuneAggressive
	default:
		return false
	}
}

func (db *DB) resolveVlogWriteMode(l *lane, dictID uint64, rawPayloadBytes, unitPayloadBytes int, outerLeafPayload bool) (vlogCompressionWriteMode, valuelog.BlockCodec, bool) {
	mode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	switch mode {
	case vlogCompressionOff:
		return vlogWriteOff, db.valueLogBlockCodec, false
	case vlogCompressionBlock:
		return vlogWriteBlock, db.valueLogBlockCodec, false
	case vlogCompressionDict:
		if unitPayloadBytes <= 0 {
			unitPayloadBytes = rawPayloadBytes
		}
		if dictID == 0 {
			if db.vlogSelectorEnabled(mode) {
				return vlogWriteBlock, db.valueLogBlockCodec, false
			}
			return vlogWriteOff, db.valueLogBlockCodec, false
		}
		if db.vlogSelectorEnabled(mode) &&
			outerLeafPayload &&
			db.indexOuterLeavesInValueLog &&
			!db.valueLogDictAllowOuterLeaf() &&
			unitPayloadBytes >= valueLogDictClassifierLargePayloadBypassMin {
			// Large outer-leaf payload streams are more stable on block codecs;
			// force block in dict+aggressive mode so explicit dict runs do not
			// regress into oversized raw-like frames on this path.
			return vlogWriteBlock, db.valueLogBlockCodec, false
		}
		if db.vlogSelectorEnabled(mode) && l != nil && l.vlogCompressionSelector != nil {
			chosenMode, chosenCodec, probe := l.vlogCompressionSelector.choose(true, rawPayloadBytes, unitPayloadBytes)
			if chosenMode == vlogWriteOff {
				// In dict mode prefer block fallback over raw/off to preserve
				// compression ratio when dict loses.
				return vlogWriteBlock, db.valueLogBlockCodec, probe
			}
			if chosenMode == vlogWriteBlock {
				// Keep explicit dict mode deterministic: selector may choose dict
				// vs block, but block writes should honor configured block codec.
				return vlogWriteBlock, db.valueLogBlockCodec, probe
			}
			return chosenMode, chosenCodec, probe
		}
		return vlogWriteDict, db.valueLogBlockCodec, false
	default:
		// Default/unset compression behavior follows auto mode.
		if unitPayloadBytes <= 0 {
			unitPayloadBytes = rawPayloadBytes
		}
		if dictID == 0 && db.storageFirstValueLogAuto(unitPayloadBytes) {
			return vlogWriteBlock, chooseLargePayloadNoDictBlockCodec(l, db.valueLogBlockCodec), false
		}
		if dictID == 0 && normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput && unitPayloadBytes >= throughputAutoBlockMinPayloadBytes {
			return vlogWriteBlock, db.valueLogBlockCodec, false
		}
		if dictID == 0 && unitPayloadBytes >= 2048 && normalizeVlogAutoPolicy(db.valueLogAutoPolicy) != vlogAutoThroughput {
			// For large payloads in balanced/size policies, keep a stable block
			// path and avoid per-write selector overhead in the hot path. Use
			// observed block ratios when available instead of a fixed codec bias.
			return vlogWriteBlock, chooseLargePayloadNoDictBlockCodec(l, db.valueLogBlockCodec), false
		}
		if l == nil || l.vlogCompressionSelector == nil {
			if dictID != 0 {
				return vlogWriteDict, db.valueLogBlockCodec, false
			}
			return vlogWriteBlock, db.valueLogBlockCodec, false
		}
		chosenMode, chosenCodec, probe := l.vlogCompressionSelector.choose(dictID != 0, rawPayloadBytes, unitPayloadBytes)
		if chosenMode == vlogWriteOff && db.storageFirstValueLogAuto(unitPayloadBytes) {
			return vlogWriteBlock, chooseLargePayloadNoDictBlockCodec(l, db.valueLogBlockCodec), probe
		}
		return chosenMode, chosenCodec, probe
	}
}

func (db *DB) shouldBypassAutoRawValueCompression(dictID uint64, records []valuelog.Record, unitPayloadBytes int, payloadKind vlogPayloadKind) bool {
	if db == nil || normalizeVlogCompressionMode(db.valueLogCompressionMode) != vlogCompressionAuto {
		return false
	}
	if dictID != 0 || payloadKind != vlogPayloadKindSingleValue {
		return false
	}
	if !db.valueLogPayloadExceedsInlineThreshold(unitPayloadBytes) {
		return false
	}
	if unitPayloadBytes < forcePointerAutoRawBypassMinPayloadBytes ||
		unitPayloadBytes > forcePointerAutoRawBypassMaxPayloadBytes ||
		len(records) == 0 {
		return false
	}
	if db.storageFirstValueLogAuto(unitPayloadBytes) && valueLogRecordsLookRetainedJSONLike(records) {
		return false
	}
	checks := [3]int{0, len(records) / 2, len(records) - 1}
	checked := 0
	prev := -1
	for _, idx := range checks {
		if idx < 0 || idx >= len(records) {
			continue
		}
		if idx == prev {
			continue
		}
		prev = idx
		checked++
		if likelyCompressibleSample(records[idx].Value) {
			return false
		}
	}
	return checked > 0
}

func valueLogRecordsLookRetainedJSONLike(records []valuelog.Record) bool {
	if len(records) == 0 {
		return false
	}
	checks := [3]int{0, len(records) / 2, len(records) - 1}
	checked := 0
	prev := -1
	for _, idx := range checks {
		if idx < 0 || idx >= len(records) || idx == prev {
			continue
		}
		prev = idx
		checked++
		if !valueLogPayloadLooksRetainedJSONLike(records[idx].Value) {
			return false
		}
	}
	return checked > 0
}

func valueLogPayloadLooksRetainedJSONLike(value []byte) bool {
	for i, b := range value {
		switch b {
		case ' ', '\n', '\r', '\t':
			continue
		case '{', '[':
			return true
		case 'T':
			return len(value)-i >= 4 &&
				(value[i+1] == 'D' && value[i+2] == '1') &&
				(value[i+3] == 'D' || value[i+3] == 'I' || value[i+3] == 'H')
		case 'c':
			return bytes.HasPrefix(value[i:], valueLogRetainedSemanticStreamV1BlockMagic) ||
				bytes.HasPrefix(value[i:], valueLogRetainedSemanticStreamV1BlockZSTDMagic)
		default:
			return false
		}
	}
	return false
}

func (db *DB) observeVlogWriteMode(l *lane, mode vlogCompressionWriteMode, blockCodec valuelog.BlockCodec, rawPayloadBytes, unitPayloadBytes, storedPayloadBytes int, probe bool, wallNs int64) {
	if db == nil || l == nil {
		return
	}
	if unitPayloadBytes <= 0 {
		unitPayloadBytes = rawPayloadBytes
	}
	recordLaneVlogWriteObservation(l, mode, rawPayloadBytes, unitPayloadBytes, storedPayloadBytes)
	if mode == vlogWriteBlock {
		observeLaneVlogBlockRatio(l, blockCodec, rawPayloadBytes, storedPayloadBytes)
	}
	compressionMode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	if !db.vlogSelectorEnabled(compressionMode) {
		return
	}
	if compressionMode == vlogCompressionAuto &&
		db.forceValueLogPointers &&
		mode == vlogWriteOff &&
		unitPayloadBytes >= forcePointerAutoRawBypassMinPayloadBytes {
		// Raw high-entropy forced-pointer batches do not teach the selector
		// anything useful after the caller already bypassed compression.
		return
	}
	if compressionMode == vlogCompressionAuto &&
		normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput &&
		mode == vlogWriteBlock &&
		unitPayloadBytes >= throughputAutoBlockMinPayloadBytes {
		// Throughput policy forces block mode for medium+ payloads in resolve.
		// Skip selector updates here to avoid unnecessary per-write churn.
		return
	}
	if compressionMode == vlogCompressionAuto &&
		db.forceValueLogPointers &&
		mode == vlogWriteBlock &&
		unitPayloadBytes >= forcePointerAutoBlockMinPayloadBytes {
		// Force-pointer large-value writes run on a stable block path; skip
		// selector updates to keep the hot path lean.
		return
	}
	if l.vlogCompressionSelector == nil {
		return
	}
	l.vlogCompressionSelector.observe(mode, blockCodec, rawPayloadBytes, storedPayloadBytes, wallNs, probe)
}

func (db *DB) isLiveLeafLogLane(l *lane) bool {
	return db != nil && db.isLeafLogAppendLane(l)
}

func (db *DB) clampLiveLeafLogFrameK(l *lane, k int) int {
	if k > leafLogBlockMaxK && db.isLiveLeafLogLane(l) {
		return leafLogBlockMaxK
	}
	return k
}

func (db *DB) chooseValueLogRawWriteK(l *lane, records int, autoRawBypass, paused bool) int {
	if records <= 1 {
		return 1
	}
	k := 1
	switch {
	case db != nil && autoRawBypass:
		k = valuelog.MaxFrameK
	case db != nil && paused && db.disableJournal:
		k = valuelog.MaxFrameK
	case db != nil:
		if cur := int(db.valueLogDictCurrentK.Load()); cur > 1 {
			k = cur
		} else {
			k = 8
			if db.disableJournal && db.forceValueLogPointers {
				k = 16
			}
		}
	default:
		k = 8
	}
	if k < 1 {
		k = 1
	}
	if k > valuelog.MaxFrameK {
		k = valuelog.MaxFrameK
	}
	return db.clampLiveLeafLogFrameK(l, k)
}

func (db *DB) chooseValueLogBlockWriteK(l *lane, records, rawPayloadBytes int, codec valuelog.BlockCodec) int {
	if records <= 1 {
		recordLaneVlogBlockK(l, codec, 1)
		return 1
	}
	compressionMode := normalizeVlogCompressionMode(db.valueLogCompressionMode)
	if compressionMode == vlogCompressionDict &&
		db.vlogSelectorEnabled(compressionMode) &&
		db.forceValueLogPointers &&
		db.disableJournal {
		// Dict+aggressive fallback in WAL-off pointer-heavy ingest should use the
		// same large grouped-frame behavior as the explicit block profile.
		k := records
		if k > valuelog.MaxFrameK {
			k = valuelog.MaxFrameK
		}
		k = db.clampLiveLeafLogFrameK(l, k)
		recordLaneVlogBlockK(l, codec, k)
		return k
	}
	avgPayloadBytes := rawPayloadBytes / records
	ratio := 1.0
	ratioSamples := uint64(0)
	// K sizing should stay on the lane-observed block ratio in explicit dict
	// mode. The selector remains responsible for dict-vs-block mode choice, but
	// feeding selector candidate ratios back into frame grouping in dict mode can
	// collapse K on fallback paths.
	useSelectorRatio := compressionMode == vlogCompressionAuto && l != nil && l.vlogCompressionSelector != nil
	stableFastPath := compressionMode == vlogCompressionAuto &&
		(db.storageFirstValueLogAuto(avgPayloadBytes) ||
			(normalizeVlogAutoPolicy(db.valueLogAutoPolicy) == vlogAutoThroughput && avgPayloadBytes >= throughputAutoBlockMinPayloadBytes))
	if useSelectorRatio && !stableFastPath {
		ratio, ratioSamples = l.vlogCompressionSelector.blockObservedRatioWithSamples(codec)
	}
	if ratio <= 0 || stableFastPath || !useSelectorRatio {
		ratio, ratioSamples = laneVlogBlockObservedRatioWithSamples(l, codec)
	}
	if avgPayloadBytes >= largePayloadBlockTargetMinPayloadBytes && ratioSamples == 0 && ratio >= 0.98 {
		ratio = largePayloadBlockBootstrapRatio
	}
	targetCompressedBytes := db.valueLogBlockTargetBytes
	if db.isLiveLeafLogLane(l) {
		if targetCompressedBytes < leafLogBlockTargetCompressedBytes {
			targetCompressedBytes = leafLogBlockTargetCompressedBytes
		}
	}
	if db.storageFirstValueLogAuto(avgPayloadBytes) &&
		ratioSamples == 0 &&
		ratio >= 0.98 {
		// Storage-owned retained payload streams are expected to be JSON-like in the
		// storage-parity workload. Bootstrap grouped frames on the first batch so
		// compression can exploit cross-record repetition before lane ratios exist.
		ratio = forcePointerBlockBootstrapRatio
	}
	if db.isLiveLeafLogLane(l) && ratioSamples == 0 {
		// Batch-only leaf-log bootstrap: without an initial lane-local block ratio,
		// the generic incompressible guard chooses K=1 and defeats AppendLeafPages.
		// Seed only the dedicated live leaf-log lane so the first multi-page batch
		// forms grouped frames; normal observed ratios take over after writes.
		ratio = 0.50
	}
	if db.storageFirstValueLogAuto(avgPayloadBytes) && targetCompressedBytes < forcePointerBlockTargetCompressedBytes {
		targetCompressedBytes = forcePointerBlockTargetCompressedBytes
	}
	if avgPayloadBytes >= largePayloadBlockTargetMinPayloadBytes {
		largePayloadTargetBytes := valuelog.NormalizeBlockTargetCompressedBytes(avgPayloadBytes * largePayloadBlockTargetMultiplier)
		if targetCompressedBytes < largePayloadTargetBytes {
			targetCompressedBytes = largePayloadTargetBytes
		}
	}
	k := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, targetCompressedBytes, ratio)
	if k < 1 {
		k = 1
	}
	if k > valuelog.MaxFrameK {
		k = valuelog.MaxFrameK
	}
	k = db.clampLiveLeafLogFrameK(l, k)
	recordLaneVlogBlockK(l, codec, k)
	return k
}

func (db *DB) chooseRetainedStorageFirstValueLogBlockWriteK(l *lane, records, rawPayloadBytes int, codec valuelog.BlockCodec) int {
	if records <= 1 {
		recordLaneVlogBlockK(l, codec, 1)
		return 1
	}
	ratio, ratioSamples := laneVlogBlockObservedRatioWithSamples(l, codec)
	if ratio <= 0 || ratioSamples == 0 || ratio >= 0.98 {
		ratio = forcePointerBlockBootstrapRatio
	}
	targetCompressedBytes := db.valueLogBlockTargetBytes
	if targetCompressedBytes < storageFirstRetainedValueLogBlockTargetCompressedBytes {
		targetCompressedBytes = storageFirstRetainedValueLogBlockTargetCompressedBytes
	}
	k := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, targetCompressedBytes, ratio)
	if k < 1 {
		k = 1
	}
	if k > valuelog.MaxFrameK {
		k = valuelog.MaxFrameK
	}
	k = db.clampLiveLeafLogFrameK(l, k)
	recordLaneVlogBlockK(l, codec, k)
	return k
}
