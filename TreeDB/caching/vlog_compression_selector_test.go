package caching

import (
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestVlogCompressionSelector_EntersHoldAndProbes(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)

	mode, _, probe := s.choose(false, 128, 128)
	if mode != vlogWriteBlock || probe {
		t.Fatalf("initial choose: mode=%v probe=%t", mode, probe)
	}
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 128, 128, 200, true)
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 128, 128, 200, true)

	mode, _, probe = s.choose(false, 64, 64)
	if mode != vlogWriteOff || probe {
		t.Fatalf("expected hold bypass (off, no-probe), got mode=%v probe=%t", mode, probe)
	}

	// Consume hold bytes until probe boundary is reached.
	for i := 0; i < 8; i++ {
		mode, _, probe = s.choose(false, 64, 64)
		if probe {
			break
		}
	}
	if !probe {
		t.Fatalf("expected periodic probe during hold")
	}
	if mode != vlogWriteBlock {
		t.Fatalf("expected block probe mode, got %v", mode)
	}
}

func TestVlogCompressionSelector_ExplorationProbeOutsideHold(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)
	s.exploreBytes = 64
	s.exploreRemaining = 64

	mode, codec, probe := s.choose(false, 64, 64)
	if !probe {
		t.Fatalf("expected exploration probe outside hold")
	}
	if mode != vlogWriteBlock {
		t.Fatalf("expected block exploration probe, got mode=%v", mode)
	}
	if codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 exploration probe for unsampled block codec, got %v", codec)
	}
}

func TestVlogCompressionSelector_ExplorationProbesDictWhenAvailable(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0

	// Warm both block candidates so dict remains the least-sampled candidate.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 760, 900, false)
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 1024, 780, 920, false)

	mode, _, probe := s.choose(true, 512, 512)
	if !probe {
		t.Fatalf("expected exploration probe with dict available")
	}
	if mode != vlogWriteDict {
		t.Fatalf("expected dict exploration probe, got %v", mode)
	}
}

func TestVlogCompressionSelector_DictSelectionByPolicy(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0

	// Establish an off baseline first.
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Dict clearly beats block on ratio while keeping throughput.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 650, 1024, false)

	mode, _, _ := s.choose(true, 1024, 1024)
	if mode != vlogWriteDict {
		t.Fatalf("expected dict mode when dict beats block materially, got %v", mode)
	}
}

func TestVlogCompressionSelector_DwellPreventsFlap(t *testing.T) {
	s := newVlogCompressionSelectorWithSeed(vlogAutoBalanced, 0, 0, valuelog.BlockCodecSnappy)
	s.dwellBytes = 4096

	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Dict looks better, but current mode should hold until dwell budget is spent.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 600, 1024, false)

	mode, _, _ := s.choose(true, 512, 512)
	if mode != vlogWriteBlock {
		t.Fatalf("expected dwell to keep block mode, got %v", mode)
	}
	mode, _, _ = s.choose(true, 4096, 4096)
	if mode != vlogWriteDict {
		t.Fatalf("expected mode switch after dwell, got %v", mode)
	}
}

func TestVlogCompressionSelector_BlockCodecSelection(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Snappy compresses slightly better but is slower.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 700, 1700, false)
	// LZ4 is much faster with close-enough ratio.
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 1024, 740, 900, false)

	mode, codec, _ := s.choose(false, 1024, 1024)
	if mode != vlogWriteBlock {
		t.Fatalf("expected block mode, got %v", mode)
	}
	if codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 selection, got %v", codec)
	}
}

func TestResolveLeafPageVlogWriteMode_DefaultAutoPrefersSnappyBlock(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogBlockCodec:      valuelog.BlockCodecLZ4,
	}
	mode, codec, probe := db.resolveLeafPageVlogWriteMode(&lane{}, 4096, 4096)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("got mode=%v codec=%v probe=%t want block/snappy/no-probe", mode, codec, probe)
	}

	db.valueLogCompressionMode = uint8(vlogCompressionDefault)
	mode, codec, probe = db.resolveLeafPageVlogWriteMode(&lane{}, 4096, 4096)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("default got mode=%v codec=%v probe=%t want block/snappy/no-probe", mode, codec, probe)
	}
}

func TestResolveLeafPageVlogWriteMode_ExplicitBlockPreservesConfiguredCodec(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionBlock),
		valueLogBlockCodec:      valuelog.BlockCodecLZ4,
	}
	mode, codec, probe := db.resolveLeafPageVlogWriteMode(&lane{}, 4096, 4096)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 || probe {
		t.Fatalf("got mode=%v codec=%v probe=%t want explicit block/lz4/no-probe", mode, codec, probe)
	}
}

func TestVlogCompressionSelector_SnapshotCounters(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 256, 256, 256, false)
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 512, 400, 600, true)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 512, 280, 512, false)
	snap := s.snapshot()
	if snap.bytesByCandidate[vlogAutoCandidateOff] == 0 {
		t.Fatalf("expected off bytes > 0")
	}
	if snap.framesByCandidate[vlogAutoCandidateBlockLZ4] == 0 {
		t.Fatalf("expected lz4 frame count > 0")
	}
}

func TestVlogCompressionSelector_ConcurrentSmoke(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 4<<20, 512<<10)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				mode, codec, probe := s.choose(i%2 == 0, 512+(i%256), 512+(i%256))
				raw := 2048
				stored := 2048
				switch mode {
				case vlogWriteBlock:
					stored = 1500
				case vlogWriteDict:
					stored = 1200
				}
				s.observe(mode, codec, raw, stored, 1800, probe)
			}
		}(g)
	}
	wg.Wait()
}

func TestObserveVlogWriteMode_NonAutoUpdatesBlockRatioForK(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
	}
	l := &lane{}

	// Without observed block ratio we should be conservative (k=1).
	k0 := db.chooseValueLogBlockWriteK(l, 128, 128*256, valuelog.BlockCodecLZ4)
	if k0 != 1 {
		t.Fatalf("expected initial k=1 without signal, got %d", k0)
	}

	// Feed compressible observations in non-auto mode.
	for i := 0; i < 8; i++ {
		db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecLZ4, 4096, 4096, 512, false, 1000)
	}

	k1 := db.chooseValueLogBlockWriteK(l, 128, 128*256, valuelog.BlockCodecLZ4)
	if k1 <= 1 {
		t.Fatalf("expected k>1 after non-auto block observations, got %d", k1)
	}
}

func TestChooseValueLogBlockWriteK_RecordsBlockKStats(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
	}
	l := &lane{}

	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 1024, false, 1000)
	k := db.chooseValueLogBlockWriteK(l, 64, 64*512, valuelog.BlockCodecSnappy)
	if k < 1 {
		t.Fatalf("invalid k=%d", k)
	}

	snap := snapshotLaneVlogBlockK(l)
	if snap.Count[0] == 0 {
		t.Fatalf("expected snappy k count > 0")
	}
	if snap.Sum[0] == 0 {
		t.Fatalf("expected snappy k sum > 0")
	}
	if snap.Max[0] == 0 {
		t.Fatalf("expected snappy k max > 0")
	}
}

func TestChooseValueLogBlockWriteK_ForcePointerLargePayloadUsesLargerTarget(t *testing.T) {
	l := &lane{}
	base := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
	}
	force := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
	}

	// Seed a compressible observed ratio so K can grow above 1.
	base.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 1024, false, 1000)

	records := 64
	rawPayloadBytes := records * forcePointerAutoBlockMinPayloadBytes
	kBase := base.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	kForce := force.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	if kForce <= kBase {
		t.Fatalf("expected forced-pointer K to increase for large payloads, base=%d force=%d", kBase, kForce)
	}
}

func TestChooseValueLogBlockWriteK_ForcePointerAutoWithSelectorUsesLaneRatio(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
	}
	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	selector.dwellBytes = 0
	l := &lane{vlogCompressionSelector: selector}

	// Large force-pointer payloads skip selector.observe() by design, but lane
	// block-ratio stats are still updated and should drive K selection.
	for i := 0; i < 8; i++ {
		db.observeVlogWriteMode(
			l,
			vlogWriteBlock,
			valuelog.BlockCodecSnappy,
			forcePointerAutoBlockMinPayloadBytes,
			forcePointerAutoBlockMinPayloadBytes,
			forcePointerAutoBlockMinPayloadBytes/4,
			false,
			1000,
		)
	}
	if samples := selector.metrics[vlogAutoCandidateBlockSnappy].samples; samples != 0 {
		t.Fatalf("expected selector samples to remain zero on forced-pointer fast path, got %d", samples)
	}

	records := 128
	rawPayloadBytes := records * forcePointerAutoBlockMinPayloadBytes
	k := db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	if k <= 1 {
		t.Fatalf("expected k>1 using lane ratio for forced-pointer auto path, got %d", k)
	}
}

func TestChooseValueLogBlockWriteK_ForcePointerSmallPayloadKeepsBaseTarget(t *testing.T) {
	l := &lane{}
	base := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
	}
	force := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
	}

	base.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 1024, false, 1000)

	records := 64
	rawPayloadBytes := records * (forcePointerAutoBlockMinPayloadBytes - 1)
	kBase := base.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	kForce := force.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	if kForce != kBase {
		t.Fatalf("expected forced-pointer small payload K to match base, base=%d force=%d", kBase, kForce)
	}
}

func TestChooseValueLogBlockWriteK_ForcePointerLargePayloadRespectsConfiguredLargeTarget(t *testing.T) {
	l := &lane{}
	configuredTargetBytes := forcePointerBlockTargetCompressedBytes + (16 << 10)
	base := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: configuredTargetBytes,
	}
	force := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: configuredTargetBytes,
		forceValueLogPointers:    true,
	}

	// Seed an observed ratio that keeps K below MaxFrameK so target differences
	// remain visible.
	base.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 2048, false, 1000)

	records := 256
	rawPayloadBytes := records * forcePointerAutoBlockMinPayloadBytes
	kBase := base.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	kForce := force.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	if kForce != kBase {
		t.Fatalf("expected forced-pointer K to preserve configured target>=32KiB, base=%d force=%d", kBase, kForce)
	}

	ratio := laneVlogBlockObservedRatio(l, valuelog.BlockCodecSnappy)
	want := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, configuredTargetBytes, ratio)
	if kForce != want {
		t.Fatalf("expected forced-pointer K to use configured target=%d, got=%d want=%d", configuredTargetBytes, kForce, want)
	}
}

func TestChooseValueLogBlockWriteK_ThroughputFastPathUsesAveragePayload(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionAuto),
		valueLogAutoPolicy:       uint8(vlogAutoThroughput),
		valueLogBlockTargetBytes: 4096,
	}
	selector := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	l := &lane{vlogCompressionSelector: selector}

	records := 64
	avgPayloadBytes := throughputAutoBlockMinPayloadBytes / 2
	rawPayloadBytes := records * avgPayloadBytes

	got := db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	want := valuelog.ChooseBlockGroupK(
		records,
		rawPayloadBytes,
		db.valueLogBlockTargetBytes,
		selector.blockObservedRatio(valuelog.BlockCodecSnappy),
	)
	if got != want {
		t.Fatalf("expected selector-ratio K for small average payloads, got=%d want=%d", got, want)
	}
	if got <= 1 {
		t.Fatalf("expected K>1 when avg payload=%d is below throughput fast-path threshold=%d", avgPayloadBytes, throughputAutoBlockMinPayloadBytes)
	}
}
func TestChooseValueLogBlockWriteK_ForcePointerLargePayloadKDistributionGuardrail(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
	}
	l := &lane{}

	// Seed a stable compression ratio and then sample K repeatedly to catch
	// distribution regressions (for example, collapsing back toward k=1/8/16).
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 2048, false, 1000)
	const (
		iterations = 64
		records    = 128
	)
	rawPayloadBytes := records * forcePointerAutoBlockMinPayloadBytes
	for i := 0; i < iterations; i++ {
		_ = db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	}

	idx := vlogBlockCodecIndex(valuelog.BlockCodecSnappy)
	snap := snapshotLaneVlogBlockK(l)
	count := snap.Count[idx]
	if count != iterations {
		t.Fatalf("expected %d K samples, got %d", iterations, count)
	}
	avgK := float64(snap.Sum[idx]) / float64(count)
	if avgK < 32 {
		t.Fatalf("expected avg K >= 32 for large forced-pointer payloads, got %.2f", avgK)
	}
	kEq1 := snap.Buckets[idx][vlogBlockKBucketIndex(1)]
	if kEq1 > count/20 { // <=5% may be k=1
		t.Fatalf("expected k=1 share <=5%%, k_eq_1=%d total=%d", kEq1, count)
	}
	var kGe32 uint64
	for bucket := vlogBlockKBucketIndex(32); bucket < vlogBlockKBucketCount; bucket++ {
		kGe32 += snap.Buckets[idx][bucket]
	}
	if kGe32*10 < count*9 { // >=90%
		t.Fatalf("expected >=90%% of samples with K>=32, got k_ge_32=%d total=%d", kGe32, count)
	}
}

func BenchmarkChooseValueLogBlockWriteK_ForcePointerLargePayloadDistribution(b *testing.B) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionBlock),
		valueLogBlockTargetBytes: 4096,
		forceValueLogPointers:    true,
	}
	l := &lane{}
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, 4096, 4096, 2048, false, 1000)

	const records = 128
	rawPayloadBytes := records * forcePointerAutoBlockMinPayloadBytes

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	}
	b.StopTimer()

	idx := vlogBlockCodecIndex(valuelog.BlockCodecSnappy)
	snap := snapshotLaneVlogBlockK(l)
	count := snap.Count[idx]
	if count == 0 {
		b.Fatalf("expected non-zero K sample count")
	}
	avgK := float64(snap.Sum[idx]) / float64(count)
	b.ReportMetric(avgK, "k_avg")
	kEq1 := snap.Buckets[idx][vlogBlockKBucketIndex(1)]
	b.ReportMetric((float64(kEq1)*100.0)/float64(count), "k_eq_1_pct")
	var kGe32 uint64
	for bucket := vlogBlockKBucketIndex(32); bucket < vlogBlockKBucketCount; bucket++ {
		kGe32 += snap.Buckets[idx][bucket]
	}
	b.ReportMetric((float64(kGe32)*100.0)/float64(count), "k_ge_32_pct")
}

func TestVlogCompressionSelector_AvoidsOffWithStrongCompressionSignal(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.50, throughput: 0.70, samples: 8}

	mode, codec, probe := s.choose(false, 4096, 4096)
	if probe {
		t.Fatalf("did not expect probe")
	}
	if mode != vlogWriteBlock {
		t.Fatalf("expected block mode, got %v", mode)
	}
	if codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 codec, got %v", codec)
	}
}

func TestVlogCompressionSelector_AllowDictSampling(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	if s.allowDictSampling(vlogWriteBlock) {
		t.Fatalf("expected dict sampling disabled before enough block signal")
	}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.92, throughput: 0.95, samples: 2}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.90, throughput: 0.90, samples: 2}
	if !s.allowDictSampling(vlogWriteBlock) {
		t.Fatalf("expected dict sampling once block signal is compressible")
	}
	s.currentCandidate = vlogAutoCandidateOff
	s.holdRemaining = 1024
	s.incompressibleStreak = 3
	if s.allowDictSampling(vlogWriteBlock) {
		t.Fatalf("expected dict sampling disabled during incompressible hold")
	}
	if !s.allowDictSampling(vlogWriteDict) {
		t.Fatalf("expected dict writes to keep dict sampling enabled")
	}
}

func TestVlogCompressionSelector_ExplorationSkipsOffWhenCompressionStrong(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockLZ4
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.50, throughput: 0.90, samples: 8}
	// Make snappy clearly dominated so off would be the only alternate candidate
	// without skip logic.
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 1.20, throughput: 0.95, samples: 8}

	mode, codec, probe := s.choose(false, 64, 64)
	if probe {
		t.Fatalf("did not expect exploration probe when compression is clearly beneficial")
	}
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected steady lz4 choice, got mode=%v codec=%v", mode, codec)
	}
}

func TestVlogCompressionSelector_ExplorationSkipsDominatedBlockCodec(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockLZ4
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.12, throughput: 0.90, samples: 8}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.26, throughput: 0.95, samples: 8}

	mode, codec, probe := s.choose(false, 64, 64)
	if probe {
		t.Fatalf("did not expect probe for dominated block codec")
	}
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 steady choice, got mode=%v codec=%v", mode, codec)
	}
}

func TestVlogCompressionSelector_ExplorationStopsAfterStrongBlockSignal(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockLZ4
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.10, throughput: 0.90, samples: 8}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.22, throughput: 0.95, samples: 8}

	mode, codec, probe := s.choose(false, 64, 64)
	if probe {
		t.Fatalf("did not expect exploration probe after strong block signal")
	}
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 steady choice, got mode=%v codec=%v", mode, codec)
	}
}

func TestVlogCompressionSelector_ExplorationContinuesUntilBothBlockCodecsSampled(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.12, throughput: 0.95, samples: 8}
	// LZ4 unsampled: exploration should still probe it before freezing.
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{}

	mode, codec, probe := s.choose(false, 64, 64)
	if !probe {
		t.Fatalf("expected exploration probe before both block codecs are sampled")
	}
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 exploration probe, got mode=%v codec=%v", mode, codec)
	}
}

func TestVlogCompressionSelector_ShouldSkipExploration_WhenAllAlternativesDominated(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockLZ4
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.013, throughput: 1.00, samples: 8}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.047, throughput: 0.95, samples: 8}
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 8}
	if !s.shouldSkipExploration(false) {
		t.Fatalf("expected exploration skip when all alternatives are dominated")
	}
}

func TestVlogCompressionSelector_ShouldContinueExploration_WhenAlternativeNotDominated(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockLZ4
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.094, throughput: 0.90, samples: 8}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.125, throughput: 1.05, samples: 8}
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 8}
	if s.shouldSkipExploration(false) {
		t.Fatalf("expected exploration to continue when block alternative is not dominated")
	}
}

func TestVlogCompressionSelector_ProbeSuccessCanSwitchCurrentCandidate(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.14, throughput: 0.90, samples: 8}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.10, throughput: 0.88, samples: 8}

	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 4096, 410, 2000, true)
	if s.currentCandidate != vlogAutoCandidateBlockLZ4 {
		t.Fatalf("expected probe success to switch current candidate to lz4, got %v", s.currentCandidate)
	}
}

func TestVlogCompressionSelector_LargePayloadBalancedPrefersLZ4(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 8}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.12, throughput: 1.05, samples: 8}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.015, throughput: 0.95, samples: 8}

	mode, codec, _ := s.choose(false, 2048, 2048)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected large-payload balanced mode to normalize to lz4, got mode=%v codec=%v", mode, codec)
	}
}

func TestVlogCompressionSelector_LargePayloadDoesNotForceLZ4WithoutVeryStrongRatio(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.09, throughput: 0.95, samples: 8}
	c := s.normalizeLargePayloadCandidate(vlogAutoCandidateBlockSnappy, 2048)
	if c != vlogAutoCandidateBlockSnappy {
		t.Fatalf("expected snappy candidate to remain allowed when lz4 ratio is not extreme, got %v", c)
	}
}

func TestResolveVlogWriteMode_LargePayloadBalancedBypassesSelectorToLZ4(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}
	l := &lane{vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)}
	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 2048, 2048)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 || probe {
		t.Fatalf("expected lz4 block write without probe for large payload, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_ThroughputPolicyBypassesSelectorForMediumPayload(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	// Bias selector away from block so we can verify the throughput fast path
	// bypasses selector scoring for medium+ payloads.
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.5, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.5, samples: 16}
	l := &lane{vlogCompressionSelector: s}

	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 256, 256)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected throughput policy medium payload to force configured block codec, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestObserveVlogWriteMode_ThroughputMediumSkipsSelectorObserve(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
	}
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	l := &lane{vlogCompressionSelector: s}

	before := s.metrics[vlogAutoCandidateBlockSnappy].samples
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, throughputAutoBlockMinPayloadBytes, throughputAutoBlockMinPayloadBytes, throughputAutoBlockMinPayloadBytes/2, false, 1000)
	after := s.metrics[vlogAutoCandidateBlockSnappy].samples
	if after != before {
		t.Fatalf("expected selector observe to be skipped for throughput medium payload, samples %d -> %d", before, after)
	}
}

func TestObserveVlogWriteMode_ThroughputSmallStillObserves(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
	}
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	l := &lane{vlogCompressionSelector: s}

	before := s.metrics[vlogAutoCandidateBlockSnappy].samples
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, throughputAutoBlockMinPayloadBytes-1, throughputAutoBlockMinPayloadBytes-1, throughputAutoBlockMinPayloadBytes-1, false, 1000)
	after := s.metrics[vlogAutoCandidateBlockSnappy].samples
	if after <= before {
		t.Fatalf("expected selector observe for small payloads, samples %d -> %d", before, after)
	}
}

func TestResolveVlogWriteMode_ForcePointersLargeBypassesSelectorToConfiguredBlock(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		forceValueLogPointers:   true,
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.5, samples: 16}
	l := &lane{vlogCompressionSelector: s}

	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 1025, 1025)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected force-pointer large payload to force configured block codec, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestObserveVlogWriteMode_ForcePointersLargeSkipsSelectorObserve(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		forceValueLogPointers:   true,
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: s}

	before := s.metrics[vlogAutoCandidateBlockSnappy].samples
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, forcePointerAutoBlockMinPayloadBytes, forcePointerAutoBlockMinPayloadBytes, forcePointerAutoBlockMinPayloadBytes, false, 1000)
	after := s.metrics[vlogAutoCandidateBlockSnappy].samples
	if after != before {
		t.Fatalf("expected selector observe to be skipped for force-pointer large payload, samples %d -> %d", before, after)
	}
}

func TestObserveVlogWriteMode_ForcePointersSmallStillObserves(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		forceValueLogPointers:   true,
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: s}

	before := s.metrics[vlogAutoCandidateBlockSnappy].samples
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecSnappy, forcePointerAutoBlockMinPayloadBytes-1, forcePointerAutoBlockMinPayloadBytes-1, forcePointerAutoBlockMinPayloadBytes-1, false, 1000)
	after := s.metrics[vlogAutoCandidateBlockSnappy].samples
	if after <= before {
		t.Fatalf("expected selector observe for force-pointer sub-threshold payloads, samples %d -> %d", before, after)
	}
}

func TestObserveVlogWriteMode_UsesUnitPayloadForSkipDecision(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		forceValueLogPointers:   true,
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: s}

	before := s.metrics[vlogAutoCandidateBlockSnappy].samples
	rawPayload := forcePointerAutoBlockMinPayloadBytes * 8
	unitPayload := forcePointerAutoBlockMinPayloadBytes / 8
	db.observeVlogWriteMode(
		l,
		vlogWriteBlock,
		valuelog.BlockCodecSnappy,
		rawPayload,
		unitPayload,
		rawPayload/2,
		false,
		1000,
	)
	after := s.metrics[vlogAutoCandidateBlockSnappy].samples
	if after <= before {
		t.Fatalf("expected selector observe to use unit payload threshold, samples %d -> %d", before, after)
	}
}
