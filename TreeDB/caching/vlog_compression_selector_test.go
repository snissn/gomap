package caching

import (
	"bytes"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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

	// Warm block candidates so dict remains the least-sampled candidate.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 760, 900, false)
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 1024, 780, 920, false)
	s.observe(vlogWriteBlock, valuelog.BlockCodecZSTD, 1024, 700, 880, false)

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
	// Dict clearly beats block on ratio while exceeding the strict throughput gate.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 650, 900, false)

	mode, _, _ := s.choose(true, 1024, 1024)
	if mode != vlogWriteDict {
		t.Fatalf("expected dict mode when dict beats block materially, got %v", mode)
	}
}

func TestVlogCompressionSelector_SeedDictCandidate_DoesNotBypassThroughputGate(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.58, throughput: 0.95, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.60, throughput: 0.96, samples: 16}

	s.seedDictCandidate(0.10)

	mode, _, _ := s.choose(true, 4096, 4096)
	if mode != vlogWriteBlock {
		t.Fatalf("expected neutral seeded dict throughput to keep throughput block mode, got %v", mode)
	}
}

func TestDBSeedVlogCompressionSelectorsDictRatio(t *testing.T) {
	db := &DB{
		lanes: []lane{
			{vlogCompressionSelector: newVlogCompressionSelector(vlogAutoThroughput, 0, 0)},
			{vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)},
		},
	}

	db.seedVlogCompressionSelectorsDictRatio(0.08, 0.12)

	for i := range db.lanes {
		s := db.lanes[i].vlogCompressionSelector
		m := s.metric(vlogAutoCandidateDict)
		if m.samples == 0 {
			t.Fatalf("lane %d: expected dict samples to be seeded", i)
		}
		if m.ratio > 0.12 {
			t.Fatalf("lane %d: expected conservative seeded ratio <= 0.12, got %.3f", i, m.ratio)
		}
	}
}

func TestVlogCompressionSelector_SeededDictGetsRealExplorationBeforeUnsampledBlockCodecs(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoSize, 0, 0)
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.exploreBytes = 64
	s.exploreRemaining = 64
	s.dwellBytes = 0
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.05, throughput: 0.90, samples: 8}
	s.seedDictCandidate(0.01)

	mode, _, probe := s.choose(true, 64, 4096)
	if !probe {
		t.Fatalf("expected seeded dict to force a real exploration probe")
	}
	if mode != vlogWriteDict {
		t.Fatalf("expected dict exploration before unsampled block codecs, got %v", mode)
	}
}

func TestVlogCompressionSelector_DwellPreventsFlap(t *testing.T) {
	s := newVlogCompressionSelectorWithSeed(vlogAutoBalanced, 0, 0, valuelog.BlockCodecSnappy)
	s.dwellBytes = 4096

	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Dict looks better, but current mode should hold until dwell budget is spent.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 600, 900, false)

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

func TestChooseValueLogBlockWriteK_DictAggressiveUsesLaneRatio(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionDict),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockTargetBytes: 4096,
		valueLogAutotuneOptions:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
	}
	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: selector}

	// Seed lane block-ratio stats with a compressible signal.
	for i := 0; i < 8; i++ {
		db.observeVlogWriteMode(
			l,
			vlogWriteBlock,
			valuelog.BlockCodecSnappy,
			4096,
			4096,
			1024,
			false,
			1000,
		)
	}
	laneRatio := laneVlogBlockObservedRatio(l, valuelog.BlockCodecSnappy)
	if laneRatio >= 0.98 {
		t.Fatalf("expected compressible lane ratio signal, got %.6f", laneRatio)
	}

	// Poison selector block ratio to mimic noisy selector state in dict mode.
	selector.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 1.10, throughput: 1.0, samples: 16}

	const (
		records         = 128
		rawPayloadBytes = records * 256
	)
	got := db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecSnappy)
	want := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, db.valueLogBlockTargetBytes, laneRatio)
	if got != want {
		t.Fatalf("expected dict/aggressive K to follow lane ratio, got=%d want=%d lane_ratio=%.6f", got, want, laneRatio)
	}

	selectorRatio := selector.blockObservedRatio(valuelog.BlockCodecSnappy)
	selectorWant := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, db.valueLogBlockTargetBytes, selectorRatio)
	if got == selectorWant {
		t.Fatalf("expected dict/aggressive K to ignore selector ratio=%.6f, got=%d selector_want=%d", selectorRatio, got, selectorWant)
	}
}

func TestChooseValueLogBlockWriteK_DictAggressiveWALOffForcePointersUsesMaxFrameK(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:  uint8(vlogCompressionDict),
		valueLogAutoPolicy:       uint8(vlogAutoBalanced),
		valueLogBlockTargetBytes: 4096,
		valueLogAutotuneOptions:  valuelog.AutotuneOptions{Mode: valuelog.AutotuneAggressive},
		forceValueLogPointers:    true,
		disableJournal:           true,
	}
	selector := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	l := &lane{vlogCompressionSelector: selector}

	const (
		records         = 256
		rawPayloadBytes = records * 128
	)
	got := db.chooseValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecLZ4)
	if got != valuelog.MaxFrameK {
		t.Fatalf("expected dict/aggressive WAL-off force-pointer K=%d, got=%d", valuelog.MaxFrameK, got)
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

func TestChooseValueLogBlockWriteK_LiveLeafLogCapsGroupedFramesForColdReads(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*DB)
	}{
		{name: "auto block"},
		{
			name: "dict aggressive fallback",
			configure: func(db *DB) {
				db.valueLogCompressionMode = uint8(vlogCompressionDict)
				db.valueLogAutotuneOptions.Mode = valuelog.AutotuneAggressive
				db.forceValueLogPointers = true
				db.disableJournal = true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{
				valueLogCompressionMode:    uint8(vlogCompressionAuto),
				valueLogBlockTargetBytes:   forcePointerBlockTargetCompressedBytes,
				indexOuterLeavesInValueLog: true,
			}
			db.leafLog = lane{id: leafLogLaneID}
			if tc.configure != nil {
				tc.configure(db)
			}

			got := db.chooseValueLogBlockWriteK(&db.leafLog, 128, 128*page.PageSize, valuelog.BlockCodecSnappy)
			if got != leafLogBlockMaxK {
				t.Fatalf("live leaf-log K=%d, want capped K=%d", got, leafLogBlockMaxK)
			}
		})
	}
}

func TestChooseValueLogBlockWriteK_LiveLeafLogKeepsLeafTargetAfterObservedRatio(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:    uint8(vlogCompressionAuto),
		valueLogAutoPolicy:         uint8(vlogAutoBalanced),
		valueLogBlockTargetBytes:   4096,
		indexOuterLeavesInValueLog: true,
	}
	db.leafLog = lane{id: leafLogLaneID}
	db.observeVlogWriteMode(&db.leafLog, vlogWriteBlock, valuelog.BlockCodecLZ4, 4*page.PageSize, page.PageSize, 2*page.PageSize, false, 1000)

	records := 64
	rawPayloadBytes := records * page.PageSize
	got := db.chooseValueLogBlockWriteK(&db.leafLog, records, rawPayloadBytes, valuelog.BlockCodecLZ4)
	want := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, leafLogBlockTargetCompressedBytes, 0.5)
	if want > leafLogBlockMaxK {
		want = leafLogBlockMaxK
	}
	if got != want {
		t.Fatalf("live leaf-log observed-ratio K=%d, want leaf target K=%d", got, want)
	}
	generic := valuelog.ChooseBlockGroupK(records, rawPayloadBytes, db.valueLogBlockTargetBytes, 0.5)
	if got <= generic {
		t.Fatalf("live leaf-log K=%d did not exceed generic target K=%d", got, generic)
	}
}

func TestPreferLeafPageBlockCodec_CompactedLeafPayloadsPreferLZ4(t *testing.T) {
	db := &DB{
		valueLogCompressionMode:    uint8(vlogCompressionAuto),
		valueLogAutoPolicy:         uint8(vlogAutoBalanced),
		indexOuterLeavesInValueLog: true,
	}
	db.leafLog = lane{id: leafLogLaneID}

	got, ok := db.preferLeafPageBlockCodec(&db.leafLog, 1024, valuelog.BlockCodecSnappy)
	if !ok || got != valuelog.BlockCodecLZ4 {
		t.Fatalf("compacted leaf payload codec=%v ok=%t, want lz4 override", got, ok)
	}

	db.valueLogAutoPolicy = uint8(vlogAutoThroughput)
	got, ok = db.preferLeafPageBlockCodec(&db.leafLog, 1024, valuelog.BlockCodecSnappy)
	if ok || got != valuelog.BlockCodecSnappy {
		t.Fatalf("throughput compacted leaf codec=%v ok=%t, want configured snappy without override", got, ok)
	}
}

func TestChooseValueLogRawWriteK_LiveLeafLogCapsGroupedFramesForColdReads(t *testing.T) {
	for _, tc := range []struct {
		name          string
		configure     func(*DB)
		autoRawBypass bool
		paused        bool
	}{
		{
			name: "paused wal-off raw",
			configure: func(db *DB) {
				db.disableJournal = true
			},
			paused: true,
		},
		{
			name: "off current raw K",
			configure: func(db *DB) {
				db.valueLogDictCurrentK.Store(uint32(leafLogBlockMaxK * 4))
			},
		},
		{
			name: "off force-pointer wal-off default",
			configure: func(db *DB) {
				db.forceValueLogPointers = true
				db.disableJournal = true
			},
		},
		{
			name: "auto raw bypass",
			configure: func(db *DB) {
				db.forceValueLogPointers = true
			},
			autoRawBypass: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{indexOuterLeavesInValueLog: true}
			db.leafLog = lane{id: leafLogLaneID}
			if tc.configure != nil {
				tc.configure(db)
			}

			got := db.chooseValueLogRawWriteK(&db.leafLog, leafLogBlockMaxK*4, tc.autoRawBypass, tc.paused)
			if got != leafLogBlockMaxK {
				t.Fatalf("live leaf-log raw K=%d, want capped K=%d", got, leafLogBlockMaxK)
			}
		})
	}
}

func TestChooseValueLogRawWriteK_NonLeafRawPolicyUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name          string
		configure     func(*DB)
		autoRawBypass bool
		paused        bool
		want          int
	}{
		{
			name: "paused wal-off raw",
			configure: func(db *DB) {
				db.disableJournal = true
			},
			paused: true,
			want:   valuelog.MaxFrameK,
		},
		{
			name: "off current raw K",
			configure: func(db *DB) {
				db.valueLogDictCurrentK.Store(uint32(leafLogBlockMaxK * 4))
			},
			want: leafLogBlockMaxK * 4,
		},
		{
			name: "off force-pointer wal-off default",
			configure: func(db *DB) {
				db.forceValueLogPointers = true
				db.disableJournal = true
			},
			want: 16,
		},
		{
			name:          "auto raw bypass",
			autoRawBypass: true,
			want:          valuelog.MaxFrameK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &DB{indexOuterLeavesInValueLog: true}
			db.leafLog = lane{id: leafLogLaneID}
			if tc.configure != nil {
				tc.configure(db)
			}

			got := db.chooseValueLogRawWriteK(&lane{}, leafLogBlockMaxK*4, tc.autoRawBypass, tc.paused)
			if got != tc.want {
				t.Fatalf("non-leaf raw K=%d, want %d", got, tc.want)
			}
		})
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
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.50, throughput: 1.05, samples: 8}

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
	s.metrics[vlogAutoCandidateBlockZSTD] = vlogCandidateMetrics{ratio: 1.10, throughput: 0.80, samples: 8}

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
	s.metrics[vlogAutoCandidateBlockZSTD] = vlogCandidateMetrics{ratio: 0.30, throughput: 0.80, samples: 8}

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
	s.metrics[vlogAutoCandidateBlockZSTD] = vlogCandidateMetrics{ratio: 0.24, throughput: 0.80, samples: 8}

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
	s.metrics[vlogAutoCandidateBlockZSTD] = vlogCandidateMetrics{ratio: 0.051, throughput: 0.80, samples: 8}
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

func TestVlogCompressionSelector_LargePayloadThroughputPrefersDictOnStrongSizeWin(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.10, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.74, throughput: 1.06, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.08, throughput: 1.12, samples: 8}

	mode, _, probe := s.choose(true, 43<<10, 43<<10)
	if probe {
		t.Fatalf("did not expect probe for steady large-payload dict preference")
	}
	if mode != vlogWriteDict {
		t.Fatalf("expected dict mode for large payload with strong observed dict ratio, got %v", mode)
	}
}

func TestVlogCompressionSelector_LargePayloadThroughputKeepsBlockWithoutStrongDictSignal(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.10, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.74, throughput: 1.06, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.40, throughput: 0.30, samples: 8}

	mode, codec, probe := s.choose(true, 43<<10, 43<<10)
	if probe {
		t.Fatalf("did not expect probe in steady throughput selection")
	}
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy {
		t.Fatalf("expected block snappy to remain preferred without strong dict signal, got mode=%v codec=%v", mode, codec)
	}
}

func TestResolveVlogWriteMode_LargePayloadBalancedBypassesSelectorToConfiguredBlock(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}
	l := &lane{vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)}
	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 2048, 2048, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected configured block codec without probe for large payload, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_LargePayloadBalancedUsesObservedBetterBlockCodec(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}
	l := &lane{vlogCompressionSelector: newVlogCompressionSelector(vlogAutoBalanced, 0, 0)}

	for i := 0; i < largePayloadBlockCodecMinSamples; i++ {
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecSnappy, 4096, 3000)
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecLZ4, 4096, 1800)
	}

	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 2048, 2048, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecLZ4 || probe {
		t.Fatalf("expected observed better block codec for large payload, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestChooseRetainedStorageFirstBlockCodec_DefaultBootstrapsZSTD(t *testing.T) {
	l := &lane{}

	got := chooseRetainedStorageFirstBlockCodec(l, valuelog.BlockCodecSnappy, vlogCompressionDefault)

	if got != valuelog.BlockCodecZSTD {
		t.Fatalf("retained storage-first bootstrap codec=%v want zstd", got)
	}
}

func TestValueLogPayloadLooksRetainedLikeSemanticStreamBlock(t *testing.T) {
	block := append([]byte("crss1blk\x00"), bytes.Repeat([]byte{0x7f}, 1024)...)
	if !valueLogPayloadLooksRetainedJSONLike(block) {
		t.Fatalf("semantic-stream-v1 block was not classified as retained-like")
	}
	zstdBlock := append([]byte("crss1zst\x00"), bytes.Repeat([]byte{0x7f}, 1024)...)
	if !valueLogPayloadLooksRetainedJSONLike(zstdBlock) {
		t.Fatalf("semantic-stream-v1 zstd block was not classified as retained-like")
	}

	locator := append([]byte("crss1loc\x00"), bytes.Repeat([]byte{0x7f}, 40)...)
	if valueLogPayloadLooksRetainedJSONLike(locator) {
		t.Fatalf("semantic-stream-v1 locator classified as retained-like; only side-root blocks should force retained storage-first compression")
	}
}

func TestRetainedStorageFirstValueLogAutoDetectsSemanticStreamBlocks(t *testing.T) {
	db := &DB{valueLogCompressionMode: uint8(vlogCompressionAuto)}
	block := append([]byte("crss1blk\x00"), bytes.Repeat([]byte{0x42}, 1024)...)
	records := []valuelog.Record{{Value: block}}

	if !db.retainedStorageFirstValueLogAuto(0, 0, records) {
		t.Fatalf("semantic-stream-v1 block did not select retained storage-first value-log compression")
	}

	zstdBlock := append([]byte("crss1zst\x00"), bytes.Repeat([]byte{0x42}, 1024)...)
	zstdRecords := []valuelog.Record{{Value: zstdBlock}}
	if !db.retainedStorageFirstValueLogAuto(0, 0, zstdRecords) {
		t.Fatalf("semantic-stream-v1 zstd block did not select retained storage-first value-log compression")
	}
}

func TestChooseRetainedStorageFirstBlockCodec_SingleConfiguredCodecHistoryKeepsZSTDBootstrap(t *testing.T) {
	l := &lane{}
	for i := 0; i < largePayloadBlockCodecMinSamples; i++ {
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecSnappy, 4096, 1800)
	}

	got := chooseRetainedStorageFirstBlockCodec(l, valuelog.BlockCodecSnappy, vlogCompressionAuto)

	if got != valuelog.BlockCodecZSTD {
		t.Fatalf("single-codec retained bootstrap codec=%v want zstd", got)
	}
}

func TestChooseRetainedStorageFirstBlockCodec_ExplicitBlockKeepsConfiguredCodec(t *testing.T) {
	l := &lane{}

	got := chooseRetainedStorageFirstBlockCodec(l, valuelog.BlockCodecLZ4, vlogCompressionBlock)

	if got != valuelog.BlockCodecLZ4 {
		t.Fatalf("explicit block retained codec=%v want configured lz4", got)
	}
}

func TestChooseRetainedStorageFirstBlockCodec_ObservedBestOverridesBootstrap(t *testing.T) {
	l := &lane{}
	for i := 0; i < largePayloadBlockCodecMinSamples; i++ {
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecSnappy, 4096, 3000)
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecLZ4, 4096, 1800)
		observeLaneVlogBlockRatio(l, valuelog.BlockCodecZSTD, 4096, 2200)
	}

	got := chooseRetainedStorageFirstBlockCodec(l, valuelog.BlockCodecSnappy, vlogCompressionAuto)

	if got != valuelog.BlockCodecLZ4 {
		t.Fatalf("retained observed codec=%v want observed-best lz4", got)
	}
}

func TestChooseRetainedStorageFirstValueLogBlockWriteKUsesMaxFrameK(t *testing.T) {
	db := &DB{}
	l := &lane{}

	records := valuelog.MaxFrameK + 64
	rawPayloadBytes := records * 1024
	got := db.chooseRetainedStorageFirstValueLogBlockWriteK(l, records, rawPayloadBytes, valuelog.BlockCodecZSTD)

	if got != valuelog.MaxFrameK {
		t.Fatalf("retained storage-first K=%d want max frame K=%d", got, valuelog.MaxFrameK)
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

	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 256, 256, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected throughput policy medium payload to force configured block codec, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_ThroughputPolicyLargePayloadWithDictCanPreferDict(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
	}
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.currentCandidate = vlogAutoCandidateBlockSnappy
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.70, throughput: 1.10, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.74, throughput: 1.06, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.08, throughput: 1.12, samples: 8}
	l := &lane{vlogCompressionSelector: s}

	mode, codec, probe := db.resolveVlogWriteMode(l, 9, 43<<10, 43<<10, false)
	if mode != vlogWriteDict || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected large payload with strong dict signal to choose dict mode, got mode=%v codec=%v probe=%t", mode, codec, probe)
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

	mode, codec, probe := db.resolveVlogWriteMode(l, 0, 1025, 1025, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected force-pointer large payload to force configured block codec, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_StorageFirstValueLogWithDictCannotSelectOff(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogThreshold:       page.DefaultInlineThreshold,
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0
	s.exploreBytes = 0
	s.exploreRemaining = 0
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	l := &lane{vlogCompressionSelector: s}

	unitPayloadBytes := forcePointerAutoBlockMinPayloadBytes + 128
	mode, codec, probe := db.resolveVlogWriteMode(l, 7, unitPayloadBytes*4, unitPayloadBytes, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected storage-first value-log payload to remap selector off to block, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_StorageFirstValueLogUsesDomainThreshold(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogThreshold:       1 << 20,
		valueLogDomainThresholds: []backenddb.ValueLogDomainThreshold{
			{Prefix: []byte("hot/"), InlineThreshold: page.DefaultInlineThreshold},
		},
	}
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0
	s.exploreBytes = 0
	s.exploreRemaining = 0
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	l := &lane{vlogCompressionSelector: s}

	unitPayloadBytes := forcePointerAutoBlockMinPayloadBytes + 128
	mode, codec, probe := db.resolveVlogWriteMode(l, 7, unitPayloadBytes*4, unitPayloadBytes, false)
	if mode != vlogWriteBlock || codec != valuelog.BlockCodecSnappy || probe {
		t.Fatalf("expected domain-threshold value-log payload to remap selector off to block, got mode=%v codec=%v probe=%t", mode, codec, probe)
	}
}

func TestResolveVlogWriteMode_ThroughputValueLogWithDictCanSelectOff(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
		valueLogBlockCodec:      valuelog.BlockCodecSnappy,
		valueLogThreshold:       page.DefaultInlineThreshold,
	}
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.exploreBytes = 0
	s.exploreRemaining = 0
	s.currentCandidate = vlogAutoCandidateOff
	s.metrics[vlogAutoCandidateOff] = vlogCandidateMetrics{ratio: 1.0, throughput: 1.0, samples: 16}
	s.metrics[vlogAutoCandidateBlockSnappy] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateBlockLZ4] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	s.metrics[vlogAutoCandidateDict] = vlogCandidateMetrics{ratio: 0.99, throughput: 0.2, samples: 16}
	l := &lane{vlogCompressionSelector: s}

	unitPayloadBytes := forcePointerAutoBlockMinPayloadBytes + 128
	mode, _, probe := db.resolveVlogWriteMode(l, 7, unitPayloadBytes*4, unitPayloadBytes, false)
	if mode != vlogWriteOff || probe {
		t.Fatalf("expected throughput value-log payload to keep selector off, got mode=%v probe=%t", mode, probe)
	}
}

func TestShouldBypassAutoRawValueCompression_StorageOwnedHighEntropy(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		forceValueLogPointers:   true,
	}
	value := make([]byte, forcePointerAutoRawBypassMinPayloadBytes)
	for i := range value {
		value[i] = byte(i)
	}
	records := []valuelog.Record{
		{RID: 1, Value: value},
		{RID: 2, Value: value},
		{RID: 3, Value: value},
	}

	if !db.shouldBypassAutoRawValueCompression(0, records, len(value), vlogPayloadKindSingleValue) {
		t.Fatal("expected high-entropy force-pointer value batch to bypass auto compression")
	}
	if !db.shouldBypassAutoRawValueCompression(0, records, forcePointerAutoBlockMinPayloadBytes, vlogPayloadKindSingleValue) {
		t.Fatal("expected threshold-sized high-entropy force-pointer values to bypass block compression")
	}
	if db.shouldBypassAutoRawValueCompression(7, records, len(value), vlogPayloadKindSingleValue) {
		t.Fatal("expected dict-backed values to stay eligible for compression")
	}
	if db.shouldBypassAutoRawValueCompression(0, records, len(value)-1, vlogPayloadKindSingleValue) {
		t.Fatal("expected sub-threshold values to stay eligible for selector sampling")
	}
	if db.shouldBypassAutoRawValueCompression(0, records, forcePointerAutoRawBypassMaxPayloadBytes+1, vlogPayloadKindSingleValue) {
		t.Fatal("expected large values to preserve normal block grouping and maintenance granularity")
	}
	if db.shouldBypassAutoRawValueCompression(0, records, len(value), vlogPayloadKindOuterLeaf) {
		t.Fatal("expected outer-leaf payloads to keep leaf-log compression selection")
	}

	thresholdDB := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogThreshold:       1,
	}
	if !thresholdDB.shouldBypassAutoRawValueCompression(0, records, len(value), vlogPayloadKindSingleValue) {
		t.Fatal("expected high-entropy threshold-owned value batch to bypass auto compression")
	}

	domainThresholdDB := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogThreshold:       1 << 20,
		valueLogDomainThresholds: []backenddb.ValueLogDomainThreshold{
			{Prefix: []byte("hot/"), InlineThreshold: 1},
		},
	}
	if !domainThresholdDB.shouldBypassAutoRawValueCompression(0, records, len(value), vlogPayloadKindSingleValue) {
		t.Fatal("expected high-entropy domain-threshold-owned value batch to bypass auto compression")
	}
}

func TestShouldBypassAutoRawValueCompression_CompressibleStorageOwnedStaysEligible(t *testing.T) {
	db := &DB{
		valueLogCompressionMode: uint8(vlogCompressionAuto),
		valueLogAutoPolicy:      uint8(vlogAutoBalanced),
		valueLogThreshold:       1,
	}
	value := bytes.Repeat([]byte(`{"text":"retained-json","did":"did:plc:abc"}`), 16)
	records := []valuelog.Record{{RID: 1, Value: value}}

	if db.shouldBypassAutoRawValueCompression(0, records, len(value), vlogPayloadKindSingleValue) {
		t.Fatal("expected JSON-like retained value batch to stay eligible for compression")
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

func TestObserveVlogWriteMode_RecordsWriteModeBytesByBucket(t *testing.T) {
	db := &DB{}
	l := &lane{}

	db.observeVlogWriteMode(l, vlogWriteDict, valuelog.BlockCodecSnappy, 40<<10, 40<<10, 12<<10, false, 1000)
	db.observeVlogWriteMode(l, vlogWriteBlock, valuelog.BlockCodecLZ4, 3<<10, 3<<10, 2<<10, false, 1000)
	db.observeVlogWriteMode(l, vlogWriteOff, valuelog.BlockCodecSnappy, 700, 700, 700, false, 1000)

	snap := snapshotLaneVlogWriteMode(l)
	if got := snap.RawBytes[vlogWriteDict]; got != 40<<10 {
		t.Fatalf("dict raw bytes=%d", got)
	}
	if got := snap.StoredBytes[vlogWriteDict]; got != 12<<10 {
		t.Fatalf("dict stored bytes=%d", got)
	}
	if got := snap.BucketRawBytes[vlogWriteDict][vlogPayloadBucketIndex(40<<10)]; got != 40<<10 {
		t.Fatalf("dict bucket raw bytes=%d", got)
	}
	if got := snap.BucketRawBytes[vlogWriteBlock][vlogPayloadBucketIndex(3<<10)]; got != 3<<10 {
		t.Fatalf("block bucket raw bytes=%d", got)
	}
	if got := snap.BucketFrames[vlogWriteOff][vlogPayloadBucketIndex(700)]; got != 1 {
		t.Fatalf("off bucket frames=%d", got)
	}
}

func TestRecordLaneVlogPayloadKindObservation(t *testing.T) {
	l := &lane{}
	recordLaneVlogPayloadKindObservation(l, vlogPayloadKindOuterLeaf, 40<<10, 10<<10)
	recordLaneVlogPayloadKindObservation(l, vlogPayloadKindSingleValue, 8<<10, 4<<10)
	recordLaneVlogPayloadKindObservation(l, vlogPayloadKindMixed, 1024, 512)

	snap := snapshotLaneVlogPayloadKind(l)
	if got := snap.RawBytes[vlogPayloadKindOuterLeaf]; got != 40<<10 {
		t.Fatalf("outer-leaf raw bytes=%d", got)
	}
	if got := snap.StoredBytes[vlogPayloadKindSingleValue]; got != 4<<10 {
		t.Fatalf("single-value stored bytes=%d", got)
	}
	if got := snap.Frames[vlogPayloadKindMixed]; got != 1 {
		t.Fatalf("mixed frames=%d", got)
	}
}

func TestRecordLaneVlogPayloadSplitObservation(t *testing.T) {
	l := &lane{}
	recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitOuterLeaf, 40<<10, 10<<10, 10)
	recordLaneVlogPayloadSplitObservation(l, vlogPayloadSplitSingleValue, 8<<10, 4<<10, 20)

	snap := snapshotLaneVlogPayloadSplit(l)
	if got := snap.RawBytes[vlogPayloadSplitOuterLeaf]; got != 40<<10 {
		t.Fatalf("outer-leaf split raw bytes=%d", got)
	}
	if got := snap.StoredBytes[vlogPayloadSplitSingleValue]; got != 4<<10 {
		t.Fatalf("single-value split stored bytes=%d", got)
	}
	if got := snap.Records[vlogPayloadSplitOuterLeaf]; got != 10 {
		t.Fatalf("outer-leaf split records=%d", got)
	}
}

func TestRecordLaneVlogPayloadSplitFromSummary_LargeMixedDoesNotOverflow(t *testing.T) {
	l := &lane{}
	maxInt := int(^uint(0) >> 1)
	stored := maxInt - 7
	split := vlogPayloadRecordSplit{
		Kind:                vlogPayloadKindMixed,
		OuterLeafRecords:    1,
		SingleValueRecords:  1,
		OuterLeafRawBytes:   maxInt - 11,
		SingleValueRawBytes: maxInt - 13,
	}

	recordLaneVlogPayloadSplitFromSummary(l, split, stored)

	snap := snapshotLaneVlogPayloadSplit(l)
	outerStored := snap.StoredBytes[vlogPayloadSplitOuterLeaf]
	singleStored := snap.StoredBytes[vlogPayloadSplitSingleValue]
	if outerStored > uint64(stored) {
		t.Fatalf("outer stored bytes overflowed: outer=%d stored=%d", outerStored, stored)
	}
	if outerStored+singleStored != uint64(stored) {
		t.Fatalf("stored split mismatch: outer=%d single=%d total=%d", outerStored, singleStored, stored)
	}
}

func TestRecordLaneVlogOuterLeafCodecObservation(t *testing.T) {
	l := &lane{}
	recordLaneVlogOuterLeafCodecObservation(l, vlogOuterLeafCodecLZ4, 40<<10, 10<<10)
	recordLaneVlogOuterLeafCodecObservation(l, vlogOuterLeafCodecNone, 8<<10, 8<<10)
	recordLaneVlogOuterLeafCodecObservation(l, vlogOuterLeafCodecMixed, 1024, 512)

	snap := snapshotLaneVlogOuterLeafCodec(l)
	if got := snap.RawBytes[vlogOuterLeafCodecLZ4]; got != 40<<10 {
		t.Fatalf("outer-leaf lz4 raw bytes=%d", got)
	}
	if got := snap.StoredBytes[vlogOuterLeafCodecNone]; got != 8<<10 {
		t.Fatalf("outer-leaf none stored bytes=%d", got)
	}
	if got := snap.Frames[vlogOuterLeafCodecMixed]; got != 1 {
		t.Fatalf("outer-leaf mixed frames=%d", got)
	}
}

func TestStats_ExposeVlogWriteModeBreakdown(t *testing.T) {
	dir := t.TempDir()
	backend := &mockBackendWithStats{MockBackend: NewMockBackend()}
	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.observeVlogWriteMode(&db.lanes[0], vlogWriteDict, valuelog.BlockCodecSnappy, 40<<10, 40<<10, 10<<10, false, 1000)
	db.observeVlogWriteMode(&db.lanes[0], vlogWriteBlock, valuelog.BlockCodecLZ4, 8<<10, 8<<10, 4<<10, false, 1000)
	recordLaneVlogPayloadKindObservation(&db.lanes[0], vlogPayloadKindOuterLeaf, 40<<10, 10<<10)
	recordLaneVlogPayloadKindObservation(&db.lanes[0], vlogPayloadKindSingleValue, 8<<10, 4<<10)
	recordLaneVlogPayloadSplitObservation(&db.lanes[0], vlogPayloadSplitOuterLeaf, 40<<10, 10<<10, 10)
	recordLaneVlogPayloadSplitObservation(&db.lanes[0], vlogPayloadSplitSingleValue, 8<<10, 4<<10, 20)
	recordLaneVlogOuterLeafCodecObservation(&db.lanes[0], vlogOuterLeafCodecLZ4, 40<<10, 10<<10)
	recordLaneVlogOuterLeafCodecObservation(&db.lanes[0], vlogOuterLeafCodecNone, 8<<10, 8<<10)

	stats := db.Stats()
	if got := stats["treedb.cache.vlog_write_mode.raw_bytes.dict"]; got != "40960" {
		t.Fatalf("dict raw bytes stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_write_mode.stored_ratio.block"]; got != "0.500000" {
		t.Fatalf("block stored ratio stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_write_mode.bucket.frames.dict.le_49152"]; got != "1" {
		t.Fatalf("dict bucket frames stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_payload_kind.raw_bytes.outer_leaf"]; got != "40960" {
		t.Fatalf("outer-leaf raw bytes stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_payload_kind.stored_ratio.single_value"]; got != "0.500000" {
		t.Fatalf("single-value stored ratio stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_payload_split.raw_bytes.outer_leaf"]; got != "40960" {
		t.Fatalf("payload split outer-leaf raw bytes stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_payload_split.stored_ratio.single_value"]; got != "0.500000" {
		t.Fatalf("payload split single-value stored ratio stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_payload_split.records.single_value"]; got != "20" {
		t.Fatalf("payload split single-value records stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_outer_leaf_codec.raw_bytes.lz4"]; got != "40960" {
		t.Fatalf("outer-leaf codec lz4 raw bytes stat=%q", got)
	}
	if got := stats["treedb.cache.vlog_outer_leaf_codec.stored_ratio.none"]; got != "1.000000" {
		t.Fatalf("outer-leaf codec none stored ratio stat=%q", got)
	}
}
