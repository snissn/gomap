package template

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestDefCacheAddBranches(t *testing.T) {
	// nil receiver branch
	var nilCache *defCache
	nilCache.Add(1, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("a")}})

	// size==0 branch
	zero := &defCache{size: 0, ids: make([]uint64, 0), defs: make(map[uint64]TemplateDef)}
	zero.Add(1, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("a")}})
	if len(zero.defs) != 0 {
		t.Fatalf("size==0 cache should not store defs")
	}

	// duplicate + ring eviction branches
	c := newDefCache(2)
	c.Add(1, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("a")}})
	c.Add(2, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("b")}})
	c.Add(2, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("bb")}}) // duplicate no-op
	if len(c.defs) != 2 {
		t.Fatalf("expected 2 defs after duplicate add, got %d", len(c.defs))
	}
	c.Add(3, TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("c")}}) // evicts id=1
	if _, ok := c.defs[1]; ok {
		t.Fatalf("expected id=1 eviction")
	}
	if _, ok := c.defs[3]; !ok {
		t.Fatalf("expected id=3 present")
	}
}

func TestEncodeMaskTemplateAdditionalBranches(t *testing.T) {
	base := make([]byte, 64)
	for i := range base {
		base[i] = 'A'
	}
	varPos := []uint16{2, 61}
	mask := buildMaskFromPositions(varPos, len(base))
	def := TemplateDef{
		Kind:           TemplateMask,
		Base:           base,
		Mask:           mask,
		VarPositions:   varPos,
		ConstPositions: buildConstPositions(mask, len(base)),
	}

	// Invalid kind branch.
	if out := encodeMaskTemplate(base, TemplateDef{Kind: TemplateAnchors}, 1, true); out != nil {
		t.Fatalf("expected nil for non-mask def")
	}

	// Mismatched value/base length branch.
	if out := encodeMaskTemplate(base[:10], def, 1, true); out != nil {
		t.Fatalf("expected nil for mismatched value length")
	}

	// Sparse reuse branch (cap(value) > payloadLen).
	value := make([]byte, len(base), len(base)+128)
	copy(value, base)
	value[2] = 'B'
	value[61] = 'C'
	out := encodeMaskTemplate(value, def, 77, true)
	if !IsEncodedPayload(out) {
		t.Fatalf("expected encoded payload from sparse reuse path")
	}
	if cap(out) != cap(value) {
		t.Fatalf("expected sparse reuse to return slice on original backing array")
	}

	// varMask == nil path should still produce full payload.
	defNoMask := def
	defNoMask.Mask = []byte{1} // invalid length => treated as nil
	out2 := encodeMaskTemplate(value[:len(base)], defNoMask, 88, true)
	if !IsEncodedPayload(out2) {
		t.Fatalf("expected encoded payload from nil-mask fallback")
	}
}

func TestTrainShardProcessGuardBranchesAndFlush(t *testing.T) {
	stats := &TemplateStats{}
	total := &atomic.Uint64{}
	baseCfg := NormalizeConfig(Config{
		FingerprintK:                 3,
		MinAnchorFreq:                1,
		MinPresenceRatio:             0.5,
		AmbiguityPct:                 1.0,
		MinSavingsBytes:              1,
		MinPublishSavingsBytes:       1,
		MinPublishRatio:              2.0,
		MinActivateHits:              1,
		MinActivateSavedBytes:        1,
		LengthBucketMinLen:           1,
		MaskMinPresenceRatio:         0.5,
		MaskMinConstBytes:            1,
		MaskMinConstFrac:             0.1,
		MaxValuesScannedPerSynthesis: 16,
		MaskMaxValuesScanned:         16,
		SynthesizeEverySamples:       1,
		PublishBatchSize:             16,
	})

	// nil store guard
	sNil := trainShard{
		cfg:            baseCfg,
		stats:          stats,
		defCache:       newDefCache(8),
		totalTemplates: total,
		buckets:        make(map[uint64]*bucket),
		maxBuckets:     2,
	}
	sNil.process(trainTask{store: nil, bucketKey: 1, value: []byte("abc")})
	if len(sNil.buckets) != 0 {
		t.Fatalf("nil store should not create buckets")
	}

	store := &trainerBatchStore{ids: []uint64{1}}

	// publishPending guard
	sPending := trainShard{
		cfg:            baseCfg,
		stats:          &TemplateStats{},
		defCache:       newDefCache(8),
		totalTemplates: &atomic.Uint64{},
		buckets: map[uint64]*bucket{
			1: {publishPending: true},
		},
		maxBuckets: 2,
	}
	sPending.process(trainTask{store: store, bucketKey: 1, value: []byte("prefix-111-suffix")})
	if len(sPending.pending) != 0 {
		t.Fatalf("publishPending guard should not enqueue publish work")
	}

	// cooldown guard
	sCooldown := trainShard{
		cfg: func() Config {
			c := baseCfg
			c.CooldownValues = 100
			return c
		}(),
		stats:          &TemplateStats{},
		defCache:       newDefCache(8),
		totalTemplates: &atomic.Uint64{},
		buckets: map[uint64]*bucket{
			1: {lastPublishSample: 0},
		},
		maxBuckets: 2,
	}
	sCooldown.process(trainTask{store: store, bucketKey: 1, value: []byte("prefix-111-suffix")})
	if len(sCooldown.pending) != 0 {
		t.Fatalf("cooldown guard should not enqueue publish work")
	}

	// max templates per bucket guard
	sPerBucket := trainShard{
		cfg: func() Config {
			c := baseCfg
			c.MaxTemplatesPerBucket = 1
			return c
		}(),
		stats:          &TemplateStats{},
		defCache:       newDefCache(8),
		totalTemplates: &atomic.Uint64{},
		buckets: map[uint64]*bucket{
			1: {templatesPublished: 1},
		},
		maxBuckets: 2,
	}
	sPerBucket.process(trainTask{store: store, bucketKey: 1, value: []byte("prefix-111-suffix")})
	if len(sPerBucket.pending) != 0 {
		t.Fatalf("per-bucket template cap should not enqueue publish work")
	}

	// max templates total guard
	totalCap := &atomic.Uint64{}
	totalCap.Store(1)
	sTotal := trainShard{
		cfg: func() Config {
			c := baseCfg
			c.MaxTemplatesTotal = 1
			return c
		}(),
		stats:          &TemplateStats{},
		defCache:       newDefCache(8),
		totalTemplates: totalCap,
		buckets:        make(map[uint64]*bucket),
		maxBuckets:     2,
	}
	sTotal.process(trainTask{store: store, bucketKey: 1, value: []byte("prefix-111-suffix")})
	if len(sTotal.pending) != 0 {
		t.Fatalf("global template cap should not enqueue publish work")
	}

	// ensure flushPublishes(nil) no-op branch is exercised
	sTotal.flushPublishes(context.Background())
}
