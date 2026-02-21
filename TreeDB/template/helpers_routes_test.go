package template

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestTemplateIDDeterministicAndSalted(t *testing.T) {
	def := []byte("template-def")
	id0a := TemplateID(def, 0)
	id0b := TemplateID(def, 0)
	if id0a == 0 || id0a != id0b {
		t.Fatalf("salt 0 id mismatch: %d vs %d", id0a, id0b)
	}
	id1 := TemplateID(def, 1)
	if id1 == 0 || id1 == id0a {
		t.Fatalf("salted id should be non-zero and different: id0=%d id1=%d", id0a, id1)
	}
}

func TestRoutingFingerprintHelpers(t *testing.T) {
	cfg := NormalizeConfig(Config{
		FingerprintK:       4,
		FingerprintW:       4,
		MaxFingerprints:    16,
		RouteFPCount:       8,
		RoutePrefixBytes:   8,
		RouteSuffixBytes:   8,
		LengthBucketMinLen: 8,
	})
	value := []byte("prefix-abcdefghijklmnop-suffix")

	fps := RoutingFingerprints(value, cfg)
	if len(fps) == 0 {
		t.Fatalf("RoutingFingerprints returned empty")
	}
	var appended []uint64
	appended = AppendRoutingFingerprints(appended, value, cfg)
	if len(appended) == 0 {
		t.Fatalf("AppendRoutingFingerprints returned empty")
	}

	legacy := RoutingFingerprintsLegacy(value, cfg)
	if len(legacy) == 0 {
		t.Fatalf("RoutingFingerprintsLegacy returned empty")
	}

	anchorsFP := RouteFingerprints([][]byte{[]byte("prefix"), []byte("suffix")}, cfg)
	if len(anchorsFP) == 0 {
		t.Fatalf("RouteFingerprints returned empty")
	}

	// Long enough values use a length fingerprint route by default.
	bucket := BucketFingerprints(value, cfg)
	if len(bucket) != 1 || bucket[0] != lengthFingerprint(len(value)) {
		t.Fatalf("BucketFingerprints=%v, want [%d]", bucket, lengthFingerprint(len(value)))
	}
}

func TestRecentPoolsEngineCloseAndStatsSnapshot(t *testing.T) {
	// Exercise recent snapshot pools directly.
	s := getRecentSnapshot(3)
	s = append(s, recentTemplate{id: 1}, recentTemplate{id: 2})
	putRecentSnapshot(s)
	s2 := getRecentSnapshot(1)
	if len(s2) != 0 {
		t.Fatalf("pooled recent snapshot should be reset, len=%d", len(s2))
	}
	putRecentSnapshot(s2)

	e := NewEngine(Config{
		FingerprintK:        4,
		TrainQueueSize:      8,
		TrainShards:         1,
		TrainRouters:        1,
		TrainShardQueueSize: 8,
	})
	defer e.Close()
	snap := e.StatsSnapshot()
	if snap == nil {
		t.Fatalf("StatsSnapshot returned nil")
	}
}

func TestTrainShardFlushPublishesSameAndMixedStores(t *testing.T) {
	stats := &TemplateStats{}
	total := &atomic.Uint64{}
	s := trainShard{
		cfg:            NormalizeConfig(Config{}),
		stats:          stats,
		defCache:       newDefCache(8),
		totalTemplates: total,
	}

	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("aa")}}
	b1 := &bucket{publishPending: true}
	b2 := &bucket{publishPending: true}
	store := &trainerBatchStore{ids: []uint64{1, 2}}
	s.pending = []pendingPublish{
		{store: store, bucket: b1, samplesSeen: 1, def: def, defBytes: []byte("d1"), routeFPs: []uint64{1}},
		{store: store, bucket: b2, samplesSeen: 2, def: def, defBytes: []byte("d2"), routeFPs: []uint64{2}},
	}
	s.flushPublishes(context.Background())
	if store.batchCalls != 1 {
		t.Fatalf("batchCalls=%d, want 1", store.batchCalls)
	}
	if got := stats.PublishBatches.Load(); got != 1 {
		t.Fatalf("PublishBatches=%d, want 1", got)
	}
	if got := stats.PublishDefs.Load(); got != 2 {
		t.Fatalf("PublishDefs=%d, want 2", got)
	}

	// Mixed stores should publish per-item.
	stats2 := &TemplateStats{}
	total2 := &atomic.Uint64{}
	s2 := trainShard{
		cfg:            NormalizeConfig(Config{}),
		stats:          stats2,
		defCache:       newDefCache(8),
		totalTemplates: total2,
	}
	f1 := &trainerFallbackStore{ids: []uint64{11}}
	f2 := &trainerFallbackStore{ids: []uint64{12}}
	b3 := &bucket{publishPending: true}
	b4 := &bucket{publishPending: true}
	s2.pending = []pendingPublish{
		{store: f1, bucket: b3, samplesSeen: 3, def: def, defBytes: []byte("x"), routeFPs: []uint64{3}},
		{store: f2, bucket: b4, samplesSeen: 4, def: def, defBytes: []byte("y"), routeFPs: []uint64{4}},
	}
	s2.flushPublishes(context.Background())
	if f1.putCalls != 1 || f2.putCalls != 1 {
		t.Fatalf("mixed-store fallback should call PutTemplateDef once each, got %d/%d", f1.putCalls, f2.putCalls)
	}
}

func TestSynthesisAndHelperFunctions(t *testing.T) {
	cfg := NormalizeConfig(Config{
		FingerprintK:                 3,
		MinAnchorLen:                 2,
		MaxAnchorLen:                 16,
		MaxAnchorsPerTemplate:        8,
		MaxAnchorBytesTotal:          128,
		MaxAnchorScanPerSynthesis:    128,
		MaxValuesScannedPerSynthesis: 16,
		MaskMaxValuesScanned:         16,
		MinAnchorFreq:                1,
		MinPresenceRatio:             0.5,
		AmbiguityPct:                 1.0,
		MinPublishSavingsBytes:       1,
		MinPublishRatio:              1.0,
		MinActivateHits:              1,
		MinActivateSavedBytes:        1,
		LengthBucketMinLen:           1,
		MaskMinPresenceRatio:         0.5,
		MaskMinConstBytes:            1,
		MaskMinConstFrac:             0.1,
	})
	samples := []sample{
		{value: []byte("AAA111BBB")},
		{value: []byte("AAA222BBB")},
		{value: []byte("AAA333BBB")},
	}

	// Mask synthesis path.
	maskDef, route, activated, ok := synthesizeMaskTemplate(samples, cfg)
	if !ok || !activated || len(route) == 0 || maskDef.Kind != TemplateMask {
		t.Fatalf("unexpected synthesizeMaskTemplate result ok=%v activated=%v kind=%v routeLen=%d", ok, activated, maskDef.Kind, len(route))
	}

	// Full synthesis entrypoint should succeed (may pick mask or anchor).
	def2, route2, activated2, ok2 := synthesizeTemplate(samples, cfg)
	if !ok2 || !activated2 || len(route2) == 0 {
		t.Fatalf("unexpected synthesizeTemplate result ok=%v activated=%v kind=%v routeLen=%d", ok2, activated2, def2.Kind, len(route2))
	}

	ambiguous := isAmbiguous([]byte("A"), samples, len(samples), 0.2)
	if !ambiguous {
		t.Fatalf("expected ambiguous anchor")
	}

	// Reference sample is the one with minimum hash.
	wantIdx := 0
	wantHash := xxh3.Hash(samples[0].value)
	for i := 1; i < len(samples); i++ {
		h := xxh3.Hash(samples[i].value)
		if h < wantHash {
			wantHash = h
			wantIdx = i
		}
	}
	if got := selectReferenceSample(samples, len(samples)); got != wantIdx {
		t.Fatalf("selectReferenceSample=%d, want %d", got, wantIdx)
	}

	a := &anchorCand{
		bytes:     []byte("11"),
		positions: []int{3, -1, -1},
		median:    3,
		start:     3,
	}
	extendAnchor(a, samples, len(samples), 0, cfg)
	if len(a.bytes) < 2 {
		t.Fatalf("extendAnchor should preserve/grow anchor, len=%d", len(a.bytes))
	}

	encSamples := []sample{
		{value: []byte("prefix-111111111111111111-suffix")},
		{value: []byte("prefix-222222222222222222-suffix")},
		{value: []byte("prefix-333333333333333333-suffix")},
	}
	hits, saved, ratio := simulateEncoding([][]byte{[]byte("prefix-"), []byte("-suffix")}, encSamples, len(encSamples), cfg)
	if hits == 0 || saved == 0 || ratio <= 0 || ratio >= 1 {
		t.Fatalf("simulateEncoding unexpected: hits=%d saved=%d ratio=%f", hits, saved, ratio)
	}

	base := bytes.Repeat([]byte("A"), 64)
	mask := buildMaskFromPositions([]uint16{2, 61}, len(base))
	v1 := append([]byte(nil), base...)
	v1[2], v1[61] = 'B', 'C'
	v2 := append([]byte(nil), base...)
	v2[2], v2[61] = 'D', 'E'
	maskSamples := []sample{
		{value: v1},
		{value: v2},
	}
	hits2, saved2, ratio2 := simulateMaskEncoding(base, mask, maskSamples, len(maskSamples), payloadHeader+uvarintLen(1)+(len(base)+7)/8, payloadHeader+uvarintLen(1))
	if hits2 == 0 || saved2 == 0 || ratio2 <= 0 || ratio2 >= 1 {
		t.Fatalf("simulateMaskEncoding unexpected: hits=%d saved=%d ratio=%f", hits2, saved2, ratio2)
	}
}

func TestMatchMaskTemplateAndEncodeMaskTemplate(t *testing.T) {
	cfg := NormalizeConfig(Config{MinSavingsBytes: 1})
	base := bytes.Repeat([]byte("A"), 64)
	varPos := []uint16{2, 61}
	mask := buildMaskFromPositions(varPos, len(base))
	def := TemplateDef{
		Kind:           TemplateMask,
		Base:           base,
		Mask:           mask,
		VarPositions:   varPos,
		ConstPositions: buildConstPositions(mask, len(base)),
	}
	value := append([]byte(nil), base...)
	value[2] = 'B'
	value[61] = 'C'

	payload, encLen, reason, matched := matchMaskTemplate(value, def, 77, cfg)
	if !matched || reason != "" || len(payload) == 0 || len(payload) != encLen {
		t.Fatalf("matchMaskTemplate unexpected: matched=%v reason=%q payload=%d encLen=%d", matched, reason, len(payload), encLen)
	}
	decoded, err := DecodePayloadAppend(nil, payload, func(id uint64) (TemplateDef, error) {
		if id != 77 {
			return TemplateDef{}, ErrMissingTemplate
		}
		return def, nil
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 64})
	if err != nil {
		t.Fatalf("DecodePayloadAppend: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Fatalf("decoded mismatch")
	}

	full := encodeMaskTemplate(value, def, 77, false)
	sparse := encodeMaskTemplate(value, def, 77, true)
	if !IsEncodedPayload(full) || !IsEncodedPayload(sparse) {
		t.Fatalf("expected encoded payloads from encodeMaskTemplate")
	}
}
