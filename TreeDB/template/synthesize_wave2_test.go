package template

import "testing"

func mkSamples(values ...string) []sample {
	out := make([]sample, len(values))
	for i, v := range values {
		out[i] = sample{value: []byte(v)}
	}
	return out
}

func TestSynthesizeTemplateAnchorPathSuccess(t *testing.T) {
	cfg := NormalizeConfig(Config{
		DisableMaskTemplates:         true,
		FingerprintK:                 3,
		MinAnchorLen:                 3,
		MaxAnchorLen:                 32,
		MaxAnchorsPerTemplate:        8,
		MaxAnchorBytesTotal:          128,
		MaxAnchorScanPerSynthesis:    256,
		MaxValuesScannedPerSynthesis: 16,
		MinAnchorFreq:                1,
		MinPresenceRatio:             0.5,
		AmbiguityPct:                 1.0,
		MinSavingsBytes:              1,
		MinPublishSavingsBytes:       1,
		MinPublishRatio:              2.0,
		MinActivateHits:              1,
		MinActivateSavedBytes:        1,
	})
	samples := mkSamples(
		"prefix-111111111111111111-suffix",
		"prefix-222222222222222222-suffix",
		"prefix-333333333333333333-suffix",
	)
	def, route, activated, ok := synthesizeTemplate(samples, cfg)
	if !ok {
		t.Fatalf("expected synthesizeTemplate to succeed")
	}
	if !activated {
		t.Fatalf("expected activated template")
	}
	if def.Kind != TemplateAnchors {
		t.Fatalf("expected anchor template, got kind=%v", def.Kind)
	}
	if len(def.Anchors) == 0 {
		t.Fatalf("expected non-empty anchors")
	}
	if len(route) == 0 {
		t.Fatalf("expected non-empty route value")
	}
}

func TestSynthesizeTemplateEarlyReturnGuards(t *testing.T) {
	cfg := NormalizeConfig(Config{DisableMaskTemplates: true, FingerprintK: 4})

	if _, _, _, ok := synthesizeTemplate(nil, cfg); ok {
		t.Fatalf("expected false for empty samples")
	}

	if _, _, _, ok := synthesizeTemplate(mkSamples("a", "b"), cfg); ok {
		t.Fatalf("expected false when all values shorter than k")
	}

	cfg2 := NormalizeConfig(Config{
		DisableMaskTemplates: true,
		FingerprintK:         3,
		MinAnchorFreq:        1000, // force candidate filtering
	})
	if _, _, _, ok := synthesizeTemplate(mkSamples("abc-1", "abc-2", "abc-3"), cfg2); ok {
		t.Fatalf("expected false when min anchor frequency cannot be met")
	}
}

func TestSynthesizeTemplateQualityGateRejects(t *testing.T) {
	cfg := NormalizeConfig(Config{
		DisableMaskTemplates:         true,
		FingerprintK:                 3,
		MinAnchorLen:                 3,
		MaxAnchorLen:                 32,
		MaxAnchorsPerTemplate:        8,
		MaxAnchorBytesTotal:          128,
		MaxAnchorScanPerSynthesis:    256,
		MaxValuesScannedPerSynthesis: 16,
		MinAnchorFreq:                1,
		MinPresenceRatio:             0.5,
		AmbiguityPct:                 1.0,
		MinSavingsBytes:              1,
		MinPublishSavingsBytes:       1 << 20, // impossible threshold
		MinPublishRatio:              0.1,     // meanRatio likely > 0.1
		MinActivateHits:              1,
		MinActivateSavedBytes:        1,
	})
	samples := mkSamples(
		"prefix-111111111111111111-suffix",
		"prefix-222222222222222222-suffix",
		"prefix-333333333333333333-suffix",
	)
	if _, _, _, ok := synthesizeTemplate(samples, cfg); ok {
		t.Fatalf("expected synthesizeTemplate quality-gate reject")
	}
}

func TestSynthesizeMaskTemplateGuardPaths(t *testing.T) {
	cfg := NormalizeConfig(Config{
		LengthBucketMinLen: 64,
		MinAnchorFreq:      2,
	})
	// bestLen below min length.
	if _, _, _, ok := synthesizeMaskTemplate(mkSamples("short", "short"), cfg); ok {
		t.Fatalf("expected false for short values")
	}

	// bestCount below min anchor freq.
	cfg2 := NormalizeConfig(Config{
		LengthBucketMinLen: 1,
		MinAnchorFreq:      10,
	})
	if _, _, _, ok := synthesizeMaskTemplate(mkSamples("A111", "A222"), cfg2); ok {
		t.Fatalf("expected false for insufficient bucket frequency")
	}
}
