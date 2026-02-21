package template

import (
	"bytes"
	"testing"
)

func TestRecentFastPathEligibilityAndHitMissLifecycle(t *testing.T) {
	e := &Engine{
		cfg: NormalizeConfig(Config{
			RecentTemplates:      2,
			FastPathMinSavings:   10,
			FastPathSavingsSlack: 2,
			FastPathMaxMisses:    2,
		}),
		recent: []recentTemplate{
			{id: 1, def: TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("pre"), []byte("suf")}}, hits: 1, avgSavings: 20},
		},
	}

	if !e.fastPathEligible(e.recent[0], 18) {
		t.Fatalf("expected savings=18 to pass threshold")
	}
	if e.fastPathEligible(e.recent[0], 17) {
		t.Fatalf("expected savings=17 to fail threshold")
	}

	e.updateRecentHit(1, 30)
	if e.recent[0].hits == 0 {
		t.Fatalf("expected hit count to increase")
	}
	if e.recent[0].avgSavings <= 20 {
		t.Fatalf("expected avgSavings to move upward, got %d", e.recent[0].avgSavings)
	}

	e.updateRecentMiss(1)
	if len(e.recent) != 1 {
		t.Fatalf("expected entry to remain after first miss")
	}
	e.updateRecentMiss(1)
	if len(e.recent) != 0 {
		t.Fatalf("expected entry eviction after max misses, len=%d", len(e.recent))
	}
}

func TestTryRecentTemplatesAnchorPath(t *testing.T) {
	cfg := NormalizeConfig(Config{
		RecentTemplates:      2,
		MinSavingsBytes:      1,
		MaxAnchorSearchOps:   128,
		FastPathMinSavings:   1,
		FastPathMinHits:      1,
		FastPathSavingsSlack: 1,
		FastPathMaxMisses:    3,
	})
	def := TemplateDef{
		Kind:    TemplateAnchors,
		Anchors: [][]byte{[]byte("prefix-"), []byte("-suffix")},
	}
	e := &Engine{
		cfg:    cfg,
		recent: []recentTemplate{{id: 10, def: def, hits: 10, avgSavings: 1}},
	}
	value := []byte("prefix-123456789012345-suffix")
	payload, ok := e.tryRecentTemplates(value)
	if !ok {
		t.Fatalf("expected anchor fast-path hit")
	}
	if !IsEncodedPayload(payload) {
		t.Fatalf("expected encoded payload")
	}
	if e.stats.Matched.Load() != 1 || e.stats.Kept.Load() != 1 {
		t.Fatalf("expected matched/kept counters to increment")
	}
	if e.lastKeepSeq.Load() != 0 {
		// tryRecentTemplates should not mutate lastKeepSeq directly.
		t.Fatalf("unexpected lastKeepSeq mutation")
	}
}

func TestTryRecentTemplatesMaskPath(t *testing.T) {
	cfg := NormalizeConfig(Config{
		RecentTemplates:      2,
		MinSavingsBytes:      1,
		FastPathMinSavings:   1,
		FastPathMinHits:      1,
		FastPathSavingsSlack: 1,
		FastPathMaxMisses:    3,
	})
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

	e := &Engine{
		cfg:    cfg,
		recent: []recentTemplate{{id: 11, def: def, hits: 10, avgSavings: 32}},
	}
	payload, ok := e.tryRecentTemplates(value)
	if !ok {
		t.Fatalf("expected mask fast-path hit")
	}
	if !IsEncodedPayload(payload) {
		t.Fatalf("expected encoded payload")
	}
	if e.stats.Matched.Load() != 1 || e.stats.Kept.Load() != 1 {
		t.Fatalf("expected matched/kept counters to increment")
	}
	if e.stats.MaskSparseUsed.Load()+e.stats.MaskFullUsed.Load() != 1 {
		t.Fatalf("expected one mask mode counter increment")
	}
}

func TestRecordRecentMaintainsWindowAndRefreshesExisting(t *testing.T) {
	cfg := NormalizeConfig(Config{RecentTemplates: 2})
	e := &Engine{cfg: cfg}

	d1 := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("a")}}
	d2 := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("b")}}
	d3 := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("c")}}
	e.recordRecent(d1, 1)
	e.recordRecent(d2, 2)
	if len(e.recent) != 2 {
		t.Fatalf("len(recent)=%d, want 2", len(e.recent))
	}

	// Refresh existing ID 1; it should move to the newest position.
	e.recordRecent(d1, 1)
	if e.recent[len(e.recent)-1].id != 1 {
		t.Fatalf("expected id=1 to be most recent")
	}

	// Insert id=3, window size stays at 2.
	e.recordRecent(d3, 3)
	if len(e.recent) != 2 {
		t.Fatalf("len(recent)=%d, want 2", len(e.recent))
	}
	if e.recent[0].id == e.recent[1].id {
		t.Fatalf("recent window should hold distinct entries")
	}
}
