package template

import (
	"bytes"
	"strconv"
	"testing"
)

func TestNormalizeConfigDefaultsAndBounds(t *testing.T) {
	cfg := NormalizeConfig(Config{
		MaxBuckets:           2,
		TrainShards:          16, // should clamp to MaxBuckets
		TrainRouters:         0,  // default from shards
		RecentTemplates:      -1, // should normalize to default
		DefCacheSize:         0,  // default
		FastPathSavingsSlack: -1,
	})
	if cfg.TrainShards != 2 {
		t.Fatalf("TrainShards=%d, want 2 (clamped by MaxBuckets)", cfg.TrainShards)
	}
	if cfg.TrainRouters < 1 || cfg.TrainRouters > 4 {
		t.Fatalf("TrainRouters=%d, want in [1,4]", cfg.TrainRouters)
	}
	if cfg.RecentTemplates <= 0 {
		t.Fatalf("RecentTemplates=%d, want default >0", cfg.RecentTemplates)
	}
	if cfg.DefCacheSize <= 0 {
		t.Fatalf("DefCacheSize=%d, want default >0", cfg.DefCacheSize)
	}
	if cfg.FastPathSavingsSlack <= 0 {
		t.Fatalf("FastPathSavingsSlack=%d, want default >0", cfg.FastPathSavingsSlack)
	}
	if cfg.PublishFlushEvery <= 0 {
		t.Fatalf("PublishFlushEvery=%v, want default >0", cfg.PublishFlushEvery)
	}
}

func TestTemplateStatsSnapshotIncludesReasons(t *testing.T) {
	var s TemplateStats
	s.Attempted.Add(3)
	s.Kept.Add(2)
	s.BytesSaved.Add(111)
	s.addReason(reasonNoCandidates)
	s.addReason(reasonNoCandidates)
	s.addReason(reasonSkipCold)

	snap := s.Snapshot()
	if snap["attempted"] != "3" {
		t.Fatalf("attempted=%q, want 3", snap["attempted"])
	}
	if snap["kept"] != "2" {
		t.Fatalf("kept=%q, want 2", snap["kept"])
	}
	if snap["bytes_saved_total"] != "111" {
		t.Fatalf("bytes_saved_total=%q, want 111", snap["bytes_saved_total"])
	}
	if snap["reason."+reasonNoCandidates] != "2" {
		t.Fatalf("reason no_candidates=%q, want 2", snap["reason."+reasonNoCandidates])
	}
	if snap["reason."+reasonSkipCold] != "1" {
		t.Fatalf("reason skip_cold=%q, want 1", snap["reason."+reasonSkipCold])
	}
}

func TestBuildMaskSpans(t *testing.T) {
	mask := buildMaskFromPositions([]uint16{1, 2, 5}, 8)
	varSpans, constSpans := buildMaskSpans(mask, 8)
	if len(varSpans) != 2 {
		t.Fatalf("len(varSpans)=%d, want 2", len(varSpans))
	}
	if len(constSpans) != 3 {
		t.Fatalf("len(constSpans)=%d, want 3", len(constSpans))
	}
}

func TestMatchTemplateLenOpsCapAndMissingAnchor(t *testing.T) {
	cfg := NormalizeConfig(Config{
		MinSavingsBytes:    1,
		MaxAnchorSearchOps: 1,
	})
	value := []byte("prefix-AAAAAAAA-mid-BBBBBBBB-suffix")
	anchors := [][]byte{[]byte("AAAAAAAA"), []byte("BBBBBBBB")}
	if _, reason, matched := matchTemplateLen(value, anchors, 1, cfg); matched || reason != reasonMatchOpsCap {
		t.Fatalf("expected ops cap failure, got matched=%v reason=%q", matched, reason)
	}

	cfg.MaxAnchorSearchOps = 0
	value2 := bytes.Repeat([]byte("x"), 128)
	missingAnchor := bytes.Repeat([]byte("Y"), 32)
	if _, reason, matched := matchTemplateLen(value2, [][]byte{missingAnchor}, 1, cfg); matched || reason != reasonMatchMissingAnchor {
		t.Fatalf("expected missing anchor failure, got matched=%v reason=%q", matched, reason)
	}
}

func TestMatchMaskTemplateLenSparseVsFull(t *testing.T) {
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

	cfg := NormalizeConfig(Config{MinSavingsBytes: 1})
	encLen, sparse, reason, matched := matchMaskTemplateLen(value, def, 1, cfg)
	if !matched || reason != "" {
		t.Fatalf("expected mask match, got matched=%v reason=%q", matched, reason)
	}
	if !sparse {
		t.Fatalf("expected sparse path for low var-count mask")
	}
	if encLen <= 0 || encLen >= len(value) {
		t.Fatalf("unexpected encLen=%d for raw=%d", encLen, len(value))
	}
}

func TestMatchMaskTemplateLenConstMismatchFallsBack(t *testing.T) {
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
	value[0] = 'Z' // constant position mismatch should force non-sparse evaluation

	cfg := NormalizeConfig(Config{MinSavingsBytes: 1})
	encLen, sparse, reason, matched := matchMaskTemplateLen(value, def, 1, cfg)
	if !matched || reason != "" {
		t.Fatalf("expected match with fallback encoding, got matched=%v reason=%q", matched, reason)
	}
	if sparse {
		t.Fatalf("expected full-mask path due to const mismatch")
	}
	if encLen <= 0 || encLen >= len(value) {
		t.Fatalf("unexpected encLen=%d for raw=%d", encLen, len(value))
	}
}

func TestEncodedLenAndMinEncodedLenConsistency(t *testing.T) {
	gaps := [][]byte{[]byte("a"), []byte("bc"), []byte("def")}
	got := encodedLen(gaps, 9)
	rawLen := len("xaybczdefw")
	min := minEncodedLen(rawLen, len(gaps), 4, 9)
	if got < min {
		t.Fatalf("encodedLen=%d smaller than minEncodedLen=%d", got, min)
	}
	// Guard output remains parseable integer if used in debug strings.
	_ = strconv.Itoa(got)
}
