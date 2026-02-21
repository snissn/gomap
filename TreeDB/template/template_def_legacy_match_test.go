package template

import (
	"bytes"
	"errors"
	"testing"
)

func TestUvarintLenBoundaries(t *testing.T) {
	cases := []struct {
		v    uint64
		want int
	}{
		{0, 1},
		{1<<7 - 1, 1},
		{1 << 7, 2},
		{1<<14 - 1, 2},
		{1 << 14, 3},
		{1<<21 - 1, 3},
		{1 << 21, 4},
		{1<<28 - 1, 4},
		{1 << 28, 5},
		{1<<35 - 1, 5},
		{1 << 35, 6},
		{1<<42 - 1, 6},
		{1 << 42, 7},
		{1<<49 - 1, 7},
		{1 << 49, 8},
		{1<<56 - 1, 8},
		{1 << 56, 9},
		{1<<63 - 1, 9},
		{1 << 63, 10},
	}
	for _, tc := range cases {
		if got := uvarintLen(tc.v); got != tc.want {
			t.Fatalf("uvarintLen(%d)=%d, want %d", tc.v, got, tc.want)
		}
	}
}

func TestRoutingFingerprintsLegacyModes(t *testing.T) {
	cfg := NormalizeConfig(Config{
		FingerprintK:       4,
		FingerprintW:       4,
		MaxFingerprints:    16,
		RouteFPCount:       6,
		RoutePrefixBytes:   8,
		RouteSuffixBytes:   8,
		LengthBucketMinLen: 8,
	})
	value := []byte("prefix-abcdefghijklmnop-suffix")

	fps := RoutingFingerprintsLegacy(value, cfg)
	if len(fps) != 1 || fps[0] != lengthFingerprint(len(value)) {
		t.Fatalf("legacy length-bucket route mismatch: %v", fps)
	}

	cfg.LengthBucketMinLen = 1 << 20 // disable length bucket for this value
	fps2 := RoutingFingerprintsLegacy(value, cfg)
	if len(fps2) == 0 {
		t.Fatalf("legacy routing should return non-empty set")
	}
	if len(fps2) > cfg.RouteFPCount {
		t.Fatalf("legacy routing length=%d exceeds RouteFPCount=%d", len(fps2), cfg.RouteFPCount)
	}
}

func TestEncodeDecodeTemplateDefValidationAndCorruption(t *testing.T) {
	cfg := NormalizeConfig(Config{
		MinAnchorLen:          2,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
	})
	if _, err := EncodeTemplateDef(TemplateDef{Kind: TemplateAnchors}, cfg); !errors.Is(err, ErrCorruptTemplateDef) {
		t.Fatalf("expected ErrCorruptTemplateDef for empty anchors, got %v", err)
	}

	def := TemplateDef{
		Kind:    TemplateAnchors,
		Anchors: [][]byte{[]byte("aa"), []byte("bb")},
	}
	enc, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	dec, err := DecodeTemplateDef(enc)
	if err != nil {
		t.Fatalf("DecodeTemplateDef: %v", err)
	}
	if dec.Kind != TemplateAnchors || len(dec.Anchors) != 2 {
		t.Fatalf("decoded def mismatch: %+v", dec)
	}

	// CRC corruption.
	corrupt := append([]byte(nil), enc...)
	corrupt[len(corrupt)-1] ^= 0xFF
	if _, err := DecodeTemplateDef(corrupt); !errors.Is(err, ErrCorruptTemplateDef) {
		t.Fatalf("expected ErrCorruptTemplateDef for crc corruption, got %v", err)
	}

	// Truncated payload.
	if _, err := DecodeTemplateDef(enc[:len(enc)-2]); !errors.Is(err, ErrCorruptTemplateDef) {
		t.Fatalf("expected ErrCorruptTemplateDef for truncation, got %v", err)
	}
}

func TestMatchMaskTemplateAdditionalBranches(t *testing.T) {
	base := bytes.Repeat([]byte("A"), 64)
	value := append([]byte(nil), base...)
	value[2] = 'B'
	value[61] = 'C'

	cfg := NormalizeConfig(Config{MinSavingsBytes: 1})

	// Branch: varMask == nil path.
	defNoMask := TemplateDef{
		Kind: TemplateMask,
		Base: base,
		Mask: []byte{1}, // invalid mask length => treated as nil by matcher
	}
	payload, encLen, reason, matched := matchMaskTemplate(value, defNoMask, 10, cfg)
	if !matched || reason != "" || len(payload) == 0 || len(payload) != encLen {
		t.Fatalf("no-mask branch mismatch: matched=%v reason=%q payload=%d encLen=%d", matched, reason, len(payload), encLen)
	}

	// Branch: var positions + const positions with no const mismatch.
	varPos := []uint16{2, 61}
	mask := buildMaskFromPositions(varPos, len(base))
	defSparse := TemplateDef{
		Kind:           TemplateMask,
		Base:           base,
		Mask:           mask,
		VarPositions:   varPos,
		ConstPositions: buildConstPositions(mask, len(base)),
	}
	payload2, encLen2, reason2, matched2 := matchMaskTemplate(value, defSparse, 11, cfg)
	if !matched2 || reason2 != "" || len(payload2) == 0 || len(payload2) != encLen2 {
		t.Fatalf("sparse/full branch mismatch: matched=%v reason=%q payload=%d encLen=%d", matched2, reason2, len(payload2), encLen2)
	}

	// Branch: const mismatch fallback path.
	valueConstMismatch := append([]byte(nil), value...)
	valueConstMismatch[0] = 'Z'
	payload3, encLen3, reason3, matched3 := matchMaskTemplate(valueConstMismatch, defSparse, 12, cfg)
	if !matched3 || reason3 != "" || len(payload3) == 0 || len(payload3) != encLen3 {
		t.Fatalf("const-mismatch branch mismatch: matched=%v reason=%q payload=%d encLen=%d", matched3, reason3, len(payload3), encLen3)
	}

	// Branch: insufficient savings.
	cfgTight := NormalizeConfig(Config{MinSavingsBytes: 1 << 20})
	if _, _, reason4, matched4 := matchMaskTemplate(value, defSparse, 13, cfgTight); matched4 || reason4 != reasonMatchExpectedSavings {
		t.Fatalf("expected expected_savings failure, got matched=%v reason=%q", matched4, reason4)
	}
}
