package template

import "testing"

func TestTemplateDefRoundtrip(t *testing.T) {
	cfg := Config{MinAnchorLen: 2, MaxAnchorLen: 64, MaxAnchorsPerTemplate: 4, MaxAnchorBytesTotal: 128}
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("aa"), []byte("bb")}}
	enc, err := EncodeTemplateDef(def, cfg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := DecodeTemplateDef(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(dec.Anchors) != len(def.Anchors) {
		t.Fatalf("anchor count mismatch")
	}
	for i := range def.Anchors {
		if string(dec.Anchors[i]) != string(def.Anchors[i]) {
			t.Fatalf("anchor %d mismatch", i)
		}
	}
}

func TestTemplateMaskRoundtrip(t *testing.T) {
	base := []byte("abcdef")
	mask := []byte{0b00101100} // variable at positions 2,3,5
	def := TemplateDef{Kind: TemplateMask, Base: base, Mask: mask}
	enc, err := EncodeTemplateDef(def, Config{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := DecodeTemplateDef(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.Kind != TemplateMask {
		t.Fatalf("kind mismatch")
	}
	if string(dec.Base) != string(base) {
		t.Fatalf("base mismatch")
	}
	if len(dec.VarPositions) != 3 {
		t.Fatalf("var positions mismatch")
	}
	vars := []byte{'X', 'Y', 'Z'}
	payload, err := EncodeMaskPayload(1, vars)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	out, err := DecodePayload(payload, func(uint64) ([]byte, error) {
		return enc, nil
	}, DecodeOptions{MaxDecodedBytes: 128})
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	want := []byte("abXYeZ")
	if string(out) != string(want) {
		t.Fatalf("payload mismatch: %q != %q", out, want)
	}
}

func TestDecodePayloadMissingTemplate(t *testing.T) {
	payload := []byte{magic0, magic1, payloadVer, flagEncoded, 1, 1, 0}
	_, err := DecodePayload(payload, func(uint64) ([]byte, error) {
		return nil, ErrMissingTemplate
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 8})
	if err == nil {
		t.Fatalf("expected error")
	}
}
