package template

import (
	"encoding/binary"
	"testing"
)

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
	vars := []byte{'X', 'Y', 'Z'}
	// Sparse payload (variable bytes only).
	sparseLen := payloadHeader + uvarintLen(1) + len(vars)
	sparse := make([]byte, sparseLen)
	sparse[0] = magic0
	sparse[1] = magic1
	sparse[2] = payloadVer
	sparse[3] = flagEncoded | flagMask
	off := payloadHeader
	off += binary.PutUvarint(sparse[off:], 1)
	copy(sparse[off:], vars)
	out, err := DecodePayload(sparse, func(uint64) ([]byte, error) {
		return enc, nil
	}, DecodeOptions{MaxDecodedBytes: 128})
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	want := []byte("abXYeZ")
	if string(out) != string(want) {
		t.Fatalf("payload mismatch: %q != %q", out, want)
	}
	// Full payload (mask + diffs).
	full, err := EncodeMaskPayload(1, mask, vars)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	out, err = DecodePayload(full, func(uint64) ([]byte, error) {
		return enc, nil
	}, DecodeOptions{MaxDecodedBytes: 128})
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
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

func TestEncodedPayloadTemplateID(t *testing.T) {
	payload, err := EncodePayload(73, [][]byte{[]byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	id, err := EncodedPayloadTemplateID(payload)
	if err != nil || id != 73 {
		t.Fatalf("template id=%d err=%v want 73", id, err)
	}
	for _, payload := range [][]byte{
		nil,
		[]byte("ordinary"),
		{magic0, magic1, payloadVer, flagEncoded},
		{magic0, magic1, payloadVer, flagEncoded, 0},
		{magic0, magic1, payloadVer, flagEncoded, 0x80},
	} {
		if id, err := EncodedPayloadTemplateID(payload); err == nil || id != 0 {
			t.Fatalf("malformed payload %x template id=%d err=%v", payload, id, err)
		}
	}
}
