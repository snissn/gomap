package template

import "testing"

func TestTemplateDefRoundtrip(t *testing.T) {
	cfg := Config{MinAnchorLen: 2, MaxAnchorLen: 64, MaxAnchorsPerTemplate: 4, MaxAnchorBytesTotal: 128}
	def := TemplateDef{Anchors: [][]byte{[]byte("aa"), []byte("bb")}}
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

func TestDecodePayloadMissingTemplate(t *testing.T) {
	payload := []byte{magic0, magic1, payloadVer, flagEncoded, 1, 1, 0}
	_, err := DecodePayload(payload, func(uint64) ([]byte, error) {
		return nil, ErrMissingTemplate
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 8})
	if err == nil {
		t.Fatalf("expected error")
	}
}
