package template

import "testing"

func TestTemplateEncodeDecode(t *testing.T) {
	engine := NewEngine(Config{
		MinPrefixBytes:  1,
		MinSuffixBytes:  1,
		MinTotalBytes:   2,
		MinSavingsBytes: 1,
		HistoryEntries:  2,
	})

	v1 := []byte("prefix-AAA-suffix")
	v2 := []byte("prefix-BBB-suffix")
	v3 := []byte("prefix-CCC-suffix")

	if enc, ok, _ := engine.Encode(v1); ok || string(enc) != string(v1) {
		t.Fatalf("expected first value to stay raw")
	}
	if enc, ok, _ := engine.Encode(v2); ok || string(enc) != string(v2) {
		t.Fatalf("expected second value to stay raw (template created after)")
	}
	enc, ok, _ := engine.Encode(v3)
	if !ok {
		t.Fatalf("expected third value to be template-encoded")
	}
	decoded, err := engine.Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded) != string(v3) {
		t.Fatalf("decoded mismatch: %q != %q", decoded, v3)
	}
}
