package template

import (
	"encoding/binary"
	"testing"
)

func TestDecodePayloadAppend_Anchors(t *testing.T) {
	def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("AA"), []byte("BB")}}
	payload, err := EncodePayload(1, [][]byte{[]byte("x"), []byte("y"), []byte("z")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	out, err := DecodePayloadAppend([]byte("prefix-"), payload, func(id uint64) (TemplateDef, error) {
		if id != 1 {
			return TemplateDef{}, ErrMissingTemplate
		}
		return def, nil
	}, DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 8})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(out) != "prefix-xAAyBBz" {
		t.Fatalf("unexpected output: %q", out)
	}
}

func TestDecodePayloadAppend_Mask(t *testing.T) {
	base := []byte("abcdef")
	mask := []byte{0b00101100} // variable at positions 2,3,5
	def := TemplateDef{Kind: TemplateMask, Base: base, Mask: mask}

	vars := []byte{'X', 'Y', 'Z'}
	want := "abXYeZ"

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

	out, err := DecodePayloadAppend(nil, sparse, func(uint64) (TemplateDef, error) {
		return def, nil
	}, DecodeOptions{MaxDecodedBytes: 128})
	if err != nil {
		t.Fatalf("decode sparse: %v", err)
	}
	if string(out) != want {
		t.Fatalf("sparse mismatch: got %q want %q", out, want)
	}

	// Full payload (mask + diffs).
	full, err := EncodeMaskPayload(1, mask, vars)
	if err != nil {
		t.Fatalf("encode full: %v", err)
	}
	out, err = DecodePayloadAppend(nil, full, func(uint64) (TemplateDef, error) {
		return def, nil
	}, DecodeOptions{MaxDecodedBytes: 128})
	if err != nil {
		t.Fatalf("decode full: %v", err)
	}
	if string(out) != want {
		t.Fatalf("full mismatch: got %q want %q", out, want)
	}
}

func TestDecodePayloadAppend_NoAllocs(t *testing.T) {
	t.Run("anchors", func(t *testing.T) {
		def := TemplateDef{Kind: TemplateAnchors, Anchors: [][]byte{[]byte("AA"), []byte("BB")}}
		payload, err := EncodePayload(1, [][]byte{[]byte("x"), []byte("y"), []byte("z")})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		lookup := func(id uint64) (TemplateDef, error) {
			if id != 1 {
				return TemplateDef{}, ErrMissingTemplate
			}
			return def, nil
		}
		opts := DecodeOptions{MaxDecodedBytes: 1 << 20, MaxGaps: 8}
		dst := make([]byte, 0, 64)
		allocs := testing.AllocsPerRun(1_000, func() {
			out, err := DecodePayloadAppend(dst[:0], payload, lookup, opts)
			if err != nil {
				panic(err)
			}
			if len(out) == 0 {
				panic("empty output")
			}
		})
		if allocs != 0 {
			t.Fatalf("expected 0 allocs, got %.3f", allocs)
		}
	})

	t.Run("mask", func(t *testing.T) {
		base := []byte("abcdef")
		mask := []byte{0b00101100} // variable at positions 2,3,5
		def := TemplateDef{Kind: TemplateMask, Base: base, Mask: mask}
		full, err := EncodeMaskPayload(1, mask, []byte{'X', 'Y', 'Z'})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		opts := DecodeOptions{MaxDecodedBytes: 128}
		dst := make([]byte, 0, 64)
		allocs := testing.AllocsPerRun(1_000, func() {
			out, err := DecodePayloadAppend(dst[:0], full, func(uint64) (TemplateDef, error) {
				return def, nil
			}, opts)
			if err != nil {
				panic(err)
			}
			if len(out) == 0 {
				panic("empty output")
			}
		})
		if allocs != 0 {
			t.Fatalf("expected 0 allocs, got %.3f", allocs)
		}
	})
}
