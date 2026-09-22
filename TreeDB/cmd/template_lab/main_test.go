package main

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func buildOuterLeafTestPage(t *testing.T, entries int) []byte {
	t.Helper()
	if entries <= 0 {
		entries = 1
	}
	buf := make([]byte, page.PageSize)
	b := node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(12345)
	for i := 0; i < entries; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(1_000_000+i*17))
		ptr := page.ValuePtr{
			Offset: uint64(2048 + i*96),
			Length: uint32(32 + (i % 7)),
			FileID: page.ValueLogFileID(3),
		}
		if err := b.AddLeafEntry(key, nil, node.FlagPointer, ptr); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return append([]byte(nil), buf...)
}

func TestOuterLeafPretransform_HeaderDirDeltaV1_RoundTrip(t *testing.T) {
	original := buildOuterLeafTestPage(t, 64)
	transformed := applyOuterLeafPretransform(original, outerLeafPretransformHeaderDirDeltaV1)
	if bytes.Equal(transformed, original) {
		t.Fatalf("expected transformed payload to differ from original")
	}
	recovered, err := reverseOuterLeafPretransform(transformed, outerLeafPretransformHeaderDirDeltaV1)
	if err != nil {
		t.Fatalf("reverse transform: %v", err)
	}
	if !bytes.Equal(recovered, original) {
		t.Fatalf("round-trip mismatch")
	}
	if !bytes.Equal(original, buildOuterLeafTestPage(t, 64)) {
		t.Fatalf("input page unexpectedly mutated")
	}
}

func TestOuterLeafPretransform_HeaderDirDeltaV1_FallbackRoundTrip(t *testing.T) {
	plain := make([]byte, page.PageSize)
	for i := range plain {
		plain[i] = byte(i % 251)
	}
	orig := append([]byte(nil), plain...)
	transformed := applyOuterLeafPretransform(plain, outerLeafPretransformHeaderDirDeltaV1)
	recovered, err := reverseOuterLeafPretransform(transformed, outerLeafPretransformHeaderDirDeltaV1)
	if err != nil {
		t.Fatalf("reverse fallback transform: %v", err)
	}
	if !bytes.Equal(recovered, orig) {
		t.Fatalf("fallback round-trip mismatch")
	}
	if !bytes.Equal(plain, orig) {
		t.Fatalf("fallback input mutated")
	}
}
