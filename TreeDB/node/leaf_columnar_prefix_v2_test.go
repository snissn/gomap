package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafColumnarPrefixV2_RoundTrip(t *testing.T) {
	keys := makeBenchKeys(64, 16)
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	ptrs := make([]page.ValuePtr, len(keys))
	values := make([][]byte, len(keys))
	flags := make([]byte, len(keys))
	for i := range keys {
		switch i % 3 {
		case 0:
			flags[i] = FlagInline
			values[i] = []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		case 1:
			flags[i] = FlagPointer
			ptrs[i] = page.ValuePtr{Offset: uint64(i + 1), Length: 123, FileID: 7}
		default:
			flags[i] = FlagTombstone
		}
		if err := b.AddLeafEntry(keys[i], values[i], flags[i], ptrs[i]); err != nil {
			t.Fatalf("AddLeafEntry: %v", err)
		}
	}

	n := b.Finish()
	if !n.leafColumnar() || !n.leafPrefixCompressed() || !n.leafPrefixV2() {
		t.Fatalf("expected leaf flags set: columnar=%v prefix=%v v2=%v", n.leafColumnar(), n.leafPrefixCompressed(), n.leafPrefixV2())
	}

	for i := range keys {
		idx, found, err := n.SearchLeaf(keys[i])
		if err != nil {
			t.Fatalf("SearchLeaf: %v", err)
		}
		if !found || idx != uint16(i) {
			t.Fatalf("SearchLeaf mismatch i=%d idx=%d found=%v", i, idx, found)
		}

		k, v, vp, gotFlags, err := n.GetLeafEntryView(idx)
		if err != nil {
			t.Fatalf("GetLeafEntryView: %v", err)
		}
		if !bytes.Equal(k, keys[i]) {
			t.Fatalf("key mismatch i=%d", i)
		}
		if gotFlags != flags[i] {
			t.Fatalf("flags mismatch i=%d got=%d want=%d", i, gotFlags, flags[i])
		}
		switch gotFlags {
		case FlagInline:
			if !bytes.Equal(v, values[i]) || vp != (page.ValuePtr{}) {
				t.Fatalf("inline mismatch i=%d val=%x want=%x ptr=%+v", i, v, values[i], vp)
			}
		case FlagPointer:
			if v != nil || vp != ptrs[i] {
				t.Fatalf("ptr mismatch i=%d val=%v ptr=%+v want=%+v", i, v, vp, ptrs[i])
			}
		case FlagTombstone:
			if v != nil || vp != (page.ValuePtr{}) {
				t.Fatalf("tombstone mismatch i=%d val=%v ptr=%+v", i, v, vp)
			}
		default:
			t.Fatalf("unexpected flags i=%d flags=%d", i, gotFlags)
		}
	}
}

func TestLeafColumnarPrefixV2_StrictShortKeysStayCombined(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	for i := 0; i < 32; i++ {
		key := []byte{0, 0, 0, 0, byte(i >> 8), byte(i), byte(i), byte(i)}
		if err := b.AddLeafEntry(key, nil, FlagPointer, page.ValuePtr{Offset: uint64(i + 1), Length: 4, FileID: 1}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.leafPrefixCompressed() || !n.leafColumnar() || !n.leafPrefixV2() {
		t.Fatalf("expected strict combined columnar+prefix encoding for short keys")
	}
}

func TestLeafColumnarPrefixV2_KeepsCombinedForLongKeys(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
	})
	b.SetPageID(1)

	keys := makeBenchKeys(32, 16)
	for i := 0; i < len(keys); i++ {
		if err := b.AddLeafEntry(keys[i], nil, FlagPointer, page.ValuePtr{Offset: uint64(i + 1), Length: 4, FileID: 1}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}

	n := b.Finish()
	if !n.leafPrefixCompressed() || !n.leafColumnar() || !n.leafPrefixV2() {
		t.Fatalf("expected combined columnar+prefix encoding for long keys")
	}
}

func TestLeafColumnarPrefixV2_SeparatedStreamsLayout(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(1)

	keys := makeBenchKeys(32, 16)
	for i := range keys {
		switch i % 3 {
		case 0:
			if err := b.AddLeafEntry(keys[i], []byte{byte(i), byte(i + 1)}, FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("AddLeafEntry inline(%d): %v", i, err)
			}
		case 1:
			ptr := page.ValuePtr{Offset: uint64(i + 1), Length: 33, FileID: 9}
			if err := b.AddLeafEntry(keys[i], nil, FlagPointer, ptr); err != nil {
				t.Fatalf("AddLeafEntry ptr(%d): %v", i, err)
			}
		default:
			if err := b.AddLeafEntry(keys[i], nil, FlagTombstone, page.ValuePtr{}); err != nil {
				t.Fatalf("AddLeafEntry tombstone(%d): %v", i, err)
			}
		}
	}

	n := b.Finish()
	count := int(n.Count())
	keyDirStart := NodeHeaderSize
	valDirStart := keyDirStart + count*DirectoryEntrySize
	flagsStart := valDirStart + count*DirectoryEntrySize
	prefixStart := flagsStart + count
	headerEnd := prefixStart + count*DirectoryEntrySize

	if headerEnd > len(n.Data()) {
		t.Fatalf("invalid combined header end: %d", headerEnd)
	}

	prevKeyOff := -1
	prevValOff := -1
	for i := 0; i < count; i++ {
		keyOff := int(getUint16(n.Data()[keyDirStart+i*2 : keyDirStart+i*2+2]))
		valOff := int(getUint16(n.Data()[valDirStart+i*2 : valDirStart+i*2+2]))
		if keyOff < headerEnd || keyOff > len(n.Data()) {
			t.Fatalf("invalid keyOff idx=%d off=%d headerEnd=%d", i, keyOff, headerEnd)
		}
		if valOff < headerEnd || valOff > len(n.Data()) {
			t.Fatalf("invalid valOff idx=%d off=%d headerEnd=%d", i, valOff, headerEnd)
		}
		if prevKeyOff != -1 && keyOff < prevKeyOff {
			t.Fatalf("expected key offsets monotonic, idx=%d prev=%d cur=%d", i, prevKeyOff, keyOff)
		}
		if prevValOff != -1 && valOff < prevValOff {
			t.Fatalf("expected val offsets monotonic, idx=%d prev=%d cur=%d", i, prevValOff, valOff)
		}
		prevKeyOff = keyOff
		prevValOff = valOff

		prefixLen := int(getUint16(n.Data()[prefixStart+i*2 : prefixStart+i*2+2]))
		if i%leafPrefixRestartInterval == 0 && prefixLen != 0 {
			t.Fatalf("restart idx=%d expected prefixLen=0, got %d", i, prefixLen)
		}
	}
}
