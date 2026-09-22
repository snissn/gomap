package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestGetLeafKeyFlagsView_RoundTripAcrossLayouts(t *testing.T) {
	type variant struct {
		name string
		opts BuilderOptions
	}

	variants := []variant{
		{name: "plain", opts: BuilderOptions{}},
		{name: "prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true}},
		{name: "columnar_v2", opts: BuilderOptions{LeafColumnar: true}},
		{name: "columnar_prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true}},
		{name: "columnar_prefix_v2_packed", opts: BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true}},
	}

	keys := makeBenchKeys(32, 16)
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			buf := make([]byte, page.PageSize)
			b := NewBuilderWithOptions(buf, page.PageTypeLeaf, v.opts)
			b.SetPageID(1)

			flagsWant := make([]byte, len(keys))
			for i := range keys {
				var value []byte
				var ptr page.ValuePtr
				switch i % 3 {
				case 0:
					flagsWant[i] = FlagInline
					value = []byte{byte(i), byte(i + 1), byte(i + 2)}
				case 1:
					flagsWant[i] = FlagPointer
					ptr = page.ValuePtr{Offset: uint64(i + 1), Length: uint32(10 + i), FileID: 7}
				default:
					flagsWant[i] = FlagTombstone
				}
				if err := b.AddLeafEntry(keys[i], value, flagsWant[i], ptr); err != nil {
					t.Fatalf("AddLeafEntry(%d): %v", i, err)
				}
			}
			n := b.Finish()

			for i := range keys {
				k, flags, err := n.GetLeafKeyFlagsView(uint16(i))
				if err != nil {
					t.Fatalf("GetLeafKeyFlagsView(%d): %v", i, err)
				}
				if !bytes.Equal(k, keys[i]) {
					t.Fatalf("key mismatch idx=%d got=%x want=%x", i, k, keys[i])
				}
				if flags != flagsWant[i] {
					t.Fatalf("flags mismatch idx=%d got=%d want=%d", i, flags, flagsWant[i])
				}

				entryKey, _, _, entryFlags, err := n.GetLeafEntryView(uint16(i))
				if err != nil {
					t.Fatalf("GetLeafEntryView(%d): %v", i, err)
				}
				if !bytes.Equal(entryKey, k) || entryFlags != flags {
					t.Fatalf("view mismatch idx=%d key=%x/%x flags=%d/%d", i, entryKey, k, entryFlags, flags)
				}
			}
		})
	}
}
