package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// Search may reconstruct intermediate prefix keys in the same backing used by
// GetLeafKeyFlagsView. A later view must reconstruct the requested row again.
func TestLeafPrefixSearchPreservesSubsequentKeyViews(t *testing.T) {
	for _, columnar := range []bool{false, true} {
		name := "prefix"
		if columnar {
			name = "columnar_prefix"
		}
		t.Run(name, func(t *testing.T) {
			b := NewBuilderWithOptions(make([]byte, page.PageSize), page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true, LeafColumnar: columnar})
			var keys [][]byte
			for i := 0; i < 24; i++ {
				key := append([]byte{byte(i >> 8), byte(i)}, bytes.Repeat([]byte{'x'}, 130)...)
				keys = append(keys, key)
				if err := b.AddLeafEntry(key, []byte("v"), FlagInline, page.ValuePtr{}); err != nil {
					t.Fatal(err)
				}
			}
			n := b.Finish()
			last := uint16(len(keys) - 1)
			for _, query := range [][]byte{keys[1], keys[15], keys[22], keys[23]} {
				if got, _, err := n.GetLeafKeyFlagsView(last); err != nil || !bytes.Equal(got, keys[last]) {
					t.Fatalf("initial last key=%x err=%v", got, err)
				}
				if _, found, err := n.SearchLeaf(query); err != nil || !found {
					t.Fatalf("search found=%v err=%v", found, err)
				}
				if got, _, err := n.GetLeafKeyFlagsView(last); err != nil || !bytes.Equal(got, keys[last]) {
					t.Fatalf("last key after SearchLeaf: got=%x want=%x err=%v", got, keys[last], err)
				}
			}
		})
	}
}
