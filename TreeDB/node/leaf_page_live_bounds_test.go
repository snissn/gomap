package node

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func buildSparseLeafPageForBoundsTest(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	})
	b.SetPageID(9)
	for i := 0; i < 3; i++ {
		if err := b.AddLeafEntry([]byte("celestia-key-"+string(rune('a'+i))), []byte("value"), FlagInline, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%d): %v", i, err)
		}
	}
	b.FinishNoNode()
	return buf
}

func TestLeafPageLiveBounds_SparseLeaf(t *testing.T) {
	leaf := buildSparseLeafPageForBoundsTest(t)

	prefixLen, suffixLen, err := LeafPageLiveBounds(leaf)
	if err != nil {
		t.Fatalf("LeafPageLiveBounds: %v", err)
	}
	if prefixLen < NodeHeaderSize {
		t.Fatalf("prefixLen=%d want >= %d", prefixLen, NodeHeaderSize)
	}
	if suffixLen <= 0 {
		t.Fatalf("suffixLen=%d want > 0", suffixLen)
	}
	if prefixLen+suffixLen >= page.PageSize {
		t.Fatalf("prefix+suffix=%d want < %d", prefixLen+suffixLen, page.PageSize)
	}
}

func TestLeafPageLiveBounds_RejectsNonLeaf(t *testing.T) {
	buf := make([]byte, page.PageSize)
	n := NewNode(buf)
	n.SetType(page.PageTypeInternal)
	n.SetCount(0)
	n.UpdateChecksum()

	if _, _, err := LeafPageLiveBounds(buf); err != ErrInvalidType {
		t.Fatalf("LeafPageLiveBounds(non-leaf) err=%v want %v", err, ErrInvalidType)
	}
}

func TestLeafPageLiveBounds_RejectsCorruptDirectoryLayout(t *testing.T) {
	leaf := buildSparseLeafPageForBoundsTest(t)
	n := NewNodeView(leaf)
	n.SetCount(uint16(page.PageSize))

	if _, _, err := LeafPageLiveBounds(leaf); err != ErrCorruptedNode {
		t.Fatalf("LeafPageLiveBounds(corrupt-count) err=%v want %v", err, ErrCorruptedNode)
	}
}
