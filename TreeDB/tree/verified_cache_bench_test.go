package tree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func setupTwoLevelTree(b *testing.B) (*Tree, *pager.Pager) {
	b.Helper()

	dir := b.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		b.Fatalf("Pager open failed: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })

	// Pages:
	// 0: Internal (root)
	// 1: Leaf (left)
	// 2: Leaf (right)
	if _, err := p.Alloc(3); err != nil {
		b.Fatalf("Alloc: %v", err)
	}

	// Leaf 1: "10", "40"
	data1, _ := p.GetForWrite(1)
	n1 := node.NewNode(data1)
	n1.SetType(page.PageTypeLeaf)
	n1.SetPageID(1)
	n1.AddLeafEntry([]byte("10"), []byte("val10"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("40"), []byte("val40"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	// Leaf 2: "60"
	data2, _ := p.GetForWrite(2)
	n2 := node.NewNode(data2)
	n2.SetType(page.PageTypeLeaf)
	n2.SetPageID(2)
	n2.AddLeafEntry([]byte("60"), []byte("val60"), node.FlagInline, page.ValuePtr{})
	n2.UpdateChecksum()

	// Root: "00"->1, "50"->2
	data0, _ := p.GetForWrite(0)
	n0 := node.NewNode(data0)
	n0.SetType(page.PageTypeInternal)
	n0.SetPageID(0)
	n0.AddInternalChild([]byte("00"), 1)
	n0.AddInternalChild([]byte("50"), 2)
	n0.UpdateChecksum()

	return New(p, panicValueReader{}, 0), p
}

func getNoCache(p *pager.Pager, sr SlabReader, root uint64, key []byte) ([]byte, error) {
	currID := root

	for depth := 0; depth < maxTraversalDepth; depth++ {
		data, err := p.Get(currID)
		if err != nil {
			return nil, err
		}

		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			return nil, fmt.Errorf("checksum mismatch on page %d", currID)
		}

		switch n.Type() {
		case page.PageTypeInternal:
			idx, _ := n.SearchInternal(key)
			childID, err := n.GetInternalChildID(idx)
			if err != nil {
				return nil, err
			}
			currID = childID
		case page.PageTypeLeaf:
			idx, found, err := n.SearchLeaf(key)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ErrKeyNotFound
			}

			_, v, ptr, flags, err := n.GetLeafEntryView(idx)
			if err != nil {
				return nil, err
			}
			if flags&node.FlagTombstone != 0 {
				return nil, ErrKeyNotFound
			}
			if flags&node.FlagPointer != 0 {
				val, err := sr.Read(ptr)
				if err != nil {
					return nil, err
				}
				return val, nil
			}

			out := make([]byte, len(v))
			copy(out, v)
			return out, nil
		default:
			return nil, fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
		}
	}

	return nil, fmt.Errorf("tree too deep")
}

func BenchmarkTreeGet_VerifiedCache(b *testing.B) {
	tr, p := setupTwoLevelTree(b)
	key := []byte("60")

	b.Run("cache", func(b *testing.B) {
		_, err := tr.Get(key)
		if err != nil {
			b.Fatalf("warmup Get: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tr.Get(key)
		}
	})

	b.Run("verify_always", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = getNoCache(p, panicValueReader{}, 0, key)
		}
	})

	// Ensure benchmark doesn't get optimized away.
	// (Keep a stable, non-constant observable use.)
	if got, _ := tr.Get(key); bytes.Equal(got, nil) {
		b.Fatal("unreachable")
	}
}
