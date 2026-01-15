package zipper

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestZipperLongKeyFanout(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetFillTargets(1_000_000, 1_000_000)

	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	const keyLen = 2000
	makeKey := func(prefix byte, idx uint64) []byte {
		key := make([]byte, keyLen)
		key[0] = prefix
		binary.BigEndian.PutUint64(key[1:], idx)
		for i := 9; i < len(key); i++ {
			key[i] = prefix
		}
		return key
	}

	bt := batch.New(sm, page.DefaultInlineThreshold)
	for i := 0; i < 256; i++ {
		key := makeKey(0x73, uint64(i))
		_ = bt.Set(key, []byte("v"))
	}
	newRoot, _, _, err := z.Apply(rootID, bt)
	_ = bt.Close()
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	stats, err := computeDepthStatsForTest(p, newRoot)
	if err != nil {
		t.Fatalf("stats failed: %v", err)
	}
	if stats.internal == 0 {
		t.Fatalf("expected internal nodes, got leaves=%d", stats.leaves)
	}
	if stats.maxInternalCount < 4 {
		t.Fatalf("fanout too low: max_internal_count=%d", stats.maxInternalCount)
	}
	if stats.maxDepth >= 20 {
		t.Fatalf("depth too high: max_depth=%d", stats.maxDepth)
	}
}

func TestZipperSeparatorShortening(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetFillTargets(1_000_000, 1_000_000)

	rootID, _ := p.Alloc(1)
	data, _ := p.Get(rootID)
	n := node.NewNode(data)
	n.SetPageID(rootID)
	n.SetType(page.PageTypeLeaf)
	n.UpdateChecksum()

	const keyLen = 2000
	makeKey := func(prefix byte, idx uint64) []byte {
		key := make([]byte, keyLen)
		key[0] = prefix
		binary.BigEndian.PutUint64(key[1:], idx)
		for i := 9; i < len(key); i++ {
			key[i] = prefix
		}
		return key
	}

	bt := batch.New(sm, page.DefaultInlineThreshold)
	_ = bt.Set(makeKey(0x66, 0), []byte("v"))
	_ = bt.Set(makeKey(0x66, 1), []byte("v"))
	_ = bt.Set(makeKey(0x66, 2), []byte("v"))
	newRoot, _, _, err := z.Apply(rootID, bt)
	_ = bt.Close()
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}

	rootData, err := p.Get(newRoot)
	if err != nil {
		t.Fatal(err)
	}
	rootNode := node.NewNodeView(rootData)
	if rootNode.Type() != page.PageTypeInternal {
		t.Fatalf("expected internal root, got %v", rootNode.Type())
	}
	stats, err := computeDepthStatsForTest(p, newRoot)
	if err != nil {
		t.Fatalf("stats failed: %v", err)
	}
	if stats.maxSeparatorLen >= 64 {
		t.Fatalf("separator too long: max_separator_len=%d", stats.maxSeparatorLen)
	}
}
