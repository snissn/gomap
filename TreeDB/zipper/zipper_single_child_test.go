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

func TestZipperSingleChildGrowth(t *testing.T) {
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

	const (
		batches     = 200
		batchSize   = 8
		targetDepth = 50
		logEvery    = 50
	)
	lowKey := makeKey(0x66, 0)
	value := []byte("v")

	for b := 0; b < batches; b++ {
		bt := batch.New(sm, page.DefaultInlineThreshold)
		_ = bt.Set(lowKey, value)
		start := uint64(b * batchSize)
		for i := 0; i < batchSize; i++ {
			key := makeKey(0x73, start+uint64(i))
			_ = bt.Set(key, value)
		}
		newRoot, _, _, err := z.Apply(rootID, bt)
		_ = bt.Close()
		if err != nil {
			t.Fatalf("apply failed: %v", err)
		}
		rootID = newRoot
		if logEvery > 0 && (b+1)%logEvery == 0 {
			stats, err := computeDepthStatsForTest(p, rootID)
			if err != nil {
				t.Fatalf("stats failed: %v", err)
			}
			t.Logf("batch=%d max_depth=%d singles=%d max_chain=%d leaves=%d internal=%d", b+1, stats.maxDepth, stats.singles, stats.maxChain, stats.leaves, stats.internal)
		}
	}

	stats, err := computeDepthStatsForTest(p, rootID)
	if err != nil {
		t.Fatalf("stats failed: %v", err)
	}
	if stats.maxDepth >= targetDepth {
		t.Fatalf("depth exceeded target: max_depth=%d target=%d", stats.maxDepth, targetDepth)
	}
	if stats.singles > 0 {
		t.Fatalf("expected no single-child internal nodes, got %d (max_depth=%d)", stats.singles, stats.maxDepth)
	}
}

type testDepthStats struct {
	maxDepth int
	leaves   int
	internal int
	singles  int
	maxChain int
}

func computeDepthStatsForTest(p *pager.Pager, rootID uint64) (testDepthStats, error) {
	stats := testDepthStats{}
	seen := make(map[uint64]struct{})
	type frame struct {
		id          uint64
		depth       int
		singleChain int
	}
	stack := []frame{{id: rootID, depth: 0, singleChain: 0}}
	for len(stack) > 0 {
		last := len(stack) - 1
		f := stack[last]
		stack = stack[:last]
		if _, ok := seen[f.id]; ok {
			continue
		}
		seen[f.id] = struct{}{}
		data, err := p.Get(f.id)
		if err != nil {
			return stats, err
		}
		n := node.NewNodeView(data)
		if f.depth > stats.maxDepth {
			stats.maxDepth = f.depth
		}
		switch n.Type() {
		case page.PageTypeLeaf:
			stats.leaves++
			if f.singleChain > stats.maxChain {
				stats.maxChain = f.singleChain
			}
		case page.PageTypeInternal:
			stats.internal++
			count := int(n.Count())
			nextChain := 0
			if count == 1 {
				stats.singles++
				nextChain = f.singleChain + 1
				if nextChain > stats.maxChain {
					stats.maxChain = nextChain
				}
			}
			for i := count - 1; i >= 0; i-- {
				_, childID, err := n.GetInternalEntryView(uint16(i))
				if err != nil {
					return stats, err
				}
				chain := 0
				if count == 1 {
					chain = nextChain
				}
				stack = append(stack, frame{id: childID, depth: f.depth + 1, singleChain: chain})
			}
		}
	}
	return stats, nil
}
