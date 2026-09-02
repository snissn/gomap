package db

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type currentRootLeafDepthStats struct {
	minLeafDepth  int
	maxLeafDepth  int
	leafCount     int
	internalCount int
}

func collectCurrentRootLeafDepthStats(t *testing.T, d *DB) currentRootLeafDepthStats {
	t.Helper()
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing index")
	}
	d.mu.RLock()
	rootID := d.meta.UserRootPageID
	d.mu.RUnlock()
	if rootID == 0 {
		return currentRootLeafDepthStats{}
	}

	stats := currentRootLeafDepthStats{minLeafDepth: int(^uint(0) >> 1)}
	seen := make(map[uint64]struct{})
	var walk func(pageID uint64, depth int)
	walk = func(pageID uint64, depth int) {
		if pageID == 0 {
			return
		}
		if _, ok := seen[pageID]; ok {
			t.Fatalf("current root contains a cycle or duplicate page reference at page %d", pageID)
		}
		seen[pageID] = struct{}{}

		data, err := idx.pager.Get(pageID)
		if err != nil {
			t.Fatalf("get page %d: %v", pageID, err)
		}
		n := node.NewNodeView(data)
		switch n.Type() {
		case page.PageTypeLeaf, 0:
			recordLeafDepth(&stats, depth)
		case page.PageTypeInternal:
			stats.internalCount++
			for i := uint16(0); i < n.Count(); i++ {
				ref, err := n.GetInternalChildRef(i)
				if err != nil {
					t.Fatalf("get child ref %d/%d from internal page %d: %v", i, n.Count(), pageID, err)
				}
				switch ref.Kind {
				case page.ChildRefPage:
					walk(ref.Page, depth+1)
				case page.ChildRefLeafLog:
					recordLeafDepth(&stats, depth+1)
				default:
					t.Fatalf("internal page %d child %d has unsupported ref kind %d", pageID, i, ref.Kind)
				}
			}
		default:
			t.Fatalf("page %d has invalid type %d", pageID, n.Type())
		}
	}
	walk(rootID, 0)
	return stats
}

func recordLeafDepth(stats *currentRootLeafDepthStats, depth int) {
	if depth < stats.minLeafDepth {
		stats.minLeafDepth = depth
	}
	if depth > stats.maxLeafDepth {
		stats.maxLeafDepth = depth
	}
	stats.leafCount++
}

func TestBatchWritesPreserveUniformLeafDepthWithOuterLeafInternalSplits(t *testing.T) {
	d, err := Open(Options{
		Dir:                        t.TempDir(),
		CommandWAL:                 true,
		IndexOuterLeavesInValueLog: true,
		PreferAppendAlloc:          true,
		InternalFillTargetPPM:      100_000,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	const (
		batches   = 20
		batchSize = 500
	)
	value := make([]byte, 512)
	var x uint64 = 1
	var key [9]byte
	for batchIdx := 0; batchIdx < batches; batchIdx++ {
		b := d.NewBatch()
		for i := 0; i < batchSize; i++ {
			x = x*2862933555777941757 + 3037000493
			key[0] = byte(x >> 56)
			binary.BigEndian.PutUint64(key[1:], x)
			if err := b.Set(key[:], value); err != nil {
				_ = b.Close()
				t.Fatalf("batch set: %v", err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("batch write %d: %v", batchIdx, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("batch close %d: %v", batchIdx, err)
		}
	}

	stats := collectCurrentRootLeafDepthStats(t, d)
	if stats.leafCount == 0 || stats.internalCount == 0 || stats.maxLeafDepth < 3 {
		t.Fatalf("test did not exercise nested internal pages: %+v", stats)
	}
	if stats.minLeafDepth != stats.maxLeafDepth {
		t.Fatalf("leaf depths are not uniform after internal splits: min=%d max=%d leaves=%d internals=%d", stats.minLeafDepth, stats.maxLeafDepth, stats.leafCount, stats.internalCount)
	}
}
