package compaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

type Candidate struct {
	FileID     uint32
	DeadBytes  uint64
	TotalBytes uint64
	DeadRatio  float64
}

type Options struct {
	DeadRatioThreshold float64
	MinTotalBytes      uint64
	MaxSlabs           int
	MicroBatchSize     int
	// IndexSwap rebuilds the index into a new file and swaps it in once after
	// compacting one or more slabs. This avoids high write amplification from
	// applying many pointer updates via COW B-Tree commits.
	IndexSwap bool
	// LiveSetMaxEntries controls the in-memory live pointer set size used to
	// skip per-record tree lookups during compaction. 0 uses a default; <0
	// disables the live-set optimization.
	LiveSetMaxEntries int

	// Assist is an optional hook invoked periodically during compaction work.
	// It must be fast and must not assume any compaction locks are held.
	// Typical use: coordinate with caching-layer backpressure by triggering a
	// bounded flush when backlog grows.
	Assist func()

	// Stats is an optional collector for compaction counters.
	Stats *Stats

	// RotateBeforeWrite forces a slab rotation once before moving any records.
	// This can reduce interference with the current active slab, but will create
	// a new slab file even if compaction ends up being a no-op.
	RotateBeforeWrite bool

	// CopyBytesPerSec limits compaction copy IO. 0 disables throttling.
	CopyBytesPerSec int64
	// CopyBurstBytes is the limiter burst size. 0 uses a 1-second burst.
	CopyBurstBytes int64
}

type Stats struct {
	LiveSetEntries     uint64
	LiveSetAborted     bool
	LiveSetBloom       bool
	TreeLookups        uint64
	TreeLookupsSkipped uint64
}

var slabStatsKeyPrefix = []byte{0x00, 's', 'l', 'a', 'b'}

func slabStatsPrefixEnd() []byte {
	end := append([]byte(nil), slabStatsKeyPrefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func decodeSlabStatsValue(v []byte) (dead, total uint64, ok bool) {
	if len(v) != 16 {
		return 0, 0, false
	}
	dead = binary.LittleEndian.Uint64(v[0:8])
	total = binary.LittleEndian.Uint64(v[8:16])
	return dead, total, true
}

func decodeSlabStatsKey(k []byte) (fileID uint32, ok bool) {
	if len(k) != len(slabStatsKeyPrefix)+4 {
		return 0, false
	}
	if !bytes.HasPrefix(k, slabStatsKeyPrefix) {
		return 0, false
	}
	return binary.BigEndian.Uint32(k[len(slabStatsKeyPrefix):]), true
}

func (c *Compactor) Candidates(opts Options) ([]Candidate, error) {
	if opts.DeadRatioThreshold < 0 {
		opts.DeadRatioThreshold = 0
	}
	if opts.DeadRatioThreshold > 1 {
		opts.DeadRatioThreshold = 1
	}

	snap := c.db.AcquireSnapshot()
	defer snap.Close()

	state := snap.State()
	if state == nil {
		return nil, fmt.Errorf("compaction: missing db state")
	}

	sysTree := tree.New(snap.Pager(), db.ValueReaderForState(state), state.SystemRootPageID)
	it := sysTree.Iterator(slabStatsKeyPrefix, slabStatsPrefixEnd())
	defer it.Close()

	activeID := c.db.SlabManager().ActiveSlabID()

	var out []Candidate
	for it.Valid() {
		k := it.UnsafeKey()
		fileID, ok := decodeSlabStatsKey(k)
		if !ok {
			it.Next()
			continue
		}
		if fileID == activeID {
			it.Next()
			continue
		}
		// Stats can outlive the physical slab file (after zombie deletion). Skip
		// missing slabs so compaction remains idempotent.
		if _, err := os.Stat(c.db.SlabManager().GetSlabPath(fileID)); err != nil {
			it.Next()
			continue
		}
		_, vPtr, entryFlags := it.UnsafeEntry()
		if entryFlags&node.FlagPointer != 0 {
			// System stats should always be inline.
			_ = vPtr
			it.Next()
			continue
		}

		dead, total, ok := decodeSlabStatsValue(it.UnsafeValue())
		if !ok || total == 0 {
			it.Next()
			continue
		}
		if total < opts.MinTotalBytes {
			it.Next()
			continue
		}

		ratio := float64(dead) / float64(total)
		if ratio < opts.DeadRatioThreshold {
			it.Next()
			continue
		}

		out = append(out, Candidate{
			FileID:     fileID,
			DeadBytes:  dead,
			TotalBytes: total,
			DeadRatio:  ratio,
		})
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].DeadRatio == out[j].DeadRatio {
			return out[i].TotalBytes > out[j].TotalBytes
		}
		return out[i].DeadRatio > out[j].DeadRatio
	})

	if opts.MaxSlabs > 0 && len(out) > opts.MaxSlabs {
		out = out[:opts.MaxSlabs]
	}
	return out, nil
}

func (c *Compactor) CompactCandidates(opts Options) error {
	return c.CompactCandidatesWithContext(context.Background(), opts)
}

func (c *Compactor) CompactCandidatesWithContext(ctx context.Context, opts Options) error {
	// Safety: OmitSlabKeys requires IndexSwap because the default compactor
	// relies on the key stored in the slab to verify liveness in the user tree.
	if c.db.SlabManager().OmitSlabKeys() && !opts.IndexSwap {
		return fmt.Errorf("compaction: IndexSwap required when OmitSlabKeys is enabled")
	}

	cands, err := c.Candidates(opts)
	if err != nil {
		return err
	}
	if len(cands) == 0 {
		return nil
	}

	if opts.RotateBeforeWrite {
		if _, err := c.db.SlabManager().Rotate(); err != nil {
			return err
		}
	}

	if opts.IndexSwap {
		ids := make([]uint32, 0, len(cands))
		for _, cand := range cands {
			ids = append(ids, cand.FileID)
		}
		return c.db.CompactSlabsIndexSwap(ctx, ids, db.IndexSwapCompactionOptions{
			CopyBytesPerSec: opts.CopyBytesPerSec,
			CopyBurstBytes:  opts.CopyBurstBytes,
			Assist:          opts.Assist,
		})
	}

	for _, cand := range cands {
		if err := c.CompactSlabWithContext(ctx, cand.FileID, opts); err != nil {
			return err
		}
	}
	return nil
}
