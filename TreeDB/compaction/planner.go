package compaction

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

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

	// RotateBeforeWrite forces a slab rotation once before moving any records.
	// This can reduce interference with the current active slab, but will create
	// a new slab file even if compaction ends up being a no-op.
	RotateBeforeWrite bool

	// CopyBytesPerSec limits compaction copy IO. 0 disables throttling.
	CopyBytesPerSec int64
	// CopyBurstBytes is the limiter burst size. 0 uses a 1-second burst.
	CopyBurstBytes int64
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

	state := c.db.State()
	if state == nil {
		return nil, fmt.Errorf("compaction: missing db state")
	}
	// Pin slabs while reading system tree.
	if state.SlabSet != nil {
		c.db.SlabManager().AcquireSlabs(state.SlabSet)
		defer c.db.SlabManager().ReleaseSlabs(state.SlabSet)
	}

	sysTree := tree.New(c.db.Pager(), state.SlabSet, state.SystemRootPageID)
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

	for _, cand := range cands {
		if err := c.CompactSlabWithOptions(cand.FileID, opts); err != nil {
			return err
		}
	}
	return nil
}
