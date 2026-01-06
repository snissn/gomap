package db

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

var errInvalidSlabStats = errors.New("invalid slab stats value")

var slabStatsKeyPrefix = []byte{0x00, 's', 'l', 'a', 'b'}

func slabStatsKey(fileID uint32) []byte {
	k := make([]byte, 0, len(slabStatsKeyPrefix)+4)
	k = append(k, slabStatsKeyPrefix...)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], fileID)
	k = append(k, b[:]...)
	return k
}

func slabStatsPrefixEnd() []byte {
	// Compute the smallest key strictly greater than all keys with the prefix.
	end := append([]byte(nil), slabStatsKeyPrefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	// No valid end key; treat as unbounded.
	return nil
}

func decodeSlabStatsValue(v []byte) (dead, total uint64, err error) {
	if len(v) != 16 {
		return 0, 0, errInvalidSlabStats
	}
	dead = binary.LittleEndian.Uint64(v[0:8])
	total = binary.LittleEndian.Uint64(v[8:16])
	return dead, total, nil
}

func encodeSlabStatsValue(dead, total uint64) []byte {
	var v [16]byte
	binary.LittleEndian.PutUint64(v[0:8], dead)
	binary.LittleEndian.PutUint64(v[8:16], total)
	return v[:]
}

// applySystemUpdates updates the internal System tree with extra operations and
// per-slab [DeadBytes][TotalBytes] counters derived from commit metrics.
//
// It returns the new System root and any retired pages from the System tree
// update. If no updates are necessary, it returns the input root and nil.
func (db *DB) applySystemUpdates(sysRootID uint64, extraOps []batch.Entry, metrics adaptive.Metrics) (uint64, []uint64, error) {
	if len(extraOps) == 0 && len(metrics.SlabWriteBytesByFile) == 0 && len(metrics.SlabDeadBytesByFile) == 0 {
		return sysRootID, nil, nil
	}

	idx := db.idx.Load()
	if idx == nil {
		return 0, nil, errors.New("missing index")
	}

	sysTree := tree.New(idx.pager, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, sysRootID)
	sysBatch := batch.New(db.slabManager, page.DefaultInlineThreshold)
	defer func() { _ = sysBatch.Close() }()

	if len(extraOps) > 0 {
		if err := sysBatch.SetOps(extraOps); err != nil {
			return 0, nil, err
		}
	}

	// Determine which slab IDs are touched this commit.
	ids := make([]uint32, 0, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	seen := make(map[uint32]struct{}, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	for id := range metrics.SlabWriteBytesByFile {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range metrics.SlabDeadBytesByFile {
		if _, ok := seen[id]; ok {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		key := slabStatsKey(id)

		var dead, total uint64
		if raw, err := sysTree.Get(key); err == nil && raw != nil {
			d, t, err := decodeSlabStatsValue(raw)
			if err == nil {
				dead, total = d, t
			}
		}

		if delta := metrics.SlabWriteBytesByFile[id]; delta > 0 {
			total += uint64(delta)
		}
		if delta := metrics.SlabDeadBytesByFile[id]; delta > 0 {
			dead += uint64(delta)
		}
		if dead > total {
			dead = total
		}

		if err := sysBatch.Set(key, encodeSlabStatsValue(dead, total)); err != nil {
			return 0, nil, err
		}
	}

	newSysRoot, sysRetired, _, err := idx.zipper.Apply(sysRootID, sysBatch)
	if err != nil {
		return 0, nil, err
	}
	return newSysRoot, sysRetired, nil
}
