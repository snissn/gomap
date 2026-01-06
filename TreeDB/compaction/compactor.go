package compaction

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
)

type Compactor struct {
	db *db.DB
}

const defaultLiveSetMaxEntries = 1_000_000

func New(d *db.DB) *Compactor {
	return &Compactor{db: d}
}

// CompactSlab performs compaction on a specific slab file.
// It rewrites live records to a new slab and updates pointers in micro-batches.
func (c *Compactor) CompactSlab(id uint32) error {
	return c.CompactSlabWithOptions(id, Options{})
}

// CompactSlabWithOptions compacts a slab using the provided options.
func (c *Compactor) CompactSlabWithOptions(id uint32, opts Options) error {
	return c.CompactSlabWithContext(context.Background(), id, opts)
}

// CompactSlabWithContext compacts a slab using the provided options and context.
// If ctx is canceled, compaction aborts promptly and returns ctx.Err().
func (c *Compactor) CompactSlabWithContext(ctx context.Context, id uint32, opts Options) error {
	// Never compact the active slab: new writes could create new live pointers
	// into it while we're scanning.
	if c.db.SlabManager().ActiveSlabID() == id {
		return errors.New("compaction: cannot compact active slab")
	}

	if opts.IndexSwap {
		return c.db.CompactSlabsIndexSwap(ctx, []uint32{id}, db.IndexSwapCompactionOptions{
			CopyBytesPerSec: opts.CopyBytesPerSec,
			CopyBurstBytes:  opts.CopyBurstBytes,
			Assist:          opts.Assist,
		})
	}

	liveSnap := c.db.AcquireSnapshot()
	liveSetMax := opts.LiveSetMaxEntries
	if liveSetMax == 0 {
		liveSetMax = defaultLiveSetMaxEntries
	}
	liveSet, err := c.buildLiveSet(ctx, liveSnap, id, liveSetMax, opts.Stats)
	_ = liveSnap.Close()
	if err != nil {
		return err
	}

	var lookupSnap *db.Snapshot
	closeLookup := func() {
		if lookupSnap != nil {
			_ = lookupSnap.Close()
			lookupSnap = nil
		}
	}
	defer closeLookup()

	assist := opts.Assist
	lastAssist := time.Now()
	bytesSinceAssist := int64(0)
	maybeAssist := func(force bool) {
		if assist == nil {
			return
		}
		// Avoid calling too frequently in tight loops; also force at useful phase
		// boundaries (e.g. before/after apply).
		if !force {
			const assistEveryBytes = 4 << 20
			const assistEveryDur = 250 * time.Millisecond
			if bytesSinceAssist < assistEveryBytes && time.Since(lastAssist) < assistEveryDur {
				return
			}
		}
		assist()
		lastAssist = time.Now()
		bytesSinceAssist = 0
	}

	path := c.db.SlabManager().GetSlabPath(id)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Determine source size once. If the file grows concurrently (it shouldn't,
	// since we don't compact the active slab), we ignore the tail.
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	microBatch := opts.MicroBatchSize
	if microBatch <= 0 {
		microBatch = 256
	}

	lim := newLimiter(opts.CopyBytesPerSec, opts.CopyBurstBytes)

	var ops []db.CompactionOp
	offset := int64(0)

	const readerSize = 256 << 10
	section := io.NewSectionReader(f, 0, size)
	r := bufio.NewReaderSize(section, readerSize)

	var headerBuf [slab.HeaderSize]byte
	var dataBuf []byte

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if offset >= size {
			break
		}
		if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		// Parse Header: CRC(4) | KeyLen(2) | ValLen(4)
		keyLen := binary.LittleEndian.Uint16(headerBuf[4:6])
		valLen := binary.LittleEndian.Uint32(headerBuf[6:10])
		totalLen := int64(slab.HeaderSize) + int64(keyLen) + int64(valLen)
		if totalLen <= 0 || offset+totalLen > size {
			// Partial tail record; stop (common crash case).
			break
		}

		// Read Key and Value
		// We need them to check liveness and re-append
		recordBytes := int(keyLen) + int(valLen)
		if cap(dataBuf) < recordBytes {
			dataBuf = make([]byte, recordBytes)
		} else {
			dataBuf = dataBuf[:recordBytes]
		}
		if _, err := io.ReadFull(r, dataBuf); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		key := dataBuf[:keyLen]
		value := dataBuf[keyLen:]

		// Construct OldPtr
		// Matches SlabManager.Append semantics:
		// Offset points to KeyLen (skipping 4-byte CRC).
		// Length excludes CRC (2 + 4 + k + v).
		oldPtr := page.ValuePtr{
			Offset: uint64(offset + 4),
			Length: uint32(totalLen - 4),
			FileID: id,
		}

		// Check liveness (optimistic):
		// - If a live set is present, use it to skip per-record tree lookups.
		// - Otherwise, verify against the snapshot. Since we never compact the
		//   active slab, no new live pointers into this slab can be created after
		//   the snapshot. ApplyCompaction will verify again against latest state
		//   to ensure safety.
		isLiveViaLookup := func() bool {
			if opts.Stats != nil {
				opts.Stats.TreeLookups++
			}
			if lookupSnap == nil {
				lookupSnap = c.db.AcquireSnapshot()
			}
			entry, err := lookupSnap.GetEntry(key)
			if err != nil {
				return false
			}
			return entry.Flags&node.FlagPointer != 0 && entry.ValuePtr == oldPtr
		}

		if liveSet != nil && liveSet.exact != nil {
			if _, ok := liveSet.exact[oldPtr]; !ok {
				if opts.Stats != nil {
					opts.Stats.TreeLookupsSkipped++
				}
				offset += totalLen
				continue
			}
			if opts.Stats != nil {
				opts.Stats.TreeLookupsSkipped++
			}
		} else if liveSet != nil && liveSet.bloom != nil {
			if !liveSet.bloom.mayContain(oldPtr) {
				if opts.Stats != nil {
					opts.Stats.TreeLookupsSkipped++
				}
				offset += totalLen
				continue
			}
			if !isLiveViaLookup() {
				offset += totalLen
				continue
			}
		} else {
			if !isLiveViaLookup() {
				offset += totalLen
				continue
			}
		}

		if err := lim.Wait(ctx, int(totalLen)); err != nil {
			return err
		}
		bytesSinceAssist += totalLen
		maybeAssist(false)

		// Append to the current active slab.
		newPtr, err := c.db.SlabManager().Append(key, value)
		if err != nil {
			return err
		}
		// Sanity: the record sizes should match (same key/value).
		_ = totalLen

		keyCopy := append([]byte(nil), key...)
		ops = append(ops, db.CompactionOp{
			Key:    keyCopy,
			OldPtr: oldPtr,
			NewPtr: newPtr,
		})

		// Apply periodically to bound memory and writer pauses.
		if len(ops) >= microBatch {
			maybeAssist(true)
			closeLookup()
			if err := c.db.ApplyCompactionMicroBatches(ops, microBatch); err != nil {
				return err
			}
			ops = ops[:0]
			maybeAssist(true)
		}

		offset += totalLen
	}

	if len(ops) > 0 {
		maybeAssist(true)
		closeLookup()
		if err := c.db.ApplyCompactionMicroBatches(ops, microBatch); err != nil {
			return err
		}
		maybeAssist(true)
	}

	// On Windows, a file cannot be removed while it is open by any process,
	// including the compactor itself. MarkZombie+RefreshSlabSet can delete the
	// compacted slab once snapshots release it, so close our reader before that.
	//
	// (On Unix, unlinking an open file is allowed, but we want consistent
	// behavior across platforms.)
	_ = f.Close()
	f = nil

	// Now that pointers have been moved, remove the old slab from future
	// snapshots. It will be deleted once no snapshots reference it.
	if err := c.db.SlabManager().MarkZombie(id); err != nil {
		return err
	}
	return c.db.RefreshSlabSet()
}

type liveSet struct {
	exact map[page.ValuePtr]struct{}
	bloom *bloomFilter
}

func (c *Compactor) buildLiveSet(ctx context.Context, snap *db.Snapshot, id uint32, maxEntries int, stats *Stats) (*liveSet, error) {
	if maxEntries < 0 {
		return nil, nil
	}
	state := snap.State()
	if state == nil {
		return nil, errors.New("compaction: missing db state")
	}

	tr := tree.New(snap.Pager(), db.ValueReaderForState(state), state.RootPageID)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	if maxEntries == 0 {
		maxEntries = defaultLiveSetMaxEntries
	}
	initialCap := 1024
	if maxEntries > 0 && maxEntries < initialCap {
		initialCap = maxEntries
	}
	ls := &liveSet{
		exact: make(map[page.ValuePtr]struct{}, initialCap),
	}
	var liveCount uint64

	for it.Valid() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer != 0 && ptr.FileID == id {
			liveCount++
			if ls.bloom != nil {
				ls.bloom.add(ptr)
				continue
			}
			ls.exact[ptr] = struct{}{}
			if maxEntries > 0 && len(ls.exact) > maxEntries {
				ls.bloom = newBloomFilter(maxEntries)
				for p := range ls.exact {
					ls.bloom.add(p)
				}
				ls.exact = nil
				if stats != nil {
					stats.LiveSetBloom = true
				}
			}
		}
		it.Next()
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	if stats != nil {
		stats.LiveSetEntries = liveCount
	}
	return ls, nil
}
