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
)

type Compactor struct {
	db *db.DB
}

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

	snap := c.db.AcquireSnapshot()
	defer snap.Close()

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
	defer f.Close()

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

		// Check Liveness (Optimistic)
		// We verify against the snapshot. Since we never compact the active slab,
		// no new live pointers into this slab can be created after the snapshot.
		// ApplyCompaction will verify again against latest state to ensure safety.
		entry, err := snap.GetEntry(key)
		if err != nil {
			// Not found or error -> Assume dead
			offset += totalLen
			continue
		}

		// Must be a pointer and match exactly
		if entry.Flags&node.FlagPointer == 0 || entry.ValuePtr != oldPtr {
			offset += totalLen
			continue
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
		if err := c.db.ApplyCompactionMicroBatches(ops, microBatch); err != nil {
			return err
		}
		maybeAssist(true)
	}

	// Now that pointers have been moved, remove the old slab from future
	// snapshots. It will be deleted once no snapshots reference it.
	if err := c.db.SlabManager().MarkZombie(id); err != nil {
		return err
	}
	return c.db.RefreshSlabSet()
}
