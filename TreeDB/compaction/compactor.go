package compaction

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

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
	// Never compact the active slab: new writes could create new live pointers
	// into it while we're scanning.
	if c.db.SlabManager().ActiveSlabID() == id {
		return errors.New("compaction: cannot compact active slab")
	}

	snap := c.db.AcquireSnapshot()
	defer snap.Close()

	path := c.db.SlabManager().GetSlabPath(id)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Rotate to a fresh active slab so compaction IO doesn't interleave with
	// existing slab contents and (critically) so the new slab ID is persisted in
	// meta by subsequent commits.
	if _, err := c.db.SlabManager().Rotate(); err != nil {
		return err
	}

	// Determine source size once. If the file grows concurrently (it shouldn't,
	// since we don't compact the active slab), we ignore the tail.
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	var ops []db.CompactionOp
	offset := int64(0)
	// Buffer for header
	var headerBuf [slab.HeaderSize]byte

	for {
		// Read Header
		if offset >= size {
			break
		}
		if _, err := f.ReadAt(headerBuf[:], offset); err != nil {
			if err == io.EOF {
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
		dataBuf := make([]byte, int(keyLen)+int(valLen))
		if _, err := f.ReadAt(dataBuf, offset+int64(slab.HeaderSize)); err != nil {
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

		// Append to the current active slab (newly rotated at start).
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
		if len(ops) >= 256 {
			if err := c.db.ApplyCompactionMicroBatches(ops, 256); err != nil {
				return err
			}
			ops = ops[:0]
		}

		offset += totalLen
	}

	if len(ops) > 0 {
		if err := c.db.ApplyCompactionMicroBatches(ops, 256); err != nil {
			return err
		}
	}

	// Now that pointers have been moved, remove the old slab from future
	// snapshots. It will be deleted once no snapshots reference it.
	return c.db.SlabManager().MarkZombie(id)
}
