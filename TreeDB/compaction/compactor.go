package compaction

import (
	"encoding/binary"
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
// It moves live records to the active slab and updates the index.
func (c *Compactor) CompactSlab(id uint32) error {
	snap := c.db.AcquireSnapshot()
	defer snap.Close()

	path := c.db.SlabManager().GetSlabPath(id)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var ops []db.CompactionOp
	offset := int64(0)

	// Buffer for header
	headerBuf := make([]byte, slab.HeaderSize)

	for {
		// Read Header
		if _, err := f.ReadAt(headerBuf, offset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Parse Header
		// CRC(4) | KeyLen(2) | ValLen(4)
		keyLen := binary.LittleEndian.Uint16(headerBuf[4:6])
		valLen := binary.LittleEndian.Uint32(headerBuf[6:10])
		totalLen := int64(slab.HeaderSize) + int64(keyLen) + int64(valLen)

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
		// We verify against the snapshot. If it's dead in snapshot, it's definitely dead.
		// If it's live in snapshot, we assume live and move it.
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

		// Append to Active Slab
		newPtr, err := c.db.SlabManager().Append(key, value)
		if err != nil {
			return err
		}

		ops = append(ops, db.CompactionOp{
			Key:    key,
			OldPtr: oldPtr,
			NewPtr: newPtr,
		})

		offset += totalLen
	}

	// Apply Atomic Update
	return c.db.ApplyCompaction(ops)
}
