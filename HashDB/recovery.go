package hashdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// Recover rebuilds the hash index from the slab file (WAL).
// It iterates through the entire slab-real file and replays operations.
func (h *DB) Recover() error {
	// If an incremental rehash was in progress, discard any old table state.
	h.rehashInProgress = false
	h.rehashOldMapFile = nil
	h.rehashOldMap = nil
	h.rehashOldKeys = nil
	h.rehashOldControls = nil
	h.rehashOldCapacity = 0
	h.rehashIdx = 0

	// Reset Index map
	for i := range h.keys {
		h.keys[i] = Key{}
	}
	for i := range h.controls {
		h.controls[i] = ctrlEmpty
	}
	*h.count = 0

	// Scan for segments
	files, err := os.ReadDir(h.dir)
	if err != nil {
		return err
	}

	maxID := -1
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "slab-") && !strings.HasSuffix(file.Name(), "-real") {
			var id int
			if _, err := fmt.Sscanf(file.Name(), "slab-%d", &id); err == nil {
				if id > maxID {
					maxID = id
				}
			}
		}
	}

	if maxID == -1 {
		// Try slab-real legacy
		if _, err := os.Stat(h.dir + "/slab-real"); err == nil {
			return h.recoverFile(h.dir+"/slab-real", 0)
		}
		return nil
	}

	for id := 0; id <= maxID; id++ {
		filename := fmt.Sprintf("%s/slab-%d", h.dir, id)
		// Offset base for this segment
		baseOffset := SlabOffset(uint64(id) << OffsetBits)

		if err := h.recoverFile(filename, baseOffset); err != nil {
			if os.IsNotExist(err) {
				continue // Gap in sequence? Should not happen but ignore
			}
			return err
		}
	}

	// Update slabOffset to point to end of last segment?
	// Or writeSlab will handle it?
	// writeSlab uses activeSegmentId.
	// We need to set activeSegmentId to maxID.
	// And *h.slabOffset to end of maxID file.

	h.activeSegmentID = uint16(maxID)

	lastFile := fmt.Sprintf("%s/slab-%d", h.dir, maxID)
	fi, err := os.Stat(lastFile)
	if err != nil {
		return err
	}

	h.activeSegmentSize = fi.Size()
	*h.slabOffset = SlabOffset((uint64(maxID) << OffsetBits) | uint64(fi.Size()))

	return nil
}

func (h *DB) recoverFile(filename string, baseOffset SlabOffset) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	var offset = baseOffset

	// If it's slab-0 or slab-real, handle sentinel?
	// Or check sentinel on every file?
	// initN only writes sentinel if *h.slabOffset == 0.
	// Only slab-0 starts at 0.
	// So check sentinel only if baseOffset == 0.

	if baseOffset == 0 {
		prefix := make([]byte, 6)
		_, err = io.ReadFull(reader, prefix)
		if err == nil && string(prefix) == "offset" {
			offset += 6
		} else {
			f.Seek(0, 0)
			reader.Reset(f)
			offset = baseOffset
		}
	}

	header := make([]byte, 16)

	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		keyLen := binary.LittleEndian.Uint64(header[:8])
		valLen := binary.LittleEndian.Uint64(header[8:])

		totalLen := 16 + keyLen
		if valLen != ^uint64(0) {
			totalLen += valLen
		}

		key := make([]byte, keyLen)
		_, err = io.ReadFull(reader, key)
		if err != nil {
			return err
		}

		if valLen == ^uint64(0) {
			h.replayDelete(key)
		} else {
			discarded, err := reader.Discard(int(valLen))
			if err != nil {
				return err
			}
			if discarded != int(valLen) {
				return io.ErrUnexpectedEOF
			}

			h.addBucket(key, Key{slabOffset: offset, hash: hash(key)})
		}

		offset += SlabOffset(totalLen)
	}
	return nil
}

func (h *DB) replayDelete(key []byte) {
	// Internal delete for recovery (doesn't write to slab)
	if len(h.keys) > 0 && h.capacity > 0 {
		idx, _, found, _ := h.probe(h.keys, h.controls, h.capacity, key)
		if found {
			h.keys[idx].slabOffset = Tombstone
			h.setDeleted(idx)
			*h.count -= 1
		}
	}
}
