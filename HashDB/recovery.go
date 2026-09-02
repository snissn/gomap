package hashdb

import (
	"bufio"
	"encoding/binary"
	"errors"
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
	h.rehashOldControlFile = nil
	h.rehashOldControlMap = nil
	h.rehashOldKeyFile = nil
	h.rehashOldKeyMap = nil
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
	f, err := os.OpenFile(filename, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	reader := bufio.NewReaderSize(f, 128*1024)

	var offset = baseOffset
	var pos int64
	type pendingOp struct {
		typ     BatchOpType
		key     []byte
		slabOff SlabOffset
		hash    Hash
	}
	inBatch := false
	var batchID uint64
	var batchBeginPos int64
	var pending []pendingOp

	// If it's slab-0 or slab-real, handle sentinel?
	// Or check sentinel on every file?
	// initN only writes sentinel if *h.slabOffset == 0.
	// Only slab-0 starts at 0.
	// So check sentinel only if baseOffset == 0.

	if baseOffset == 0 {
		prefix := make([]byte, 6)
		n, err := io.ReadFull(reader, prefix)
		if err == nil && string(prefix) == "offset" {
			offset += 6
			pos += int64(n)
		} else {
			_, _ = f.Seek(0, 0)
			reader.Reset(f)
			offset = baseOffset
			pos = 0
		}
	}

	header := make([]byte, 16)

scan:
	for {
		recordPos := pos

		_, err := io.ReadFull(reader, header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Treat a torn tail record as clean truncation and drop it.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				truncPos := recordPos
				if inBatch {
					truncPos = batchBeginPos
				}
				if err := f.Truncate(truncPos); err != nil {
					return err
				}
				break
			}
			return err
		}
		pos += 16

		keyLen := binary.LittleEndian.Uint64(header[:8])
		valLenPacked := binary.LittleEndian.Uint64(header[8:])

		// Control record (used for batch atomicity markers).
		if keyLen == slabKeyLenControl {
			valLen, flags := unpackLength(valLenPacked)
			totalLen := uint64(16) + valLen
			remaining := uint64(fi.Size() - recordPos)

			if flags&FlagControl == 0 || totalLen > remaining || valLen < 9 {
				truncPos := recordPos
				if inBatch {
					truncPos = batchBeginPos
				}
				if err := f.Truncate(truncPos); err != nil {
					return err
				}
				break
			}

			payload := make([]byte, int(valLen))
			if _, err := io.ReadFull(reader, payload); err != nil {
				truncPos := recordPos
				if inBatch {
					truncPos = batchBeginPos
				}
				if err2 := f.Truncate(truncPos); err2 != nil {
					return err2
				}
				break
			}
			pos += int64(valLen)

			typ := payload[0]
			id := binary.LittleEndian.Uint64(payload[1:9])

			switch typ {
			case controlBatchBegin:
				// Nested begin: treat the previous batch as incomplete and drop it.
				if inBatch {
					if err := f.Truncate(batchBeginPos); err != nil {
						return err
					}
					inBatch = false
					break scan
				}
				inBatch = true
				batchID = id
				batchBeginPos = recordPos
				pending = pending[:0]
			case controlBatchCommit:
				if !inBatch {
					break // commit without begin: ignore
				}
				if id != batchID {
					if err := f.Truncate(batchBeginPos); err != nil {
						return err
					}
					inBatch = false
					break scan
				}
				for _, p := range pending {
					switch p.typ {
					case BatchOpPut:
						if err := h.addBucket(p.key, Key{slabOffset: p.slabOff, hash: p.hash}); err != nil {
							return err
						}
					case BatchOpDelete:
						h.replayDelete(p.key)
					default:
						return fmt.Errorf("recover %s: unknown pending op type %d", filename, p.typ)
					}
				}
				inBatch = false
				batchID = 0
				pending = pending[:0]
			default:
				truncPos := recordPos
				if inBatch {
					truncPos = batchBeginPos
				}
				if err := f.Truncate(truncPos); err != nil {
					return err
				}
				break scan
			}

			offset += SlabOffset(totalLen)
			continue
		}

		isDelete := valLenPacked == ^uint64(0)
		var valLen uint64
		if !isDelete {
			valLen, _ = unpackLength(valLenPacked)
		}

		totalLen := uint64(16) + keyLen + valLen
		remaining := uint64(fi.Size() - recordPos)
		if totalLen > remaining {
			// The record claims bytes beyond the end of the file.
			// We can't find the next record boundary, so truncate the tail.
			truncPos := recordPos
			if inBatch {
				truncPos = batchBeginPos
			}
			if err := f.Truncate(truncPos); err != nil {
				return err
			}
			break
		}

		if keyLen > uint64(int(^uint(0)>>1)) {
			return fmt.Errorf("recover %s: key length too large: %d", filename, keyLen)
		}
		key := make([]byte, int(keyLen))
		if _, err := io.ReadFull(reader, key); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				truncPos := recordPos
				if inBatch {
					truncPos = batchBeginPos
				}
				if err2 := f.Truncate(truncPos); err2 != nil {
					return err2
				}
				break
			}
			return err
		}
		pos += int64(keyLen)

		if isDelete {
			if inBatch {
				pending = append(pending, pendingOp{typ: BatchOpDelete, key: key})
			} else {
				h.replayDelete(key)
			}
		} else {
			if valLen > uint64(int64(^uint64(0)>>1)) {
				return fmt.Errorf("recover %s: value length too large: %d", filename, valLen)
			}
			if _, err := io.CopyN(io.Discard, reader, int64(valLen)); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					truncPos := recordPos
					if inBatch {
						truncPos = batchBeginPos
					}
					if err2 := f.Truncate(truncPos); err2 != nil {
						return err2
					}
					break
				}
				return err
			}
			pos += int64(valLen)

			if inBatch {
				pending = append(pending, pendingOp{typ: BatchOpPut, key: key, slabOff: offset, hash: hash(key)})
			} else {
				if err := h.addBucket(key, Key{slabOffset: offset, hash: hash(key)}); err != nil {
					return err
				}
			}
		}

		offset += SlabOffset(totalLen)
	}

	// If the file ends mid-batch (no commit record), drop the entire batch by truncating
	// back to the begin marker.
	if inBatch {
		if err := f.Truncate(batchBeginPos); err != nil {
			return err
		}
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
