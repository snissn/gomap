package gomap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// Recover rebuilds the hash index from the slab file (WAL).
// It iterates through the entire slab-real file and replays operations.
func (h *Hashmap) Recover() error {
	// If an incremental rehash was in progress, discard any old table state.
	h.rehashInProgress = false
	h.rehashOldMapFile = nil
	h.rehashOldMap = nil
	h.rehashOldKeys = nil
	h.rehashOldCapacity = 0
	h.rehashIdx = 0

	// Reset Index map
	for i := range *h.Keys {
		(*h.Keys)[i] = Key{}
	}
	*h.Count = 0
	
	// Scan for segments
	files, err := os.ReadDir(h.Folder)
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
		if _, err := os.Stat(h.Folder + "/slab-real"); err == nil {
			return h.recoverFile(h.Folder+"/slab-real", 0)
		}
		return nil
	}
	
	for id := 0; id <= maxID; id++ {
		filename := fmt.Sprintf("%s/slab-%d", h.Folder, id)
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
	
	h.activeSegmentId = uint16(maxID)
	
	lastFile := fmt.Sprintf("%s/slab-%d", h.Folder, maxID)
	fi, err := os.Stat(lastFile)
	if err != nil {
		return err
	}
	
	h.activeSegmentSize = fi.Size()
	*h.slabOffset = SlabOffset((uint64(maxID) << OffsetBits) | uint64(fi.Size()))
	
	return nil
}

func (h *Hashmap) recoverFile(filename string, baseOffset SlabOffset) error {
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

func (h *Hashmap) replayDelete(key []byte) {
	// Internal delete for recovery (doesn't write to slab)
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity
		mybucket := (*h.Keys)[myKeyIndex]
		
		if mybucket.slabOffset == 0 {
			return // Not found
		}
		if mybucket.slabOffset == Tombstone {
			count++
			continue
		}
		if mybucket.hash == myhash {
			// We MUST read key to confirm identity?
			// Yes, otherwise we delete wrong key on collision!
			// But we are in recovery. The slab-real IS the source of truth.
			// We have the key from the log!
			// But to find the slot, we probe.
			// Probing requires comparing key in index with key in hand.
			// Index points to OLD slab entry.
			item, err := h.unmarshalItemFromSlab(mybucket)
			if err != nil {
				// If unmarshal fails during recovery? 
				// Maybe old data is corrupt?
				// Ignore?
				return
			}
			if bytes.Equal(item.Key, key) {
				(*h.Keys)[myKeyIndex].slabOffset = Tombstone
				*h.Count -= 1
				return
			}
		}
		count++
	}
}
