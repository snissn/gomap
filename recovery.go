package gomap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// Recover rebuilds the hash index from the slab file (WAL).
// It iterates through the entire slab-real file and replays operations.
func (h *Hashmap) Recover() error {
	// Reset in-memory state (assuming h.Keys is allocated but empty or we clear it)
	// We need to clear the existing index if it exists.
	// Efficient way: zero out h.hashMap (mmap).
	// But Recover is usually called on startup.
	
	// Reset Index map
	for i := range *h.Keys {
		(*h.Keys)[i] = Key{}
	}
	*h.Count = 0
	
	// Open slab file for reading
	f, err := os.Open(h.Folder + "/slab-real")
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No data, nothing to recover
		}
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	
	var offset SlabOffset = 0
	// Handle sentinel if present?
	// initN writes sentinel "offset" (6 bytes).
	// If slab exists, we should skip sentinel?
	// But initN sets *h.slabOffset to len(sentinel).
	// So we should start reading from 0?
	// The first "entry" might be garbage if we don't skip sentinel.
	// But wait, addSlab writes sequentially.
	// If sentinel is written, it's just bytes.
	// "offset" is 6 bytes.
	// Our header is 16 bytes.
	// If we try to read header at 0, we get "offset" + junk.
	// We should probably rely on *h.slabOffset being correct? 
	// No, recovery assumes metadata might be lost/wrong.
	
	// Assumption: Slab starts with "offset" sentinel if created by initN.
	// Let's check first 6 bytes.
	prefix := make([]byte, 6)
	_, err = io.ReadFull(reader, prefix)
	if err == nil && string(prefix) == "offset" {
		offset += 6
	} else {
		// Not sentinel? Or file too short.
		// If too short, maybe empty?
		f.Seek(0, 0)
		reader.Reset(f)
		offset = 0
	}

	header := make([]byte, 16)
	
	for {
		_, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err // Unexpected error
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
			// Delete operation
			// We replay delete on the index
			h.replayDelete(key)
		} else {
			// Add operation
			// Skip value bytes in reader
			discarded, err := reader.Discard(int(valLen))
			if err != nil {
				return err
			}
			if discarded != int(valLen) {
				return io.ErrUnexpectedEOF
			}
			
			// Replay add
			// We use the OFFSET where this entry started.
			// Current entry started at 'offset'.
			// But we need to verify if this entry is valid?
			// It is valid if we read it successfully.
			
			h.addBucket(key, Key{slabOffset: offset, hash: hash(key)})
		}
		
		offset += SlabOffset(totalLen)
	}
	
	// Update slabOffset in metadata to match where we ended
	*h.slabOffset = offset
	
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
