package gomap

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"
)

func (h *Hashmap) checkResize() bool {
	if h.rehashInProgress {
		return false
	}
	threshold := h.resizeThreshold
	if threshold == 0 {
		threshold = 65
	}
	return *h.Count*100 > h.Capacity*threshold
}

const rehashBucketsPerWrite = 8

// startRehash initializes an incremental rehash by allocating a new index
// with double capacity and switching the active table to it. The old table
// is kept around and migrated gradually.
func (h *Hashmap) startRehash() error {
	if h.rehashInProgress {
		return nil
	}

	newCap := h.Capacity * 2
	if newCap == 0 {
		newCap = DEFAULTMAPSIZE
	}

	// Save current index as "old".
	oldMap := h.hashMap
	oldFile := h.hashMapFile
	oldCap := h.Capacity
	var oldKeys []Key
	if h.Keys != nil {
		oldKeys = *h.Keys
	}

	// Allocate new index file/mmap.
	newMap, newFile, err := h.openMmapHash(newCap)
	if err != nil {
		return err
	}

	// Persist new capacity for future opens. This mirrors the old resize behavior,
	// which wrote capacity at the moment of resizing.
	if err := h.writeCapacity(newCap); err != nil {
		newMap.Unmap()
		newFile.Close()
		return err
	}

	// Switch active index to the new table.
	h.hashMap = newMap
	h.hashMapFile = newFile
	h.Capacity = newCap
	keys := h.getKeys()
	h.Keys = &keys

	// Record old index for incremental migration.
	h.rehashOldMap = oldMap
	h.rehashOldMapFile = oldFile
	h.rehashOldCapacity = oldCap
	if oldKeys == nil && len(oldMap) > 0 {
		// Fallback: build slice view over old mmap if Keys wasn't initialized.
		tmp := (*Key)(unsafe.Pointer(&oldMap[0]))
		oldKeys = unsafe.Slice(tmp, oldCap)
	}
	h.rehashOldKeys = oldKeys
	h.rehashIdx = 0
	h.rehashInProgress = true

	return nil
}

// rehashStep migrates up to maxToMove buckets from the old table into the new one.
// It should be called while holding the shard's write lock.
func (h *Hashmap) rehashStep(maxToMove uint64) error {
	if !h.rehashInProgress || h.rehashOldCapacity == 0 {
		return nil
	}
	if maxToMove == 0 {
		return nil
	}

	moved := uint64(0)
	for h.rehashIdx < h.rehashOldCapacity && moved < maxToMove {
		idx := h.rehashIdx
		h.rehashIdx++

		k := h.rehashOldKeys[idx]
		if k.slabOffset == 0 || k.slabOffset == Tombstone {
			continue
		}

		// Read key bytes from slab to locate its slot in the new table.
		item, err := h.unmarshalItemFromSlab(k)
		if err != nil {
			return err
		}

		hkey, isNew, err := h.probeForAdd(item.Key)
		if err != nil {
			return err
		}

		if isNew {
			(*h.Keys)[hkey] = k
			// Do not modify Count; logical key cardinality doesn't change.
		}

		// Mark old bucket as migrated so we don't revisit it.
		h.rehashOldKeys[idx].slabOffset = 0
		moved++
	}

	if h.rehashIdx >= h.rehashOldCapacity {
		h.finishRehash()
	}

	return nil
}

// finishRehash releases resources for the old table once migration is complete.
func (h *Hashmap) finishRehash() {
	if !h.rehashInProgress {
		return
	}

	// Close and unmap old index.
	if h.rehashOldMapFile != nil {
		h.rehashOldMapFile.Close()
	}
	if h.rehashOldMap != nil {
		h.rehashOldMap.Unmap()
	}

	// Best-effort cleanup of old index files on disk.
	// After a successful rehash there should be exactly one hashkeys-* file,
	// corresponding to the current Capacity.
	files, err := os.ReadDir(h.Folder)
	if err == nil {
		current := fmt.Sprintf("hashkeys-%d", h.Capacity)
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "hashkeys-") && f.Name() != current {
				_ = os.Remove(h.Folder + "/" + f.Name())
			}
		}
	}

	h.rehashOldMapFile = nil
	h.rehashOldMap = nil
	h.rehashOldKeys = nil
	h.rehashOldCapacity = 0
	h.rehashIdx = 0
	h.rehashInProgress = false
}

// resize performs a full, immediate resize for callers that explicitly invoke it
// (e.g., tests). Normal writes use incremental rehash via startRehash+rehashStep.
func (h *Hashmap) resize() {
	startTime := time.Now()
	if !h.rehashInProgress {
		if err := h.startRehash(); err != nil {
			panic(err)
		}
	}
	// Migrate everything in a tight loop.
	for h.rehashInProgress {
		if err := h.rehashStep(^uint64(0)); err != nil {
			panic(err)
		}
	}
	resizeTime := getRunTime(startTime)
	h.resizeTime += resizeTime
}
