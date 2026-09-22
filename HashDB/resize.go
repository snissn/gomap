package hashdb

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"
)

func (h *DB) checkResize() bool {
	if h.rehashInProgress {
		return false
	}
	threshold := h.resizeThreshold
	if threshold == 0 {
		threshold = 65
	}
	return *h.count*100 > h.capacity*threshold
}

const rehashBucketsPerWrite = 8

// startRehash initializes an incremental rehash by allocating a new index
// with double capacity and switching the active table to it. The old table
// is kept around and migrated gradually.
func (h *DB) startRehash() (err error) {
	if h.rehashInProgress {
		return nil
	}

	policy := h.indexMemoryPolicyOrDefault()

	newCap := h.capacity * 2
	if newCap == 0 {
		newCap = DefaultCapacity
	}

	// Save current index as "old".
	oldControlMap := h.controlMap
	oldControlFile := h.controlFile
	oldKeyMap := h.keyMap
	oldKeyFile := h.keyFile
	oldCap := h.capacity
	oldKeys := h.keys
	oldControls := h.controls
	oldControlsLocked := h.controlsLocked

	// Allocate new index file/mmap.
	newControlMap, newControlFile, newKeyMap, newKeyFile, err := h.openIndexMaps(newCap)
	if err != nil {
		return err
	}

	newControlsLocked := false
	oldControlsUnlocked := false
	swapped := false
	defer func() {
		if err == nil || swapped {
			return
		}

		if newControlsLocked {
			_ = unlockBytes([]byte(newControlMap))
		}

		_ = newControlMap.Unmap()
		_ = newControlFile.Close()
		_ = newKeyMap.Unmap()
		_ = newKeyFile.Close()

		if oldControlsUnlocked && oldControlsLocked {
			if lockErr := lockBytes([]byte(oldControlMap)); lockErr == nil {
				h.controlsLocked = true
			}
		}
	}()

	// ---------------------------------------------------------
	// 1. The Swiss Hash (Control Bytes) -> "Really Mean It"
	// ---------------------------------------------------------
	if policy.LockControls {
		lockErr := lockBytes([]byte(newControlMap))
		if lockErr != nil && oldControlsLocked {
			// Free memlock budget and retry.
			if unlockErr := unlockBytes([]byte(oldControlMap)); unlockErr == nil {
				h.controlsLocked = false
				oldControlsUnlocked = true
				lockErr = lockBytes([]byte(newControlMap))
			}
		}

		if lockErr != nil {
			if policy.LockControlsStrict {
				return fmt.Errorf("failed to hard-pin control bytes (check memlock/ulimit -l): %w", lockErr)
			}
		} else {
			newControlsLocked = true
			if oldControlsLocked && !oldControlsUnlocked {
				if unlockErr := unlockBytes([]byte(oldControlMap)); unlockErr == nil {
					h.controlsLocked = false
					oldControlsUnlocked = true
				}
			}
		}
	}

	// ---------------------------------------------------------
	// 2. The Key Array (Data) -> "Hope / Hint"
	// ---------------------------------------------------------
	if policy.AdviseKeysWillNeed {
		_ = adviseWillNeed([]byte(newKeyMap))
	}
	if policy.AdviseKeysRandom {
		_ = adviseRandom([]byte(newKeyMap))
	}

	// Persist new capacity for future opens. This mirrors the old resize behavior,
	// which wrote capacity at the moment of resizing.
	if err := h.writeCapacity(newCap); err != nil {
		return err
	}

	// Switch active index to the new table.
	h.controlMap = newControlMap
	h.controlFile = newControlFile
	h.keyMap = newKeyMap
	h.keyFile = newKeyFile
	h.capacity = newCap

	// Controls
	h.controls = []byte(newControlMap)
	h.controlsLocked = newControlsLocked

	// Keys
	keyPtr := (*Key)(unsafe.Pointer(&newKeyMap[0]))
	h.keys = unsafe.Slice(keyPtr, newCap)

	// Record old index for incremental migration.
	h.rehashOldControlMap = oldControlMap
	h.rehashOldControlFile = oldControlFile
	h.rehashOldKeyMap = oldKeyMap
	h.rehashOldKeyFile = oldKeyFile
	h.rehashOldCapacity = oldCap
	if oldKeys == nil && len(oldKeyMap) > 0 && len(oldControlMap) > 0 {
		// Fallback: build slice view over old mmaps if slices weren't initialized.
		oldControls = []byte(oldControlMap)
		tmp := (*Key)(unsafe.Pointer(&oldKeyMap[0]))
		oldKeys = unsafe.Slice(tmp, oldCap)
	}
	h.rehashOldKeys = oldKeys
	h.rehashOldControls = oldControls
	h.rehashIdx = 0
	h.rehashInProgress = true
	swapped = true

	return nil
}

// rehashStep migrates up to maxToMove buckets from the old table into the new one.
// It should be called while holding the shard's write lock.
func (h *DB) rehashStep(maxToMove uint64) error {
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

		// Optimization: We don't need to read the key from disk.
		// We know the key is unique (duplicates were invalidated in Old by Add/Delete).
		// We rely on the stored hash.
		hkey, err := h.probeForRehash(uint64(k.hash))
		if err != nil {
			return err
		}

		h.keys[hkey] = k

		// Set Control Byte
		h2 := byte(k.hash&0x7f) | 0x80
		h.controls[hkey] = h2

		// Do not modify Count; logical key cardinality doesn't change.

		// Mark old bucket as migrated so we don't revisit it.
		h.rehashOldKeys[idx].slabOffset = Tombstone
		moved++
	}

	if h.rehashIdx >= h.rehashOldCapacity {
		h.finishRehash()
	}

	return nil
}

// finishRehash releases resources for the old table once migration is complete.
func (h *DB) finishRehash() {
	if !h.rehashInProgress {
		return
	}

	// Close and unmap old index.
	if h.rehashOldControlFile != nil {
		h.rehashOldControlFile.Close()
	}
	if h.rehashOldControlMap != nil {
		h.rehashOldControlMap.Unmap()
	}
	if h.rehashOldKeyFile != nil {
		h.rehashOldKeyFile.Close()
	}
	if h.rehashOldKeyMap != nil {
		h.rehashOldKeyMap.Unmap()
	}

	// Best-effort cleanup of old index files on disk.
	// After a successful rehash there should be exactly one hashkeys-* file and
	// one hashctl-* file, corresponding to the current Capacity.
	files, err := os.ReadDir(h.dir)
	if err == nil {
		currentKeys := fmt.Sprintf("hashkeys-%d", h.capacity)
		currentCtl := fmt.Sprintf("hashctl-%d", h.capacity)
		for _, f := range files {
			if strings.HasPrefix(f.Name(), "hashkeys-") && f.Name() != currentKeys {
				_ = os.Remove(h.dir + "/" + f.Name())
			}
			if strings.HasPrefix(f.Name(), "hashctl-") && f.Name() != currentCtl {
				_ = os.Remove(h.dir + "/" + f.Name())
			}
		}
	}

	h.rehashOldControlFile = nil
	h.rehashOldControlMap = nil
	h.rehashOldKeyFile = nil
	h.rehashOldKeyMap = nil
	h.rehashOldKeys = nil
	h.rehashOldControls = nil
	h.rehashOldCapacity = 0
	h.rehashIdx = 0
	h.rehashInProgress = false
}

// resize performs a full, immediate resize for callers that explicitly invoke it
// (e.g., tests). Normal writes use incremental rehash via startRehash+rehashStep.
func (h *DB) resize() {
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
	resizeTime := time.Since(startTime)
	h.resizeTime += resizeTime
}
