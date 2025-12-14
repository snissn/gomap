package hashdb

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"unsafe"

	"github.com/edsrzf/mmap-go"
)

func (h *DB) closeFPs() error {
	var firstErr error
	recordErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	// Unmap before closing files (important for Windows compatibility).
	if h.hashMap != nil {
		recordErr(h.hashMap.Unmap())
		h.hashMap = nil
	}
	if h.hashMapFile != nil {
		recordErr(h.hashMapFile.Close())
		h.hashMapFile = nil
	}

	if h.metadataMap != nil {
		recordErr(h.metadataMap.Unmap())
		h.metadataMap = nil
	}
	if h.metadataFile != nil {
		recordErr(h.metadataFile.Close())
		h.metadataFile = nil
	}

	// Close and unmap the old index if an incremental rehash was in progress.
	if h.rehashOldMap != nil {
		recordErr(h.rehashOldMap.Unmap())
		h.rehashOldMap = nil
	}
	if h.rehashOldMapFile != nil {
		recordErr(h.rehashOldMapFile.Close())
		h.rehashOldMapFile = nil
	}
	h.rehashOldKeys = nil
	h.rehashOldControls = nil
	h.rehashOldCapacity = 0
	h.rehashIdx = 0
	h.rehashInProgress = false

	for _, f := range h.slabFiles {
		recordErr(f.Close())
	}
	h.slabFiles = nil

	return firstErr
}

// Close releases mmap and file descriptors held by the DB.
func (h *DB) Close() error {
	return h.closeFPs()
}

// Get retrieves the value for a given key.
// It returns nil, nil if the key is not found.
func (h *DB) Get(key []byte) ([]byte, error) {
	// Probe current (new) table first.
	if len(h.keys) > 0 && h.capacity > 0 {
		_, item, found, err := h.probe(h.keys, h.controls, h.capacity, key)
		if err != nil {
			return nil, err
		}
		if found {
			return item.Value, nil
		}
	}

	// If an incremental rehash is in progress, also probe the old table
	// for keys that haven't been migrated yet.
	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		_, item, found, err := h.probe(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key)
		if err != nil {
			return nil, err
		}
		if found {
			return item.Value, nil
		}
	}

	return nil, nil
}

// Delete removes a key from the map.
func (h *DB) Delete(key []byte) error {
	foundNew := false

	// Delete in the current (new) table.
	if len(h.keys) > 0 && h.capacity > 0 {
		idx, _, found, err := h.probe(h.keys, h.controls, h.capacity, key)
		if err != nil {
			return err
		}
		if found {
			// Found it in new table.
			if err := h.addDeleteSlab(key); err != nil {
				return err
			}
			h.keys[idx].slabOffset = Tombstone
			h.setDeleted(idx)
			*h.count -= 1
			foundNew = true
		}
	}

	// If rehash in progress, also tombstone any copy in the old table so it
	// doesn't get resurrected during migration. Do not adjust Count again.
	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		idx, _, found, err := h.probe(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key)
		if err != nil {
			return err
		}
		if found {
			h.rehashOldKeys[idx].slabOffset = Tombstone
		}
	}

	if !foundNew {
		// Key not present; no delete slab written.
		return nil
	}
	return nil
}

// Update performs an atomic read-modify-write operation on a key.
// The callback receives the current value (or nil if not found) and returns the new value.
func (h *DB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	// Simple implementation: Get then Add.
	// Since Hashmap is NOT thread-safe, the caller (HashmapDistributed) must hold the lock.
	// So we can just reuse Get logic (or duplicate probing for efficiency) then Add.
	// But Add appends new slab.
	// So we can just call Get, run callback, then Add.
	// The atomicity is provided by the caller holding the lock.

	val, err := h.Get(key)
	if err != nil {
		return err
	}

	newVal, err := callback(val)
	if err != nil {
		return err
	}

	return h.Add(key, newVal)
}

// AddMany inserts multiple items in a batch.
// It is not thread-safe.
func (h *DB) AddMany(items []Item) error {

	startTime := time.Now()
	slabOffsets, err := h.addManySlabs(items)
	if err != nil {
		return err
	}
	slabTime := time.Since(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	for i, item := range items {
		if err := h.addBucket(item.Key, slabOffsets[i]); err != nil {
			return err
		}
	}
	if h.rehashInProgress {
		// Migrate proportional to the batch size to keep up with the growth.
		steps := uint64(len(items)) * rehashBucketsPerWrite
		if err := h.rehashStep(steps); err != nil {
			return err
		}
	}
	hashTime := time.Since(startTime)
	h.hashTime += hashTime
	return nil
}

// Add inserts a single key-value pair.
// It is not thread-safe.
func (h *DB) Add(key []byte, value []byte) error {
	item := Item{Key: key, Value: value}
	startTime := time.Now()
	slabOffset, err := h.addSlab(item)
	if err != nil {
		return err
	}
	slabTime := time.Since(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	err = h.addBucket(key, slabOffset)
	if err == nil && h.rehashInProgress {
		if err2 := h.rehashStep(rehashBucketsPerWrite); err2 != nil {
			return err2
		}
	}
	hashTime := time.Since(startTime)
	h.hashTime += hashTime
	return err
}

// mlock locks the data in memory to prevent it from being swapped to disk.
func (h *DB) mlock(data mmap.MMap) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), 0)
	if errno != 0 {
		return fmt.Errorf("mlock: %w", errno)
	}
	return nil
}

// New initializes a Hashmap in the given folder.
func (h *DB) New(folder string) error {
	h.dir = folder
	N, err := h.readCapacity()
	if err != nil {
		return err
	}
	return h.initN(folder, N)
}

// SetCompression enables or disables value compression.
// Default is true.
func (h *DB) SetCompression(enabled bool) {
	h.compressionEnabled = enabled
}

// SetResizeThreshold sets the load factor percentage at which the hashmap resizes.
// For example, 65 means resize when Count/Capacity > 0.65.
// Values <= 0 reset to the default of 65.
func (h *DB) SetResizeThreshold(percent uint64) {
	if percent == 0 {
		percent = 65
	}
	h.resizeThreshold = percent
}

// Clear wipes the database (deletes all data) and resets it.
func (h *DB) Clear() error {
	// Close resources
	if err := h.closeFPs(); err != nil {
		return err
	}

	// Delete files
	files, err := os.ReadDir(h.dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(h.dir + "/" + f.Name()); err != nil {
			return err
		}
	}

	// Re-initialize
	// initN expects folder to exist (it might have been deleted? No, we deleted contents)
	// We read capacity? No, files are gone.
	// Default capacity.
	return h.initN(h.dir, DefaultCapacity)
}

// Stats returns statistics about the database.
func (h *DB) Stats() Stats {
	// Calculate total file size
	var size uint64
	for _, f := range h.slabFiles {
		fi, _ := f.Stat()
		size += uint64(fi.Size())
	}

	return Stats{
		KeyCount: *h.count,
		Capacity: h.capacity,
		DataSize: size,
		Segments: len(h.slabFiles),
	}
}

// Compact rewrites the database to reclaim space from deleted/updated keys.
// It creates a new copy of the database and swaps it in.
func (h *DB) Compact() error {
	tmpFolder := h.dir + "-compact"
	_ = os.RemoveAll(tmpFolder) // Clean start

	var newH Hashmap
	// Use same capacity, or maybe shrink if Count << Capacity?
	// For now maintain capacity.
	// But we need to know capacity. h.Capacity.
	if err := newH.New(tmpFolder); err != nil {
		return err
	}
	// Force capacity to match? New reads capacity from file (doesn't exist) -> Default.
	// We should init with specific capacity.
	// Call initN directly on newH?
	// But New called initN(Default).
	// We can close newH and re-init? Or just use initN.
	// Better: Don't use New. Use initN.

	// Close newH first (New opened it)
	newH.closeFPs()
	_ = os.RemoveAll(tmpFolder)

	if err := newH.initN(tmpFolder, h.capacity); err != nil {
		return err
	}
	newH.SetCompression(h.compressionEnabled)

	// Migrate Data
	// Iterate through all buckets
	keys := h.getKeys()
	for _, k := range keys {
		if k.slabOffset == 0 || k.slabOffset == Tombstone {
			continue
		}

		// Read Item
		item, err := h.unmarshalItemFromSlab(k)
		if err != nil {
			// If corruption, we skip? Or fail?
			// Fail is safer.
			newH.closeFPs()
			os.RemoveAll(tmpFolder)
			return err
		}

		// Write to newH
		if err := newH.Add(item.Key, item.Value); err != nil {
			newH.closeFPs()
			os.RemoveAll(tmpFolder)
			return err
		}
	}

	// Swap
	// 1. Close both
	h.closeFPs()
	newH.closeFPs()

	// 2. Rename folders
	backupFolder := h.dir + "-old"
	_ = os.RemoveAll(backupFolder)

	if err := os.Rename(h.dir, backupFolder); err != nil {
		// Try to reopen h?
		return err
	}
	if err := os.Rename(tmpFolder, h.dir); err != nil {
		// Restore backup
		os.Rename(backupFolder, h.dir)
		return err
	}

	// 3. Re-open h on new files
	if err := h.initN(h.dir, h.capacity); err != nil {
		return err
	}

	// 4. Delete backup
	os.RemoveAll(backupFolder)

	return nil
}

/*

Example usage:
	folder := "./folder"

	var obj Hashmap
	obj.init(folder)
	obj.Add("key", "value")

*/
