package hashdb

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/HashDB/internal/lockfile"
)

func (h *DB) closeFPs() error {
	var firstErr error
	recordErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	// Unmap before closing files (important for Windows compatibility).
	if h.controlMap != nil {
		h.unlockControlsIfNeeded([]byte(h.controlMap))
		recordErr(h.controlMap.Unmap())
		h.controlMap = nil
	}
	if h.controlFile != nil {
		recordErr(h.controlFile.Close())
		h.controlFile = nil
	}
	if h.keyMap != nil {
		recordErr(h.keyMap.Unmap())
		h.keyMap = nil
	}
	if h.keyFile != nil {
		recordErr(h.keyFile.Close())
		h.keyFile = nil
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
	if h.rehashOldControlMap != nil {
		_ = unlockBytes([]byte(h.rehashOldControlMap))
		recordErr(h.rehashOldControlMap.Unmap())
		h.rehashOldControlMap = nil
	}
	if h.rehashOldControlFile != nil {
		recordErr(h.rehashOldControlFile.Close())
		h.rehashOldControlFile = nil
	}
	if h.rehashOldKeyMap != nil {
		recordErr(h.rehashOldKeyMap.Unmap())
		h.rehashOldKeyMap = nil
	}
	if h.rehashOldKeyFile != nil {
		recordErr(h.rehashOldKeyFile.Close())
		h.rehashOldKeyFile = nil
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
	var errs []error
	if err := h.closeFPs(); err != nil {
		errs = append(errs, err)
	}
	if h.lock != nil {
		if err := h.lock.Close(); err != nil {
			errs = append(errs, err)
		}
		h.lock = nil
	}
	return errors.Join(errs...)
}

// Open opens (or creates) the primary HashDB store rooted at dir.
//
// This is the sharded/distributed engine (formerly "gomap_distributed").
func Open(dir string) (*HashDB, error) {
	db := &HashDB{}
	if err := db.New(dir); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenWithShards opens the primary HashDB store with an explicit shard count.
func OpenWithShards(dir string, numShards int) (*HashDB, error) {
	db := &HashDB{}
	if err := db.NewWithShards(dir, numShards); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenSingle opens (or creates) a single-shard DB rooted at dir.
// The single-shard DB is not thread-safe; prefer Open/OpenWithShards in most cases.
func OpenSingle(dir string) (*DB, error) {
	db := &DB{}
	if err := db.Open(dir); err != nil {
		return nil, err
	}
	return db, nil
}

// Get retrieves the value for a given key.
// It returns nil, nil if the key is not found.
func (h *DB) Get(key []byte) ([]byte, error) {
	return h.getWithHash(key, hash(key))
}

func (h *DB) getWithHash(key []byte, keyHash Hash) ([]byte, error) {
	// Probe current (new) table first.
	if len(h.keys) > 0 && h.capacity > 0 {
		_, item, found, err := h.probeWithHash(h.keys, h.controls, h.capacity, key, keyHash)
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
		_, item, found, err := h.probeWithHash(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key, keyHash)
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
	keyHash := hash(key)

	// Delete in the current (new) table.
	if len(h.keys) > 0 && h.capacity > 0 {
		idx, _, found, err := h.probeWithHash(h.keys, h.controls, h.capacity, key, keyHash)
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
		idx, _, found, err := h.probeWithHash(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key, keyHash)
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
	// Since DB is NOT thread-safe, the caller (HashDB) must hold the lock.
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

	return h.Put(key, newVal)
}

// PutMany inserts multiple entries in a batch.
// It is not thread-safe.
func (h *DB) PutMany(items []Item) error {

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

// AddMany is a compatibility wrapper for older code.
func (h *DB) AddMany(items []Item) error {
	return h.PutMany(items)
}

// Put inserts a single key/value pair.
// It is not thread-safe.
func (h *DB) Put(key []byte, value []byte) error {
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

// Add is a compatibility wrapper for older code.
func (h *DB) Add(key []byte, value []byte) error {
	return h.Put(key, value)
}

// Open initializes a DB in the given folder.
func (h *DB) Open(folder string) error {
	if folder == "" {
		return errors.New("db dir required")
	}
	h.dir = folder
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return err
	}
	lock, err := lockfile.Acquire(filepath.Join(folder, "LOCK"))
	if err != nil {
		return err
	}
	h.lock = lock

	N, err := h.readCapacity()
	if err != nil {
		_ = h.Close()
		return err
	}
	if err := h.initN(folder, N); err != nil {
		_ = h.Close()
		return err
	}
	return nil
}

// New is a compatibility wrapper for older code.
func (h *DB) New(folder string) error {
	return h.Open(folder)
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
	if h.dir == "" {
		return errors.New("compact: db dir required")
	}

	dir := h.dir
	capacity := h.capacity
	compressionEnabled := h.compressionEnabled

	tmpFolder := dir + "-compact"
	_ = os.RemoveAll(tmpFolder) // clean start (best-effort)

	// Build a compacted copy in tmpFolder.
	var newH DB
	if err := newH.initN(tmpFolder, capacity); err != nil {
		return err
	}
	newH.SetCompression(compressionEnabled)

	keys := h.getKeys()
	for _, k := range keys {
		if k.slabOffset == 0 || k.slabOffset == Tombstone {
			continue
		}

		item, err := h.unmarshalItemFromSlab(k)
		if err != nil {
			_ = newH.closeFPs()
			_ = os.RemoveAll(tmpFolder)
			return err
		}

		if err := newH.Put(item.Key, item.Value); err != nil {
			_ = newH.closeFPs()
			_ = os.RemoveAll(tmpFolder)
			return err
		}
	}

	if err := newH.Close(); err != nil {
		_ = os.RemoveAll(tmpFolder)
		return err
	}

	// Swap directories. We fully close h so Windows can rename/delete directories reliably.
	if err := h.Close(); err != nil {
		_ = os.RemoveAll(tmpFolder)
		return err
	}

	backupFolder := dir + "-old"
	_ = os.RemoveAll(backupFolder)

	if err := os.Rename(dir, backupFolder); err != nil {
		_ = os.RemoveAll(tmpFolder)
		_ = h.Open(dir)
		return err
	}
	if err := os.Rename(tmpFolder, dir); err != nil {
		_ = os.Rename(backupFolder, dir)
		_ = os.RemoveAll(tmpFolder)
		_ = h.Open(dir)
		return err
	}

	if err := h.Open(dir); err != nil {
		// Best-effort rollback: restore the original directory.
		_ = os.Rename(dir, tmpFolder)
		_ = os.Rename(backupFolder, dir)
		_ = os.RemoveAll(tmpFolder)
		return err
	}

	_ = os.RemoveAll(backupFolder)
	return nil
}

/*

Example usage:
	folder := "./folder"

	var obj DB
	obj.init(folder)
	obj.Add("key", "value")

*/
