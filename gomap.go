package gomap

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"time"

	"log"
	"reflect"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

var size uintptr = reflect.TypeOf(uint64(0)).Size()
var DEFAULTMAPSIZE uint64 = uint64(32 * 1024)

func getRunTime(startTime time.Time) time.Duration {
	endTime := time.Now()
	return endTime.Sub(startTime)
}

func printTotalRunTime(startTime time.Time) {
	endTime := time.Now()
	totalRunTime := endTime.Sub(startTime)
	fmt.Printf("Total run time: %s\n", totalRunTime)
}

func (h *Hashmap) closeFPs() error {
	if h.hashMapFile != nil {
		if err := h.hashMapFile.Close(); err != nil {
			return err
		}
	}
	if h.hashMap != nil {
		if err := h.hashMap.Unmap(); err != nil {
			return err
		}
	}

	// If an incremental rehash is in progress, close the old index as well.
	if h.rehashInProgress {
		if h.rehashOldMapFile != nil {
			if err := h.rehashOldMapFile.Close(); err != nil {
				return err
			}
		}
		if h.rehashOldMap != nil {
			if err := h.rehashOldMap.Unmap(); err != nil {
				return err
			}
		}
		h.rehashOldMapFile = nil
		h.rehashOldMap = nil
		h.rehashOldKeys = nil
		h.rehashOldCapacity = 0
		h.rehashIdx = 0
		h.rehashInProgress = false
	}

	for _, f := range h.slabFiles {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the value for a given key.
// It returns nil, nil if the key is not found.
func (h *Hashmap) Get(key []byte) ([]byte, error) {
	// Probe current (new) table first.
	if h.Keys != nil && h.Capacity > 0 {
		myhash := hash(key)
		count := uint64(0)
		for count < h.Capacity {
			myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity

			mybucket := (*h.Keys)[myKeyIndex]

			if mybucket.slabOffset == 0 {
				return nil, nil
			}

			if mybucket.slabOffset == Tombstone {
				count++
				continue
			}

			if mybucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(mybucket)
				if err != nil {
					return nil, err
				}
				if bytes.Equal(item.Key, key) {
					return item.Value, nil
				}
			}
			count++
		}
	}

	// If an incremental rehash is in progress, also probe the old table
	// for keys that haven't been migrated yet.
	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		myhash := hash(key)
		count := uint64(0)
		for count < h.rehashOldCapacity {
			myKeyIndex := ((uint64(myhash) % h.rehashOldCapacity) + count) % h.rehashOldCapacity

			mybucket := h.rehashOldKeys[myKeyIndex]

			if mybucket.slabOffset == 0 {
				return nil, nil
			}

			if mybucket.slabOffset == Tombstone {
				count++
				continue
			}

			if mybucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(mybucket)
				if err != nil {
					return nil, err
				}
				if bytes.Equal(item.Key, key) {
					return item.Value, nil
				}
			}
			count++
		}
	}

	return nil, nil
}

// Delete removes a key from the map.
func (h *Hashmap) Delete(key []byte) error {
	myhash := hash(key)
	foundNew := false

	// Delete in the current (new) table.
	if h.Keys != nil && h.Capacity > 0 {
		count := uint64(0)
		for count < h.Capacity {
			myKeyIndex := ((uint64(myhash) % h.Capacity) + count) % h.Capacity

			mybucket := (*h.Keys)[myKeyIndex]

			if mybucket.slabOffset == 0 {
				break // Key not found in new table
			}

			if mybucket.slabOffset == Tombstone {
				count++
				continue
			}

			if mybucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(mybucket)
				if err != nil {
					return err
				}
				if bytes.Equal(item.Key, key) {
					// Found it in new table.
					if err := h.addDeleteSlab(key); err != nil {
						return err
					}
					(*h.Keys)[myKeyIndex].slabOffset = Tombstone
					*h.Count -= 1
					foundNew = true
					break
				}
			}
			count++
		}
	}

	// If rehash in progress, also tombstone any copy in the old table so it
	// doesn't get resurrected during migration. Do not adjust Count again.
	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		count := uint64(0)
		for count < h.rehashOldCapacity {
			myKeyIndex := ((uint64(myhash) % h.rehashOldCapacity) + count) % h.rehashOldCapacity

			mybucket := h.rehashOldKeys[myKeyIndex]

			if mybucket.slabOffset == 0 {
				break
			}

			if mybucket.slabOffset == Tombstone {
				count++
				continue
			}

			if mybucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(mybucket)
				if err != nil {
					return err
				}
				if bytes.Equal(item.Key, key) {
					h.rehashOldKeys[myKeyIndex].slabOffset = Tombstone
					break
				}
			}
			count++
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
func (h *Hashmap) Update(key []byte, callback func([]byte) ([]byte, error)) error {
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
func (h *Hashmap) AddMany(items []Item) error {

	startTime := time.Now()
	slabOffsets, err := h.addManySlabs(items)
	if err != nil {
		return err
	}
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	for i, item := range items {
		if err := h.addBucket(item.Key, slabOffsets[i]); err != nil {
			return err
		}
	}
	if h.rehashInProgress {
		if err := h.rehashStep(rehashBucketsPerWrite); err != nil {
			return err
		}
	}
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
	return nil
}

// Add inserts a single key-value pair.
// It is not thread-safe.
func (h *Hashmap) Add(key []byte, value []byte) error {
	item := Item{Key: key, Value: value}
	startTime := time.Now()
	slabOffset, err := h.addSlab(item)
	if err != nil {
		return err
	}
	slabTime := getRunTime(startTime)
	h.slabTime += slabTime

	startTime = time.Now()
	err = h.addBucket(key, slabOffset)
	if err == nil && h.rehashInProgress {
		if err2 := h.rehashStep(rehashBucketsPerWrite); err2 != nil {
			return err2
		}
	}
	hashTime := getRunTime(startTime)
	h.hashTime += hashTime
	return err
}

// mlock locks the data in memory to prevent it from being swapped to disk.
func (h *Hashmap) mlock(data mmap.MMap) {
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), 0)
	if errno != 0 {
		// If the syscall fails, it could be because the user does not have
		// sufficient privileges to lock memory. To fix this, edit the
		// /etc/security/limits.conf file and add the following line:
		//
		// <username> soft memlock unlimited
		//
		// where <username> is the name of the user running the program.
		// Then, log out and log back in for the changes to take effect.
		//
		// Alternatively, you can run the program with sudo privileges to
		// bypass this error.
		log.Fatalf("syscall.Syscall(SYS_MLOCK) failed: %v\n"+
			"To fix this, edit the /etc/security/limits.conf file and add the following line:\n"+
			"<username> soft memlock unlimited\n"+
			"where <username> is the name of the user running the program.\n"+
			"Then, log out and log back in for the changes to take effect.\n"+
			"Alternatively, you can run the program with sudo privileges to bypass this error.", errno)
	}
}

// New initializes a Hashmap in the given folder.
func (h *Hashmap) New(folder string) error {
	h.Folder = folder
	N, err := h.readCapacity()
	if err != nil {
		return err
	}
	return h.initN(folder, N)
}

// SetCompression enables or disables value compression.
// Default is true.
func (h *Hashmap) SetCompression(enabled bool) {
	h.CompressionEnabled = enabled
}

// SetResizeThreshold sets the load factor percentage at which the hashmap resizes.
// For example, 65 means resize when Count/Capacity > 0.65.
// Values <= 0 reset to the default of 65.
func (h *Hashmap) SetResizeThreshold(percent uint64) {
	if percent == 0 {
		percent = 65
	}
	h.resizeThreshold = percent
}

// Clear wipes the database (deletes all data) and resets it.
func (h *Hashmap) Clear() error {
	// Close resources
	if err := h.closeFPs(); err != nil {
		return err
	}

	// Delete files
	files, err := os.ReadDir(h.Folder)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(h.Folder + "/" + f.Name()); err != nil {
			return err
		}
	}

	// Re-initialize
	// initN expects folder to exist (it might have been deleted? No, we deleted contents)
	// We read capacity? No, files are gone.
	// Default capacity.
	return h.initN(h.Folder, DEFAULTMAPSIZE)
}

// Stats returns statistics about the database.
func (h *Hashmap) Stats() Stats {
	// Calculate total file size
	var size uint64
	for _, f := range h.slabFiles {
		fi, _ := f.Stat()
		size += uint64(fi.Size())
	}

	return Stats{
		KeyCount: *h.Count,
		Capacity: h.Capacity,
		DataSize: size,
		Segments: len(h.slabFiles),
	}
}

// Compact rewrites the database to reclaim space from deleted/updated keys.
// It creates a new copy of the database and swaps it in.
func (h *Hashmap) Compact() error {
	tmpFolder := h.Folder + "-compact"
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

	if err := newH.initN(tmpFolder, h.Capacity); err != nil {
		return err
	}
	newH.SetCompression(h.CompressionEnabled)

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
	backupFolder := h.Folder + "-old"
	_ = os.RemoveAll(backupFolder)

	if err := os.Rename(h.Folder, backupFolder); err != nil {
		// Try to reopen h?
		return err
	}
	if err := os.Rename(tmpFolder, h.Folder); err != nil {
		// Restore backup
		os.Rename(backupFolder, h.Folder)
		return err
	}

	// 3. Re-open h on new files
	if err := h.initN(h.Folder, h.Capacity); err != nil {
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
