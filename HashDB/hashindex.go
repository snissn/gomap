package hashdb

import (
	"bytes"
	"fmt"
	"math/bits"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

const (
	// Control Byte States
	// Empty: 0x00 (matches OS zero-fill)
	// Deleted: 0x01
	// Full: (h2 | 0x80) -> 0x80..0xFF
	ctrlEmpty   = 0x00
	ctrlDeleted = 0x01
	groupSize   = 8
)

func (h *DB) addKey(key []byte, slabOffset Key) error {
	hkey, isnew, err := h.probeForAdd(key)
	if err != nil {
		return err
	}

	// If rehash is in progress, ensure we mark any existing key in the old table
	// as Tombstone so it doesn't get migrated and overwrite this new value.
	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		idx, _, found, err := h.probe(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key)
		if err != nil {
			return err
		}
		if found {
			h.rehashOldKeys[idx].slabOffset = Tombstone
		}
	}

	(*h.Keys)[hkey] = slabOffset
	if isnew {
		*h.Count += 1
		// Set Control Byte
		myhash := hash(key)
		h2 := byte(myhash&0x7f) | 0x80
		(*h.Controls)[hkey] = h2
	}
	return nil
}

func (h *DB) addBucket(key []byte, slabOffset Key) error {
	if !h.rehashInProgress && h.checkResize() {
		if err := h.startRehash(); err != nil {
			return err
		}
	}

	return h.addKey(key, slabOffset)
}

func hash(key []byte) Hash {
	return Hash(xxhash.Sum64(key))
}

func (h *DB) getKeys() []Key {
	// Deprecated/Internal use: returns slice of keys.
	// We assume h.Keys is set.
	if h.Keys != nil {
		return *h.Keys
	}
	// Fallback should not happen in normal operation
	return nil
}

// matchH2 returns a bitmask where each set bit corresponds to a byte in 'group'
// that matches 'h2'.
func matchH2(h2 byte, group uint64) uint64 {
	pattern := uint64(h2) * 0x0101010101010101
	diff := group ^ pattern
	return (diff - 0x0101010101010101) & ^diff & 0x8080808080808080
}

// matchEmptyOrDeleted returns a bitmask where each set bit corresponds to a
// byte in 'group' that is Empty (0x00) or Deleted (0x01).
// Since Full buckets have the top bit set (0x80..0xFF), and Empty/Deleted have
// the top bit clear, we simply check for the top bit being 0.
func matchEmptyOrDeleted(group uint64) uint64 {
	return ^group & 0x8080808080808080
}

// loadGroup loads 8 bytes from controls at index i, handling wrap-around.
func loadGroup(controls []byte, i uint64, capacity uint64) uint64 {
	if i+8 <= capacity {
		return *(*uint64)(unsafe.Pointer(&controls[i]))
	}
	// Handle wrap-around
	var val uint64
	for k := 0; k < 8; k++ {
		idx := (i + uint64(k)) % capacity
		val |= uint64(controls[idx]) << (k * 8)
	}
	return val
}

// probe searches for a key in the provided keys slice.
// It returns the index, the item (if found), whether it was found, and any error.
func (h *DB) probe(keys []Key, controls []byte, capacity uint64, key []byte) (uint64, *Item, bool, error) {
	myhash := hash(key)
	h1 := uint64(myhash >> 7)
	h2 := byte(myhash&0x7f) | 0x80

	idx := h1 % capacity

	probes := uint64(0)

	// Limit probe count to avoid infinite loop in full map (shouldn't happen with resizing)
	// But strictly, we loop until we find Empty.

	for probes < capacity {
		group := loadGroup(controls, idx, capacity)

		// Check for matches
		matchMask := matchH2(h2, group)

		for matchMask != 0 {
			bitPos := bits.TrailingZeros64(matchMask)
			groupOffset := uint64(bitPos >> 3)
			matchMask &= (matchMask - 1) // clear lowest set bit

			candidateIdx := (idx + groupOffset) % capacity

			// Verify candidate
			bucket := keys[candidateIdx]
			if bucket.slabOffset != Tombstone && bucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(bucket)
				if err != nil {
					return 0, nil, false, err
				}
				if bytes.Equal(item.Key, key) {
					return candidateIdx, &item, true, nil
				}
			}
		}

		// Check for empty slots to terminate search
		emptyMask := matchEmptyOrDeleted(group)
		if emptyMask != 0 {
			// Iterate over potential empty/deleted slots
			tmpMask := emptyMask
			for tmpMask != 0 {
				bitPos := bits.TrailingZeros64(tmpMask)
				groupOffset := uint64(bitPos >> 3)
				tmpMask &= (tmpMask - 1) // clear lowest set bit

				candidateIdx := (idx + groupOffset) % capacity
				if controls[candidateIdx] == ctrlEmpty {
					// Found empty slot -> Key not found.
					return 0, nil, false, nil
				}
			}
		}

		idx = (idx + groupSize) % capacity
		probes += groupSize
	}
	return 0, nil, false, nil
}

// probeForAdd searches for a key or an insertion slot.
// It returns the index to insert at, and whether the key is new.
func (h *DB) probeForAdd(key []byte) (uint64, bool, error) {
	myhash := hash(key)
	h1 := uint64(myhash >> 7)
	h2 := byte(myhash&0x7f) | 0x80

	idx := h1 % h.Capacity

	var firstDeletedIdx uint64
	foundDeleted := false
	probes := uint64(0)

	for probes < h.Capacity {
		group := loadGroup(*h.Controls, idx, h.Capacity)

		// 1. Check if key already exists
		matchMask := matchH2(h2, group)
		for matchMask != 0 {
			bitPos := bits.TrailingZeros64(matchMask)
			groupOffset := uint64(bitPos >> 3)
			matchMask &= (matchMask - 1) // clear lowest set bit

			candidateIdx := (idx + groupOffset) % h.Capacity

			// Verify
			bucket := (*h.Keys)[candidateIdx]
			if bucket.slabOffset != Tombstone && bucket.hash == myhash {
				item, err := h.unmarshalItemFromSlab(bucket)
				if err != nil {
					return 0, false, err
				}
				if bytes.Equal(item.Key, key) {
					return candidateIdx, false, nil // Found existing
				}
			}
		}

		// 2. Check for empty/deleted slots
		emptyMask := matchEmptyOrDeleted(group)
		if emptyMask != 0 {
			// Find the first Empty or Deleted slot
			// We want to terminate on Empty.
			// If we find Deleted, we record it (if first) and continue.

			tmpMask := emptyMask
			for tmpMask != 0 {
				bitPos := bits.TrailingZeros64(tmpMask)
				groupOffset := uint64(bitPos >> 3)
				tmpMask &= (tmpMask - 1) // clear lowest set bit

				candidateIdx := (idx + groupOffset) % h.Capacity
				ctrl := (*h.Controls)[candidateIdx]

				if ctrl == ctrlEmpty {
					// Found Empty. Stop search.
					if foundDeleted {
						return firstDeletedIdx, true, nil
					}
					return candidateIdx, true, nil
				} else if ctrl == ctrlDeleted {
					if !foundDeleted {
						firstDeletedIdx = candidateIdx
						foundDeleted = true
					}
				}
			}
		}

		idx = (idx + groupSize) % h.Capacity
		probes += groupSize
	}
	// Map is full or probed entire capacity
	if foundDeleted {
		return firstDeletedIdx, true, nil
	}
	return 0, false, fmt.Errorf("hashmap is full (probed %d slots)", probes)
}

func (h *DB) addManyBuckets(items []Item, slabOffsets []Key) error {
	if h.checkResize() {
		h.resize()
	}

	return h.addManyKeys(items, slabOffsets)
}

func (h *DB) addManyKeys(items []Item, slabOffsets []Key) error {
	totalNewKey := uint64(0)

	for i, item := range items {
		hkey, isnew, err := h.probeForAdd(item.Key)
		if err != nil {
			return err
		}

		(*h.Keys)[hkey] = slabOffsets[i]
		if isnew {
			totalNewKey++
			// Set Control Byte
			myhash := hash(item.Key)
			h2 := byte(myhash&0x7f) | 0x80
			(*h.Controls)[hkey] = h2
		}
	}

	*h.Count += totalNewKey
	return nil
}

func (h *DB) setDeleted(idx uint64) {
	if h.Controls != nil {
		(*h.Controls)[idx] = ctrlDeleted
	}
}

// probeForRehash finds a slot for a key during resizing.
// It assumes the key is NOT present in the map (guaranteed by resizing logic).
// It returns the index to insert at.
func (h *DB) probeForRehash(hash uint64) (uint64, error) {
	h1 := uint64(hash >> 7)

	idx := h1 % h.Capacity
	probes := uint64(0)

	for probes < h.Capacity {
		group := loadGroup(*h.Controls, idx, h.Capacity)

		// We look for any Empty or Deleted slot.
		emptyMask := matchEmptyOrDeleted(group)

		if emptyMask != 0 {
			// Find first available slot
			bitPos := bits.TrailingZeros64(emptyMask)
			groupOffset := uint64(bitPos >> 3)

			candidateIdx := (idx + groupOffset) % h.Capacity
			return candidateIdx, nil
		}

		idx = (idx + groupSize) % h.Capacity
		probes += groupSize
	}
	return 0, fmt.Errorf("hashmap is full during rehash (probed %d slots)", probes)
}
