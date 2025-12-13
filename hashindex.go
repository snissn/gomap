package gomap

import (
	"bytes"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

func (h *Hashmap) addKey(key []byte, slabOffset Key) error {
	hkey, isnew, err := h.probeForAdd(key)
	if err != nil {
		return err
	}
	(*h.Keys)[hkey] = slabOffset
	if isnew {
		*h.Count += 1
	}
	return nil
}

func (h *Hashmap) addBucket(key []byte, slabOffset Key) error {
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

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}

// probe searches for a key in the provided keys slice.
// It returns the index, the item (if found), whether it was found, and any error.
func (h *Hashmap) probe(keys []Key, capacity uint64, key []byte) (uint64, *Item, bool, error) {
	myhash := hash(key)
	count := uint64(0)
	hkey := uint64(myhash) % capacity

	for count < capacity {
		bucket := keys[hkey]

		if bucket.slabOffset == 0 {
			return 0, nil, false, nil
		}

		if bucket.slabOffset != Tombstone && bucket.hash == myhash {
			item, err := h.unmarshalItemFromSlab(bucket)
			if err != nil {
				return 0, nil, false, err
			}
			if bytes.Equal(item.Key, key) {
				return hkey, &item, true, nil
			}
		}

		hkey = (hkey + 1) % capacity
		count++
	}
	return 0, nil, false, nil
}

// probeForAdd searches for a key or an insertion slot.
// It returns the index to insert at, and whether the key is new.
func (h *Hashmap) probeForAdd(key []byte) (uint64, bool, error) {
	myhash := hash(key)
	hkey := uint64(myhash) % (h.Capacity)

	var firstTombstoneIndex uint64
	foundTombstone := false

	// Loop until we find the key or an empty slot
	for {
		mybucket := (*h.Keys)[hkey]

		if mybucket.slabOffset == 0 {
			// Found Empty Slot.
			// If we saw a Tombstone earlier, use that instead to reduce fragmentation.
			if foundTombstone {
				return firstTombstoneIndex, true, nil
			}
			return hkey, true, nil
		}

		if mybucket.slabOffset == Tombstone {
			if !foundTombstone {
				firstTombstoneIndex = hkey
				foundTombstone = true
			}
			// We MUST continue probing to ensure the key doesn't exist further down the chain.
			// Ending early here would allow duplicate keys if a key exists after a tombstone.
		} else if mybucket.hash == myhash {
			item, err := h.unmarshalItemFromSlab(mybucket)
			if err != nil {
				return 0, false, err
			}
			if bytes.Equal(item.Key, key) {
				return hkey, false, nil
			}
		}
		hkey = (hkey + 1) % h.Capacity
	}
}

func (h *Hashmap) addManyBuckets(items []Item, slabOffsets []Key) error {
	if h.checkResize() {
		h.resize()
	}

	return h.addManyKeys(items, slabOffsets)
}

func (h *Hashmap) addManyKeys(items []Item, slabOffsets []Key) error {
	totalNewKey := uint64(0)

	for i, item := range items {
		hkey, isnew, err := h.probeForAdd(item.Key)
		if err != nil {
			return err
		}

		(*h.Keys)[hkey] = slabOffsets[i]
		if isnew {
			totalNewKey++
		}
	}

	*h.Count += totalNewKey
	return nil
}