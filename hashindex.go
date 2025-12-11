package gomap

import (
	"bytes"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

func (h *Hashmap) addKey(key []byte, slabOffset Key) error {
	hkey, isnew, err := h.getKeyOffsetToAdd(key)
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
	if h.checkResize() {
		h.resize()
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

func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool, error) {
	myhash := hash(key)
	hkey := uint64(myhash) % (h.Capacity)

	var firstTombstoneIndex uint64
	foundTombstone := false

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
			// Continue probing to ensure key doesn't exist further down
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
		hkey, isnew, err := h.getKeyOffsetToAdd(item.Key)
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
