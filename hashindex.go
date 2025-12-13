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
	(*h.Hashes)[hkey] = slabOffset.hash
	(*h.Offsets)[hkey] = slabOffset.slabOffset
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
	h := Hash(xxhash.Sum64(key))
	if h == 0 {
		return 1
	}
	if h == HashTombstone {
		return HashTombstone - 1
	}
	return h
}

func (h *Hashmap) getHashes() []Hash {
	tmp := (*Hash)(unsafe.Pointer(&h.hashMap[0]))
	return unsafe.Slice(tmp, h.Capacity)
}

func (h *Hashmap) getOffsets() []SlabOffset {
	hashBytes := uintptr(h.Capacity) * unsafe.Sizeof(Hash(0))
	tmp := (*SlabOffset)(unsafe.Add(unsafe.Pointer(&h.hashMap[0]), hashBytes))
	return unsafe.Slice(tmp, h.Capacity)
}

func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool, error) {
	myhash := hash(key)
	hkey := uint64(myhash) % (h.Capacity)

	var firstTombstoneIndex uint64
	foundTombstone := false

	for {
		probeHash := (*h.Hashes)[hkey]

		if probeHash == 0 {
			// Found Empty Slot.
			// If we saw a Tombstone earlier, use that instead to reduce fragmentation.
			if foundTombstone {
				return firstTombstoneIndex, true, nil
			}
			return hkey, true, nil
		}

		if probeHash == HashTombstone {
			if !foundTombstone {
				firstTombstoneIndex = hkey
				foundTombstone = true
			}
			// Continue probing to ensure key doesn't exist further down
		} else if probeHash == myhash {
			offset := (*h.Offsets)[hkey]
			item, err := h.unmarshalItemFromSlab(Key{slabOffset: offset, hash: probeHash})
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

		(*h.Hashes)[hkey] = slabOffsets[i].hash
		(*h.Offsets)[hkey] = slabOffsets[i].slabOffset
		if isnew {
			totalNewKey++
		}
	}

	*h.Count += totalNewKey
	return nil
}
