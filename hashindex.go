package gomap

import (
	"bytes"
	"unsafe"

	"github.com/segmentio/fasthash/fnv1"
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
	return Hash(fnv1.HashBytes32(key))
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}

func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool, error) {
	myhash := hash(key)
	hkey := uint64(myhash) % (h.Capacity)
	for {
		mybucket := (*h.Keys)[hkey]
		if mybucket.slabOffset == 0 {
			return hkey, true, nil
		}
		if mybucket.hash == myhash {
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
