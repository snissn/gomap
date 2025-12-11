package gomap

import (
	"bytes"
	"unsafe"

	"github.com/segmentio/fasthash/fnv1"
)

func (h *Hashmap) addKey(key []byte, slabOffset Key) {
	hkey, newKey := h.getKeyOffsetToAdd(key)
	(*h.Keys)[hkey] = slabOffset
	if newKey {
		*h.Count += 1
	}
}

func (h *Hashmap) addBucket(key []byte, slabOffset Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addKey(key, slabOffset)

}

func hash(key []byte) Hash {
	return Hash(fnv1.HashBytes32(key))
}

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}
func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool) {
	myhash := hash(key)
	hkey := uint64(myhash) % (h.Capacity)
	for {
		mybucket := (*h.Keys)[hkey]
		if mybucket.slabOffset == 0 {
			return hkey, true
		}
		if mybucket.hash == myhash {
			item := h.unmarshalItemFromSlab(mybucket)
			if bytes.Equal(item.Key, key) {
				return hkey, false
			}
		}
		hkey = (hkey + 1) % h.Capacity
	}
}

func (h *Hashmap) addManyBuckets(items []Item, slabOffsets []Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addManyKeys(items, slabOffsets)
}

func (h *Hashmap) addManyKeys(items []Item, slabOffsets []Key) {
	totalNewKey := uint64(0)

	for i, item := range items {
		hkey, isnew := h.getKeyOffsetToAdd(item.Key)
		
		(*h.Keys)[hkey] = slabOffsets[i]
		if isnew {
			totalNewKey++
		}
	}
	
	*h.Count += totalNewKey
}
