package gomap

import (
	"bytes"
	"sync"
	"unsafe"
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

func (h *Hashmap) getKeys() []Key {
	tmpkeys := (*Key)(unsafe.Pointer(&h.hashMap[0]))
	ret := unsafe.Slice(tmpkeys, h.Capacity)
	return ret
}
func (h *Hashmap) getKeyOffsetToAdd(key []byte) (uint64, bool) {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		hkey := ((uint64(myhash) % (h.Capacity)) + count) % h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket.slabOffset == 0 {
			return hkey, true
		} else {
			if mybucket.hash == myhash {
				item := h.unmarshalItemFromSlab(mybucket)
				if bytes.Equal(item.Key, key) {
					return hkey, false
				}
			}
			count++
		}
	}
	panic("why")
}

func (h *Hashmap) addManyBuckets(items []Item, slabOffsets []Key) {
	if h.checkResize() {
		h.resize()
	}

	h.addManyKeys(items, slabOffsets)
}

func (h *Hashmap) addManyKeys(items []Item, slabOffsets []Key) {
	var wg sync.WaitGroup
	seenSet := NewSet()
	extra_items := make([]Item, 0)
	extra_slabOffsets := make([]Key, 0)

	hkeys, totalNewKey := ConcurrentMap(items, h.getKeyOffsetToAdd)
	for i, hkey := range hkeys {
		alreadyExists := seenSet.Add(hkey)
		if alreadyExists {
			extra_items = append(extra_items, items[i])
			extra_slabOffsets = append(extra_slabOffsets, slabOffsets[i])
			totalNewKey -= 1
		} else {
			//todo put this in gothing
			wg.Add(1)
			go func(i int, hkey uint64) {
				defer wg.Done()
				(*h.Keys)[hkey] = slabOffsets[i]
			}(i, hkey)
		}
	}
	wg.Wait()
	*h.Count += totalNewKey
	for i, item := range extra_items {
		h.addBucket(item.Key, extra_slabOffsets[i])
	}
}
