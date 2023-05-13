package gomap

import (
	"fmt"
	"time"
)

func (h *Hashmap) checkResize() bool {
	return *h.Count*100 > h.Capacity*65
}

func (h *Hashmap) addKeyResize(key []byte, slabOffset Key) {
	hkey := h.getKeyOffsetToAddResize(key)
	(*h.Keys)[hkey] = slabOffset
	*h.Count += 1
}

func (h *Hashmap) getKeyOffsetToAddResize(key []byte) uint64 {
	myhash := hash(key)
	count := uint64(0)
	for count < h.Capacity {
		hkey := ((uint64(myhash) % (h.Capacity)) + count) % h.Capacity
		mybucket := (*h.Keys)[hkey]
		if mybucket.slabOffset == 0 {
			return hkey
		} else {
			count++
		}
	}
	panic("why")
}

func (h *Hashmap) resize() {
	startTime := time.Now()
	defer printTotalRunTime(startTime)

	var newH Hashmap
	fmt.Println("Resizing")
	fmt.Println("Count: ", *h.Count)
	fmt.Println("Capacity: ", h.Capacity)
	fmt.Println("Hash Time: ", h.hashTime)
	fmt.Println("Slab Time: ", h.slabTime)
	fmt.Println("")
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity), (h.slabSize))

	index := uint64(0)
	for index < h.Capacity {
		mykey := (*h.Keys)[index]
		index += 1

		if mykey.slabOffset != 0 {
			item := h.unmarshalItemFromSlab(mykey)
			newH.addKeyResize(item.Key, mykey)
		}
	}

	h.replaceHashmap(newH)
}

func (h *Hashmap) replaceHashmap(newH Hashmap) {
	//TODO close and delete old file, can be async
	// see closeFPs

	h.hashMap = newH.hashMap
	h.hashMapFile = newH.hashMapFile
	h.Capacity = newH.Capacity
	h.Keys = newH.Keys

	h.slabMap = newH.slabMap
}
