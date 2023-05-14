package gomap

import (
	"fmt"
	"time"
)

func (h *Hashmap) checkResize() bool {
	return *h.Count*100 > h.Capacity*65
}

func (h *Hashmap) addKeyResize(slabOffset Key) {
	h.mapLk.Lock()
	defer h.mapLk.Unlock()

	hkey := h.getKeyOffsetToAddResize(slabOffset)
	(*h.Keys)[hkey] = slabOffset
}

func (h *Hashmap) getKeyOffsetToAddResize(slabOffset Key) uint64 {
	myhash := slabOffset.hash
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

func (h *Hashmap) copyToNewMap() {
	h.resizeLk.Lock()
	defer h.resizeLk.Unlock()

	startTime := time.Now()

	index := uint64(0)
	for index < h.oldCapacity {
		mykey := (*h.oldKeys)[index]
		index += 1

		if mykey.slabOffset != 0 {
			h.addKeyResize(mykey)
		}
	}

	resizeTime := getRunTime(startTime)
	h.resizeTime += resizeTime
	fmt.Println("Count: ", *h.Count)
	fmt.Println("Capacity: ", h.Capacity)
	fmt.Println("Resizing Time this iteration: ", resizeTime)
	fmt.Println("Total Resizing Time: ", h.resizeTime)
	fmt.Println("Hash Time: ", h.hashTime)
	fmt.Println("Slab Time: ", h.slabTime)
	fmt.Println("")

	fmt.Println("/copyToNewMap")
}

func (h *Hashmap) resize() {
	fmt.Println("resize waitinf or lock")
	h.resizeLk.Lock()

	var newH Hashmap
	fmt.Println("Resizing")
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity), (h.slabSize))

	h.replaceHashmap(newH)

	h.resizeLk.Unlock()
	fmt.Println("/Resizing")
	go h.copyToNewMap()

}

func (h *Hashmap) replaceHashmap(newH Hashmap) {
	//TODO close and delete old file, can be async
	// see closeFPs
	h.oldKeys = h.Keys
	h.oldCapacity = h.Capacity
	h.slabMapOld = h.slabMap

	h.hashMap = newH.hashMap
	h.hashMapFile = newH.hashMapFile
	h.Capacity = newH.Capacity
	h.Keys = newH.Keys
	h.slabMap = newH.slabMap
}
