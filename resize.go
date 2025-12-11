package gomap

import (
	"fmt"
	"os"
	"time"
)

func (h *Hashmap) checkResize() bool {
	return *h.Count*100 > h.Capacity*65
}

func (h *Hashmap) addKeyResize(slabOffset Key) {
	hkey := h.getKeyOffsetToAddResize(slabOffset)
	(*h.Keys)[hkey] = slabOffset
	*h.Count += 1
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

func (h *Hashmap) resize() {
	startTime := time.Now()
	// defer printTotalRunTime(startTime) // Optional: keep or remove, but it prints too

	var newH Hashmap
	//todo create a new init function that doesn't take a slabSize and doesn't resize the slab
	newH.initN(h.Folder, 2*(h.Capacity))

	index := uint64(0)
	for index < h.Capacity {
		mykey := (*h.Keys)[index]
		index += 1

		if mykey.slabOffset != 0 {
			newH.addKeyResize(mykey)
		}
	}

	h.replaceHashmap(newH)
	resizeTime := getRunTime(startTime)
	h.resizeTime += resizeTime
}

func (h *Hashmap) replaceHashmap(newH Hashmap) {
	// Close and delete old file
	// We reconstruct the filename based on current capacity
	oldFilename := h.Folder + "/hashkeys-" + fmt.Sprint(h.Capacity)

	// Close the file handle and unmap memory
	h.closeFPs()

	// Delete the old file from disk
	err := os.Remove(oldFilename)
	if err != nil {
		fmt.Println("Failed to remove old hash map file:", err)
	}

	// Close old metadata and slab handles to prevent leaks
	// Note: We ignore errors here as we are replacing them anyway
	if h.metadataFile != nil {
		h.metadataFile.Close()
	}
	if h.metadataMap != nil {
		h.metadataMap.Unmap()
	}
	if h.realSlabFILE != nil {
		h.realSlabFILE.Close()
	}

	h.hashMap = newH.hashMap
	h.hashMapFile = newH.hashMapFile
	h.Capacity = newH.Capacity
	h.Keys = newH.Keys
	
	h.metadataMap = newH.metadataMap
	h.metadataFile = newH.metadataFile
	h.realSlabFILE = newH.realSlabFILE

	// Update pointers to point to the new mmap region
	h.Count = newH.Count
	h.slabOffset = newH.slabOffset
}
