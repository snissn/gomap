package gomap

import (
	"log"
	"os"
	"strconv"

	"github.com/go-errors/errors"
)

func (h *Hashmap) initN(folder string, N uint64) {
	h.Folder = folder
	
	// Create directory is handled inside openMmapHash if needed, but safer here.
	h.createDirectory()

	m, f_map, err := h.openMmapHash(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	meta, f_meta, err := h.openMetadata()
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	f_data, err := h.openDataFile()
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	err = h.writeCapacity(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}
	
	h.hashMap = m
	h.hashMapFile = f_map

	h.metadataMap = meta
	h.metadataFile = f_meta
	
	h.realSlabFILE = f_data

	//todo
	h.slabOffset = getSlabOffset(h.metadataMap)
	//xxx

	if *h.slabOffset == 0 {
		sentinel := []byte("offset")
		h.writeSlab(sentinel)
		*h.slabOffset = SlabOffset(len(sentinel))
	}

	h.Capacity = N
	h.Count = getCount(h.metadataMap)
	keys := h.getKeys()
	h.Keys = &keys

}

func (h *Hashmap) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.Folder+"/capacity", []byte(s), 0655)
}
func (h *Hashmap) readCapacity() uint64 {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DEFAULTMAPSIZE
	}
	capacity, err := strconv.ParseUint(string(dat), 10, 64)
	handleError(err)

	return capacity
}

func (h *Hashmap) createDirectory() {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
}
