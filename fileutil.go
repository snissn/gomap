package gomap

import (
	"log"
	"os"
	"strconv"

	"github.com/go-errors/errors"
)

func (h *Hashmap) initN(folder string, N uint64, slabSize int64) {
	h.Folder = folder
	m, f_map, err := h.openMmapHash(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	slab, f_slab, err := h.openMmapSlab(slabSize)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	err = h.writeCapacity(N)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}
	err = h.writeSlabSize(slabSize)
	if err != nil {
		log.Fatal(errors.Wrap(err, 1))
	}

	h.hashMap = m
	h.hashMapFile = f_map

	h.slabMap = slab
	h.slabFILE = f_slab
	h.slabSize = slabSize

	//todo
	h.slabOffset = getSlabOffset(h.slabMap)
	//xxx

	if *h.slabOffset == 0 {
		sentinel := []byte("offset")
		h.writeSlab(sentinel)
		*h.slabOffset = SlabOffset(len(sentinel))
	}

	h.Capacity = N
	h.Count = getCount(h.slabMap)
	keys := h.getKeys()
	h.Keys = &keys

}
func (h *Hashmap) writeSlabSize(slabSize int64) error {
	s := strconv.FormatInt(slabSize, 10)
	return os.WriteFile(h.Folder+"/slabSize", []byte(s), 0655)
}

func (h *Hashmap) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.Folder+"/capacity", []byte(s), 0655)
}
func (h *Hashmap) readCapacity() (uint64, int64) {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DEFAULTMAPSIZE, DEFAULTSLABSIZE
	}
	capacity, err := strconv.ParseUint(string(dat), 10, 64)
	handleError(err)

	slabdat, err := os.ReadFile(h.Folder + "/slabSize")
	if err != nil {
		return DEFAULTMAPSIZE, DEFAULTSLABSIZE
	}
	slabSize, err := strconv.ParseInt(string(slabdat), 10, 64)
	handleError(err)

	return capacity, slabSize
}

func (h *Hashmap) createDirectory() {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}
}
