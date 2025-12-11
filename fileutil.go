package gomap

import (
	"os"
	"strconv"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
)

func doesFileExist(fileName string) bool {
	_, error := os.Stat(fileName)
	// check if error is "file not exists"
	if os.IsNotExist(error) {
		return false
	} else {
		return true
	}
}

func getSlabOffset(slabMap mmap.MMap) *SlabOffset {
	cap := (*SlabOffset)(unsafe.Pointer(&slabMap[0]))
	return cap
}

func getCount(slabMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&slabMap[8]))
}

func (h *Hashmap) initN(folder string, N uint64) error {
	h.Folder = folder
	
	// Create directory is handled inside openMmapHash if needed, but safer here.
	if err := h.createDirectory(); err != nil {
		return err
	}

	m, f_map, err := h.openMmapHash(N)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	meta, f_meta, err := h.openMetadata()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	if err := h.openSlabSegments(); err != nil {
		return errors.Wrap(err, 1)
	}

	err = h.writeCapacity(N)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	
	h.hashMap = m
	h.hashMapFile = f_map

	h.metadataMap = meta
	h.metadataFile = f_meta
	
	h.CompressionEnabled = true

	//todo
	h.slabOffset = getSlabOffset(h.metadataMap)
	//xxx

	if *h.slabOffset == 0 {
		sentinel := []byte("offset")
		if err := h.writeSlab(sentinel); err != nil {
			return err
		}
		*h.slabOffset = SlabOffset(len(sentinel))
	}

	h.Capacity = N
	h.Count = getCount(h.metadataMap)
	keys := h.getKeys()
	h.Keys = &keys
	return nil
}

func (h *Hashmap) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.Folder+"/capacity", []byte(s), 0655)
}
func (h *Hashmap) readCapacity() (uint64, error) {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DEFAULTMAPSIZE, nil // Default if not found
	}
	capacity, err := strconv.ParseUint(string(dat), 10, 64)
	if err != nil {
		return 0, err
	}

	return capacity, nil
}

func (h *Hashmap) createDirectory() error {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	return nil
}
