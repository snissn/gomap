package hashdb

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

func (h *DB) initN(folder string, N uint64) error {
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

	if h.resizeThreshold == 0 {
		h.resizeThreshold = 65
	}

	//todo
	h.slabOffset = getSlabOffset(h.metadataMap)
	//xxx

	// Ensure slabOffset matches actual file size to avoid desync on crash recovery.
	// The file system size is the source of truth for where we append.
	diskOffset := SlabOffset((uint64(h.activeSegmentId) << OffsetBits) | uint64(h.activeSegmentSize))

	// Always sync memory offset to disk offset.
	// If metadata < disk: we crashed after write but before metadata update. Sync to disk (safe).
	// If metadata > disk: we allocated offset but crashed before write (or filesystem truncated).
	//    We must reset to disk size so next write offset matches file position.
	*h.slabOffset = diskOffset

	if *h.slabOffset == 0 {
		sentinel := []byte("offset")
		if err := h.writeSlab(sentinel); err != nil {
			return err
		}
		*h.slabOffset = SlabOffset(len(sentinel))
	}

	h.Capacity = N
	h.Count = getCount(h.metadataMap)

	// Controls are the first N bytes
	ctrlPtr := (*byte)(unsafe.Pointer(&h.hashMap[0]))
	controls := unsafe.Slice(ctrlPtr, N)
	h.Controls = &controls

	// Keys are after controls
	keyPtr := (*Key)(unsafe.Pointer(&h.hashMap[N]))
	keys := unsafe.Slice(keyPtr, N)
	h.Keys = &keys
	return nil
}

func (h *DB) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.Folder+"/capacity", []byte(s), 0655)
}
func (h *DB) readCapacity() (uint64, error) {
	dat, err := os.ReadFile(h.Folder + "/capacity")
	if err != nil {
		return DefaultCapacity, nil // Default if not found
	}
	capacity, err := strconv.ParseUint(string(dat), 10, 64)
	if err != nil {
		return 0, err
	}

	return capacity, nil
}

func (h *DB) createDirectory() error {
	err := os.MkdirAll(h.Folder, 0755)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	return nil
}
