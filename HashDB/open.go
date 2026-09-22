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

func (h *DB) initN(folder string, N uint64) (err error) {
	h.dir = folder

	// Create directory is handled inside openMmapHash if needed, but safer here.
	if err := h.createDirectory(); err != nil {
		return err
	}

	controlMap, controlFile, keyMap, keyFile, err := h.openIndexMaps(N)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	h.controlMap = controlMap
	h.controlFile = controlFile
	h.keyMap = keyMap
	h.keyFile = keyFile

	// Controls are stored in a separate mmap.
	h.controls = []byte(h.controlMap)

	// Keys are stored in a separate mmap.
	keyPtr := (*Key)(unsafe.Pointer(&h.keyMap[0]))
	h.keys = unsafe.Slice(keyPtr, N)

	defer func() {
		if err != nil {
			_ = h.closeFPs()
		}
	}()

	if err := h.applyIndexMemoryPolicy([]byte(h.controlMap), []byte(h.keyMap)); err != nil {
		return err
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

	h.metadataMap = meta
	h.metadataFile = f_meta

	h.compressionEnabled = true

	if h.resizeThreshold == 0 {
		h.resizeThreshold = 65
	}

	//todo
	h.slabOffset = getSlabOffset(h.metadataMap)
	//xxx

	// Ensure slabOffset matches actual file size to avoid desync on crash recovery.
	// The file system size is the source of truth for where we append.
	diskOffset := SlabOffset((uint64(h.activeSegmentID) << OffsetBits) | uint64(h.activeSegmentSize))

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

	h.capacity = N
	h.count = getCount(h.metadataMap)
	return nil
}

func (h *DB) writeCapacity(N uint64) error {
	s := strconv.FormatUint(N, 10)
	return os.WriteFile(h.dir+"/capacity", []byte(s), 0o644)
}
func (h *DB) readCapacity() (uint64, error) {
	dat, err := os.ReadFile(h.dir + "/capacity")
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
	err := os.MkdirAll(h.dir, 0o755)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	return nil
}
