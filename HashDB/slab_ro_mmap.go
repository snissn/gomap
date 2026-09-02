package hashdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/edsrzf/mmap-go"
)

func (h *DB) closeSlabReadOnlyMaps() error {
	var firstErr error
	recordErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for segmentID, m := range h.slabROMaps {
		if m != nil {
			recordErr(m.Unmap())
		}
		delete(h.slabROMaps, segmentID)
	}
	for segmentID, f := range h.slabROFiles {
		if f != nil {
			recordErr(f.Close())
		}
		delete(h.slabROFiles, segmentID)
	}

	return firstErr
}

var errSlabSegmentNotSealed = errors.New("slab segment is not sealed")

func (h *DB) slabReadOnlyMap(segmentID uint16) (mmap.MMap, error) {
	if h.dir == "" {
		return nil, fmt.Errorf("slab mmap: db dir required")
	}
	if segmentID >= h.activeSegmentID {
		return nil, errSlabSegmentNotSealed
	}

	if h.slabROMaps == nil {
		h.slabROMaps = make(map[uint16]mmap.MMap)
	}
	if h.slabROFiles == nil {
		h.slabROFiles = make(map[uint16]*os.File)
	}

	if m := h.slabROMaps[segmentID]; m != nil {
		return m, nil
	}

	// Open a separate read-only fd; the active writer fd for this segment may be
	// open with write flags, and we want the mmap to be RDONLY.
	path := filepath.Join(h.dir, fmt.Sprintf("slab-%d", segmentID))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	h.slabROFiles[segmentID] = f
	h.slabROMaps[segmentID] = m
	return m, nil
}
