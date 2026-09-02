package hashdb

import (
	"errors"

	"github.com/edsrzf/mmap-go"
)

// Sync flushes mmap-backed index/metadata files and fsyncs slab segments.
// It is safe to call multiple times.
func (h *DB) Sync() error {
	var errs []error

	for _, f := range h.slabFiles {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			errs = append(errs, err)
		}
	}

	errs = append(errs, flushMmapFile(h.controlMap, h.controlFile)...)
	errs = append(errs, flushMmapFile(h.keyMap, h.keyFile)...)
	errs = append(errs, flushMmapFile(h.metadataMap, h.metadataFile)...)

	return errors.Join(errs...)
}

func flushMmapFile(m mmap.MMap, f fileSyncer) []error {
	var errs []error
	if m != nil {
		if err := m.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if f != nil {
		if err := f.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

type fileSyncer interface {
	Sync() error
}

func (h *DB) needsRecoveryOnOpen() (bool, error) {
	versionPtr, err := h.metadataVersionPtr()
	if err != nil {
		return false, err
	}
	statePtr, err := h.metadataStatePtr()
	if err != nil {
		return false, err
	}

	// Unknown/legacy metadata versions should rebuild the index by scanning the log.
	if *versionPtr != metadataVersionCurrent {
		return true, nil
	}

	// Anything other than "clean" means the previous process didn't close cleanly.
	// Recovery truncates torn slab records and rebuilds the mmap index from the slab log.
	return *statePtr != metadataStateClean, nil
}

func (h *DB) markDirty() error {
	versionPtr, err := h.metadataVersionPtr()
	if err != nil {
		return err
	}
	statePtr, err := h.metadataStatePtr()
	if err != nil {
		return err
	}

	*versionPtr = metadataVersionCurrent
	*statePtr = metadataStateDirty
	return h.flushMetadata()
}

func (h *DB) markClean() error {
	versionPtr, err := h.metadataVersionPtr()
	if err != nil {
		return err
	}
	statePtr, err := h.metadataStatePtr()
	if err != nil {
		return err
	}

	*versionPtr = metadataVersionCurrent
	*statePtr = metadataStateClean
	return h.flushMetadata()
}

func (h *DB) flushMetadata() error {
	if h.metadataMap == nil || h.metadataFile == nil {
		return nil
	}
	var errs []error
	if err := h.metadataMap.Flush(); err != nil {
		errs = append(errs, err)
	}
	if err := h.metadataFile.Sync(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
