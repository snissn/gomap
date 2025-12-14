package hashdb

import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
)

func bytesForControls(N uint64) int64 {
	return int64(N)
}

func bytesForKeys(N uint64) int64 {
	return int64(unsafe.Sizeof(Key{})) * int64(N)
}

func (h *DB) openIndexMaps(N uint64) (mmap.MMap, *os.File, mmap.MMap, *os.File, error) {
	controlMap, controlFile, err := h.openMmapControls(N)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	keyMap, keyFile, err := h.openMmapKeys(N)
	if err != nil {
		_ = controlMap.Unmap()
		_ = controlFile.Close()
		return nil, nil, nil, nil, err
	}

	return controlMap, controlFile, keyMap, keyFile, nil
}

func (h *DB) openMmapControls(N uint64) (mmap.MMap, *os.File, error) {
	bytes := bytesForControls(N)
	if err := h.createDirectory(); err != nil {
		return nil, nil, err
	}
	filename := filepath.Join(h.dir, fmt.Sprintf("hashctl-%d", N))

	if !doesFileExist(filename) {
		if err := h.createFile(filename, bytes); err != nil {
			return nil, nil, err
		}
	}

	mappedData, file, err := h.openMmapFile(filename)
	if err != nil {
		return nil, nil, err
	}

	return mappedData, file, nil
}

func (h *DB) openMmapKeys(N uint64) (mmap.MMap, *os.File, error) {
	bytes := bytesForKeys(N)
	if err := h.createDirectory(); err != nil {
		return nil, nil, err
	}
	filename := filepath.Join(h.dir, fmt.Sprintf("hashkeys-%d", N))

	if !doesFileExist(filename) {
		if err := h.createFile(filename, bytes); err != nil {
			return nil, nil, err
		}
	}

	mappedData, file, err := h.openMmapFile(filename)
	if err != nil {
		return nil, nil, err
	}

	return mappedData, file, nil
}

func (h *DB) openMmapFile(filename string) (mmap.MMap, *os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to stat file %s: %w", filename, err)
	}

	// Apply cross-platform file access hint
	applyFadvise(int(file.Fd()), fi.Size())

	// mmap the whole file into memory with read-write permissions.
	data, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	applyMadvise(data)

	return data, file, nil
}

func (h *DB) createFile(filename string, bytes int64) error {
	f, err := os.Create(filename)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	if err := f.Truncate(bytes); err != nil {
		f.Close()
		return errors.Wrap(err, 1)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return errors.Wrap(err, 1)
	}
	if err := f.Close(); err != nil {
		return errors.Wrap(err, 1)
	}
	return nil
}
