package hashdb

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
)

func NtoBytesHashmap(N uint64) int64 {
	i := Key{}
	// N bytes for controls + N * sizeof(Key) for keys
	return int64(N) + int64(unsafe.Sizeof(i))*int64(N)
}

func (h *DB) openMmapHash(N uint64) (mmap.MMap, *os.File, error) {
	bytes := NtoBytesHashmap(N)
	if err := h.createDirectory(); err != nil {
		return nil, nil, err
	}
	filename := h.dir + "/hashkeys-" + fmt.Sprint(N)

	if !doesFileExist(filename) {
		if err := h.createFile(filename, bytes); err != nil {
			return nil, nil, err
		}
	}

	mappedData, file, err := h.openMmapFile(filename)
	if err != nil {
		return nil, nil, err
	}

	// h.mlock(mappedData) // todo see if matters

	return mappedData, file, err
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
	f.Seek(bytes-1, 0)
	f.Write([]byte("\x00"))
	f.Seek(0, 0)
	f.Sync()
	f.Close()
	return nil
}
