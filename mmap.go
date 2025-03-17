package gomap

import (
	"fmt"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
)

func (h *Hashmap) openMmapHash(N uint64) (mmap.MMap, *os.File, error) {
	bytes := NtoBytesHashmap(N)
	h.createDirectory()
	filename := h.Folder + "/hashkeys-" + fmt.Sprint(N)

	if !doesFileExist(filename) {
		h.createFile(filename, bytes)
	}

	mappedData, file, err := h.openMmapFile(filename)
	if err != nil {
		return nil, nil, err
	}

	// h.mlock(mappedData) // todo see if matters

	return mappedData, file, err
}

func (h *Hashmap) openMmapFile(filename string) (mmap.MMap, *os.File, error) {
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

func (h *Hashmap) createFile(filename string, bytes int64) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("2", errors.Wrap(err, 1))
	}
	f.Seek(bytes-1, 0)
	f.Write([]byte("\x00"))
	f.Seek(0, 0)
	f.Sync()
	f.Close()
}
