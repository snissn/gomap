package gomap

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"github.com/edsrzf/mmap-go"
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

	//h.mlock(mappedData)

	return mappedData, file, err
}

func (h *Hashmap) openMmapFile(filename string) (mmap.MMap, *os.File, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat file %s: %w", filename, err)
	}

	// Advise the kernel that we intend to access the file randomly
	// and we want to avoid page cache pollution.
	if err := unix.Fadvise(int(file.Fd()), 0, int64(fi.Size()), unix.FADV_RANDOM|unix.FADV_DONTNEED); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	// mmap the whole file into memory with read-write permissions.
	// This avoids copy-on-write overhead and ensures that the file is never modified.
	data, err := unix.Mmap(int(file.Fd()), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	// Advise the kernel to keep the whole file in memory and avoid swapping.
	if err := unix.Madvise(data, unix.MADV_WILLNEED); err != nil {
		unix.Munmap(data)
		file.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	return data, file, nil
}
