package pager

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap-gemini/TreeDB/page"
	"golang.org/x/sys/unix"
)

var (
	ErrPageOutOfBounds = errors.New("page index out of bounds")
	ErrFileSize        = errors.New("file size is not a multiple of chunk size")
)

// Pager manages the index.db file using chunked mmap.
type Pager struct {
	file      *os.File
	chunks    [][]byte
	chunkSize int64
	numPages  uint64 // The number of pages logically allocated
	mu        sync.RWMutex
	path      string
}

// Open opens the pager at the given path.
// If the file doesn't exist, it creates it.
// chunkSize determines the size of each mmap region.
func Open(path string, chunkSize int64) (*Pager, error) {
	if chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of page size (%d)", page.PageSize)
	}
	if chunkSize%int64(os.Getpagesize()) != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of OS page size (%d)", os.Getpagesize())
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	size := info.Size()
	
	p := &Pager{
		file:      f,
		chunkSize: chunkSize,
		path:      path,
	}

	if size > 0 {
		// Align size to chunk size if needed
		if size%chunkSize != 0 {
			newSize := ((size / chunkSize) + 1) * chunkSize
			if err := f.Truncate(newSize); err != nil {
				_ = f.Close()
				return nil, err
			}
			size = newSize
		}

		numChunks := size / chunkSize
		p.chunks = make([][]byte, numChunks)

		for i := int64(0); i < numChunks; i++ {
			// unix.Mmap(fd int, offset int64, length int, prot int, flags int)
			data, err := unix.Mmap(int(f.Fd()), i*chunkSize, int(chunkSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				p.Close()
				return nil, err
			}
			p.chunks[i] = data
		}
	}

	return p, nil
}

// SetPageCount updates the logical page count. 
// Should be called by the DB layer after recovery.
func (p *Pager) SetPageCount(count uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.numPages = count
}

// PageCount returns the current logical number of pages.
func (p *Pager) PageCount() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numPages
}

// Close closes the pager and unmaps memory.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, chunk := range p.chunks {
		if err := unix.Munmap(chunk); err != nil {
			return err
		}
	}
	p.chunks = nil
	return p.file.Close()
}

// Alloc allocates `count` new pages and returns the ID of the first one.
// It grows the file if necessary.
func (p *Pager) Alloc(count int) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	startID := p.numPages
	newTotal := startID + uint64(count)

	// Check if we need to grow physical file
	requiredBytes := int64(newTotal) * int64(page.PageSize)
	currentCapacity := int64(len(p.chunks)) * p.chunkSize

	if requiredBytes > currentCapacity {
		// Calculate how many new chunks needed
		// We align to ChunkSize
		needed := requiredBytes - currentCapacity
		chunksNeeded := (needed + p.chunkSize - 1) / p.chunkSize
		newCapacity := currentCapacity + (chunksNeeded * p.chunkSize)

		// Grow file
		if err := p.file.Truncate(newCapacity); err != nil {
			return 0, err
		}

		// Map new chunks
		for i := int64(0); i < chunksNeeded; i++ {
			offset := currentCapacity + (i * p.chunkSize)
			data, err := unix.Mmap(int(p.file.Fd()), offset, int(p.chunkSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				return 0, err
			}
			p.chunks = append(p.chunks, data)
		}
	}

	p.numPages = newTotal
	return startID, nil
}

// Get returns the byte slice for the given page ID.
// CAUTION: The returned slice points directly to mmapped memory.
// Do not hold references to it after closing the pager.
func (p *Pager) Get(pageID uint64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if pageID >= p.numPages {
		return nil, ErrPageOutOfBounds
	}

	byteOffset := int64(pageID) * int64(page.PageSize)
	chunkIdx := byteOffset / p.chunkSize
	offsetInChunk := byteOffset % p.chunkSize

	if int(chunkIdx) >= len(p.chunks) {
		return nil, ErrPageOutOfBounds
	}

	chunk := p.chunks[chunkIdx]
	return chunk[offsetInChunk : offsetInChunk+page.PageSize], nil
}

// Write copies data into the page.
// The data slice must be exactly PageSize bytes (or less, but we usually write full pages).
func (p *Pager) Write(pageID uint64, data []byte) error {
	dst, err := p.Get(pageID)
	if err != nil {
		return err
	}
	copy(dst, data)
	return nil
}

// Sync msyncs the memory maps to disk.
func (p *Pager) Sync() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	// MS_SYNC is synchronous.
	for _, chunk := range p.chunks {
		if err := unix.Msync(chunk, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return nil
}