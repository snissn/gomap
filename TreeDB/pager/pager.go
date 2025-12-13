package pager

import (
	"errors" // Added import
	"fmt"
	"os"
	"sync"

	"github.com/snissn/gomap-gemini/TreeDB/page"
	"golang.org/x/sys/unix"
)

var (
	ErrPageOutOfBounds = errors.New("page index out of bounds") // Added declaration
	ErrFileSize        = errors.New("file size is not a multiple of chunk size")
)

// Pager manages the index.db file using chunked mmap.
type Pager struct {
	file        *os.File
	chunks      [][]byte
	chunkSize   int64
	numPages    uint64 // The number of pages logically allocated
	dirtyChunks map[int]struct{}
	mu          sync.RWMutex
	path        string
	verifiedBits []uint64 // Bitset: 1 = Verified, 0 = Unverified
}

// Open opens the pager at the given path.
// If the file doesn't exist, it creates it.
// chunkSize determines the size of each mmap region.
func Open(path string, chunkSize int64) (*Pager, error) {
	if chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of page size (%d)", page.PageSize)
	}
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
		file:        f,
		chunkSize:   chunkSize,
		path:        path,
		dirtyChunks: make(map[int]struct{}),
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
		
		// Initial guess for numPages (will be corrected by DB recovery)
		p.numPages = uint64(size / page.PageSize)
		
		// Initialize Bitset
		p.resizeBitset(p.numPages)
	} else {
		// Initialize Bitset (empty)
		p.verifiedBits = make([]uint64, 0)
	}

	return p, nil
}

func (p *Pager) resizeBitset(numPages uint64) {
	needed := (numPages + 63) / 64
	current := uint64(len(p.verifiedBits))
	if needed > current {
		newBits := make([]uint64, needed)
		copy(newBits, p.verifiedBits)
		p.verifiedBits = newBits
	}
}

// IsVerified returns true if the page has passed CRC verification.
// Thread-safe (protected by p.mu in caller, or we can add internal locking if needed, 
// but Pager methods usually hold lock).
// Currently Pager.Get holds RLock.
func (p *Pager) IsVerified(pageID uint64) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isVerifiedLocked(pageID)
}

func (p *Pager) isVerifiedLocked(pageID uint64) bool {
	idx := pageID / 64
	bit := uint64(1) << (pageID % 64)
	if idx < uint64(len(p.verifiedBits)) {
		return (p.verifiedBits[idx] & bit) != 0
	}
	return false
}

// MarkVerified marks a page as verified.
func (p *Pager) MarkVerified(pageID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.resizeBitset(pageID + 1) // Ensure capacity
	idx := pageID / 64
	bit := uint64(1) << (pageID % 64)
	p.verifiedBits[idx] |= bit
}

// MarkUnverified marks a page as unverified (dirty/reused).
func (p *Pager) MarkUnverified(pageID uint64) {
	p.mu.Lock() // Must be Lock (write)
	// Caller likely holds Lock?
	// If caller holds Lock (e.g. GetForWrite), we can't Lock again.
	// But GetForWrite holds p.mu.Lock.
	// So we need an internal helper.
	p.markUnverifiedLocked(pageID)
	p.mu.Unlock()
}

func (p *Pager) markUnverifiedLocked(pageID uint64) {
	idx := pageID / 64
	bit := uint64(1) << (pageID % 64)
	if idx < uint64(len(p.verifiedBits)) {
		p.verifiedBits[idx] &^= bit
	}
}

// SetPageCount updates the logical page count. 
// Should be called by the DB layer after recovery.
func (p *Pager) SetPageCount(count uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.numPages = count
	p.resizeBitset(count)
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

// Truncate resizes the file to the specified number of pages.
// Safety: Shrinking is forbidden. Only growing is allowed.
func (p *Pager) Truncate(targetPages uint64) error {
	p.mu.Lock()
	currentPages := p.numPages
	p.mu.Unlock() 
	
	if targetPages < currentPages {
		return fmt.Errorf("truncation (shrinking) is forbidden: current %d, target %d", currentPages, targetPages)
	}
	
	if targetPages == currentPages {
		return nil
	}
	
	// diff is guaranteed positive
	diff := int(targetPages - currentPages)
	_, err := p.Alloc(diff)
	return err
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
	p.resizeBitset(newTotal)
	return startID, nil
}

// GetForWrite returns the byte slice for the given page ID and marks the chunk dirty.
func (p *Pager) GetForWrite(pageID uint64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID >= p.numPages {
		return nil, ErrPageOutOfBounds
	}
	
	p.markUnverifiedLocked(pageID) // Invalidate cache

	byteOffset := int64(pageID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offsetInChunk := byteOffset % p.chunkSize

	if chunkIdx >= len(p.chunks) {
		return nil, ErrPageOutOfBounds
	}

	p.dirtyChunks[chunkIdx] = struct{}{}

	chunk := p.chunks[chunkIdx]
	return chunk[offsetInChunk : offsetInChunk+page.PageSize], nil
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

// ReadPage returns a copy of the page data.
// Safe for concurrent use including checksum verification.
func (p *Pager) ReadPage(pageID uint64) ([]byte, error) {
	src, err := p.Get(pageID)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, nil
}

// Write copies data into the page.
// The data slice must be exactly PageSize bytes (or less, but we usually write full pages).
func (p *Pager) Write(pageID uint64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID >= p.numPages {
		return ErrPageOutOfBounds
	}
	
	p.markUnverifiedLocked(pageID) // Invalidate cache

	byteOffset := int64(pageID) * int64(page.PageSize)
	chunkIdx := byteOffset / p.chunkSize
	offsetInChunk := byteOffset % p.chunkSize

	if int(chunkIdx) >= len(p.chunks) {
		return ErrPageOutOfBounds
	}

	// Mark chunk as dirty
	p.dirtyChunks[int(chunkIdx)] = struct{}{}

	chunk := p.chunks[chunkIdx]
	dst := chunk[offsetInChunk : offsetInChunk+page.PageSize]
	copy(dst, data)
	return nil
}

// Sync msyncs the memory maps to disk.
func (p *Pager) Sync() error {
	p.mu.Lock()
	// Copy dirty list and clear
	toSync := make([]int, 0, len(p.dirtyChunks))
	for idx := range p.dirtyChunks {
		toSync = append(toSync, idx)
	}
	// We clear the map now. If Msync fails, we must restore these.
	p.dirtyChunks = make(map[int]struct{})
	p.mu.Unlock()
	
	// Perform msync under read lock
	p.mu.RLock()
	// We defer RUnlock to ensure we hold it during the entire sync process
	// including file sync.
	
	var syncErr error
	for _, idx := range toSync {
		if idx < len(p.chunks) {
			if err := unix.Msync(p.chunks[idx], unix.MS_SYNC); err != nil {
				syncErr = err
				break // Stop on first error
			}
		}
	}
	
	if syncErr == nil {
		if err := p.file.Sync(); err != nil {
			syncErr = err
		}
	}
	p.mu.RUnlock()

	// If error occurred, restore the dirty chunks
	if syncErr != nil {
		p.mu.Lock()
		for _, idx := range toSync {
			p.dirtyChunks[idx] = struct{}{}
		}
		p.mu.Unlock()
		return syncErr
	}

	return nil
}