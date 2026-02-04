package pager

import (
	"errors" // Added import
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrPageOutOfBounds = errors.New("page index out of bounds") // Added declaration
	ErrFileSize        = errors.New("file size is not a multiple of page size")
	ErrReadOnly        = errors.New("pager is read-only")
)

type chunkList struct {
	data [][]byte
}

type verifiedBitset struct {
	chunks [][]uint64
}

const verifiedChunkPages = 1 << 16 // 65536 pages per chunk (8KiB bitset)
const verifiedChunkWords = verifiedChunkPages / 64

type OpenOptions struct {
	// MmapPopulate enables MAP_POPULATE on Linux to pre-fault page tables for
	// mmapped index chunks (best-effort; ignored on non-Linux).
	MmapPopulate bool
}

// Pager manages the index.db file using chunked mmap.
type Pager struct {
	file         *os.File
	chunks       [][]byte
	atomicChunks atomic.Pointer[chunkList] // Lock-free view for Get
	chunkSize    int64
	numPages     atomic.Uint64 // The number of pages logically allocated
	dirtyChunks  map[int]struct{}
	mu           sync.RWMutex
	allocMu      sync.Mutex
	path         string
	readOnly     bool
	verified     atomic.Pointer[verifiedBitset]
	verifyOnRead atomic.Bool

	growMu       sync.Mutex
	growStopOnce sync.Once
	growStop     chan struct{}
	growWake     chan struct{}
	growDone     chan struct{}
	growTarget   atomic.Int64 // byte capacity target (aligned to chunkSize by grower)

	syncConcurrency atomic.Int32

	mmapPopulate bool
}

// Open opens the pager at the given path.
// If the file doesn't exist, it creates it.
// chunkSize determines the size of each mmap region.
func Open(path string, chunkSize int64) (*Pager, error) {
	return OpenWithOptions(path, chunkSize, OpenOptions{})
}

// OpenWithOptions opens the pager at the given path with optional mmap behavior
// controls (Linux-only flags may be ignored on other platforms).
func OpenWithOptions(path string, chunkSize int64, opts OpenOptions) (*Pager, error) {
	if chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of page size (%d)", page.PageSize)
	}
	if chunkSize%int64(os.Getpagesize()) != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of OS page size (%d)", os.Getpagesize())
	}
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of mmap allocation granularity (%d)", gran)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if err := mmapAvailable(); err != nil {
		_ = f.Close()
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	size := info.Size()

	p := &Pager{
		file:         f,
		chunkSize:    chunkSize,
		path:         path,
		dirtyChunks:  make(map[int]struct{}),
		mmapPopulate: opts.MmapPopulate,
	}
	p.syncConcurrency.Store(1)

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
			data, err := mmapFile(f.Fd(), i*chunkSize, int(chunkSize), opts.MmapPopulate)
			if err != nil {
				p.Close()
				return nil, err
			}
			madviseChunk(data)
			p.chunks[i] = data
		}

		p.atomicChunks.Store(&chunkList{data: p.chunks})

		// Initial guess for numPages (will be corrected by DB recovery)
		p.numPages.Store(uint64(size / page.PageSize))

		// Initialize verified bitset
		p.ensureVerifiedCapacityLocked(p.numPages.Load())
	} else {
		// Initialize verified bitset (empty)
		p.ensureVerifiedCapacityLocked(0)
		p.atomicChunks.Store(&chunkList{data: nil})
	}

	p.startGrower()
	return p, nil
}

// OpenReadOnly opens an existing pager at path without modifying the underlying file.
//
// The returned pager does not support Alloc/GetForWrite/Write/Sync.
func OpenReadOnly(path string, chunkSize int64) (*Pager, error) {
	return OpenReadOnlyWithOptions(path, chunkSize, OpenOptions{})
}

func OpenReadOnlyWithOptions(path string, chunkSize int64, opts OpenOptions) (*Pager, error) {
	if chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of page size (%d)", page.PageSize)
	}
	if chunkSize%int64(os.Getpagesize()) != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of OS page size (%d)", os.Getpagesize())
	}
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		return nil, fmt.Errorf("chunk size must be a multiple of mmap allocation granularity (%d)", gran)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := mmapAvailable(); err != nil {
		_ = f.Close()
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	size := info.Size()
	if size%int64(page.PageSize) != 0 {
		_ = f.Close()
		return nil, ErrFileSize
	}

	p := &Pager{
		file:         f,
		chunkSize:    chunkSize,
		path:         path,
		readOnly:     true,
		mmapPopulate: opts.MmapPopulate,
	}

	if size > 0 {
		numChunks := int64((size + chunkSize - 1) / chunkSize)
		p.chunks = make([][]byte, numChunks)

		for i := int64(0); i < numChunks; i++ {
			offset := i * chunkSize
			length := int(chunkSize)
			remaining := size - offset
			if remaining < int64(length) {
				length = int(remaining)
			}
			data, err := mmapFileReadOnly(f.Fd(), offset, length, opts.MmapPopulate)
			if err != nil {
				p.Close()
				return nil, err
			}
			madviseChunk(data)
			p.chunks[i] = data
		}

		p.atomicChunks.Store(&chunkList{data: p.chunks})
		p.numPages.Store(uint64(size / int64(page.PageSize)))
		p.ensureVerifiedCapacityLocked(p.numPages.Load())
	} else {
		p.ensureVerifiedCapacityLocked(0)
		p.atomicChunks.Store(&chunkList{data: nil})
	}

	return p, nil
}

func (p *Pager) ensureVerifiedCapacityLocked(numPages uint64) {
	needChunks := int((numPages + verifiedChunkPages - 1) / verifiedChunkPages)
	if needChunks <= 0 {
		if p.verified.Load() == nil {
			p.verified.Store(&verifiedBitset{chunks: nil})
		}
		return
	}

	cur := p.verified.Load()
	if cur != nil && len(cur.chunks) >= needChunks {
		return
	}

	var oldChunks [][]uint64
	if cur != nil {
		oldChunks = cur.chunks
	}

	newChunks := make([][]uint64, needChunks)
	copy(newChunks, oldChunks)
	for i := len(oldChunks); i < needChunks; i++ {
		newChunks[i] = make([]uint64, verifiedChunkWords)
	}
	p.verified.Store(&verifiedBitset{chunks: newChunks})
}

// IsVerified returns true if the page has passed CRC verification.
// Thread-safe (protected by p.mu in caller, or we can add internal locking if needed,
// but Pager methods usually hold lock).
// Currently Pager.Get holds RLock.
func (p *Pager) IsVerified(pageID uint64) bool {
	vb := p.verified.Load()
	if vb == nil {
		return false
	}
	chunkIdx := int(pageID / verifiedChunkPages)
	if chunkIdx < 0 || chunkIdx >= len(vb.chunks) {
		return false
	}
	wordIdx := (pageID % verifiedChunkPages) / 64
	bit := uint64(1) << (pageID % 64)
	word := atomic.LoadUint64(&vb.chunks[chunkIdx][wordIdx])
	return (word & bit) != 0
}

// VerifyOnRead reports whether checksum verification should happen on every read.
func (p *Pager) VerifyOnRead() bool {
	return p.verifyOnRead.Load()
}

// SetVerifyOnRead enables or disables checksum verification on every read.
func (p *Pager) SetVerifyOnRead(always bool) {
	p.verifyOnRead.Store(always)
}

// MarkVerified marks a page as verified.
func (p *Pager) MarkVerified(pageID uint64) {
	for {
		vb := p.verified.Load()
		if vb == nil {
			p.mu.Lock()
			p.ensureVerifiedCapacityLocked(pageID + 1)
			p.mu.Unlock()
			continue
		}
		chunkIdx := int(pageID / verifiedChunkPages)
		if chunkIdx < 0 || chunkIdx >= len(vb.chunks) {
			p.mu.Lock()
			p.ensureVerifiedCapacityLocked(pageID + 1)
			p.mu.Unlock()
			continue
		}

		wordIdx := (pageID % verifiedChunkPages) / 64
		bit := uint64(1) << (pageID % 64)
		addr := &vb.chunks[chunkIdx][wordIdx]
		for {
			old := atomic.LoadUint64(addr)
			if old&bit != 0 {
				return
			}
			if atomic.CompareAndSwapUint64(addr, old, old|bit) {
				return
			}
		}
	}
}

// MarkUnverified marks a page as unverified (dirty/reused).
func (p *Pager) MarkUnverified(pageID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markUnverifiedLocked(pageID)
}

func (p *Pager) markUnverifiedLocked(pageID uint64) {
	p.ensureVerifiedCapacityLocked(pageID + 1)
	vb := p.verified.Load()
	if vb == nil {
		return
	}
	chunkIdx := int(pageID / verifiedChunkPages)
	if chunkIdx < 0 || chunkIdx >= len(vb.chunks) {
		return
	}
	wordIdx := (pageID % verifiedChunkPages) / 64
	bit := uint64(1) << (pageID % 64)
	addr := &vb.chunks[chunkIdx][wordIdx]
	for {
		old := atomic.LoadUint64(addr)
		if old&bit == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(addr, old, old&^bit) {
			return
		}
	}
}

// SetPageCount updates the logical page count.
// Should be called by the DB layer after recovery.
func (p *Pager) SetPageCount(count uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.numPages.Store(count)
	p.ensureVerifiedCapacityLocked(count)
}

// PageCount returns the current logical number of pages.
func (p *Pager) PageCount() uint64 {
	return p.numPages.Load()
}

// Close closes the pager and unmaps memory.
func (p *Pager) Close() error {
	p.stopGrower()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, chunk := range p.chunks {
		if err := munmapFile(chunk); err != nil {
			return err
		}
	}
	p.chunks = nil
	p.atomicChunks.Store(nil)
	return p.file.Close()
}

// Truncate resizes the file to the specified number of pages.
// Safety: Shrinking is forbidden. Only growing is allowed.
func (p *Pager) Truncate(targetPages uint64) error {
	if p.readOnly {
		return ErrReadOnly
	}
	currentPages := p.numPages.Load()

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
	if p.readOnly {
		return 0, ErrReadOnly
	}
	p.allocMu.Lock()
	defer p.allocMu.Unlock()

	p.mu.Lock()
	startID := p.numPages.Load()
	newTotal := startID + uint64(count)

	// Check if we need to grow physical file
	requiredBytes := int64(newTotal) * int64(page.PageSize)
	currentCapacity := int64(len(p.chunks)) * p.chunkSize
	p.mu.Unlock()

	if requiredBytes > currentCapacity {
		if err := p.growToCapacity(requiredBytes); err != nil {
			return 0, err
		}
	}

	p.mu.Lock()
	p.numPages.Store(newTotal)
	p.ensureVerifiedCapacityLocked(newTotal)
	p.mu.Unlock()
	p.maybeSchedulePreGrow(requiredBytes)
	return startID, nil
}

// GetForWrite returns the byte slice for the given page ID and marks the chunk dirty.
func (p *Pager) GetForWrite(pageID uint64) ([]byte, error) {
	if p.readOnly {
		return nil, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID >= p.numPages.Load() {
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
	// Optimization: Lock-free read path
	// p.numPages is atomic. p.atomicChunks is atomic.
	// We read chunks THEN numPages to ensure if numPages says OK, chunks must be ready.
	// Wait, earlier I said "Alloc MUST update chunks first, THEN numPages".
	// So Reader must read numPages first?
	// If reader reads numPages=100.
	// Then reads chunks. If chunks hasn't been updated yet?
	// Impossible if Alloc updates chunks BEFORE numPages.
	// So Reader reads numPages -> 100.
	// Alloc updated chunks (len=10) -> numPages=100.
	// Reader reads chunks -> len=10. OK.
	// What if reader reads chunks first?
	// Reader reads chunks (len=10).
	// Reader reads numPages (100).
	// If chunkIdx=9, OK.
	// So order in Reader doesn't strictly matter as long as bounds check uses the loaded chunks len.

	limit := p.numPages.Load()
	if pageID >= limit {
		return nil, ErrPageOutOfBounds
	}

	byteOffset := int64(pageID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offsetInChunk := byteOffset % p.chunkSize

	cl := p.atomicChunks.Load()
	if cl == nil {
		return nil, ErrPageOutOfBounds
	}
	chunks := cl.data

	if chunkIdx >= len(chunks) {
		return nil, ErrPageOutOfBounds
	}

	chunk := chunks[chunkIdx]
	return chunk[offsetInChunk : offsetInChunk+page.PageSize], nil
}

// SetSyncConcurrency configures how many goroutines may msync dirty chunks
// in parallel. Values <= 0 default to 1.
func (p *Pager) SetSyncConcurrency(n int) {
	if n <= 0 {
		n = 1
	}
	p.syncConcurrency.Store(int32(n))
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
	if p.readOnly {
		return ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID >= p.numPages.Load() {
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
	if p.readOnly {
		return ErrReadOnly
	}
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
	concurrency := int(p.syncConcurrency.Load())
	if concurrency <= 1 || len(toSync) <= 1 {
		for _, idx := range toSync {
			if idx < len(p.chunks) {
				if err := msyncFile(p.chunks[idx]); err != nil {
					syncErr = err
					break // Stop on first error
				}
			}
		}
	} else {
		if concurrency > len(toSync) {
			concurrency = len(toSync)
		}
		var (
			wg    sync.WaitGroup
			jobs  = make(chan int)
			errCh = make(chan error, 1)
		)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					if idx >= len(p.chunks) {
						continue
					}
					if err := msyncFile(p.chunks[idx]); err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}
			}()
		}
		for _, idx := range toSync {
			jobs <- idx
		}
		close(jobs)
		wg.Wait()
		select {
		case syncErr = <-errCh:
		default:
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
