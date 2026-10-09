package pager

import (
	"errors" // Added import
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrPageOutOfBounds = errors.New("page index out of bounds") // Added declaration
	ErrFileSize        = errors.New("file size is not a multiple of page size")
	ErrReadOnly        = errors.New("pager is read-only")
)

// testHookTruncateAfterPageCount is a package-local scheduling barrier for
// proving that overlapping grow intents do not append stale page-count deltas.
var testHookTruncateAfterPageCount func(uint64)

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
	// PrefetchOnRead enables best-effort read-side prefetch support (madvise
	// WILLNEED). Callers can trigger prefetch explicitly via PrefetchPage.
	PrefetchOnRead bool
}

type prefetchBitset struct {
	words []uint64
}

// Pager manages the index.db file using chunked mmap.
type Pager struct {
	file         *os.File
	chunks       [][]byte
	atomicChunks atomic.Pointer[chunkList] // Lock-free view for Get
	chunkSize    int64
	numPages     atomic.Uint64 // The number of pages logically allocated
	// durableFileSize is the file length covered by a completed file-wide
	// durability fence. Sparse range durability is restricted to this prefix.
	durableFileSize atomic.Int64
	dirtyChunks     dirtyChunkBits
	closing         bool
	mu              sync.RWMutex
	allocMu         sync.Mutex
	path            string
	readOnly        bool
	memoryOnly      bool
	// pageIDBase gives private overlay pagers a disjoint logical ID namespace.
	// IDs below the base are read through fallback and are never writable here.
	pageIDBase         uint64
	fallback           *Pager
	primaryBanks       atomic.Pointer[Pager]
	metadataReadMu     sync.RWMutex
	immutableReadRoots primaryReadRootTableV6 // Single accountable address/custody index.
	verified           atomic.Pointer[verifiedBitset]
	verifyOnRead       atomic.Bool

	growMu       sync.Mutex
	growStopOnce sync.Once
	growStop     chan struct{}
	growWake     chan struct{}
	growDone     chan struct{}
	growTarget   atomic.Int64 // byte capacity target (aligned to chunkSize by grower)

	syncConcurrency atomic.Int32

	mmapPopulate   bool
	prefetchOnRead bool
	prefetched     atomic.Pointer[prefetchBitset]
}

// WithStableResourceFile calls fn with the exact index handle owned by this
// pager while preventing concurrent close. The handle is callback-scoped;
// callers that need to retain it must duplicate it before fn returns.
func (p *Pager) WithStableResourceFile(fn func(*os.File) error) error {
	if p == nil || fn == nil {
		return errors.New("pager: stable resource file unavailable")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.file == nil || p.memoryOnly {
		return errors.New("pager: stable resource file unavailable")
	}
	return fn(p.file)
}

// Open opens the pager at the given path.
// If the file doesn't exist, it creates it.
// chunkSize determines the size of each mmap region.
func Open(path string, chunkSize int64) (*Pager, error) {
	return OpenWithOptions(path, chunkSize, OpenOptions{})
}

// NewOverlay returns an in-memory private pager with a disjoint logical page
// namespace. It is intended for speculative COW work whose pages are relocated
// into a durable pager only after publication validation succeeds.
func NewOverlay(chunkSize int64, pageIDBase uint64, fallback *Pager) (*Pager, error) {
	if chunkSize <= 0 || chunkSize%page.PageSize != 0 {
		return nil, fmt.Errorf("pager: overlay chunk size must be a positive multiple of %d", page.PageSize)
	}
	if pageIDBase == 0 {
		return nil, errors.New("pager: overlay page id base must be non-zero")
	}
	if fallback == nil {
		return nil, errors.New("pager: overlay fallback is required")
	}
	p := &Pager{
		chunkSize:  chunkSize,
		memoryOnly: true,
		pageIDBase: pageIDBase,
		fallback:   fallback,
	}
	p.syncConcurrency.Store(1)
	p.verifyOnRead.Store(fallback.VerifyOnRead())
	p.prefetchOnRead = fallback.prefetchOnRead
	p.numPages.Store(pageIDBase)
	p.ensurePrefetchCapacityLocked(0)
	p.ensureVerifiedCapacityLocked(0)
	p.atomicChunks.Store(&chunkList{data: nil})
	return p, nil
}

func (p *Pager) localPageID(pageID uint64) (uint64, bool) {
	if p == nil || pageID < p.pageIDBase {
		return 0, false
	}
	return pageID - p.pageIDBase, true
}

func (p *Pager) localPageCount(logicalCount uint64) uint64 {
	if logicalCount < p.pageIDBase {
		return 0
	}
	return logicalCount - p.pageIDBase
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
	p := newFilePager(f, path, chunkSize, opts)
	if err := initializeFilePager(p, opts); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

// OpenOwnedFileWithOptions consumes file immediately. diagnosticPath is used
// only for observations. If cleanup fails, the returned pager owns the exact
// remaining file/mappings and is disabled until Close completes.
func OpenOwnedFileWithOptions(file *os.File, diagnosticPath string, chunkSize int64, opts OpenOptions) (*Pager, error) {
	if file == nil {
		return nil, fmt.Errorf("nil owned pager file")
	}
	p := newFilePager(file, diagnosticPath, chunkSize, opts)
	fail := func(err error) (*Pager, error) {
		if closeErr := p.Close(); closeErr != nil {
			return p, errors.Join(err, closeErr)
		}
		return nil, err
	}
	if chunkSize <= 0 {
		return fail(fmt.Errorf("chunk size must be positive"))
	}
	if chunkSize%page.PageSize != 0 {
		return fail(fmt.Errorf("chunk size must be a multiple of page size (%d)", page.PageSize))
	}
	if chunkSize%int64(os.Getpagesize()) != 0 {
		return fail(fmt.Errorf("chunk size must be a multiple of OS page size (%d)", os.Getpagesize()))
	}
	if gran := mmapOffsetGranularity(); gran > 0 && chunkSize%gran != 0 {
		return fail(fmt.Errorf("chunk size must be a multiple of mmap allocation granularity (%d)", gran))
	}

	if err := initializeFilePager(p, opts); err != nil {
		return fail(err)
	}
	return p, nil
}

func newFilePager(file *os.File, path string, chunkSize int64, opts OpenOptions) *Pager {
	p := &Pager{file: file, path: path, chunkSize: chunkSize, mmapPopulate: opts.MmapPopulate, prefetchOnRead: opts.PrefetchOnRead}
	p.syncConcurrency.Store(1)
	return p
}

// Both entry points use the same mapping/growth initialization. The caller
// determines the legacy or transferred-file failure ownership contract.
func initializeFilePager(p *Pager, opts OpenOptions) error {
	f, chunkSize := p.file, p.chunkSize
	if err := mmapAvailable(); err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	size := info.Size()
	durableSize := size

	p.durableFileSize.Store(durableSize)

	if size > 0 {
		// Align size to chunk size if needed
		if size%chunkSize != 0 {
			newSize := ((size / chunkSize) + 1) * chunkSize
			if err := f.Truncate(newSize); err != nil {
				return err
			}
			size = newSize
		}

		numChunks := size / chunkSize
		p.chunks = make([][]byte, numChunks)
		p.ensurePrefetchCapacityLocked(int(numChunks))
		p.dirtyChunks.grow(int(numChunks))

		for i := int64(0); i < numChunks; i++ {
			data, err := mmapFile(f.Fd(), i*chunkSize, int(chunkSize), opts.MmapPopulate)
			if err != nil {
				return err
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
		p.ensurePrefetchCapacityLocked(0)
		p.ensureVerifiedCapacityLocked(0)
		p.atomicChunks.Store(&chunkList{data: nil})
	}

	p.startGrower()
	return nil
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
		file:           f,
		chunkSize:      chunkSize,
		path:           path,
		readOnly:       true,
		mmapPopulate:   opts.MmapPopulate,
		prefetchOnRead: opts.PrefetchOnRead,
	}

	if size > 0 {
		numChunks := int64((size + chunkSize - 1) / chunkSize)
		p.chunks = make([][]byte, numChunks)
		p.ensurePrefetchCapacityLocked(int(numChunks))

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
		p.ensurePrefetchCapacityLocked(0)
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

func (p *Pager) ensurePrefetchCapacityLocked(numChunks int) {
	if numChunks <= 0 {
		if p.prefetched.Load() == nil {
			p.prefetched.Store(&prefetchBitset{words: nil})
		}
		return
	}
	needWords := (numChunks + 63) / 64
	cur := p.prefetched.Load()
	if cur != nil && len(cur.words) >= needWords {
		return
	}
	next := make([]uint64, needWords)
	if cur != nil {
		// Prefetch bits are set via atomics; preserve them using atomic loads to
		// avoid races under -race.
		limit := len(cur.words)
		if limit > len(next) {
			limit = len(next)
		}
		for i := 0; i < limit; i++ {
			next[i] = atomic.LoadUint64(&cur.words[i])
		}
	}
	p.prefetched.Store(&prefetchBitset{words: next})
}

// IsVerified returns true if the page has passed CRC verification.
// Thread-safe (protected by p.mu in caller, or we can add internal locking if needed,
// but Pager methods usually hold lock).
// Currently Pager.Get holds RLock.
func (p *Pager) IsVerified(pageID uint64) bool {
	if p.immutableReadRoots.retention != nil {
		p.metadataReadMu.RLock()
		defer p.metadataReadMu.RUnlock()
	}

	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			return companion.IsVerified(pageID - page.PrimaryBankNamespace)
		}
	}
	if pageID >= PrimaryReadRootLocalBaseV6 && pageID < page.PrimaryBankNamespace {
		_, ok := p.primaryReadRootImageV6(pageID)
		return ok
	}

	localID, local := p.localPageID(pageID)
	if !local {
		return p.fallback != nil && p.fallback.IsVerified(pageID)
	}
	pageID = localID
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
	if companion := p.primaryBanks.Load(); companion != nil {
		companion.SetVerifyOnRead(always)
	}
}

// MarkVerified marks a page as verified.
func (p *Pager) MarkVerified(pageID uint64) {
	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			companion.MarkVerified(pageID - page.PrimaryBankNamespace)
			return
		}
	}
	if pageID >= PrimaryReadRootLocalBaseV6 && pageID < page.PrimaryBankNamespace {
		return
	}

	localID, local := p.localPageID(pageID)
	if !local {
		if p.fallback != nil {
			p.fallback.MarkVerified(pageID)
		}
		return
	}
	pageID = localID
	if p.immutableReadRoots.retention != nil {
		p.metadataReadMu.RLock()
		defer p.metadataReadMu.RUnlock()
		vb := p.verified.Load()
		if vb == nil || pageID/verifiedChunkPages >= uint64(len(vb.chunks)) {
			return
		}
	}
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
	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			companion.MarkUnverified(pageID - page.PrimaryBankNamespace)
			return
		}
	}
	if pageID >= PrimaryReadRootLocalBaseV6 && pageID < page.PrimaryBankNamespace {
		return
	}

	localID, local := p.localPageID(pageID)
	if !local {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markUnverifiedLocked(localID)
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
	if count < p.pageIDBase {
		count = p.pageIDBase
	}
	p.numPages.Store(count)
	p.ensureVerifiedCapacityLocked(p.localPageCount(count))
}

// PageCount returns the current logical number of pages.
func (p *Pager) PageCount() uint64 {
	return p.numPages.Load()
}

// Close closes the pager and unmaps memory.
func (p *Pager) Close() error {
	p.stopGrower()
	p.growMu.Lock()
	defer p.growMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closing = true          // Includes a partial-unmap failure; no pending install may pass.
	p.atomicChunks.Store(nil) // Do not publish partially unmapped storage on failure.
	if !p.memoryOnly {
		for i, chunk := range p.chunks {
			if chunk == nil {
				continue
			}
			if err := munmapFile(chunk); err != nil {
				return err
			}
			p.chunks[i] = nil
		}
	}
	p.chunks = nil
	p.atomicChunks.Store(nil)
	if p.memoryOnly {
		return nil
	}
	if p.file == nil {
		return nil
	}
	return p.file.Close()
}

// Truncate resizes the file to the specified number of pages.
// Safety: Shrinking is forbidden. Only growing is allowed.
func (p *Pager) Truncate(targetPages uint64) error {
	if p.readOnly {
		return ErrReadOnly
	}
	currentPages := p.numPages.Load()
	if testHookTruncateAfterPageCount != nil {
		testHookTruncateAfterPageCount(currentPages)
	}
	if targetPages < currentPages {
		return fmt.Errorf("truncation (shrinking) is forbidden: current %d, target %d", currentPages, targetPages)
	}
	return p.GrowTo(targetPages)
}

// GrowTo ensures the pager contains at least targetPages pages.
func (p *Pager) GrowTo(targetPages uint64) error {
	if p.readOnly {
		return ErrReadOnly
	}
	if targetPages <= p.numPages.Load() {
		return nil
	}
	p.allocMu.Lock()
	defer p.allocMu.Unlock()
	currentPages := p.numPages.Load()
	if targetPages <= currentPages {
		return nil
	}

	diff := int(targetPages - currentPages)
	_, err := p.allocLocked(diff)
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
	return p.allocLocked(count)
}

func (p *Pager) allocLocked(count int) (uint64, error) {
	if count < 0 {
		return 0, fmt.Errorf("pager: negative allocation")
	}
	start := p.numPages.Load()
	if uint64(count) > ^uint64(0)-start {
		return 0, fmt.Errorf("pager: page count overflow")
	}
	g, _, e := NewGrowthForPager(p, start+uint64(count), nil)
	if e != nil {
		return 0, e
	}
	for {
		done, e := g.stepLocked(p, nil, 0, 0, nil)
		if e == ErrGrowthStale {
			g, _, e = NewGrowthForPager(p, start+uint64(count), nil)
			if e != nil {
				return 0, e
			}
			continue
		}
		if e != nil {
			return 0, e
		}
		if done {
			break
		}
	}
	p.maybeSchedulePreGrow(int64(p.localPageCount(start+uint64(count))) * page.PageSize)
	return start, nil
}

// GetForWrite returns the byte slice for the given page ID and marks the chunk dirty.
func (p *Pager) GetForWrite(pageID uint64) ([]byte, error) {
	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			return companion.GetForWrite(pageID - page.PrimaryBankNamespace)
		}
	}
	if p.readOnly {
		return nil, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return nil, os.ErrClosed
	}

	localID, local := p.localPageID(pageID)
	if !local || pageID >= p.numPages.Load() {
		return nil, ErrPageOutOfBounds
	}

	p.markUnverifiedLocked(localID) // Invalidate cache

	byteOffset := int64(localID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offsetInChunk := byteOffset % p.chunkSize

	if chunkIdx >= len(p.chunks) {
		return nil, ErrPageOutOfBounds
	}

	p.dirtyChunks.set(chunkIdx)

	chunk := p.chunks[chunkIdx]
	return chunk[offsetInChunk : offsetInChunk+page.PageSize], nil
}

// Get returns the byte slice for the given page ID.
// CAUTION: The returned slice points directly to mmapped memory.
// Do not hold references to it after closing the pager.
func (p *Pager) Get(pageID uint64) ([]byte, error) {
	data, _, err := p.GetWithWork(pageID, nil)
	return data, err
}

// GetWithWork admits the actual mapping owner, page limit and chunk slot before
// reading them. The returned view belongs only to the caller's guarded cut.
// Ordinary nil work uses exactly the same mapping/fallback implementation.
func (p *Pager) GetWithWork(pageID uint64, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if ok, err := pagerReserve(work, 3, 512); !ok || err != nil {
		return nil, false, err
	}
	return p.getWithWork(pageID, work)
}

func (p *Pager) getWithWork(pageID uint64, work *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			return companion.GetWithWork(pageID-page.PrimaryBankNamespace, work)
		}
		// A speculative overlay borrows read authority from its retained source
		// pager. It has no bank write or allocation authority of its own.
		if p.fallback != nil {
			return p.fallback.GetWithWork(pageID, work)
		}
		return nil, false, ErrPageOutOfBounds
	}
	if pageID >= PrimaryReadRootLocalBaseV6 && pageID < page.PrimaryBankNamespace {
		if ok, err := pagerReserve(work, 1, 64); !ok || err != nil {
			return nil, ok, err
		}
		value, ok := p.primaryReadRootImageV6(pageID)
		if !ok {
			return nil, false, ErrPageOutOfBounds
		}
		return value, true, nil
	}
	localID, local := p.localPageID(pageID)
	if !local {
		if p.fallback != nil {
			return p.fallback.GetWithWork(pageID, work)
		}
		return nil, false, ErrPageOutOfBounds
	}
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

	if p.immutableReadRoots.retention != nil {
		p.metadataReadMu.RLock()
		defer p.metadataReadMu.RUnlock()
	}
	limit := p.numPages.Load()
	if pageID >= limit {
		return nil, false, ErrPageOutOfBounds
	}

	byteOffset := int64(localID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offsetInChunk := byteOffset % p.chunkSize

	cl := p.atomicChunks.Load()
	if cl == nil {
		return nil, false, ErrPageOutOfBounds
	}
	chunks := cl.data

	if chunkIdx >= len(chunks) {
		return nil, false, ErrPageOutOfBounds
	}

	chunk := chunks[chunkIdx]
	return chunk[offsetInChunk : offsetInChunk+page.PageSize], true, nil
}

// PrefetchPage issues a best-effort prefetch hint for the chunk containing
// pageID. It is safe for concurrent use.
func (p *Pager) PrefetchPage(pageID uint64) {
	if isPrimaryBankID(pageID) {
		if companion := p.primaryBanks.Load(); companion != nil {
			companion.PrefetchPage(pageID - page.PrimaryBankNamespace)
			return
		}
	}
	localID, local := p.localPageID(pageID)
	if !local {
		if p.fallback != nil {
			p.fallback.PrefetchPage(pageID)
		}
		return
	}
	if !p.prefetchOnRead {
		return
	}
	if p.immutableReadRoots.retention != nil {
		p.metadataReadMu.RLock()
		defer p.metadataReadMu.RUnlock()
	}
	limit := p.numPages.Load()
	if pageID >= limit {
		return
	}

	byteOffset := int64(localID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	if chunkIdx < 0 {
		return
	}

	cl := p.atomicChunks.Load()
	if cl == nil {
		return
	}
	chunks := cl.data
	if chunkIdx >= len(chunks) {
		return
	}
	p.prefetchChunkOnce(chunkIdx, chunks[chunkIdx])
}

func (p *Pager) prefetchChunkOnce(chunkIdx int, data []byte) {
	if !p.prefetchOnRead || chunkIdx < 0 {
		return
	}
	bits := p.prefetched.Load()
	if bits == nil || len(bits.words) == 0 {
		return
	}
	wordIdx := chunkIdx / 64
	bit := uint64(1) << uint(chunkIdx%64)
	if wordIdx >= len(bits.words) {
		return
	}
	for {
		cur := atomic.LoadUint64(&bits.words[wordIdx])
		if cur&bit != 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&bits.words[wordIdx], cur, cur|bit) {
			madviseWillNeedChunk(data)
			return
		}
	}
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
	_, err := p.WriteWithWork(pageID, data, nil)
	return err
}

// FlushDirtyChunksFrom synchronously writes dirty memory-map chunks at or
// above firstChunk to the backing file without issuing a file sync. Lower
// chunks remain dirty for a later Sync at the durability boundary.
func (p *Pager) FlushDirtyChunksFrom(firstChunk int) error {
	if firstChunk < 0 {
		firstChunk = 0
	}
	return p.syncDirtyChunks(false, firstChunk)
}

// Sync makes mapped writes durable through the backing file, with explicit
// mapped-range flushes where required by the platform.
func (p *Pager) Sync() error {
	return p.syncDirtyChunksWithFile(true, 0, p.file)
}

// SyncIndexData is the production index-publication data barrier. It performs
// the same durable pager sync as Sync while exposing the named before/after
// boundary used by the power-loss oracle. Meta-page syncs continue to use Sync
// and therefore are not mislabeled as pre-publication index-data barriers.
func (p *Pager) SyncIndexData() error {
	return p.SyncIndexDataWithStableFile(p.file)
}

// SyncIndexDataWithStableFile is the index-publication data barrier bound to
// an exact retained index handle. While the pager is live it claims dirty mmap
// chunks and syncs that handle, flushing mapped ranges where the platform
// requires it. After Pager.Close unmaps and closes the pager-owned descriptor,
// the retained handle still supplies the file
// durability fence required by an outstanding stable-resource token.
func (p *Pager) SyncIndexDataWithStableFile(file *os.File) error {
	_, err := p.SyncIndexDataWithStableFileWithWork(file, nil)
	return err
}

// Admission is bound to the actual dirty snapshot under mu. Refusal occurs
// before dirty detachment or durability observation; no prior writes are replayed.
func (p *Pager) SyncIndexDataWithStableFileWithWork(file *os.File, w *iterator.OrdinalScanWork) (bool, error) {
	return p.SyncIndexDataWithStableFileWithCallerControl(file, w, 0, 0)
}

// Caller controls were already reserved on this same grant. They contribute to
// fresh impossibility without charging the native snapshot a second time.
func (p *Pager) SyncIndexDataWithStableFileWithCallerControl(file *os.File, w *iterator.OrdinalScanWork, callerRecords, callerBytes uint64) (bool, error) {
	if file == nil {
		return false, errors.New("pager: stable index file unavailable")
	}
	done, err := p.syncDirtyChunksWithFileWork(true, 0, file, w, true, callerRecords, callerBytes)
	if !done || err != nil {
		return done, err
	}
	if info, e := file.Stat(); e == nil {
		p.durableFileSize.Store(info.Size())
	}
	if !p.readOnly && !p.memoryOnly {
		if e := durabilitycut.EmitPath(durabilitycut.AfterIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path); e != nil {
			return false, e
		}
	}
	return true, nil
}

func (p *Pager) syncDirtyChunks(syncFile bool, firstChunk int) error {
	return p.syncDirtyChunksWithFile(syncFile, firstChunk, p.file)
}

func (p *Pager) syncDirtyChunksWithFile(syncFile bool, firstChunk int, syncTarget *os.File) error {
	_, err := p.syncDirtyChunksWithFileWork(syncFile, firstChunk, syncTarget, nil, false, 0, 0)
	return err
}
func (p *Pager) syncDirtyChunksWithFileWork(syncFile bool, firstChunk int, syncTarget *os.File, w *iterator.OrdinalScanWork, indexCut bool, callerRecords, callerBytes uint64) (bool, error) {
	if p.readOnly {
		return false, ErrReadOnly
	}
	if p.memoryOnly {
		return true, nil
	}
	p.mu.Lock()
	// takeFrom scans the actual dirty word span, writes selected membership,
	// allocates its owned ordinal snapshot, then rechecks range hints. Include
	// the restoration path and the ordinary parallel worker/channel control.
	n := uint64(p.dirtyChunks.count)
	span := uint64(p.dirtyChunks.high - p.dirtyChunks.low)
	concurrency := int(p.syncConcurrency.Load())
	workers := min(uint64(max(1, concurrency)), n)
	records := uint64(5) + 4*span + 5*n + workers
	traffic := uint64(4096) + 128*span + 256*n + 1024*workers
	if indexCut {
		traffic += uint64(len(p.path))*8 + 2048
	}
	if err := pagerCallerMinimum(w, records, traffic, callerRecords, callerBytes); err != nil {
		p.mu.Unlock()
		return false, err
	}
	if ok, err := pagerReserve(w, records, traffic); !ok || err != nil {
		p.mu.Unlock()
		return false, err
	}
	// Move the selected dirty chunks into this sync attempt. Lower chunks can be
	// intentionally retained for a later durability-boundary Sync.
	toSync := p.dirtyChunks.takeFrom(firstChunk)
	p.mu.Unlock()

	var syncErr error
	if indexCut {
		syncErr = durabilitycut.EmitPath(durabilitycut.BeforeIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path)
	}
	// Hold mappings stable through mapped-range and file sync.
	p.mu.RLock()

	// The file data barrier covers Linux mapped writes. Flush-only calls still
	// need msync, and other platforms retain their explicit mapped-write fence.
	if syncErr == nil && (!syncFile || mappedRangeSyncRequired()) {
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
	}

	if syncErr == nil && syncFile {
		if syncTarget == nil {
			syncErr = errors.New("pager: sync file unavailable")
		} else if err := syncPageFile(syncTarget); err != nil {
			syncErr = err
		} else {
			p.durableFileSize.Store(int64(len(p.chunks)) * p.chunkSize)
		}
	}
	p.mu.RUnlock()

	// If error occurred, restore the dirty chunks
	if syncErr != nil {
		p.mu.Lock()
		for _, idx := range toSync {
			p.dirtyChunks.set(idx)
		}
		p.mu.Unlock()
		return false, syncErr
	}

	return true, nil
}

type syncPageRange struct {
	chunk int
	start int
	end   int
}

func alignDown(value, alignment int64) int64 {
	return value - value%alignment
}

func alignUp(value, alignment int64) int64 {
	if rem := value % alignment; rem != 0 {
		return value + alignment - rem
	}
	return value
}

func planSyncPageRanges(pageIDs []uint64, pageIDBase, pageCount uint64, chunkSize, granularity int64, chunkLengths []int) ([]syncPageRange, error) {
	if len(pageIDs) == 0 {
		return nil, nil
	}
	if granularity <= 0 {
		granularity = int64(os.Getpagesize())
	}
	ids := append([]uint64(nil), pageIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ranges := make([]syncPageRange, 0, len(ids))
	var previous uint64 = ^uint64(0)
	for _, pageID := range ids {
		if pageID == previous {
			continue
		}
		previous = pageID
		if pageID < pageIDBase || pageID >= pageCount {
			return nil, ErrPageOutOfBounds
		}
		localID := pageID - pageIDBase
		byteOffset := int64(localID) * int64(page.PageSize)
		chunk := int(byteOffset / chunkSize)
		if chunk < 0 || chunk >= len(chunkLengths) {
			return nil, ErrPageOutOfBounds
		}
		pageStart := byteOffset % chunkSize
		pageEnd := pageStart + int64(page.PageSize)
		start := alignDown(pageStart, granularity)
		end := alignUp(pageEnd, granularity)
		if end > int64(chunkLengths[chunk]) {
			end = int64(chunkLengths[chunk])
		}
		if start < 0 || start >= end || end > int64(chunkLengths[chunk]) {
			return nil, ErrPageOutOfBounds
		}
		n := len(ranges)
		if n > 0 && ranges[n-1].chunk == chunk && int(start) <= ranges[n-1].end {
			if int(end) > ranges[n-1].end {
				ranges[n-1].end = int(end)
			}
			continue
		}
		ranges = append(ranges, syncPageRange{chunk: chunk, start: int(start), end: int(end)})
	}
	return ranges, nil
}

// SyncPages durably synchronizes the named pages. Platforms that require an
// explicit mapped-view flush receive granularity-aligned ranges before the
// final file data-sync boundary. Linux fdatasync includes file-size metadata
// needed to retrieve appended pages; Darwin uses F_FULLFSYNC and Windows uses
// FlushFileBuffers after its mapped-view flush. Unsupported adapters fail
// closed.
// This method never promises that only the named bytes reach stable storage
// and deliberately leaves dirtyChunks unchanged so ordinary commit bookkeeping
// remains exact.
func (p *Pager) SyncPages(pageIDs []uint64) error {
	return p.SyncPagesWithStableFile(p.file, pageIDs)
}

// SyncPagesWithStableFile durably synchronizes the named mapped pages through
// an exact retained handle for this pager's file. The handle is validated
// against the live pager identity before any mapped-view or file barrier. This
// is the scoped primitive used for the durability-critical meta-page cut: it
// never reopens the diagnostic path and it fails closed on a rebound handle.
func (p *Pager) SyncPagesWithStableFile(file *os.File, pageIDs []uint64) error {
	_, err := p.SyncPagesWithStableFileWithWork(file, pageIDs, nil)
	return err
}
func (p *Pager) SyncPagesWithStableFileWithWork(file *os.File, pageIDs []uint64, w *iterator.OrdinalScanWork) (bool, error) {
	return p.SyncPagesWithStableFileWithCallerControl(file, pageIDs, w, 0, 0)
}
func (p *Pager) SyncPagesWithStableFileWithCallerControl(file *os.File, pageIDs []uint64, w *iterator.OrdinalScanWork, callerRecords, callerBytes uint64) (bool, error) {
	if p.readOnly {
		return false, ErrReadOnly
	}
	if p.memoryOnly {
		if ok, err := pagerReserve(w, uint64(len(pageIDs))+1, uint64(len(pageIDs))*64+512); !ok || err != nil {
			return false, err
		}
		for _, pageID := range pageIDs {
			if _, local := p.localPageID(pageID); !local || pageID >= p.numPages.Load() {
				return false, ErrPageOutOfBounds
			}
		}
		return true, nil
	}
	if file == nil {
		return false, errors.New("pager: stable index file unavailable")
	}
	if len(pageIDs) == 0 {
		return true, nil
	}

	p.mu.RLock()
	n, c := uint64(len(pageIDs)), uint64(len(p.chunks))
	// The existing planner copies/sorts IDs, builds owned ranges and reads the
	// full mapped directory. n*n covers the existing sort's comparisons/shifts
	// conservatively. Oversized inputs are refused before allocation or I/O.
	records := uint64(5) + c + 8*n + 2*n*n
	traffic := uint64(4096) + 64*c + 512*n + 64*n*n
	// Exact selected page traffic, including alignment expansion. Fdatasync
	// may persist unrelated kernel pages, but performs no product-side copy of
	// that historical mapping. Kernel scheduling is not a byte-copy admission.
	traffic += n * uint64(page.PageSize+2*mmapOffsetGranularity())
	if err := pagerCallerMinimum(w, records, traffic, callerRecords, callerBytes); err != nil {
		p.mu.RUnlock()
		return false, err
	}
	if ok, err := pagerReserve(w, records, traffic); !ok || err != nil {
		p.mu.RUnlock()
		return false, err
	}
	if p.file == nil {
		p.mu.RUnlock()
		return false, errors.New("pager: stable index file unavailable")
	}
	ownedInfo, err := p.file.Stat()
	if err != nil {
		p.mu.RUnlock()
		return false, err
	}
	stableInfo, err := file.Stat()
	if err != nil {
		p.mu.RUnlock()
		return false, err
	}
	if !os.SameFile(ownedInfo, stableInfo) {
		p.mu.RUnlock()
		return false, errors.New("pager: stable index file identity mismatch")
	}
	chunkLengths := make([]int, len(p.chunks))
	for i := range p.chunks {
		chunkLengths[i] = len(p.chunks[i])
	}
	ranges, err := planSyncPageRanges(pageIDs, p.pageIDBase, p.numPages.Load(), p.chunkSize, mmapOffsetGranularity(), chunkLengths)
	if err != nil {
		p.mu.RUnlock()
		return false, err
	}
	handled := false
	if syncPageRangesWithinDurableFileSize(ranges, p.chunkSize, p.durableFileSize.Load()) {
		handled, err = syncPageRangesFn(file, p.chunks, ranges, p.chunkSize)
		if err != nil {
			p.mu.RUnlock()
			return false, err
		}
	}
	if !handled && mappedRangeSyncRequired() {
		for _, r := range ranges {
			err := msyncFile(p.chunks[r.chunk][r.start:r.end])
			if err != nil {
				p.mu.RUnlock()
				return false, err
			}
		}
	}
	if !handled {
		err = syncPageFile(file)
		if err == nil {
			p.durableFileSize.Store(int64(len(p.chunks)) * p.chunkSize)
		}
	}
	p.mu.RUnlock()
	return err == nil, err
}

func pagerCallerMinimum(w *iterator.OrdinalScanWork, r, b, cr, cb uint64) error {
	if w == nil {
		return nil
	}
	add := func(a, b uint64) uint64 {
		if ^uint64(0)-a < b {
			return ^uint64(0)
		}
		return a + b
	}
	r, b = add(r, cr), add(b, cb)
	if r > w.RecordLimit || b > w.ByteLimit {
		return &iterator.OrdinalUnitTooLarge{Records: r, Bytes: b}
	}
	return nil
}

func isPrimaryBankID(id uint64) bool {
	return id >= page.PrimaryBankNamespace && id < 2*page.PrimaryBankNamespace
}

// AttachPrimaryBankPager installs the immutable physical routing owner once.
// The containing index generation owns both pager lifetimes. Attachment does
// not alter DATA extent, allocation or durability; resource fences cover the
// two exact physical files separately.
func (p *Pager) AttachPrimaryBankPager(companion *Pager) error {
	if p == nil || companion == nil || p == companion || p.memoryOnly || companion.memoryOnly || companion.pageIDBase != 0 {
		return errors.New("pager: invalid primary bank attachment")
	}
	if !p.primaryBanks.CompareAndSwap(nil, companion) {
		return errors.New("pager: primary bank owner already attached")
	}
	companion.SetVerifyOnRead(p.VerifyOnRead())
	return nil
}

// PrimaryBankPageCount reports only the attached companion's physical extent.
func (p *Pager) PrimaryBankPageCount() uint64 {
	if p == nil {
		return 0
	}
	if bank := p.primaryBanks.Load(); bank != nil {
		return bank.PageCount()
	}
	return 0
}
