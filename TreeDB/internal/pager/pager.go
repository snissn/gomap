package pager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"treedb/internal/page"
)

const (
	// DefaultChunkSize is 256MB.
	DefaultChunkSize = 256 << 20
)

type OpenOptions struct {
	Dir                       string
	ChunkSize                 int64
	SkipZeroFileExtendedPages bool
}

type chunk struct {
	index       int
	startOffset int64 // bytes in file
	// mapped is the raw mmap region aligned to the OS page size.
	mapped []byte
	// data is the logical chunk view within mapped.
	data []byte
	refs atomic.Int64
}

type Pager struct {
	path                      string
	file                      *os.File
	chunkSize                 int64
	sysPageSize               int64
	preallocSize              int64
	freelistCount             uint64
	reserveBatch              uint64
	reserveNext               uint64
	reserveEnd                uint64
	skipZeroFileExtendedPages bool

	mutableRangeStart page.PageID
	mutableRangeEnd   page.PageID
	mutableExtra      map[page.PageID]struct{}

	mu     sync.Mutex
	chunks []*chunk

	meta         Meta
	activeMetaID page.PageID
	totalPages   uint64
	closed       bool

	verifiedPages sync.Map // map[page.PageID]struct{}
}

func (p *Pager) clearVerifiedPage(pid page.PageID) {
	if p == nil {
		return
	}
	p.verifiedPages.Delete(pid)
}

// IsPageVerified reports whether the page body CRC has been verified for pid.
// Verified status is best-effort and is cleared when pages are freed or reused.
func (p *Pager) IsPageVerified(pid page.PageID) bool {
	if p == nil {
		return false
	}
	_, ok := p.verifiedPages.Load(pid)
	return ok
}

// MarkPageVerified records that pid's page body CRC was verified.
func (p *Pager) MarkPageVerified(pid page.PageID) {
	if p == nil {
		return
	}
	p.verifiedPages.Store(pid, struct{}{})
}

// chunkPin keeps a chunk pinned until Release is called.
// It is intended for internal use and unit tests.
type chunkPin struct {
	c *chunk
}

func (p *chunkPin) Release() {
	if p == nil || p.c == nil {
		return
	}
	p.c.refs.Add(-1)
}

// PageRef pins a single mmap-backed page until Release is called.
// Callers must not retain the returned Bytes slice after releasing.
type PageRef struct {
	pid      page.PageID
	c        *chunk
	pageData []byte
	released atomic.Bool
}

var pageRefPool = sync.Pool{
	New: func() any { return &PageRef{} },
}

func (r *PageRef) PageID() page.PageID {
	if r == nil {
		return 0
	}
	return r.pid
}

func (r *PageRef) Bytes() []byte {
	if r == nil {
		return nil
	}
	return r.pageData
}

func (r *PageRef) Release() {
	if r == nil || r.c == nil {
		return
	}
	if r.released.Swap(true) {
		return
	}
	r.c.refs.Add(-1)
	r.c = nil
	r.pid = 0
	r.pageData = nil
	pageRefPool.Put(r)
}

// Open opens or creates index.db in dir.
func Open(dir string, chunkSize int64) (*Pager, error) {
	return OpenWithOptions(OpenOptions{Dir: dir, ChunkSize: chunkSize})
}

func OpenWithOptions(opts OpenOptions) (*Pager, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("pager: dir required")
	}
	chunkSize := opts.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize%page.PageSize != 0 {
		return nil, ErrInvalidChunkSize
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(opts.Dir, "index.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	p := &Pager{
		path:                      path,
		file:                      f,
		chunkSize:                 chunkSize,
		sysPageSize:               int64(os.Getpagesize()),
		skipZeroFileExtendedPages: opts.SkipZeroFileExtendedPages,
	}

	if err := p.initOrLoad(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return p, nil
}

func (p *Pager) initOrLoad() error {
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	p.preallocSize = size
	if size == 0 {
		return p.initNew()
	}
	if size%page.PageSize != 0 {
		return ErrFileCorrupt
	}

	// Ensure the file is extended to a full chunk boundary for safe mapping.
	mapSize := alignUp(size, p.chunkSize)
	if mapSize > size {
		if err := p.preallocate(mapSize); err != nil {
			return err
		}
	}
	if err := p.mapExisting(mapSize); err != nil {
		return err
	}

	// Establish physical size before reading metas.
	physicalPages := uint64(mapSize / page.PageSize)
	p.totalPages = physicalPages

	m0, ok0 := p.readMetaFromMapped(0)
	m1, ok1 := p.readMetaFromMapped(1)
	switch {
	case ok0 && ok1:
		if m1.CommitSeq > m0.CommitSeq {
			p.meta, p.activeMetaID = m1, 1
		} else {
			p.meta, p.activeMetaID = m0, 0
		}
	case ok0:
		p.meta, p.activeMetaID = m0, 0
	case ok1:
		p.meta, p.activeMetaID = m1, 1
	default:
		return ErrFileCorrupt
	}

	if p.meta.TotalPages != 0 && p.meta.TotalPages <= physicalPages {
		p.totalPages = p.meta.TotalPages
	} else {
		p.totalPages = physicalPages
		p.meta.TotalPages = physicalPages
	}

	// Compute freelist count once on open; future updates are tracked incrementally.
	if err := p.loadFreelistCountLocked(); err != nil {
		return err
	}
	return nil
}

func (p *Pager) initNew() error {
	const initialPages = 3 // meta0, meta1, freelist0
	initialSize := int64(initialPages * page.PageSize)
	if err := p.preallocate(initialSize); err != nil {
		return err
	}
	mapSize := alignUp(initialSize, p.chunkSize)
	if err := p.mapExisting(mapSize); err != nil {
		return err
	}

	p.totalPages = initialPages
	p.meta = Meta{
		CommitSeq:        0,
		UserRootPageID:   0,
		SystemRootPageID: 0,
		FreelistHeadID:   2,
		TotalPages:       initialPages,
		ActiveSlabID:     0,
		ActiveSlabTail:   0,
		LastCommitHeight: 0,
	}
	p.activeMetaID = 0

	// Initialize freelist page (empty list).
	fltBuf, err := p.pageSlice(2)
	if err != nil {
		return err
	}
	fp := freelistPage{pageID: 2, next: 0, ids: nil}
	if err := encodeFreelistPage(fp, fltBuf); err != nil {
		return err
	}

	// Write both meta pages identical.
	if err := p.writeMetaToPage(0, p.meta); err != nil {
		return err
	}
	if err := p.writeMetaToPage(1, p.meta); err != nil {
		return err
	}
	p.freelistCount = 0
	return nil
}

// TotalPages returns the current size in pages.
func (p *Pager) TotalPages() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalPages
}

// ReadPageRef returns a pinned reference to the mmap-backed page bytes.
// The caller must call Release() when done.
func (p *Pager) ReadPageRef(pid page.PageID) (*PageRef, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pager: closed")
	}
	if uint64(pid) >= p.totalPages {
		p.mu.Unlock()
		return nil, ErrPageOutOfBounds
	}
	c, off, err := p.chunkForPage(pid)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	c.refs.Add(1)
	data := c.data[off : off+page.PageSize]
	p.mu.Unlock()

	ref := pageRefPool.Get().(*PageRef)
	ref.pid = pid
	ref.c = c
	ref.pageData = data
	ref.released.Store(false)
	return ref, nil
}

// HasFreePages reports whether the freelist currently contains at least one page ID.
// This is tracked incrementally and does not scan freelist pages.
func (p *Pager) HasFreePages() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, fmt.Errorf("pager: closed")
	}
	return p.freelistCount > 0, nil
}

// ReadPage returns a copy of the page data.
func (p *Pager) ReadPage(pid page.PageID) ([]byte, error) {
	buf := make([]byte, page.PageSize)
	if err := p.withPage(pid, func(src []byte) error {
		copy(buf, src)
		return nil
	}); err != nil {
		return nil, err
	}
	return buf, nil
}

// WritePage overwrites the page with the provided data.
func (p *Pager) WritePage(pid page.PageID, data []byte) error {
	if len(data) != page.PageSize {
		return fmt.Errorf("pager: write expects %d bytes", page.PageSize)
	}
	isMeta := isMetaPageWrite(pid, data)
	if err := p.withPage(pid, func(dst []byte) error {
		copy(dst, data)
		return nil
	}); err != nil {
		return err
	}
	if isMeta {
		p.mu.Lock()
		p.clearMutablePagesLocked()
		p.mu.Unlock()
	}
	p.clearVerifiedPage(pid)
	return nil
}

// GrowToPages expands the file to at least newTotal pages.
func (p *Pager) GrowToPages(newTotal uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.growToPagesLocked(newTotal)
}

func (p *Pager) growToPagesLocked(newTotal uint64) error {
	if p.closed {
		return fmt.Errorf("pager: closed")
	}
	if newTotal < p.totalPages {
		return ErrShrinkForbidden
	}
	if newTotal == p.totalPages {
		return nil
	}
	newSize := int64(newTotal * page.PageSize)
	if err := p.preallocate(newSize); err != nil {
		return err
	}

	oldChunks := len(p.chunks)
	mapSize := alignUp(newSize, p.chunkSize)
	newChunks := int((mapSize + p.chunkSize - 1) / p.chunkSize)
	for i := oldChunks; i < newChunks; i++ {
		if err := p.mapChunk(i, mapSize); err != nil {
			return err
		}
	}
	p.totalPages = newTotal
	p.meta.TotalPages = newTotal
	return nil
}

// AllocPage allocates a new page, preferring freelist reuse.
func (p *Pager) AllocPage() (page.PageID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, fmt.Errorf("pager: closed")
	}

	head := p.meta.FreelistHeadID
	for head != 0 {
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return 0, err
		}
		alloc, next, ok, err := popFreelistID(buf)
		if err != nil {
			return 0, err
		}
		if !ok {
			head = next
			p.meta.FreelistHeadID = head
			continue
		}
		if p.freelistCount > 0 {
			p.freelistCount--
		}
		p.clearVerifiedPage(alloc)
		p.markMutableExtraLocked(alloc)
		return alloc, nil
	}

	// No freelist entries: allocate from reserve if available, else extend file.
	var newID page.PageID
	if p.reserveNext < p.reserveEnd {
		newID = page.PageID(p.reserveNext)
		p.reserveNext++
	} else if p.reserveBatch > 0 {
		start := p.totalPages
		if p.reserveBatch > (^uint64(0) - start) {
			return 0, fmt.Errorf("pager: reserve overflow")
		}
		end := start + p.reserveBatch
		if err := p.growToPagesLocked(end); err != nil {
			return 0, err
		}
		p.reserveNext = start
		p.reserveEnd = end
		newID = page.PageID(p.reserveNext)
		p.reserveNext++
	} else {
		newID = page.PageID(p.totalPages)
		if err := p.growToPagesLocked(p.totalPages + 1); err != nil {
			return 0, err
		}
	}
	// Zero the new page for determinism.
	if !p.skipZeroFileExtendedPages {
		b, err := p.pageSliceLocked(newID)
		if err != nil {
			return 0, err
		}
		clear(b)
	}
	p.clearVerifiedPage(newID)
	p.markMutableRangeLocked(newID)
	return newID, nil
}

// WithMutablePage yields a mutable mmap-backed page slice for pages newly allocated
// since the last meta-page write. It recomputes the page body CRC after fn returns.
func (p *Pager) WithMutablePage(pid page.PageID, fn func([]byte) error) error {
	if fn == nil {
		return fmt.Errorf("pager: nil mutable callback")
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("pager: closed")
	}
	if uint64(pid) >= p.totalPages {
		p.mu.Unlock()
		return ErrPageOutOfBounds
	}
	if !p.isMutablePageLocked(pid) {
		p.mu.Unlock()
		return ErrMutablePageNotNew
	}
	c, off, err := p.chunkForPage(pid)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	c.refs.Add(1)
	buf := c.data[off : off+page.PageSize]
	p.mu.Unlock()

	defer c.refs.Add(-1)

	if err := fn(buf); err != nil {
		return err
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	h.SetBodyCRC(body)
	return nil
}

// BeginAllocReserve reserves up to estimatePages fresh PageIDs for subsequent AllocPage calls.
// Unused pages must be returned via EndAllocReserve to avoid leaking free space.
func (p *Pager) BeginAllocReserve(estimatePages uint64, maxChunks int) error {
	if estimatePages == 0 || maxChunks <= 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("pager: closed")
	}
	if p.reserveBatch != 0 || p.reserveNext != p.reserveEnd {
		return fmt.Errorf("pager: alloc reserve already active")
	}

	chunkPages := uint64(p.chunkSize / page.PageSize)
	if chunkPages == 0 {
		return fmt.Errorf("pager: invalid chunk size")
	}
	maxPages := chunkPages * uint64(maxChunks)
	if estimatePages > maxPages {
		estimatePages = maxPages
	}
	if estimatePages == 0 {
		return nil
	}
	p.reserveBatch = estimatePages
	return nil
}

// EndAllocReserve returns unused reserved pages to the freelist.
func (p *Pager) EndAllocReserve() error {
	p.mu.Lock()
	p.reserveBatch = 0
	start := p.reserveNext
	end := p.reserveEnd
	p.reserveNext = 0
	p.reserveEnd = 0
	p.mu.Unlock()

	if start >= end {
		return nil
	}

	const batchSize = 1024
	buf := make([]page.PageID, 0, batchSize)
	for id := start; id < end; id++ {
		buf = append(buf, page.PageID(id))
		if len(buf) == cap(buf) {
			if err := p.FreePages(buf); err != nil {
				return err
			}
			buf = buf[:0]
		}
	}
	if len(buf) == 0 {
		return nil
	}
	return p.FreePages(buf)
}

// FreePages appends page IDs to the on-disk freelist.
func (p *Pager) FreePages(ids []page.PageID) error {
	if len(ids) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("pager: closed")
	}
	for _, id := range ids {
		p.clearVerifiedPage(id)
		if p.mutableRangeStart != p.mutableRangeEnd && id >= p.mutableRangeStart && id < p.mutableRangeEnd {
			// We can't represent holes in the range cheaply; disable mutable access for this commit.
			p.mutableRangeStart = 0
			p.mutableRangeEnd = 0
			continue
		}
		delete(p.mutableExtra, id)
	}

	head := p.meta.FreelistHeadID
	if head == 0 {
		// Allocate a new freelist page at end.
		newID := page.PageID(p.totalPages)
		if err := p.growToPagesLocked(p.totalPages + 1); err != nil {
			return err
		}
		head = newID
		p.meta.FreelistHeadID = head
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return err
		}
		if err := initFreelistPage(head, 0, buf); err != nil {
			return err
		}
	}

	remaining := ids
	for len(remaining) != 0 {
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return err
		}
		n, err := appendFreelistIDs(buf, remaining)
		if err != nil {
			return err
		}
		if n == 0 {
			// Need a new freelist page; push current head down.
			newHead := page.PageID(p.totalPages)
			if err := p.growToPagesLocked(p.totalPages + 1); err != nil {
				return err
			}
			newBuf, err := p.pageSliceLocked(newHead)
			if err != nil {
				return err
			}
			if err := initFreelistPage(newHead, head, newBuf); err != nil {
				return err
			}
			head = newHead
			p.meta.FreelistHeadID = head
			continue
		}
		remaining = remaining[n:]
	}
	p.freelistCount += uint64(len(ids))
	return nil
}

// ReadActiveMeta returns the meta selected on open.
func (p *Pager) ReadActiveMeta() Meta {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.meta
}

// WriteInactiveMeta writes meta to the inactive superblock page.
func (p *Pager) WriteInactiveMeta(m Meta) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	inactive := page.PageID(0)
	if p.activeMetaID == 0 {
		inactive = 1
	}
	if err := p.writeMetaToPageLocked(inactive, m); err != nil {
		return err
	}
	// Do not flip active here; Phase 7 will handle commit semantics.
	return nil
}

// SyncIndex flushes dirty mmap pages and fsyncs index.db.
func (p *Pager) SyncIndex() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.chunks {
		if err := unix.Msync(c.mapped, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return p.file.Sync()
}

// Close msyncs and unmaps all chunks.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	for _, c := range p.chunks {
		if c.refs.Load() != 0 {
			return ErrChunkPinned
		}
		if err := unix.Msync(c.mapped, unix.MS_SYNC); err != nil {
			return err
		}
		if err := syscall.Munmap(c.mapped); err != nil {
			return err
		}
	}
	p.chunks = nil
	p.closed = true
	return p.file.Close()
}

// Internal helpers below.

func alignUp(size, multiple int64) int64 {
	if multiple <= 0 {
		return size
	}
	rem := size % multiple
	if rem == 0 {
		return size
	}
	return size + multiple - rem
}

func isMetaPageWrite(pid page.PageID, data []byte) bool {
	if pid != 0 && pid != 1 {
		return false
	}
	if len(data) < page.HeaderSize {
		return false
	}
	flags := page.PageFlags(binary.LittleEndian.Uint16(data[12:14]))
	return flags == page.PageTypeMeta
}

func (p *Pager) markMutableRangeLocked(pid page.PageID) {
	switch {
	case p.mutableRangeStart == p.mutableRangeEnd:
		p.mutableRangeStart = pid
		p.mutableRangeEnd = pid + 1
	case pid == p.mutableRangeEnd:
		p.mutableRangeEnd++
	case pid >= p.mutableRangeStart && pid < p.mutableRangeEnd:
		// already covered
	default:
		p.markMutableExtraLocked(pid)
	}
}

func (p *Pager) markMutableExtraLocked(pid page.PageID) {
	if p.mutableExtra == nil {
		p.mutableExtra = make(map[page.PageID]struct{}, 16)
	}
	p.mutableExtra[pid] = struct{}{}
}

func (p *Pager) isMutablePageLocked(pid page.PageID) bool {
	if p.mutableRangeStart != p.mutableRangeEnd && pid >= p.mutableRangeStart && pid < p.mutableRangeEnd {
		return true
	}
	_, ok := p.mutableExtra[pid]
	return ok
}

func (p *Pager) clearMutablePagesLocked() {
	p.mutableRangeStart = 0
	p.mutableRangeEnd = 0
	clear(p.mutableExtra)
}

func (p *Pager) preallocate(size int64) error {
	mapSize := alignUp(size, p.chunkSize)
	aligned := mapSize
	sysPS := p.sysPageSize
	if sysPS <= 0 {
		sysPS = int64(os.Getpagesize())
	}
	if rem := aligned % sysPS; rem != 0 {
		aligned += sysPS - rem
	}
	if aligned <= p.preallocSize {
		return nil
	}

	// Prevent shrink while mapped.
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size() < p.preallocSize {
		return ErrFileCorrupt
	}
	if aligned < fi.Size() {
		return ErrShrinkForbidden
	}
	if fi.Size() == aligned {
		p.preallocSize = aligned
		return nil
	}
	if err := p.file.Truncate(aligned); err != nil {
		return err
	}
	p.preallocSize = aligned
	return nil
}

func (p *Pager) mapExisting(size int64) error {
	p.chunks = nil
	numChunks := int((size + p.chunkSize - 1) / p.chunkSize)
	for i := 0; i < numChunks; i++ {
		if err := p.mapChunk(i, size); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) mapChunk(index int, size int64) error {
	start := int64(index) * p.chunkSize
	if start >= size {
		return nil
	}
	length := p.chunkSize
	if start+length > size {
		length = size - start
	}
	sysPS := p.sysPageSize
	if sysPS <= 0 {
		sysPS = int64(os.Getpagesize())
	}
	mapStart := start - (start % sysPS)
	delta := start - mapStart
	mapLength := length + delta
	if rem := mapLength % sysPS; rem != 0 {
		mapLength += sysPS - rem
	}
	mapped, err := syscall.Mmap(int(p.file.Fd()), mapStart, int(mapLength), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	data := mapped[delta : delta+length]
	p.chunks = append(p.chunks, &chunk{
		index:       index,
		startOffset: start,
		mapped:      mapped,
		data:        data,
	})
	return nil
}

func (p *Pager) chunkForPage(pid page.PageID) (*chunk, int, error) {
	byteOff := int64(pid) * page.PageSize
	if byteOff < 0 {
		return nil, 0, ErrPageOutOfBounds
	}
	chunkIndex := int(byteOff / p.chunkSize)
	if chunkIndex >= len(p.chunks) {
		return nil, 0, ErrPageOutOfBounds
	}
	c := p.chunks[chunkIndex]
	offInChunk := int(byteOff - c.startOffset)
	if offInChunk < 0 || offInChunk+page.PageSize > len(c.data) {
		return nil, 0, ErrPageOutOfBounds
	}
	return c, offInChunk, nil
}

func (p *Pager) withPage(pid page.PageID, fn func([]byte) error) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("pager: closed")
	}
	// Ensure file large enough.
	if uint64(pid) >= p.totalPages {
		p.mu.Unlock()
		return ErrPageOutOfBounds
	}
	c, off, err := p.chunkForPage(pid)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	c.refs.Add(1)
	p.mu.Unlock()
	defer c.refs.Add(-1)
	return fn(c.data[off : off+page.PageSize])
}

func (p *Pager) pageSlice(pid page.PageID) ([]byte, error) {
	var out []byte
	err := p.withPage(pid, func(b []byte) error {
		out = b
		return nil
	})
	return out, err
}

// pageSliceLocked returns the mmap-backed page slice. Caller must hold p.mu.
func (p *Pager) pageSliceLocked(pid page.PageID) ([]byte, error) {
	if p.closed {
		return nil, fmt.Errorf("pager: closed")
	}
	if uint64(pid) >= p.totalPages {
		return nil, ErrPageOutOfBounds
	}
	c, off, err := p.chunkForPage(pid)
	if err != nil {
		return nil, err
	}
	return c.data[off : off+page.PageSize], nil
}

func (p *Pager) pinChunk(pid page.PageID) (*chunkPin, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("pager: closed")
	}
	if uint64(pid) >= p.totalPages {
		return nil, ErrPageOutOfBounds
	}
	c, _, err := p.chunkForPage(pid)
	if err != nil {
		return nil, err
	}
	c.refs.Add(1)
	return &chunkPin{c: c}, nil
}

func (p *Pager) readMetaFromMapped(pid page.PageID) (Meta, bool) {
	buf, err := p.ReadPage(pid)
	if err != nil {
		return Meta{}, false
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return Meta{}, false
	}
	if h.Flags != page.PageTypeMeta {
		return Meta{}, false
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		return Meta{}, false
	}
	if len(body) < metaBodySize {
		return Meta{}, false
	}
	return decodeMetaBody(body), true
}

func (p *Pager) writeMetaToPage(pid page.PageID, m Meta) error {
	buf, err := p.pageSlice(pid)
	if err != nil {
		return err
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	h.PageID = pid
	h.Flags = page.PageTypeMeta
	h.Count = 0
	if len(body) < metaBodySize {
		return ErrFileCorrupt
	}
	encodeMetaBody(m, body)
	// Zero remainder.
	for i := metaBodySize; i < len(body); i++ {
		body[i] = 0
	}
	h.SetBodyCRC(body)
	return nil
}

func (p *Pager) writeMetaToPageLocked(pid page.PageID, m Meta) error {
	buf, err := p.pageSliceLocked(pid)
	if err != nil {
		return err
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return err
	}
	h.PageID = pid
	h.Flags = page.PageTypeMeta
	h.Count = 0
	if len(body) < metaBodySize {
		return ErrFileCorrupt
	}
	encodeMetaBody(m, body)
	for i := metaBodySize; i < len(body); i++ {
		body[i] = 0
	}
	h.SetBodyCRC(body)
	return nil
}

// CopyPageTo writes the raw page bytes to w. For debugging.
func (p *Pager) CopyPageTo(pid page.PageID, w io.Writer) error {
	data, err := p.ReadPage(pid)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func (p *Pager) loadFreelistCountLocked() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("pager: closed")
	}
	var count uint64
	head := p.meta.FreelistHeadID
	for head != 0 {
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return err
		}
		h, body, err := page.SplitPage(buf)
		if err != nil {
			return err
		}
		if h.Flags != page.PageTypeFreelist {
			return ErrFileCorrupt
		}
		if err := h.VerifyBodyCRC(body); err != nil {
			return err
		}
		if len(body) < freelistHeaderExtra {
			return ErrFileCorrupt
		}
		count += uint64(h.Count)
		head = page.PageID(binary.LittleEndian.Uint64(body[0:8]))
	}
	p.freelistCount = count
	return nil
}
