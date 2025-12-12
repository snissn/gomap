package pager

import (
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

type chunk struct {
	index       int
	startOffset int64 // bytes in file
	// mapped is the raw mmap region aligned to the OS page size.
	mapped []byte
	// data is the logical chunk view within mapped.
	data []byte
	refs        atomic.Int64
}

type Pager struct {
	path      string
	file      *os.File
	chunkSize int64
	sysPageSize int64

	mu     sync.Mutex
	chunks []*chunk

	meta         Meta
	activeMetaID page.PageID
	totalPages   uint64
	closed       bool
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

// Open opens or creates index.db in dir.
func Open(dir string, chunkSize int64) (*Pager, error) {
	if dir == "" {
		return nil, fmt.Errorf("pager: dir required")
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize%page.PageSize != 0 {
		return nil, ErrInvalidChunkSize
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "index.db")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	p := &Pager{
		path:      path,
		file:      f,
		chunkSize: chunkSize,
		sysPageSize: int64(os.Getpagesize()),
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
	return nil
}

// TotalPages returns the current size in pages.
func (p *Pager) TotalPages() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalPages
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
	return p.withPage(pid, func(dst []byte) error {
		copy(dst, data)
		return nil
	})
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
		fp, err := decodeFreelistPage(head, buf)
		if err != nil {
			return 0, err
		}
		if len(fp.ids) == 0 {
			// Advance to next freelist page.
			head = fp.next
			p.meta.FreelistHeadID = head
			continue
		}
		// Pop last id.
		n := len(fp.ids) - 1
		alloc := fp.ids[n]
		fp.ids = fp.ids[:n]
		if err := encodeFreelistPage(fp, buf); err != nil {
			return 0, err
		}
		return alloc, nil
	}

	// No freelist entries; extend file.
	// Optimization: Grow by a batch to amortize ftruncate costs.
	const growthBatch = 1024 // 4MB
	startID := page.PageID(p.totalPages)
	if err := p.growToPagesLocked(p.totalPages + uint64(growthBatch)); err != nil {
		return 0, err
	}

	// The first page (startID) is returned.
	alloc := startID

	// Zero the allocated page.
	b, err := p.pageSliceLocked(alloc)
	if err != nil {
		return 0, err
	}
	for i := range b {
		b[i] = 0
	}

	// Add the remaining pages to the freelist.
	// We need to release the lock to call FreePages? 
	// No, FreePages takes the lock. We are holding the lock.
	// We must use an internal helper or add to freelist manually here.
	// Re-using FreePages is risky due to deadlock.
	// Let's implement freelist addition logic here or extract a helper.
	
	// Actually, growing the file updates p.totalPages.
	// The pages startID+1 ... startID+growthBatch-1 are now valid but unused.
	// We should add them to the freelist.
	
	// Since we hold the lock, we can't call FreePages.
	// We'll extract FreePages logic to freePagesLocked.
	
	var extra []page.PageID
	for i := 1; i < growthBatch; i++ {
		extra = append(extra, startID+page.PageID(i))
	}
	
	if err := p.freePagesLocked(extra); err != nil {
		// If we fail to add to freelist, we leak space but don't corrupt data.
		// But returning error is better.
		return 0, err
	}

	return alloc, nil
}

// FreePages appends page IDs to the on-disk freelist.
func (p *Pager) FreePages(ids []page.PageID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.freePagesLocked(ids)
}

func (p *Pager) freePagesLocked(ids []page.PageID) error {
	if len(ids) == 0 {
		return nil
	}
	if p.closed {
		return fmt.Errorf("pager: closed")
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
		fp := freelistPage{pageID: head, next: 0, ids: nil}
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return err
		}
		if err := encodeFreelistPage(fp, buf); err != nil {
			return err
		}
	}

	remaining := append([]page.PageID(nil), ids...)
	for len(remaining) > 0 {
		buf, err := p.pageSliceLocked(head)
		if err != nil {
			return err
		}
		fp, err := decodeFreelistPage(head, buf)
		if err != nil {
			return err
		}
		capacity := freelistCapacity()
		space := capacity - len(fp.ids)
		if space == 0 {
			// Need a new freelist page; push current head down.
			newHead := page.PageID(p.totalPages)
			if err := p.growToPagesLocked(p.totalPages + 1); err != nil {
				return err
			}
			newBuf, err := p.pageSliceLocked(newHead)
			if err != nil {
				return err
			}
			newFP := freelistPage{pageID: newHead, next: head, ids: nil}
			if err := encodeFreelistPage(newFP, newBuf); err != nil {
				return err
			}
			head = newHead
			p.meta.FreelistHeadID = head
			continue
		}
		n := space
		if n > len(remaining) {
			n = len(remaining)
		}
		fp.ids = append(fp.ids, remaining[:n]...)
		remaining = remaining[n:]
		if err := encodeFreelistPage(fp, buf); err != nil {
			return err
		}
	}
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

func (p *Pager) preallocate(size int64) error {
	// Prevent shrink while mapped.
	fi, err := p.file.Stat()
	if err != nil {
		return err
	}
	mapSize := alignUp(size, p.chunkSize)
	aligned := mapSize
	sysPS := p.sysPageSize
	if sysPS <= 0 {
		sysPS = int64(os.Getpagesize())
	}
	if rem := aligned % sysPS; rem != 0 {
		aligned += sysPS - rem
	}
	if aligned < fi.Size() {
		return ErrShrinkForbidden
	}
	return p.file.Truncate(aligned)
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
