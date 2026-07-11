package pager

import (
	"errors"
	"io"

	"github.com/snissn/gomap/TreeDB/page"
)

// SyncMetaPageImage durably writes exactly one of the two recovery meta-page
// images using caller-owned scratch storage. Unlike SyncPages, this dedicated
// publication path does not build range plans or copy the pager's growing
// chunk table. The final file data boundary retains the same stable-storage
// semantics as ordinary page synchronization.
func (p *Pager) SyncMetaPageImage(pageID uint64, scratch []byte) error {
	if p == nil {
		return errors.New("pager: nil pager")
	}
	if p.readOnly {
		return ErrReadOnly
	}
	if len(scratch) < page.PageSize {
		return errors.New("pager: meta-page scratch buffer too small")
	}

	p.mu.RLock()
	localID, local := p.localPageID(pageID)
	if !local || localID > 1 || pageID >= p.numPages.Load() {
		p.mu.RUnlock()
		return ErrPageOutOfBounds
	}
	if p.memoryOnly {
		p.mu.RUnlock()
		return nil
	}
	byteOffset := int64(localID) * int64(page.PageSize)
	chunkIdx := int(byteOffset / p.chunkSize)
	offsetInChunk := int(byteOffset % p.chunkSize)
	if chunkIdx >= len(p.chunks) || offsetInChunk+page.PageSize > len(p.chunks[chunkIdx]) {
		p.mu.RUnlock()
		return ErrPageOutOfBounds
	}
	generation := p.dirtyPages[pageID]
	copy(scratch[:page.PageSize], p.chunks[chunkIdx][offsetInChunk:offsetInChunk+page.PageSize])
	durableSize := int64(len(p.chunks)) * p.chunkSize
	p.mu.RUnlock()

	offset := byteOffset
	remaining := scratch[:page.PageSize]
	for len(remaining) != 0 {
		n, err := p.file.WriteAt(remaining, offset)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		remaining = remaining[n:]
		offset += int64(n)
	}
	if err := syncPageFile(p.file); err != nil {
		return err
	}
	p.recordDurableFileSize(durableSize)

	p.mu.Lock()
	if p.dirtyPages[pageID] == generation {
		delete(p.dirtyPages, pageID)
	}
	p.mu.Unlock()
	return nil
}
