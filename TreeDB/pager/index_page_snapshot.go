package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

// IndexPageSnapshot is an immutable copy of the index pages owned by one
// publication fence. It lets foreground builders resume before the durable
// write completes without allowing a reused in-place page (currently the
// freelist head) to race the fence.
type IndexPageSnapshot struct {
	pageIDs     []uint64
	generations []uint64
	data        []byte
}

var syncIndexPageSnapshotFn = syncIndexPageSnapshotData

// NewIndexPageSnapshot returns reusable storage for the sole publication
// worker. Keeping the buffer with that worker avoids relying on sync.Pool,
// whose contents may be discarded at every GC while publication is active.
func NewIndexPageSnapshot() *IndexPageSnapshot {
	return new(IndexPageSnapshot)
}

// CaptureIndexPages copies the named non-meta pages while root construction is
// serialized. It is primarily a convenience for tests and one-shot callers.
func (p *Pager) CaptureIndexPages(ids []uint64) (*IndexPageSnapshot, error) {
	snapshot := NewIndexPageSnapshot()
	if err := p.CaptureIndexPagesInto(snapshot, ids); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// CaptureIndexPagesInto replaces snapshot with immutable copies of the named
// non-meta pages. A single publication worker may reuse the same snapshot
// after the preceding SyncIndexPageSnapshot call completes.
func (p *Pager) CaptureIndexPagesInto(snapshot *IndexPageSnapshot, ids []uint64) error {
	if p == nil {
		return errors.New("pager: nil pager")
	}
	if snapshot == nil {
		return errors.New("pager: nil index page snapshot")
	}
	snapshot.pageIDs = append(snapshot.pageIDs[:0], ids...)
	sort.Slice(snapshot.pageIDs, func(i, j int) bool { return snapshot.pageIDs[i] < snapshot.pageIDs[j] })
	write := 0
	for _, id := range snapshot.pageIDs {
		if write != 0 && snapshot.pageIDs[write-1] == id {
			continue
		}
		snapshot.pageIDs[write] = id
		write++
	}
	snapshot.pageIDs = snapshot.pageIDs[:write]
	snapshot.generations = growUint64s(snapshot.generations, len(snapshot.pageIDs))
	snapshot.data = growBytes(snapshot.data, len(snapshot.pageIDs)*page.PageSize)

	p.mu.RLock()
	defer p.mu.RUnlock()
	for i, id := range snapshot.pageIDs {
		if id == 0 || id == 1 {
			return errors.New("pager: index data snapshot includes meta page")
		}
		localID, local := p.localPageID(id)
		if !local || id >= p.numPages.Load() {
			return ErrPageOutOfBounds
		}
		byteOffset := int64(localID) * int64(page.PageSize)
		chunkIdx := int(byteOffset / p.chunkSize)
		offsetInChunk := int(byteOffset % p.chunkSize)
		if chunkIdx >= len(p.chunks) || offsetInChunk+page.PageSize > len(p.chunks[chunkIdx]) {
			return ErrPageOutOfBounds
		}
		snapshot.generations[i] = p.dirtyPages[id]
		copy(snapshot.data[i*page.PageSize:(i+1)*page.PageSize], p.chunks[chunkIdx][offsetInChunk:offsetInChunk+page.PageSize])
	}
	return nil
}

func growUint64s(buf []uint64, size int) []uint64 {
	if cap(buf) < size {
		return make([]uint64, size)
	}
	return buf[:size]
}

func growBytes(buf []byte, size int) []byte {
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

// ReleaseIndexPageSnapshot clears references held by a one-shot snapshot.
// Coordinator-owned snapshots are reused directly instead.
func ReleaseIndexPageSnapshot(snapshot *IndexPageSnapshot) {
	if snapshot == nil {
		return
	}
	snapshot.pageIDs = snapshot.pageIDs[:0]
	snapshot.generations = snapshot.generations[:0]
	snapshot.data = snapshot.data[:0]
}

// SyncIndexPageSnapshot durably writes exactly the captured non-meta images.
// A successor may append or mutate its own mmap pages concurrently; the fence
// reads only the immutable copies above.
func (p *Pager) SyncIndexPageSnapshot(snapshot *IndexPageSnapshot) error {
	if p.readOnly {
		return ErrReadOnly
	}
	if snapshot == nil || len(snapshot.pageIDs) == 0 || p.memoryOnly {
		return nil
	}
	if !p.readOnly && !p.memoryOnly {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path); err != nil {
			return err
		}
	}
	for _, id := range snapshot.pageIDs {
		if id == 0 || id == 1 {
			return errors.New("pager: index data snapshot includes meta page")
		}
	}
	if err := p.validateIndexPageSnapshotGenerations(snapshot, "before write"); err != nil {
		return err
	}
	p.mu.RLock()
	durableSize := int64(len(p.chunks)) * p.chunkSize
	p.mu.RUnlock()
	if err := syncIndexPageSnapshotFn(p.file, p.pageIDBase, snapshot.pageIDs, snapshot.data); err != nil {
		return err
	}
	// The snapshot writer finishes with a file-wide data barrier, which also
	// makes the observed file length durable. Recording that boundary prevents
	// a redundant size-only barrier before the next range publication.
	p.recordDurableFileSize(durableSize)
	p.mu.Lock()
	var staleErr error
	for i, id := range snapshot.pageIDs {
		if p.dirtyPages[id] == snapshot.generations[i] {
			delete(p.dirtyPages, id)
		} else if staleErr == nil {
			staleErr = fmt.Errorf("pager: index snapshot generation changed during write: page=%d captured=%d current=%d", id, snapshot.generations[i], p.dirtyPages[id])
		}
	}
	p.mu.Unlock()
	if staleErr != nil {
		return staleErr
	}
	if !p.readOnly && !p.memoryOnly {
		if err := durabilitycut.EmitPath(durabilitycut.AfterIndexDataSync, durabilitycut.ResourceIndex, filepath.Dir(p.path), p.path); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pager) validateIndexPageSnapshotGenerations(snapshot *IndexPageSnapshot, phase string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i, id := range snapshot.pageIDs {
		if current := p.dirtyPages[id]; current != snapshot.generations[i] {
			return fmt.Errorf("pager: stale index snapshot %s: page=%d captured=%d current=%d", phase, id, snapshot.generations[i], current)
		}
	}
	return nil
}

func syncIndexPageSnapshotData(file *os.File, pageIDBase uint64, pageIDs []uint64, data []byte) error {
	for first := 0; first < len(pageIDs); {
		last := first + 1
		for last < len(pageIDs) && pageIDs[last] == pageIDs[last-1]+1 {
			last++
		}
		offset := int64(pageIDs[first]-pageIDBase) * int64(page.PageSize)
		buf := data[first*page.PageSize : last*page.PageSize]
		for len(buf) > 0 {
			n, err := file.WriteAt(buf, offset)
			if err != nil {
				return err
			}
			if n <= 0 {
				return io.ErrShortWrite
			}
			buf = buf[n:]
			offset += int64(n)
		}
		first = last
	}
	return syncPageFileBarrier(file)
}
