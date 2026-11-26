package pager

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestPagerGetForWriteAndSyncConcurrent(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "concurrent.db"), syncPagesTestChunkSize(4))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.Alloc(64); err != nil {
		t.Fatalf("Alloc initial: %v", err)
	}
	if err := p.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		stop.Store(true)
	}

	// Concurrent writers calling GetForWrite and GrowTo on disjoint page ranges.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte(worker + 1)}, page.PageSize)
			base := uint64(2 + worker*16)
			for i := 0; i < 200 && !stop.Load(); i++ {
				id := base + uint64(i%16)
				buf, err := p.GetForWrite(id)
				if err != nil {
					recordErr(err)
					return
				}
				copy(buf, payload)
				if p.IsVerified(id) {
					recordErr(ErrPageOutOfBounds)
					return
				}
				if i%40 == 0 {
					if err := p.GrowTo(p.PageCount() + 1); err != nil {
						recordErr(err)
						return
					}
				}
			}
		}(w)
	}

	// Concurrent syncers exercising SyncPages and Sync while GetForWrite runs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40 && !stop.Load(); i++ {
			if err := p.SyncPages([]uint64{uint64(i % 2)}); err != nil {
				recordErr(err)
				return
			}
			if i%5 == 0 {
				if err := p.Sync(); err != nil {
					recordErr(err)
					return
				}
			}
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("concurrent pager operation failed: %v", err)
	default:
	}
}

func TestPagerGrowToDirtyBitmapExpansionAndGetForWriteConcurrent(t *testing.T) {
	// Use a small chunk size so growing past 64 chunks forces dirtyChunks.words
	// expansion (g.dirtyOutput != nil), directly exercising concurrent
	// Growth.valid dirty-revision reads against GetForWrite fast-path updates.
	chunkSize := syncPagesTestChunkSize(1)
	pagesPerChunk := uint64(chunkSize / int64(page.PageSize))
	p, err := Open(filepath.Join(t.TempDir(), "grow_dirty_race.db"), chunkSize)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = p.Close() }()

	if _, err := p.Alloc(int(pagesPerChunk * 2)); err != nil {
		t.Fatalf("Alloc initial: %v", err)
	}
	if err := p.Sync(); err != nil {
		t.Fatalf("initial Sync: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		stop.Store(true)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 4000 && !stop.Load(); i++ {
			buf, err := p.GetForWrite(uint64(i % int(pagesPerChunk*2)))
			if err != nil {
				recordErr(err)
				return
			}
			buf[0] = byte(i)
		}
	}()

	// Grow across multiple 64-chunk dirty-bitmap word boundaries.
	for chunks := uint64(8); chunks <= 140 && !stop.Load(); chunks += 16 {
		if err := p.GrowTo(chunks * pagesPerChunk); err != nil {
			recordErr(err)
			break
		}
	}
	stop.Store(true)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("concurrent GrowTo + GetForWrite failed: %v", err)
	default:
	}
}
