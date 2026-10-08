package pager

import (
	"fmt"
	"unsafe"
)

// Keep async pre-grow enabled for the default TreeDB main chunk size (256KiB)
// while still avoiding excessive churn for very small side-store chunks.
const minAsyncPregrowChunkSize = 256 << 10 // 256KiB

func (p *Pager) startGrower() {
	p.growStop = make(chan struct{})
	p.growWake = make(chan struct{}, 1)
	p.growDone = make(chan struct{})
	go p.growLoop()
}

func (p *Pager) stopGrower() {
	if p.growStop == nil {
		return
	}
	p.growStopOnce.Do(func() { close(p.growStop) })
	<-p.growDone
}

func (p *Pager) maybeSchedulePreGrow(requiredBytes int64) {
	if p.growWake == nil || p.chunkSize < minAsyncPregrowChunkSize {
		return
	}
	currentCapacity := p.currentCapacityBytes()
	free := currentCapacity - requiredBytes
	if free >= p.chunkSize/2 {
		return
	}

	desired := currentCapacity + p.chunkSize
	for {
		cur := p.growTarget.Load()
		if desired <= cur {
			break
		}
		if p.growTarget.CompareAndSwap(cur, desired) {
			break
		}
	}

	select {
	case p.growWake <- struct{}{}:
	default:
	}
}

func (p *Pager) growLoop() {
	defer close(p.growDone)

	for {
		select {
		case <-p.growWake:
		case <-p.growStop:
			return
		}

		for {
			target := p.growTarget.Load()
			if target <= 0 {
				break
			}
			if err := p.growToCapacity(target); err != nil {
				p.growTarget.Store(0)
				break
			}

			capacity := p.currentCapacityBytes()
			for {
				cur := p.growTarget.Load()
				if cur <= 0 {
					break
				}
				if cur <= capacity {
					if p.growTarget.CompareAndSwap(cur, 0) {
						break
					}
					continue
				}
				break
			}

			if p.growTarget.Load() == 0 {
				break
			}
		}
	}
}

func (p *Pager) currentCapacityBytes() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(len(p.chunks)) * p.chunkSize
}

func (p *Pager) growToCapacity(targetCapacity int64) error {
	if err := p.beginOperation(); err != nil {
		return err
	}
	defer p.endOperation()
	if targetCapacity < 0 {
		return fmt.Errorf("invalid target capacity: %d", targetCapacity)
	}

	p.growMu.Lock()
	defer p.growMu.Unlock()

	currentCapacity := p.currentCapacityBytes()
	if targetCapacity <= currentCapacity {
		return nil
	}

	targetCapacity = ((targetCapacity + p.chunkSize - 1) / p.chunkSize) * p.chunkSize
	if p.memoryOnly {
		chunksNeeded := (targetCapacity - currentCapacity) / p.chunkSize
		newChunks := make([][]byte, chunksNeeded)
		for i := range newChunks {
			newChunks[i] = make([]byte, p.chunkSize)
		}
		p.mu.Lock()
		p.chunks = append(p.chunks, newChunks...)
		updated := make([][]byte, len(p.chunks))
		copy(updated, p.chunks)
		p.atomicChunks.Store(&chunkList{data: updated})
		p.ensurePrefetchCapacityLocked(len(p.chunks))
		p.mu.Unlock()
		return nil
	}

	chunksNeeded := (targetCapacity - currentCapacity) / p.chunkSize
	if chunksNeeded <= 0 {
		return nil
	}
	oldCount := int(currentCapacity / p.chunkSize)
	total := oldCount + int(chunksNeeded)
	if err := p.reserveKnown(uint64(total)*uint64(unsafe.Sizeof([]byte{})), true); err != nil {
		return err
	}
	if err := p.reserveKnown(uint64(unsafe.Sizeof(chunkList{})), true); err != nil {
		return err
	}
	p.mu.Lock()
	prefetchErr := p.ensurePrefetchCapacityLocked(total)
	p.mu.Unlock()
	if prefetchErr != nil {
		return prefetchErr
	}
	// Best-effort preallocation to fail fast on ENOSPC and reduce SIGBUS risk
	// on mmap writes (platform/filesystem dependent).
	if err := preallocateFile(p.file, targetCapacity); err != nil {
		return err
	}
	if err := p.file.Truncate(targetCapacity); err != nil {
		return err
	}

	next := make([][]byte, total)
	p.mu.Lock()
	copy(next, p.chunks)
	p.mu.Unlock()
	for i := oldCount; i < total; i++ {
		data, err := mmapFile(p.file.Fd(), int64(i)*p.chunkSize, int(p.chunkSize), p.mmapPopulate)
		if err != nil {
			// Keep every actual mapped suffix slot on its original owner. Close will
			// retry them; no temporary cleanup error loses a mapping or FD.
			p.mu.Lock()
			p.chunks = next
			p.atomicChunks.Store(nil)
			p.mu.Unlock()
			p.lifetime.mu.Lock()
			p.lifetime.closing = true
			p.lifetime.mu.Unlock()
			return err
		}
		next[i] = data
		madviseChunk(data)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chunks = next
	p.atomicChunks.Store(&chunkList{data: next})
	return nil
}
