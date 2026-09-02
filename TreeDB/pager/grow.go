package pager

import "fmt"

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

	// Best-effort preallocation to fail fast on ENOSPC and reduce SIGBUS risk
	// on mmap writes (platform/filesystem dependent).
	if err := preallocateFile(p.file, targetCapacity); err != nil {
		return err
	}
	if err := p.file.Truncate(targetCapacity); err != nil {
		return err
	}

	chunksNeeded := (targetCapacity - currentCapacity) / p.chunkSize
	if chunksNeeded <= 0 {
		return nil
	}

	newChunks := make([][]byte, 0, chunksNeeded)
	for i := int64(0); i < chunksNeeded; i++ {
		offset := currentCapacity + (i * p.chunkSize)
		data, err := mmapFile(p.file.Fd(), offset, int(p.chunkSize), p.mmapPopulate)
		if err != nil {
			for _, c := range newChunks {
				_ = munmapFile(c)
			}
			return err
		}
		madviseChunk(data)
		newChunks = append(newChunks, data)
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
