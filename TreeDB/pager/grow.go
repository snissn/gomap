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
				p.growTarget.CompareAndSwap(target, 0)
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

// Capacity-only and logical growth drain the same private operation. A stale
// ordinary attempt cancels before retry, without exposing a new Busy outcome.
func (p *Pager) growToCapacity(target int64) error {
	p.allocMu.Lock()
	defer p.allocMu.Unlock()
	if target < 0 {
		return fmt.Errorf("invalid target capacity: %d", target)
	}
	for {
		g, _, err := NewGrowthForPager(p, 0, nil)
		if err != nil {
			return err
		}
		g.logical = false
		g.capacityTarget = target
		for {
			done, e := g.stepLocked(p, nil, 0, 0, nil)
			if e == ErrGrowthStale {
				break
			}
			if e != nil {
				return e
			}
			if done {
				return nil
			}
		}
	}
}
