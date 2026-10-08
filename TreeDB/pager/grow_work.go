package pager

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"
	"weak"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrGrowthStale = errors.New("pager: growth source changed")
var ErrGrowthClosed = errors.New("pager: growth source closed")

const growthReady = 9

// Growth is the ordinary growth operation's private destination. Source
// authorities are non-owning; old mapping descriptors only occur in the actual
// prepared destination. Cancel owns just the newly mapped, unpublished suffix.
type Growth struct {
	targetPages                                                               uint64
	capacityTarget                                                            int64
	logical, policy                                                           bool
	owner                                                                     weak.Pointer[Pager]
	source                                                                    weak.Pointer[chunkList]
	file                                                                      weak.Pointer[os.File]
	verified                                                                  weak.Pointer[verifiedBitset]
	prefetch                                                                  weak.Pointer[prefetchBitset]
	count                                                                     uint64
	oldChunks, totalChunks, oldVerified, needVerified, oldPrefetch, needWords int
	dirtyRevision                                                             uint64
	dirtyCount, dirtyLow, dirtyHigh                                           int
	primary, atomicView                                                       [][]byte
	preparedView                                                              *chunkList
	memoryOnly                                                                bool
	verifiedOutput                                                            *verifiedBitset
	prefetchOutput                                                            *prefetchBitset
	dirtyOutput                                                               []uint64
	phase, position, mapped                                                   int
	captured, installed, cancelled                                            bool
	failure                                                                   error
	metadata                                                                  *retainedalloc.Owner
	metadataCharge, metadataOld                                               uint64
}

func NewGrowth(targetPages uint64, w *iterator.OrdinalScanWork) (*Growth, bool, error) {
	if ok, e := pagerReserve(w, 1, 6*uint64(unsafe.Sizeof(Growth{}))+256); !ok || e != nil {
		return nil, false, e
	}
	return &Growth{targetPages: targetPages, logical: true, policy: w != nil}, true, nil
}

// NewGrowthForPager admits its actual private operation wrapper before creation.
func NewGrowthForPager(p *Pager, target uint64, w *iterator.OrdinalScanWork) (*Growth, bool, error) {
	var owner *retainedalloc.Owner
	if p != nil {
		owner = p.immutableReadRoots.retention
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Growth{})))
	if owner != nil {
		if err := owner.Add(charge); err != nil {
			return nil, false, err
		}
	}
	g, ready, err := NewGrowth(target, w)
	if !ready || err != nil {
		if owner != nil {
			owner.Remove(charge)
		}
		return nil, ready, err
	}
	g.metadata = owner
	if owner != nil {
		g.metadataCharge = charge
	}
	return g, true, nil
}

func (g *Growth) valid(p *Pager) bool {
	return !p.closing && g.owner.Value() == p && g.source.Value() == p.atomicChunks.Load() &&
		g.file.Value() == p.file && g.count == p.numPages.Load() && len(p.chunks) == g.oldChunks &&
		g.verified.Value() == p.verified.Load() && g.prefetch.Value() == p.prefetched.Load() &&
		(g.dirtyOutput == nil || g.dirtyRevision == p.dirtyChunks.revision)
}

func (g *Growth) Step(p *Pager, w *iterator.OrdinalScanWork) (bool, error) {
	return g.StepWithInstall(p, w, 0, 0, nil)
}

// StepWithInstall joins a caller's already guarded, infallible authority install
// to pager publication. It admits both fixed tails once before any live change.
// The callback must acquire no locks, allocate nothing and cannot fail.
func (g *Growth) StepWithInstall(p *Pager, w *iterator.OrdinalScanWork, r, b uint64, install func()) (bool, error) {
	p.allocMu.Lock()
	defer p.allocMu.Unlock()
	return g.stepLocked(p, w, r, b, install)
}

// StepWithInstallAtCount rejects foreign logical growth before capturing the
// source, and keeps that same expected count protected through installation.
func (g *Growth) StepWithInstallAtCount(p *Pager, expected uint64, w *iterator.OrdinalScanWork, r, b uint64, install func()) (bool, error) {
	p.allocMu.Lock()
	defer p.allocMu.Unlock()
	return g.stepLockedAtCount(p, w, r, b, install, expected, true)
}

// InstallAtCount joins a no-growth caller install to the same pager guard. The
// callback obeys the same infallible contract as Growth.StepWithInstall.
func (p *Pager) InstallAtCount(expected uint64, w *iterator.OrdinalScanWork, r, b uint64, install func()) (bool, error) {
	if ok, e := pagerReserve(w, 2+r, 6*uint64(unsafe.Sizeof(Growth{}))+2048+b); !ok || e != nil {
		return false, e
	}
	p.allocMu.Lock()
	defer p.allocMu.Unlock()
	p.growMu.Lock()
	defer p.growMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readOnly {
		return false, ErrReadOnly
	}
	if p.closing {
		return false, ErrGrowthClosed
	}
	if p.numPages.Load() != expected {
		return false, ErrGrowthStale
	}
	install()
	return true, nil
}

// Caller owns allocMu (ordinary allocation), or is a capacity-only grower.
func (g *Growth) stepLocked(p *Pager, w *iterator.OrdinalScanWork, r, b uint64, install func()) (bool, error) {
	return g.stepLockedAtCount(p, w, r, b, install, 0, false)
}

func (g *Growth) stepLockedAtCount(p *Pager, w *iterator.OrdinalScanWork, r, b uint64, install func(), expected uint64, checkCount bool) (bool, error) {
	if g.installed {
		return true, nil
	}
	if g.cancelled {
		return false, g.failure
	}
	if g.failure != nil {
		return g.finishFailure(w)
	}
	if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(Growth{}))+1024); !ok || e != nil {
		return false, e
	}
	p.growMu.Lock()
	defer p.growMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	defer runtime.KeepAlive(p)
	if p.readOnly {
		return false, ErrReadOnly
	}
	if p.closing {
		g.failure = ErrGrowthClosed
		return g.finishFailure(w)
	}
	if checkCount && p.numPages.Load() != expected {
		g.failure = ErrGrowthStale
		return g.finishFailure(w)
	}
	if !g.captured {
		if ok, e := pagerReserve(w, 1, 2*uint64(unsafe.Sizeof(Growth{}))+1024); !ok || e != nil {
			return false, e
		}
		if e := g.capture(p); e != nil {
			g.failure = e
			return g.finishFailure(w)
		}
	} else if !g.valid(p) {
		g.failure = ErrGrowthStale
		return g.finishFailure(w)
	}
	for {
		switch g.phase {
		case 0:
			if g.totalChunks > g.oldChunks {
				if ok, e := pagerReserve(w, 1, uint64(g.totalChunks)*48+256); !ok || e != nil {
					return false, e
				}
				g.primary = make([][]byte, g.totalChunks)
				g.phase++
			} else {
				g.phase = 3
			}
		case 1:
			if ok, e := pagerReserve(w, 1, uint64(g.totalChunks)*48+256); !ok || e != nil {
				return false, e
			}
			g.atomicView = make([][]byte, g.totalChunks)
			g.phase++
		case 2:
			for g.position < g.oldChunks {
				if ok, e := pagerReserve(w, 3, 192); !ok || e != nil {
					return false, e
				}
				c := p.chunks[g.position]
				g.primary[g.position] = c
				g.atomicView[g.position] = c
				g.position++
			}
			g.position = 0
			g.phase++
		case 3:
			if g.needVerified > g.oldVerified {
				if g.verifiedOutput == nil {
					if ok, e := pagerReserve(w, 1, uint64(g.needVerified)*48+256); !ok || e != nil {
						return false, e
					}
					g.verifiedOutput = &verifiedBitset{chunks: make([][]uint64, g.needVerified)}
				}
				for g.position < g.needVerified {
					if g.position < g.oldVerified {
						if ok, e := pagerReserve(w, 2, 144); !ok || e != nil {
							return false, e
						}
						g.verifiedOutput.chunks[g.position] = p.verified.Load().chunks[g.position]
					} else {
						if ok, e := pagerReserve(w, 1, 2*verifiedChunkWords*8+256); !ok || e != nil {
							return false, e
						}
						g.verifiedOutput.chunks[g.position] = make([]uint64, verifiedChunkWords)
					}
					g.position++
				}
			}
			g.position = 0
			g.phase++
		case 4:
			if g.needWords > g.oldPrefetch {
				if g.prefetchOutput == nil {
					if ok, e := pagerReserve(w, 1, uint64(g.needWords)*16+256); !ok || e != nil {
						return false, e
					}
					g.prefetchOutput = &prefetchBitset{words: make([]uint64, g.needWords)}
				}
				for g.position < g.oldPrefetch {
					if ok, e := pagerReserve(w, 1, 64); !ok || e != nil {
						return false, e
					}
					g.prefetchOutput.words[g.position] = atomic.LoadUint64(&p.prefetched.Load().words[g.position])
					g.position++
				}
			}
			g.position = 0
			g.phase++
		case 5:
			if g.needWords > len(p.dirtyChunks.words) {
				if g.dirtyOutput == nil {
					if ok, e := pagerReserve(w, 1, uint64(g.needWords)*16+512); !ok || e != nil {
						return false, e
					}
					g.dirtyOutput = make([]uint64, g.needWords)
					g.dirtyRevision = p.dirtyChunks.revision
					g.dirtyCount, g.dirtyLow, g.dirtyHigh = p.dirtyChunks.count, p.dirtyChunks.low, p.dirtyChunks.high
				}
				for g.position < len(p.dirtyChunks.words) {
					if ok, e := pagerReserve(w, 1, 64); !ok || e != nil {
						return false, e
					}
					g.dirtyOutput[g.position] = p.dirtyChunks.words[g.position]
					g.position++
				}
			}
			g.position = 0
			g.phase++
		case 6:
			if g.totalChunks > g.oldChunks && !p.memoryOnly {
				if ok, e := pagerReserve(w, 1, 2048); !ok || e != nil {
					return false, e
				}
				// Hold the file lifetime read guard during physical I/O;
				// concurrent dirty changes are checked again before publication.
				p.mu.Unlock()
				p.mu.RLock()
				var e error
				if !g.valid(p) {
					e = ErrGrowthStale
				} else {
					var info os.FileInfo
					info, e = p.file.Stat()
					if e == nil && info.Size() < g.capacityTarget {
						e = preallocateFile(p.file, g.capacityTarget)
						if e == nil {
							e = p.file.Truncate(g.capacityTarget)
						}
					}
				}
				p.mu.RUnlock()
				p.mu.Lock()
				if e == nil && !g.valid(p) {
					e = ErrGrowthStale
				}
				if e != nil {
					g.failure = e
					return g.finishFailure(w)
				}
			}
			g.mapped = g.oldChunks
			g.phase++
		case 7:
			for g.mapped < g.totalChunks {
				traffic := uint64(2048)
				if p.memoryOnly {
					traffic += 2 * uint64(p.chunkSize)
				} else if p.mmapPopulate {
					traffic += uint64(p.chunkSize)
				}
				if ok, e := pagerReserve(w, 3, traffic); !ok || e != nil {
					return false, e
				}
				var c []byte
				var e error
				if p.memoryOnly {
					c = make([]byte, p.chunkSize)
				} else {
					p.mu.Unlock()
					p.mu.RLock()
					if !g.valid(p) {
						e = ErrGrowthStale
					} else {
						c, e = mmapFile(p.file.Fd(), int64(g.mapped)*p.chunkSize, int(p.chunkSize), p.mmapPopulate)
						if e == nil {
							madviseChunk(c)
						}
					}
					p.mu.RUnlock()
					p.mu.Lock()
				}
				if e != nil {
					g.failure = e
					return g.finishFailure(w)
				}
				g.primary[g.mapped] = c
				g.atomicView[g.mapped] = c
				g.mapped++
				if !g.valid(p) {
					g.failure = ErrGrowthStale
					return g.finishFailure(w)
				}
			}
			g.phase++
		case 8:
			// Prepare the immutable carrier before the final swap; no allocation there.
			if g.primary != nil {
				if ok, e := pagerReserve(w, 1, 256); !ok || e != nil {
					return false, e
				}
				g.preparedView = &chunkList{data: g.atomicView}
				g.phase = growthReady
			} else {
				g.phase = growthReady
			}
		case growthReady:
			if ok, e := pagerReserve(w, 2+r, 6*uint64(unsafe.Sizeof(Growth{}))+2048+b); !ok || e != nil {
				return false, e
			}
			// Same uninterrupted mu section as the validation at this invocation's cut.
			// No source or dirty mutation can interleave with the following publication.
			if !g.valid(p) {
				g.failure = ErrGrowthStale
				return g.finishFailure(w)
			}
			g.install(p)
			if install != nil {
				install()
			}
			return true, nil
		}
	}
}

func (g *Growth) capture(p *Pager) error {
	count := p.numPages.Load()
	local := p.localPageCount(max(count, g.targetPages))
	if !g.logical {
		local = p.localPageCount(count)
	}
	if local > math.MaxInt64/page.PageSize {
		return fmt.Errorf("pager: growth size overflow")
	}
	capacity := int64(len(p.chunks)) * p.chunkSize
	target := max(capacity, g.capacityTarget)
	if g.logical {
		target = max(target, int64(local)*page.PageSize)
	}
	if g.policy {
		target = max(target, p.growTarget.Load())
	}
	if target > math.MaxInt64-(p.chunkSize-1) {
		return fmt.Errorf("pager: growth size overflow")
	}
	target = ((target + p.chunkSize - 1) / p.chunkSize) * p.chunkSize
	if g.policy && p.growWake != nil && p.chunkSize >= minAsyncPregrowChunkSize && target-int64(local)*page.PageSize < p.chunkSize/2 {
		if target > math.MaxInt64-p.chunkSize {
			return fmt.Errorf("pager: pregrowth size overflow")
		}
		target += p.chunkSize
	}
	g.owner = weak.Make(p)
	g.source = weak.Make(p.atomicChunks.Load())
	g.file = weak.Make(p.file)
	g.verified = weak.Make(p.verified.Load())
	g.prefetch = weak.Make(p.prefetched.Load())
	g.memoryOnly = p.memoryOnly
	g.count = count
	g.oldChunks = len(p.chunks)
	g.totalChunks = int(target / p.chunkSize)
	g.capacityTarget = target
	if v := p.verified.Load(); v != nil {
		g.oldVerified = len(v.chunks)
	}
	g.needVerified = int((local + verifiedChunkPages - 1) / verifiedChunkPages)
	if v := p.prefetched.Load(); v != nil {
		g.oldPrefetch = len(v.words)
	}
	g.needWords = (g.totalChunks + 63) / 64
	if g.metadata != nil {
		alloc := retainedalloc.AllocationCharge
		ptr := uint64(unsafe.Sizeof(uintptr(0)))
		var next, old uint64
		if g.totalChunks > g.oldChunks {
			next += 2*alloc(uint64(g.totalChunks)*3*ptr) + alloc(uint64(unsafe.Sizeof(chunkList{})))
			old += alloc(uint64(cap(p.chunks)) * 3 * ptr)
			if prior := p.atomicChunks.Load(); prior != nil {
				old += alloc(uint64(unsafe.Sizeof(chunkList{})))
				if len(p.chunks) == 0 || len(prior.data) == 0 || &p.chunks[0] != &prior.data[0] {
					old += alloc(uint64(cap(prior.data)) * 3 * ptr)
				}
			}
		}
		if g.needVerified > g.oldVerified {
			next += alloc(uint64(unsafe.Sizeof(verifiedBitset{}))) + alloc(uint64(g.needVerified)*3*ptr) + uint64(g.needVerified-g.oldVerified)*alloc(verifiedChunkWords*8)
			if prior := p.verified.Load(); prior != nil {
				old += alloc(uint64(unsafe.Sizeof(verifiedBitset{}))) + alloc(uint64(cap(prior.chunks))*3*ptr)
			}
		}
		if g.needWords > g.oldPrefetch {
			next += alloc(uint64(unsafe.Sizeof(prefetchBitset{}))) + alloc(uint64(g.needWords)*8)
			if prior := p.prefetched.Load(); prior != nil {
				old += alloc(uint64(unsafe.Sizeof(prefetchBitset{}))) + alloc(uint64(cap(prior.words))*8)
			}
		}
		if g.needWords > len(p.dirtyChunks.words) {
			next += alloc(uint64(g.needWords) * 8)
			old += alloc(uint64(cap(p.dirtyChunks.words)) * 8)
		}
		if err := g.metadata.Add(next); err != nil {
			return err
		}
		g.metadataCharge += next
		g.metadataOld = old
	}
	g.captured = true
	return nil
}

func (g *Growth) install(p *Pager) {
	if g.metadata != nil {
		p.metadataReadMu.Lock()
		defer p.metadataReadMu.Unlock()
	}

	if g.primary != nil {
		p.chunks = g.primary
		p.atomicChunks.Store(g.preparedView)
	}
	if g.dirtyOutput != nil {
		p.dirtyChunks.words = g.dirtyOutput
		p.dirtyChunks.count = g.dirtyCount
		p.dirtyChunks.low = g.dirtyLow
		p.dirtyChunks.high = g.dirtyHigh
		p.dirtyChunks.revision++
	}
	if g.prefetchOutput != nil {
		p.prefetched.Store(g.prefetchOutput)
	}
	if g.verifiedOutput != nil {
		p.verified.Store(g.verifiedOutput)
	}
	if g.logical && g.targetPages > g.count {
		p.numPages.Store(g.targetPages)
	}
	if g.metadata != nil {
		// New backing is now owned by Pager; source descriptor readers have ended.
		g.metadata.Remove(g.metadataOld + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Growth{}))))
		g.metadataCharge = 0
		g.metadataOld = 0
	}
	g.installed = true
	g.primary = nil
	g.atomicView = nil
	g.preparedView = nil
	g.dirtyOutput = nil
	g.prefetchOutput = nil
	g.verifiedOutput = nil
}

func (g *Growth) finishFailure(w *iterator.OrdinalScanWork) (bool, error) {
	done, e := g.Cancel(w)
	if !done || e != nil {
		return false, e
	}
	return false, g.failure
}

// Cancel admits each private unmap and clears that ownership only on success.
// It never touches copied source mappings, and needs no live pager/file lease.
func (g *Growth) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if g == nil || g.installed || g.cancelled {
		return true, nil
	}
	for g.mapped > g.oldChunks {
		if ok, e := pagerReserve(w, 1, 512); !ok || e != nil {
			return false, e
		}
		i := g.mapped - 1
		c := g.primary[i]
		if c != nil && !g.memoryOnly {
			if e := munmapFile(c); e != nil {
				return false, e
			}
		}
		g.primary[i] = nil
		g.atomicView[i] = nil
		g.mapped--
	}
	if ok, e := pagerReserve(w, 1, 6*uint64(unsafe.Sizeof(Growth{}))+512); !ok || e != nil {
		return false, e
	}
	failure := g.failure
	owner, charge := g.metadata, g.metadataCharge
	*g = Growth{cancelled: true, failure: failure}
	if owner != nil {
		owner.Remove(charge)
	}
	return true, nil
}
