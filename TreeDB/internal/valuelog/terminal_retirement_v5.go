package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"unsafe"
)

var ErrTerminalRetirementV5 = errors.New("valuelog: terminal retirement ownership unavailable")

// BeginTerminalRetirementV5 closes deletion admission and joins workers admitted
// by earlier ordinary operations. It holds no manager lock while joining. This
// terminal shutdown control is distinct from each charged Set/File ref release;
// it does not charge or run physical File.Close/unlink through a scalar reserve.
func (m *Manager) BeginTerminalRetirementV5(w *iterator.OrdinalScanWork) (bool, error) {
	if m == nil || w == nil {
		return false, ErrTerminalRetirementV5
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Manager{}))) {
		return false, nil
	}
	m.mu.Lock()
	if m.closeDone != nil {
		m.mu.Unlock()
		return false, ErrTerminalRetirementV5
	}
	m.closing = true
	if m.retryStop != nil && !m.retryStopped {
		close(m.retryStop)
		m.retryStopped = true
	}
	m.mu.Unlock()
	m.retryWorkers.Wait()
	return true, nil
}

// TerminalSetRetirementV5 retains exact immutable constructor-created file refs.
// Its list is frozen during ordinary Set construction, not rediscovered while
// releasing. Manager owns tracked files until subsequent physical DB shutdown.
// Last-zombie ref release never creates a retry worker or deletes a pinned path.
type TerminalSetRetirementV5 struct {
	manager               *Manager
	set                   *Set
	at                    int
	started, last, done   bool
	live                  bool
	parent                *os.File
	parentBorrow          *retirementParentHandle
	admitted              bool
	pending               *File
	filePhase             uint8
	deleteLease           *rootpublication.IdentityDeleteLease
	cache                 *groupedFrameCache
	cacheShard, cacheSlot int
	cacheRaw, scratch     []byte
	scratchSpare          [][]byte
	scratchAt             int
	mappings              []ownedMmapV6
	mappingAt             int
	namespace             *ownedSegmentRemovalV6
	parentToClose         *os.File
}

func (m *Manager) PrepareTerminalSetRetirementV5(set *Set, w *iterator.OrdinalScanWork) (*TerminalSetRetirementV5, bool, error) {
	if m == nil || w == nil {
		return nil, false, ErrTerminalRetirementV5
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Set{}))+2*uint64(unsafe.Sizeof(Manager{}))+uint64(unsafe.Sizeof(TerminalSetRetirementV5{}))) {
		return nil, false, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.closing || m.closeDone != nil || (set != nil && (!set.retentionKnown || len(set.retirementFiles) != set.retentionShape.FileCount)) {
		return nil, false, ErrTerminalRetirementV5
	}
	return &TerminalSetRetirementV5{manager: m, set: set}, true, nil
}

// PrepareLiveSetRetirementV6 transfers one real Set edge to the same finite
// cursor. Each actual zombie uses its constructor-bound retained parent.
// Later leaf/value/foreign-parent registrations remain ordinary operations.
func (m *Manager) PrepareLiveSetRetirementV6(set *Set, w *iterator.OrdinalScanWork) (*TerminalSetRetirementV5, bool, error) {
	if m == nil || w == nil {
		return nil, false, ErrTerminalRetirementV5
	}
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(Set{}))+2*uint64(unsafe.Sizeof(Manager{}))+uint64(unsafe.Sizeof(TerminalSetRetirementV5{}))+uint64(len(m.dir))*4+512) {
		return nil, false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.closeDone != nil || m.stableResourcePins == nil || (m.deferredDeletionSync != nil && m.deferredDeletionSyncOwnedV6 == nil) || (set != nil && (!set.retentionKnown || len(set.retirementFiles) != set.retentionShape.FileCount || set.retentionShape.FileCount > 17 || set.RefCount.Load() <= 0)) {
		return nil, false, ErrLiveRetirementParentV6
	}
	// Each captured File's real producer/remaining mapping authority is checked
	// before consuming Set's edge. Unsupported ad-hoc mappings remain ordinary.
	if set != nil {
		for _, f := range set.retirementFiles {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(File{}))+2*uint64(unsafe.Sizeof(ownedMmapV6{}))) {
				return nil, false, nil
			}
			f.remapMu.Lock()
			data, _ := f.mmapData.Load().([]byte)
			valid := !f.legacyMappingV6 && (len(data) == 0 || f.currentMappingV6.matchesViewV6(data))
			f.remapMu.Unlock()
			if !valid {
				return nil, false, errOwnedMappingV6
			}
		}
	}
	m.ownedSetRetirementsV6++
	return &TerminalSetRetirementV5{manager: m, set: set, live: true, admitted: true}, true, nil
}

var ErrLiveRetirementParentV6 = errors.New("valuelog: missing exact retained retirement parent")

func (c *TerminalSetRetirementV5) Step(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if c == nil || w == nil {
		return false, false, ErrTerminalRetirementV5
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(TerminalSetRetirementV5{}))+2*uint64(unsafe.Sizeof(Manager{}))) {
		return false, false, nil
	}
	if c.done {
		return true, false, nil
	}
	m := c.manager
	m.mu.RLock()
	valid := m.closeDone == nil && (c.live || m.closing)
	terminal := m.closing
	m.mu.RUnlock()
	if !valid {
		return false, false, ErrTerminalRetirementV5
	}
	progress := false
	if terminal && c.live && c.filePhase < 2 {
		// Before physical deletion admission, terminal shutdown retains this exact
		// File in Manager. Independent pins continue to protect its persistent path.
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(File{}))+2*uint64(unsafe.Sizeof(Manager{}))) {
			return false, false, nil
		}
		m.mu.Lock()
		if c.parentBorrow != nil {
			final, ready := c.parentBorrow.dropWithWorkV6(false, w)
			if !ready {
				m.mu.Unlock()
				return false, false, nil
			}
			if final != nil {
				panic("terminal borrow lost its File parent owner")
			}
			c.parentBorrow, c.parent = nil, nil
		}
		if c.pending != nil && c.pending.ownedRetirementV6.Load() == c {
			c.pending.ownedRetirementV6.Store(nil)
			c.pending.deletionAdmissions--
		}
		m.mu.Unlock()
		c.pending = nil
		c.filePhase = 0
		c.live = false
		progress = true
	}
	if !c.started {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Set{}))) {
			return false, false, nil
		}
		if c.set != nil {
			// One transferred owner edge, as in ordinary Release. No failed CAS can
			// masquerade as finite progress or cause the edge to be decremented twice.
			refs := c.set.RefCount.Add(-1)
			c.started = true
			if refs < 0 {
				return false, true, ErrTerminalRetirementV5
			}
			c.last = refs == 0
		} else {
			c.started = true
		}
		progress = true
	}
	if c.last {
		for c.at < len(c.set.retirementFiles) || c.pending != nil {
			if c.pending == nil {
				// The last edge and its exclusive zombie claim share manager.mu;
				// no return can expose zero refs to an ordinary deletion helper.
				m.mu.Lock()
				f := c.set.retirementFiles[c.at]
				zombie := c.live && f.IsZombie.Load()
				headerBytes := 2*uint64(unsafe.Sizeof(File{})) + 2*uint64(unsafe.Sizeof(Manager{})) + 16
				if zombie {
					// Current guard, exact borrow, edge and possible nonlast
					// drop must all fit before exposing a zero-reference File.
					bytes := headerBytes*3 + 4*uint64(unsafe.Sizeof(retirementParentHandle{})) + 4*uint64(unsafe.Sizeof(retirementParentPool{}))
					if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit || 7 > w.RecordLimit-w.Records || bytes > w.ByteLimit-w.Bytes {
						m.mu.Unlock()
						return false, progress, nil
					}
					if !w.Reserve(1, headerBytes) {
						panic("admitted zombie header lost")
					}
					if m.files[f.ID] != f || f.deletionAdmissions != 0 || f.retryDeletePending.Load() || f.ownedRetirementV6.Load() != nil {
						m.mu.Unlock()
						return false, progress, ErrTerminalRetirementV5
					}
				}
				var parent *retirementParentHandle
				if zombie {
					var e error
					var ready bool
					parent, ready, e = borrowRetirementParentHandle(f, w)
					if e != nil || !ready {
						m.mu.Unlock()
						return false, progress, e
					}
				}
				if !w.Reserve(1, headerBytes) {
					m.mu.Unlock()
					return false, progress, nil
				}
				refs := f.RefCount.Add(-1)
				c.at++
				progress = true
				if refs < 0 {
					m.mu.Unlock()
					return false, progress, ErrTerminalRetirementV5
				}
				if zombie && refs == 0 {
					f.ownedRetirementV6.Store(c)
					f.deletionAdmissions++
					c.pending, c.filePhase = f, 1
					c.parentBorrow, c.parent = parent, parent.file
				} else if parent != nil {
					final, ready := parent.dropWithWorkV6(false, w)
					if !ready || final != nil {
						panic("nonlast borrow lost admitted File owner unwind")
					}
				}
				m.mu.Unlock()
			}
			if c.pending != nil {
				ready, p, e := c.retirePendingFileV6(w)
				progress = progress || p
				if e != nil || !ready {
					return false, progress, e
				}
				c.pending = nil
			}
		}
	}
	if c.admitted {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Manager{}))+16) {
			return false, progress, nil
		}
		m.mu.Lock()
		m.ownedSetRetirementsV6--
		m.mu.Unlock()
		c.admitted = false
		progress = true
	}
	c.set = nil
	c.manager = nil
	c.done = true
	return true, true, nil
}

// The physical phases are implemented by the same cursor. Until admitted they
// remain exact debt, including a separately pinned identity; no background
// deletion or ordinary Release receives the edge.
func (c *TerminalSetRetirementV5) retirePendingFileV6(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(File{}))+2*uint64(unsafe.Sizeof(Manager{}))+2*uint64(unsafe.Sizeof(TerminalSetRetirementV5{}))) {
		return false, false, nil
	}
	m, f := c.manager, c.pending
	progress := false
	for {
		switch c.filePhase {
		case 1:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(rootpublication.IdentityDeleteLease{}))+512+uint64(len(f.stableNamespace))*2) {
				return false, progress, nil
			}
			m.mu.Lock()
			lease, e := m.stableDeleteLease(f)
			m.mu.Unlock()
			if e != nil {
				return false, progress, e
			}
			c.deleteLease = lease
			c.filePhase = 2
			progress = true
		case 2:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(File{}))) {
				return false, progress, nil
			}
			f.closeMu.Lock()
			f.closed.Store(true)
			f.cacheMu.Lock()
			if f.cacheRawPooled {
				c.cacheRaw = f.cacheRaw
			}
			f.cacheRaw = nil
			f.cacheRawPooled = false
			f.cacheK, f.cacheLen, f.cacheFlags = 0, 0, 0
			f.cacheStart.Store(0)
			f.cacheMu.Unlock()
			f.scratchMu.Lock()
			c.scratch, c.scratchSpare = f.decodeScratch, f.decodeScratchSpare
			f.decodeScratch, f.decodeScratchSpare = nil, nil
			f.scratchMu.Unlock()
			f.groupedMu.Lock()
			c.cache = f.groupedFrameCache.Load()
			f.groupedFrameCache.Store(nil)
			f.groupedMu.Unlock()
			f.closeMu.Unlock()
			c.filePhase = 3
			progress = true
		case 3:
			if c.cache != nil && c.cacheShard < len(c.cache.shards) {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(groupedFrameCacheShard{}))) {
					return false, progress, nil
				}
				shard := &c.cache.shards[c.cacheShard]
				shard.mu.Lock()
				if c.cacheSlot == len(shard.slots) {
					shard.clock = 0
					shard.mu.Unlock()
					c.cacheShard++
					c.cacheSlot = 0
					progress = true
					continue
				}
				if !w.Reserve(2, 2*uint64(unsafe.Sizeof(groupedFrameCacheSlot{}))+2*uint64(unsafe.Sizeof(groupedFrameCacheBudget{}))+64) {
					shard.mu.Unlock()
					return false, progress, nil
				}
				shard.evictSlotLocked(c.cache, c.cacheSlot)
				c.cacheSlot++
				shard.mu.Unlock()
				progress = true
				continue
			}
			c.cache = nil
			c.filePhase = 4
		case 4:
			var buf []byte
			switch {
			case c.cacheRaw != nil:
				buf = c.cacheRaw
			case c.scratch != nil:
				buf = c.scratch
			case c.scratchAt < len(c.scratchSpare):
				buf = c.scratchSpare[c.scratchAt]
			default:
				c.scratchSpare = nil
				c.filePhase = 5
				continue
			}
			// putDecodeScratch touches fixed counters/channel and a slice header, not
			// every payload byte or a variable pool traversal.
			if !w.Reserve(1, 128) {
				return false, progress, nil
			}
			putDecodeScratch(buf)
			switch {
			case c.cacheRaw != nil:
				c.cacheRaw = nil
			case c.scratch != nil:
				c.scratch = nil
			default:
				c.scratchSpare[c.scratchAt] = nil
				c.scratchAt++
			}
			progress = true
		case 5:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(File{}))+uint64(unsafe.Sizeof(ownedMmapV6{}))*uint64(len(f.deadMappings)+1)) {
				return false, progress, nil
			}
			f.remapMu.Lock()
			data, _ := f.mmapData.Load().([]byte)
			c.mappings = f.deadMappings
			if len(data) != 0 || f.currentMappingV6.constructor {
				c.mappings = append(c.mappings, f.detachMmapOwnerV6Locked(data))
			}
			f.deadMappings = nil
			f.mmapData.Store([]byte(nil))
			f.remapMu.Unlock()
			c.filePhase = 6
			progress = true
		case 6:
			if c.mappingAt < len(c.mappings) {
				mapping := &c.mappings[c.mappingAt]
				if !mapping.constructor {
					if !w.Reserve(1, uint64(len(mapping.data))+64) {
						return false, progress, nil
					}
					if e := mapping.closeV6(); e != nil {
						return false, progress, e
					}
					c.mappingAt++
					progress = true
					continue
				}
				n := mapping.remainingV6()
				if n == 0 {
					c.mappingAt++
					progress = true
					continue
				}
				page := uintptr(os.Getpagesize())
				max := uintptr(256<<10) / page * page
				if max == 0 {
					max = page
				}
				if n > max {
					n = max
				}
				if !w.Reserve(1, uint64(n)+2*uint64(unsafe.Sizeof(ownedMmapV6{}))) {
					return false, progress, nil
				}
				if _, e := unmapOwnedRangeV6(mapping, n); e != nil {
					return false, progress, e
				}
				progress = true
				continue
			}
			c.mappings = nil
			f.deadMappingsCount.Store(0)
			f.deadMappedBytes.Store(0)
			c.filePhase = 7
		case 7:
			if !w.Reserve(1, 256) {
				return false, progress, nil
			}
			if e := f.File.Close(); e != nil {
				return false, progress, e
			}
			c.filePhase = 8
			progress = true
		case 8:
			if c.namespace == nil {
				if !w.Reserve(1, uint64(unsafe.Sizeof(ownedSegmentRemovalV6{}))+uint64(len(f.Path))*6+512) {
					return false, progress, nil
				}
				c.namespace = &ownedSegmentRemovalV6{file: f, parent: c.parent, identity: f.retirementIdentity}
				progress = true
			}
			ready, p, e := c.namespace.Step(w)
			progress = progress || p
			if e != nil || !ready {
				return false, progress, e
			}
			f.deleteMu.Lock()
			f.deleteCompleted = true
			f.deleteMu.Unlock()
			c.filePhase = 9
		case 9:
			m.mu.RLock()
			fence := m.deferredDeletionSyncOwnedV6
			m.mu.RUnlock()
			if fence != nil {
				ready, e := fence(c.parent, f.Path, segmentNamespaceResource(f.Path), w)
				if e != nil || !ready {
					return false, progress, e
				}
			} else {
				if !w.Reserve(3, 512+uint64(len(f.Path))*4) {
					return false, progress, nil
				}
				if e := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, segmentNamespaceResource(f.Path), filepath.Dir(f.Path), filepath.Dir(f.Path)); e != nil {
					return false, progress, e
				}
				if e := c.parent.Sync(); e != nil {
					return false, progress, e
				}
				if e := durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, segmentNamespaceResource(f.Path), filepath.Dir(f.Path), filepath.Dir(f.Path)); e != nil {
					return false, progress, e
				}
			}
			c.filePhase = 10
			progress = true
		case 10:
			ready, p, e := c.deleteLease.CommitDeletedWithWorkV6(w)
			progress = progress || p
			if e != nil || !ready {
				return false, progress, e
			}
			c.filePhase = 11
		case 11:
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(Manager{}))+2*uint64(unsafe.Sizeof(File{}))+512) {
				return false, progress, nil
			}
			m.mu.Lock()
			if m.files[f.ID] != f || f.ownedRetirementV6.Load() != c {
				m.mu.Unlock()
				return false, progress, ErrTerminalRetirementV5
			}
			e := m.unobserveStableFileLocked(f)
			if e == nil {
				m.detachSegmentLocked(f)
			}
			m.mu.Unlock()
			if e != nil {
				return false, progress, e
			}
			c.filePhase = 12
			progress = true
		case 12:
			// The same pool core removes this exact File membership. It never
			// searches a bucket or rebuilds unrelated retained directory nodes.
			final, ready := releaseRetirementParentWithWorkV6(f, w)
			if !ready {
				return false, progress, nil
			}
			c.parentToClose = final
			c.filePhase = 13
			progress = true
		case 13:
			if c.parentBorrow != nil {
				final, ready := c.parentBorrow.dropWithWorkV6(false, w)
				if !ready {
					return false, progress, nil
				}
				c.parentBorrow, c.parent = nil, nil
				if final != nil {
					if c.parentToClose != nil {
						panic("duplicate exact parent close owner")
					}
					c.parentToClose = final
				}
				progress = true
			}
			if c.parentToClose != nil {
				if !w.Reserve(1, 256) {
					return false, progress, nil
				}
				if e := c.parentToClose.Close(); e != nil {
					return false, progress, e
				}
				c.parentToClose = nil
				progress = true
			}
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(Manager{}))+2*uint64(unsafe.Sizeof(File{}))) {
				return false, progress, nil
			}
			m.mu.Lock()
			f.ownedRetirementV6.Store(nil)
			f.deletionAdmissions--
			m.mu.Unlock()
			c.filePhase = 0
			c.deleteLease = nil
			c.namespace = nil
			c.cacheShard = 0
			c.cacheSlot = 0
			c.scratchAt = 0
			c.mappingAt = 0
			return true, true, nil
		}
	}
}

var ErrLiveRetirementPhysicalV6 = errors.New("valuelog: owned physical retirement remains pending")

// Ordinary captures keep Set and its immutable file-ref tail in one allocation
// for the bounded file closure. Larger ordinary Sets retain an explicit tail
// allocation; the constructor's retention envelope records its real capacity.
func retirementSetCapacityV5(hint int) int {
	switch {
	case hint <= 0:
		return 0
	case hint <= 1:
		return 1
	case hint <= 2:
		return 2
	case hint <= 4:
		return 4
	case hint <= 8:
		return 8
	case hint <= 17:
		return 17
	default:
		return hint
	}
}
func newRetirementSetV5(hint int) *Set {
	switch retirementSetCapacityV5(hint) {
	case 0:
		return &Set{}
	case 1:
		p := new(struct {
			set   Set
			files [1]*File
		})
		p.set.retirementFiles = p.files[:0]
		return &p.set
	case 2:
		p := new(struct {
			set   Set
			files [2]*File
		})
		p.set.retirementFiles = p.files[:0]
		return &p.set
	case 4:
		p := new(struct {
			set   Set
			files [4]*File
		})
		p.set.retirementFiles = p.files[:0]
		return &p.set
	case 8:
		p := new(struct {
			set   Set
			files [8]*File
		})
		p.set.retirementFiles = p.files[:0]
		return &p.set
	case 17:
		p := new(struct {
			set   Set
			files [17]*File
		})
		p.set.retirementFiles = p.files[:0]
		return &p.set
	default:
		return &Set{retirementFiles: make([]*File, 0, hint)}
	}
}

// The real typed producer supplies actual charged fence/error operations.
// Replacing ordinary callback configuration invalidates this provenance.
func (m *Manager) SetDeferredDeletionSyncOwnedV6(fence func(*os.File, string, durabilitycut.Resource, *iterator.OrdinalScanWork) (bool, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ownedSetRetirementsV6 != 0 {
		panic("valuelog: replacing live owned deletion fence")
	}
	m.deferredDeletionSyncOwnedV6 = fence
}
