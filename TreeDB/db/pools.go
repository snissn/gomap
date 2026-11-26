package db

import (
	"bytes"
	"errors"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Iterator is the internal interface for iteration.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	KeyCopy(dst []byte) []byte
	ValueCopy(dst []byte) []byte
	Close() error
	Error() error
	// Reset resets the iterator for reuse.
	Reset(start, end []byte)
}

// SnapshotPool centralizes Snapshot allocation and cleanup.
//
// Exported Snapshot handles are deliberately not reused. A caller can retain a
// pointer after Close, and reactivating that same address would let the stale
// alias operate on an unrelated snapshot. No generation stored in the reused
// object can distinguish those aliases.
type SnapshotPool struct{}

// oneShotRead is never exposed to callers, callbacks, or iterators. Its owner
// performs exactly one synchronous owned-value read before returning it to the
// pool. Exported Snapshot handles must never enter this pool.
type oneShotRead struct {
	snapshot Snapshot
	// Fixed synchronous successor storage belongs to this existing private read
	// owner. It contains no borrowed page, value-reader or tree aliases.
	successor     tree.OwnedSuccessorScratch
	successorKey  [page.PageSize]byte
	successorLeaf [page.PageSize]byte
	// Scoped alternate-root tree used transiently during OneShotReadView key
	// scans and always scrubbed via Reset(nil, nil, 0) before returning.
	altTree tree.Tree
}

const dbOneShotReadCacheSlots = 8

var oneShotReadPool = sync.Pool{New: func() any {
	return &oneShotRead{snapshot: Snapshot{registryShardHint: snapshotShardHintUnset}}
}}

func (db *DB) getOneShotRead() *oneShotRead {
	if db != nil {
		for i := range db.oneShotReadCache {
			if db.oneShotReadCache[i].Load() != nil {
				if r := db.oneShotReadCache[i].Swap(nil); r != nil {
					return r
				}
			}
		}
	}
	return oneShotReadPool.Get().(*oneShotRead)
}

func (db *DB) putOneShotRead(r *oneShotRead) {
	if r == nil {
		return
	}
	if db != nil && !db.closing.Load() {
		for i := range db.oneShotReadCache {
			if db.oneShotReadCache[i].Load() == nil && db.oneShotReadCache[i].CompareAndSwap(nil, r) {
				if db.closing.Load() {
					db.drainOneShotReadCache()
				}
				return
			}
		}
	}
	oneShotReadPool.Put(r)
}

func (db *DB) drainOneShotReadCache() {
	if db == nil {
		return
	}
	for i := range db.oneShotReadCache {
		if r := db.oneShotReadCache[i].Swap(nil); r != nil {
			oneShotReadPool.Put(r)
		}
	}
}

func (db *DB) acquireOneShotReadOrErr() (*oneShotRead, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	r := db.getOneShotRead()
	db.valueLogPublicationMu.RLock()
	ok := db.captureSnapshotForReadWithValueLogPublicationLockHeld(&r.snapshot, true)
	db.valueLogPublicationMu.RUnlock()
	if !ok {
		db.putOneShotRead(r)
		return nil, ErrClosed
	}
	return r, nil
}

func (r *oneShotRead) close() error {
	// Capture the owning DB before closeRead/putRead scrubs snapshot.db so
	// successfully scrubbed one-shot guards return to the DB's non-lossy cache.
	owner := r.snapshot.db
	// Close shares all registry, value-log, and leaf-generation release logic.
	// The read has returned, so there are no outstanding readers or iterators.
	err := r.snapshot.closeRead(true)
	if err != nil {
		// The exact Snapshot may now be linked to its original cleanup owner.
		// Reusing it would overwrite that owner's surviving physical debt.
		return err
	}
	// This private capture exposes no Snapshot/iterator and its one synchronous
	// owned-value read has returned. Close therefore completed the finalizer,
	// including SnapshotPool.Put's scrub before stablePagerCompletion. Repeat
	// neither that scrub nor failed-cleanup reuse here.
	owner.putOneShotRead(r)
	return err
}

// OneShotReadView is a scoped, non-escaping point-read view valid only during
// the execution of a DB.WithOneShotRead callback. Callers must not retain or
// use a copy of this view after the callback returns.
type OneShotReadView struct {
	r   *oneShotRead
	gen uint64
}

func (v OneShotReadView) active() bool {
	return v.r != nil && v.r.snapshot.generation.Load() == v.gen && !v.r.snapshot.closed.Load()
}

// StateToken returns the immutable scalar state pinned by this one-shot read.
func (v OneShotReadView) StateToken() (StateToken, bool) {
	if !v.active() {
		return StateToken{}, false
	}
	return stateTokenFromState(v.r.snapshot.state)
}

// Pager returns the pager pinned by this one-shot read.
func (v OneShotReadView) Pager() *pager.Pager {
	if !v.active() || v.r.snapshot.idx == nil {
		return nil
	}
	return v.r.snapshot.idx.pager
}

// GetAppendAtRoot appends the value for key in rootID to dst and reports
// whether the key was found. Alternate roots use a stack-local tree bound to
// this one-shot read's pager and value reader so pooled one-shot state never
// caches or retains alternate-root trees.
func (v OneShotReadView) GetAppendAtRoot(rootID uint64, key, dst []byte) ([]byte, bool, error) {
	if !v.active() {
		return dst, false, ErrClosed
	}
	if rootID == 0 {
		return dst, false, nil
	}
	s := &v.r.snapshot
	var out []byte
	var err error
	if s.state != nil && rootID == s.state.RootPageID {
		out, err = s.tree.GetAppend(key, dst)
	} else {
		if s.idx == nil || s.idx.pager == nil {
			return dst, false, ErrClosed
		}
		var tr tree.Tree
		bindings := s.reader.appendReadBindings()
		tr.ResetAppendReadBindings(s.idx.pager, &bindings, rootID)
		out, err = tr.GetAppend(key, dst)
	}
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst, false, nil
	}
	if err != nil {
		return dst, false, err
	}
	return out, true, nil
}

// SeekFirstLiveKeyAtRoot finds the smallest non-tombstone key in [start, end)
// at rootID and appends it to dst. It uses the private one-shot fixed successor
// scratch without acquiring a tree.Iterator or retaining alternate-root state.
func (v OneShotReadView) SeekFirstLiveKeyAtRoot(rootID uint64, start, end, dst []byte) ([]byte, bool, error) {
	if !v.active() {
		return dst, false, ErrClosed
	}
	if rootID == 0 {
		return dst, false, nil
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return dst, false, nil
	}
	r := v.r
	s := &r.snapshot
	var tr *tree.Tree
	if s.state != nil && rootID == s.state.RootPageID {
		tr = &s.tree
	} else {
		if s.idx == nil || s.idx.pager == nil {
			return dst, false, ErrClosed
		}
		r.altTree.Reset(s.idx.pager, &s.reader, rootID)
		defer r.altTree.Reset(nil, nil, 0)
		tr = &r.altTree
	}
	r.successor.BeginLeafBatch()
	defer r.successor.EndLeafBatch()
	curStart := start
	var nextBuf [256]byte
	for {
		entry, found, err := tr.SeekGEWithFixedScratch(curStart, end, r.successorKey[:], r.successorLeaf[:], &r.successor, r.readSuccessorLeaf)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return dst, false, nil
			}
			return dst, false, err
		}
		if !found {
			return dst, false, nil
		}
		if entry.Flags&node.FlagTombstone == 0 {
			return append(dst, entry.Key...), true, nil
		}
		if len(entry.Key)+1 <= len(nextBuf) {
			n := copy(nextBuf[:], entry.Key)
			nextBuf[n] = 0x00
			curStart = nextBuf[:n+1]
		} else {
			next := make([]byte, len(entry.Key)+1)
			copy(next, entry.Key)
			curStart = next
		}
		if end != nil && bytes.Compare(curStart, end) >= 0 {
			return dst, false, nil
		}
	}
}

// VisitKeysAtRoot visits keys in [start, end) at rootID in ascending order
// without exposing an exported Snapshot or Iterator. Borrowed keys passed to fn
// are valid only for the duration of each callback invocation. Returning
// cont=false stops iteration cleanly.
func (v OneShotReadView) VisitKeysAtRoot(rootID uint64, start, end []byte, includeTombstones bool, fn func(key []byte, isDeleted bool) (bool, error)) error {
	if !v.active() {
		return ErrClosed
	}
	if rootID == 0 || fn == nil {
		return nil
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	r := v.r
	s := &r.snapshot
	// Copy bounded start/end into pooled scratch when they fit (and clone only
	// oversized bounds) so caller stack-allocated key buffers never escape into
	// the pooled tree.Iterator.
	var sStart, sEnd []byte
	if len(start)+len(end) <= len(r.successorKey) {
		off := 0
		if start != nil {
			copy(r.successorKey[off:], start)
			sStart = r.successorKey[off : off+len(start) : off+len(start)]
			off += len(start)
		}
		if end != nil {
			copy(r.successorKey[off:], end)
			sEnd = r.successorKey[off : off+len(end) : off+len(end)]
		}
	} else {
		if start != nil {
			sStart = bytes.Clone(start)
		}
		if end != nil {
			sEnd = bytes.Clone(end)
		}
	}
	var tr *tree.Tree
	if s.state != nil && rootID == s.state.RootPageID {
		tr = &s.tree
	} else {
		if s.idx == nil || s.idx.pager == nil {
			return ErrClosed
		}
		r.altTree.Reset(s.idx.pager, &s.reader, rootID)
		defer r.altTree.Reset(nil, nil, 0)
		tr = &r.altTree
	}
	it := tr.IteratorWithOptions(sStart, sEnd, tree.IteratorOptions{
		Mode:              tree.IteratorModeKeysOnly,
		IncludeTombstones: includeTombstones,
	})
	defer func() { _ = it.Close() }()
	if ordinary, ok := it.(*tree.Iterator); ok {
		for ordinary.Valid() {
			cont, err := fn(ordinary.UnsafeKey(), ordinary.IsDeleted())
			if err != nil || !cont {
				return err
			}
			ordinary.Next()
		}
		return ordinary.Error()
	}
	for it.Valid() {
		cont, err := fn(it.UnsafeKey(), it.IsDeleted())
		if err != nil || !cont {
			return err
		}
		it.Next()
	}
	return it.Error()
}

// WithOneShotRead executes fn against a pooled private one-shot read view
// bracketed by the installed foreground-read observer. The view is valid only
// until fn returns and never exposes an exported *Snapshot.
func (db *DB) WithOneShotRead(fn func(OneShotReadView) error) (err error) {
	if db == nil {
		return ErrClosed
	}
	if fn == nil {
		return nil
	}
	r, err := db.acquireOneShotReadOrErr()
	if err != nil {
		return err
	}
	endForegroundRead := db.beginForegroundRead()
	if endForegroundRead != nil {
		defer endForegroundRead()
	}
	gen := r.snapshot.generation.Add(1)
	defer func() {
		r.snapshot.generation.Add(1)
		if closeErr := r.close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	return fn(OneShotReadView{r: r, gen: gen})
}

func NewSnapshotPool() *SnapshotPool {
	return &SnapshotPool{}
}

func (p *SnapshotPool) Get() *Snapshot {
	s := &Snapshot{registryShardHint: snapshotShardHintUnset}
	s.iteratorMu.Lock()
	s.generation.Add(1)
	s.closed.Store(true)
	s.iteratorMu.Unlock()
	return s
}

func (p *SnapshotPool) Put(s *Snapshot) {
	p.putRead(s, false)
}

func (p *SnapshotPool) putRead(s *Snapshot, oneShot bool) {
	if s == nil {
		return
	}
	// Exported handles are never reused: scrub all tree/pager aliases before
	// publishing completion of their original cleanup owner.
	s.tree.Reset(nil, nil, 0)
	s.treePager = nil
	s.treeRoot = 0
	if !oneShot {
		clear(s.rootTrees)
		s.rootTrees = nil
		// Overflow may have moved the active cache away from its first inline
		// entry. Only a populated entry carries an alias that still needs scrubbing.
		if s.rootTreeFirst[0].root != 0 {
			s.rootTreeFirst[0] = snapshotRootTree{}
		}
		// The private one-shot pool never caches an alternate-root reader,
		// historical rebind, or iterator. Its entries keep these fields empty.
		s.primaryRebindRoot = primaryStateRootV5{}
		s.primaryRebindErr = nil
	}
	s.db = nil
	s.idx = nil
	s.state = nil
	s.vlogManager = nil
	s.vlogPinned = false
	s.leafGenerationIDs = nil
	s.leafGenerationPinnedIDs = nil
	if cap(s.leafGenerationRefs) > 0 {
		clear(s.leafGenerationRefs[:cap(s.leafGenerationRefs)])
	}
	s.leafGenerationRefs = s.leafGenerationRefs[:0]
	s.leafGenerationPinSet = nil
	s.reader = valueReader{}
	s.registryID = 0
	if !oneShot {
		s.iteratorMu.Lock()
		clear(s.iterators)
	}
	s.readState.Store(snapshotReadClosedBit)
	s.closed.Store(true)
	s.finalized.Store(false)
	if !oneShot {
		s.iteratorMu.Unlock()
	}
	// Do not make the exported handle reusable. See SnapshotPool's contract.
}

type ghostIndex struct {
	gen       *indexGen
	retiredAt time.Time
}

type indexGhostManager struct {
	retirementMu   sync.Mutex
	stopped        bool
	db             *DB
	metadata       *retainedalloc.Owner
	metadataCharge uint64
	ghostCharge    uint64
	mu             sync.Mutex
	ghosts         []ghostIndex
	stopCh         chan struct{}
	doneCh         chan struct{}
	stopOnce       sync.Once
}

func newIndexGhostManager(metadata *retainedalloc.Owner) (*indexGhostManager, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(indexGhostManager{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	return &indexGhostManager{metadata: metadata, metadataCharge: charge}, nil
}

func (m *indexGhostManager) closeOriginalGeneration(gen *indexGen) (*retainedalloc.Owner, uint64) {
	if gen == nil {
		return nil, 0
	}
	metadata, charge, err := gen.closeWithCompletionV6()
	if err != nil {
		m.db.retainOriginalCleanupError(gen.originalCloseFailure(err))
		m.db.quiesceFailedRetiredGeneration(gen)
	}
	gen = nil
	return metadata, charge
}

func (m *indexGhostManager) start(db *DB) {
	m.db = db
	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	go m.loop()
}

func (m *indexGhostManager) loop() {
	defer close(m.doneCh)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.scavenge(5 * time.Second)
		}
	}
}

func (m *indexGhostManager) add(gen *indexGen) (*retainedalloc.Owner, uint64, error) {
	m.retirementMu.Lock()
	defer m.retirementMu.Unlock()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return gen.closeWithCompletionV6()
	}
	defer m.mu.Unlock()
	if len(m.ghosts) == cap(m.ghosts) {
		capacity := cap(m.ghosts) * 2
		if capacity < 4 {
			capacity = 4
		}
		if capacity < cap(m.ghosts) || uint64(capacity) > ^uint64(0)/uint64(unsafe.Sizeof(ghostIndex{})) {
			return nil, 0, retainedalloc.ErrCapacity
		}
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(ghostIndex{})))
		if err := m.metadata.AddPending(charge); err != nil {
			return nil, 0, err
		}
		old, oldCharge := m.ghosts, m.ghostCharge
		next := make([]ghostIndex, len(old), capacity)
		copy(next, old)
		m.ghosts, m.ghostCharge = next, charge
		clear(old[:cap(old)])
		old = nil
		m.metadata.RemovePending(oldCharge)
	}
	m.ghosts = append(m.ghosts, ghostIndex{gen: gen, retiredAt: time.Now()})
	return nil, 0, nil
}

func (m *indexGhostManager) scavenge(maxAge time.Duration) {
	// The original queue backing doubles as the retirement batch. Serialize
	// growth until its tail is consumed, so no copied list or premature refund
	// appears and one partition remains linear in the number of generations.
	m.retirementMu.Lock()
	defer m.retirementMu.Unlock()
	m.mu.Lock()
	original := m.ghosts
	now := time.Now()
	kept, boundary := 0, len(original)
	for kept < boundary {
		ghost := original[kept]
		if now.Sub(ghost.retiredAt) <= maxAge || (ghost.gen.registry != nil && ghost.gen.registry.MinPinnedSeq() != math.MaxUint64) {
			kept++
			continue
		}
		boundary--
		original[kept], original[boundary] = original[boundary], original[kept]
	}
	m.ghosts = original[:boundary]
	m.mu.Unlock()
	for i := boundary; i < len(original); i++ {
		metadata, charge := m.closeOriginalGeneration(original[i].gen)
		original[i] = ghostIndex{}
		if charge != 0 {
			metadata.RemovePending(charge)
		}
	}
}

func (m *indexGhostManager) stopWithCompletionV6() (*retainedalloc.Owner, uint64) {
	stopCh := m.stopCh
	doneCh := m.doneCh
	if stopCh != nil {
		m.stopOnce.Do(func() { close(stopCh) })
	}
	if doneCh != nil {
		<-doneCh
	}
	return m.closeAllWithCompletionV6()
}

func (m *indexGhostManager) closeAllWithCompletionV6() (*retainedalloc.Owner, uint64) {
	m.retirementMu.Lock()
	m.mu.Lock()
	m.stopped = true
	original := m.ghosts
	m.ghosts = nil
	m.mu.Unlock()
	for i := range original {
		metadata, charge := m.closeOriginalGeneration(original[i].gen)
		original[i] = ghostIndex{}
		if charge != 0 {
			metadata.RemovePending(charge)
		}
	}
	clear(original[:cap(original)])
	original = nil
	m.mu.Lock()
	owner, charge := m.metadata, m.ghostCharge+m.metadataCharge
	m.metadata, m.ghostCharge, m.metadataCharge = nil, 0, 0
	m.stopCh, m.doneCh = nil, nil
	m.db = nil
	m.mu.Unlock()
	m.retirementMu.Unlock()
	return owner, charge
}
