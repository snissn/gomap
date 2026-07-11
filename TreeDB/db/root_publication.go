package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const (
	rootPublicationMaxPendingBytes   = 256 << 20
	rootPublicationMaxPendingCommits = 4096
)

// PreparedRootCandidate is the immutable handoff from root construction to
// stable publication. Persistent side stores can extend dependencies without
// changing the meta publication state machine.
type PreparedRootCandidate struct {
	CommitSeq            uint64
	UserRootPageID       uint64
	SystemRootPageID     uint64
	FreelistHeadID       uint64
	TotalPages           uint64
	AppliedCommandLSN    uint64
	MaxEntryRevision     uint64
	TouchedIndexPages    []uint64
	RetiredPages         []uint64
	TouchedValueLogFiles []uint32
	DependencyPath       string
	DependencyPaths      []string
	OuterLeafFrontier    uint64
	Meta                 page.MetaPageBody
	OldValueLogSet       *valuelog.Set
	ValueLogAppender     ValueLogAppender
	LeafPageLog          LeafPageLog
	OwnedBytes           uint64
	DependenciesFlushed  bool
	holdsPreparePin      bool
}

type rootPublicationSnapshot struct {
	visibleCommitSeq           uint64
	durableCommitSeq           uint64
	oldestRecoverableCommitSeq uint64
	pendingCandidates          uint64
	pendingBytes               uint64
	candidatesCoalesced        uint64
	dependencySyncs            uint64
	metaSyncs                  uint64
	publicationStalls          uint64
	poisonEvents               uint64
	waiterCount                uint64
	waiterLatency              time.Duration
	waiterLatencyMax           time.Duration
}

// RootPublicationCoordinator is the sole ordinary-production owner of meta
// installation. Root builders publish visibility and enqueue candidates; this
// worker owns dependency ordering and durable meta-slot advancement.
type RootPublicationCoordinator struct {
	db *DB

	publishMu     sync.Mutex
	recoveryMu    sync.RWMutex
	mu            sync.Mutex
	cond          *sync.Cond
	wake          chan struct{}
	stop          chan struct{}
	done          chan struct{}
	indexSnapshot *pager.IndexPageSnapshot
	metaPageImage [page.PageSize]byte

	pending                    []*PreparedRootCandidate
	pendingBytes               uint64
	durableMeta                page.MetaPageBody
	durableMetaPageID          uint64
	metaSlotSeq                [2]uint64
	metaSlotValid              [2]bool
	metaSlotMeta               [2]page.MetaPageBody
	recoveryClosureGeneration  uint64
	visibleCommitSeq           uint64
	durableCommitSeq           uint64
	oldestRecoverableCommitSeq uint64
	stall                      error
	stallCommitSeq             uint64
	stallAttempt               uint64
	durabilityDemandSeq        uint64
	poison                     error
	attempts                   uint64
	stopped                    bool

	candidatesCoalesced uint64
	dependencySyncs     uint64
	metaSyncs           uint64
	publicationStalls   uint64
	poisonEvents        uint64
	waiterCount         uint64
	waiterLatency       time.Duration
	waiterLatencyMax    time.Duration
}

func newRootPublicationCoordinator(db *DB) *RootPublicationCoordinator {
	r := &RootPublicationCoordinator{
		db:                         db,
		wake:                       make(chan struct{}, 1),
		stop:                       make(chan struct{}),
		done:                       make(chan struct{}),
		indexSnapshot:              pager.NewIndexPageSnapshot(),
		durableMeta:                db.meta,
		durableMetaPageID:          db.metaPageID,
		visibleCommitSeq:           db.meta.CommitSeq,
		durableCommitSeq:           db.meta.CommitSeq,
		oldestRecoverableCommitSeq: db.meta.CommitSeq,
	}
	r.cond = sync.NewCond(&r.mu)
	for id := uint64(0); id < 2; id++ {
		if m, ok := db.readMeta(id); ok {
			r.metaSlotSeq[id] = m.CommitSeq
			r.metaSlotValid[id] = true
			r.metaSlotMeta[id] = m
		}
	}
	r.recomputeOldestRecoverableLocked()
	go r.run()
	return r
}

func (r *RootPublicationCoordinator) recomputeOldestRecoverableLocked() {
	if !r.metaSlotValid[0] {
		if r.metaSlotValid[1] {
			r.oldestRecoverableCommitSeq = r.metaSlotSeq[1]
		}
		return
	}
	if !r.metaSlotValid[1] {
		r.oldestRecoverableCommitSeq = r.metaSlotSeq[0]
		return
	}
	a, b := r.metaSlotSeq[0], r.metaSlotSeq[1]
	if a < b {
		r.oldestRecoverableCommitSeq = a
	} else {
		r.oldestRecoverableCommitSeq = b
	}
}

// durableAppliedCommandLSN returns the command-WAL coverage recorded by the
// most recently synced meta page. Visible state can run ahead of this value
// while publication is queued or stalled and must not authorize source-log
// deletion.
func (r *RootPublicationCoordinator) durableAppliedCommandLSN() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	applied := r.durableMeta.AppliedCommandLSN
	r.mu.Unlock()
	return applied
}

// recoverableValueLogRoots returns the user and system roots named by every
// valid meta slot. Value-log GC scans this exact recovery closure before it
// can zombify a segment; retaining whole DBState value-log sets here would pin
// unrelated, unreachable files and prevent reclamation.
func (r *RootPublicationCoordinator) recoverableValueLogRoots() (userRoots, systemRoots []uint64, generation uint64) {
	if r == nil {
		return nil, nil, 0
	}
	r.mu.Lock()
	for id, valid := range r.metaSlotValid {
		if !valid {
			continue
		}
		meta := r.metaSlotMeta[id]
		if meta.UserRootPageID != 0 {
			userRoots = append(userRoots, meta.UserRootPageID)
		}
		if meta.SystemRootPageID != 0 {
			systemRoots = append(systemRoots, meta.SystemRootPageID)
		}
	}
	for _, candidate := range r.pending {
		if candidate == nil {
			continue
		}
		if candidate.UserRootPageID != 0 {
			userRoots = append(userRoots, candidate.UserRootPageID)
		}
		if candidate.SystemRootPageID != 0 {
			systemRoots = append(systemRoots, candidate.SystemRootPageID)
		}
	}
	generation = r.recoveryClosureGeneration
	r.mu.Unlock()
	return userRoots, systemRoots, generation
}

func (r *RootPublicationCoordinator) lockRecoverableValueLogRoots(generation uint64) (func(), bool) {
	if r == nil {
		return func() {}, generation == 0
	}
	r.recoveryMu.RLock()
	r.mu.Lock()
	matches := r.recoveryClosureGeneration == generation
	r.mu.Unlock()
	if !matches {
		r.recoveryMu.RUnlock()
		return nil, false
	}
	return r.recoveryMu.RUnlock, true
}

func (r *RootPublicationCoordinator) register(candidate *PreparedRootCandidate) error {
	if r == nil || candidate == nil {
		return ErrClosed
	}
	// Registration is called while db.mu serializes the visible-root install, so
	// it must never wait for a queued maintenance writer here. Every production
	// candidate transfers the already-held preparation pin before registration.
	if !candidate.holdsPreparePin {
		return errors.New("root publication: candidate missing preparation pin")
	}
	releaseOnError := func() {
		if candidate.holdsPreparePin {
			candidate.holdsPreparePin = false
			r.db.publishPrepareMu.RUnlock()
		}
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		releaseOnError()
		return ErrClosed
	}
	if r.poison != nil {
		err := r.poison
		r.mu.Unlock()
		releaseOnError()
		return err
	}
	hadStall := r.stall != nil
	debtExceeded := r.pendingBytes > rootPublicationMaxPendingBytes ||
		candidate.OwnedBytes > rootPublicationMaxPendingBytes-r.pendingBytes
	if len(r.pending) >= rootPublicationMaxPendingCommits || debtExceeded {
		if r.stall == nil {
			r.stall = ErrPublicationStalled
			r.stallCommitSeq = candidate.CommitSeq
			r.publicationStalls++
		}
		err := r.stall
		r.mu.Unlock()
		releaseOnError()
		r.signal()
		return err
	}
	wasEmpty := len(r.pending) == 0
	r.pending = append(r.pending, candidate)
	r.recoveryClosureGeneration++
	if candidate.OwnedBytes > ^uint64(0)-r.pendingBytes {
		r.pendingBytes = ^uint64(0)
	} else {
		r.pendingBytes += candidate.OwnedBytes
	}
	r.visibleCommitSeq = candidate.CommitSeq
	r.mu.Unlock()
	if wasEmpty || hadStall {
		r.signal()
	}
	return nil
}

func (r *RootPublicationCoordinator) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *RootPublicationCoordinator) run() {
	defer close(r.done)
	for {
		select {
		case <-r.wake:
			for {
				r.mu.Lock()
				if len(r.pending) == 0 || r.poison != nil {
					r.mu.Unlock()
					break
				}
				r.mu.Unlock()
				// Publication is event-driven. Acquiring the writer barrier drains
				// already-admitted builders; the latest complete candidate observed
				// after that barrier becomes the coalesced frontier.
				idx, lockErr := r.lockRootSerialization()
				if lockErr != nil {
					r.recordFailure(0, lockErr, false)
					break
				}
				r.mu.Lock()
				if len(r.pending) == 0 || r.poison != nil {
					r.mu.Unlock()
					fenceErr := idx.allocator.EndPublicationFence()
					r.unlockRootSerialization()
					r.publishMu.Unlock()
					if fenceErr != nil {
						r.recordFailure(0, fenceErr, false)
					}
					break
				}
				frontier := r.pending[len(r.pending)-1]
				closure := r.closureThroughLocked(frontier)
				r.attempts++
				r.mu.Unlock()
				publicationFrontier := r.sealedAllocatorFrontier(idx, frontier)
				publicationClosure := append([]*PreparedRootCandidate(nil), closure...)
				publicationClosure[len(publicationClosure)-1] = publicationFrontier
				publicationPages := rootPublicationIndexPages(publicationClosure)
				r.unlockRootSerialization()

				// The fence defers frees and forces successor allocations to append,
				// keeping every page in the captured publication generation immutable.
				// Copy outside root serialization to keep foreground builders moving.
				captureErr := idx.pager.CaptureIndexPagesInto(r.indexSnapshot, publicationPages)
				if captureErr != nil {
					captureErr = errors.Join(captureErr, r.closePublicationFence(idx))
					r.recordFailure(frontier.CommitSeq, captureErr, false)
					break
				}

				r.recoveryMu.Lock()
				err, postMeta := r.publish(publicationFrontier, publicationClosure, r.indexSnapshot)
				if err != nil {
					r.recoveryMu.Unlock()
					err = errors.Join(err, r.closePublicationFence(idx))
					r.recordFailure(frontier.CommitSeq, err, postMeta)
					break
				}
				r.completeLocked(frontier, publicationFrontier)
				r.recoveryMu.Unlock()

				// The pwrite/meta phase is complete. Drain builders admitted during
				// it before ending the deferred-free freelist fence. An allocator call
				// is atomic, but an aborted builder can free after that call returns.
				r.relockRootSerialization()
				fenceErr := idx.allocator.EndPublicationFence()
				r.unlockRootSerialization()
				r.publishMu.Unlock()
				if fenceErr != nil {
					r.recordFailure(frontier.CommitSeq, fenceErr, true)
					break
				}
			}
		case <-r.stop:
			return
		}
	}
}

// sealAllocatorFrontier captures the mutable allocator state only after the
// writer barrier has drained every admitted optimistic builder. A builder that
// loses validation can allocate and return pages without registering a root
// candidate, so the head recorded at candidate preparation is not necessarily
// the allocator image that exists when publication begins.
func (r *RootPublicationCoordinator) sealedAllocatorFrontier(idx *indexGen, frontier *PreparedRootCandidate) *PreparedRootCandidate {
	if idx == nil || idx.allocator == nil || idx.pager == nil || frontier == nil {
		return frontier
	}
	sealed := *frontier
	head := idx.allocator.Head()
	totalPages := idx.pager.PageCount()
	sealed.FreelistHeadID = head
	sealed.TotalPages = totalPages
	sealed.Meta.FreelistHeadID = head
	sealed.Meta.TotalPages = totalPages
	return &sealed
}

func (r *RootPublicationCoordinator) lockRootSerialization() (*indexGen, error) {
	// Queue as a writer before publishMu. Destructive maintenance serializes on
	// publishMu before attempting the preparation gate, so it cannot queue an
	// exclusive preparation waiter that blocks an admitted builder here.
	// First give already-runnable foreground builders a bounded chance to join
	// this frontier. The blocking fallback is essential: a continuous reader
	// stream must not starve stable publication.
	const optimisticWriterAttempts = 64
	locked := false
	for attempt := 0; attempt < optimisticWriterAttempts; attempt++ {
		if r.db.writeMu.TryLock() {
			locked = true
			break
		}
		runtime.Gosched()
	}
	if !locked {
		r.db.writeMu.Lock()
	}
	r.publishMu.Lock()
	r.db.commitMu.Lock()
	r.db.mu.Lock()
	idx := r.db.idx.Load()
	if idx == nil || idx.allocator == nil {
		r.db.mu.Unlock()
		r.db.commitMu.Unlock()
		r.publishMu.Unlock()
		r.db.writeMu.Unlock()
		return nil, errors.New("missing index")
	}
	if err := idx.allocator.BeginPublicationFence(); err != nil {
		r.db.mu.Unlock()
		r.db.commitMu.Unlock()
		r.publishMu.Unlock()
		r.db.writeMu.Unlock()
		return nil, err
	}
	return idx, nil
}

func (r *RootPublicationCoordinator) unlockRootSerialization() {
	r.db.mu.Unlock()
	r.db.commitMu.Unlock()
	r.db.writeMu.Unlock()
}

func (r *RootPublicationCoordinator) relockRootSerialization() {
	// Do not queue an RWMutex writer here. An admitted optimistic builder may
	// legitimately enter another read-side build before releasing its outer read
	// lock; Go's writer preference would block that nested read and deadlock the
	// fence. The publication fence remains safe while we wait for a quiet point.
	for !r.db.writeMu.TryLock() {
		runtime.Gosched()
	}
	r.db.commitMu.Lock()
	r.db.mu.Lock()
}

func (r *RootPublicationCoordinator) closePublicationFence(idx *indexGen) error {
	r.relockRootSerialization()
	err := idx.allocator.EndPublicationFence()
	r.unlockRootSerialization()
	r.publishMu.Unlock()
	return err
}

// lockMaintenancePublication excludes the publisher before attempting the
// preparation gate. TryLock is essential: queuing an RWMutex writer would stop
// an admitted builder from obtaining its preparation read pin, while the
// publisher may be waiting for that builder to release writeMu.RLock.
func (r *RootPublicationCoordinator) lockMaintenancePublication() {
	for {
		r.publishMu.Lock()
		if r.db.publishPrepareMu.TryLock() {
			return
		}
		r.publishMu.Unlock()
		runtime.Gosched()
	}
}

func (r *RootPublicationCoordinator) closureThroughLocked(frontier *PreparedRootCandidate) []*PreparedRootCandidate {
	closure := make([]*PreparedRootCandidate, 0, len(r.pending))
	for _, candidate := range r.pending {
		closure = append(closure, candidate)
		if candidate == frontier {
			break
		}
	}
	return closure
}

func (r *RootPublicationCoordinator) publish(candidate *PreparedRootCandidate, closure []*PreparedRootCandidate, indexSnapshot *pager.IndexPageSnapshot) (error, bool) {
	if hook := r.db.testRootPublicationBeforeDependencySync; hook != nil {
		hook()
	}
	idx := r.db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing index"), false
	}
	if err := r.db.flushRootPublicationClosureDurability(idx, closure, indexSnapshot); err != nil {
		return err, false
	}
	r.mu.Lock()
	r.dependencySyncs++
	target := uint64(0)
	if r.durableMetaPageID == 0 {
		target = 1
	}
	r.mu.Unlock()
	return r.writeAndSyncMeta(target, candidate.Meta)
}

func (r *RootPublicationCoordinator) writeAndSyncMeta(target uint64, meta page.MetaPageBody) (error, bool) {
	idx := r.db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing index"), false
	}
	// Keep the exact path even when no observer is installed yet. Publication
	// runs asynchronously, so a test observer can be installed after this worker
	// starts but before a later cut point is emitted.
	metaPath := filepath.Join(r.db.dir, "index.db")
	metaOffset := int64(target) * int64(page.PageSize)
	if err := durabilitycut.EmitRange(durabilitycut.BeforeMetaWrite, durabilitycut.ResourceMeta, r.db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return err, false
	}
	if err := r.db.writeMeta(target, meta); err != nil {
		// Once the target slot write is attempted, partial mutation cannot be
		// distinguished from a clean failure. Poison until reopen.
		return err, true
	}
	if err := durabilitycut.EmitRange(durabilitycut.AfterMetaWrite, durabilitycut.ResourceMeta, r.db.dir, metaPath, metaOffset, int64(page.PageSize)); err != nil {
		return err, true
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeMetaSync, durabilitycut.ResourceMeta, r.db.dir, metaPath); err != nil {
		return err, true
	}
	if r.db.testFailSyncMeta.Load() {
		return errTestSyncMetaFailpoint, true
	}
	if err := idx.pager.SyncMetaPageImage(target, r.metaPageImage[:]); err != nil {
		return err, true
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterMetaSync, durabilitycut.ResourceMeta, r.db.dir, metaPath); err != nil {
		return err, true
	}
	return nil, false
}

func (r *RootPublicationCoordinator) complete(frontier *PreparedRootCandidate) {
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	r.completeLocked(frontier, frontier)
}

// completeLocked removes candidates through queuedFrontier while recording the
// exact sealed frontier whose meta image was written. The two pointers differ
// when publication refreshes mutable allocator state after draining builders.
func (r *RootPublicationCoordinator) completeLocked(queuedFrontier, durableFrontier *PreparedRootCandidate) {
	r.mu.Lock()
	count := 0
	bytes := uint64(0)
	var completed []*PreparedRootCandidate
	for count < len(r.pending) {
		c := r.pending[count]
		completed = append(completed, c)
		bytes += c.OwnedBytes
		count++
		if c == queuedFrontier {
			break
		}
	}
	r.pending = append([]*PreparedRootCandidate(nil), r.pending[count:]...)
	if bytes <= r.pendingBytes {
		r.pendingBytes -= bytes
	} else {
		r.pendingBytes = 0
	}
	if count > 1 {
		r.candidatesCoalesced += uint64(count - 1)
	}
	target := uint64(0)
	if r.durableMetaPageID == 0 {
		target = 1
	}
	r.durableMeta = durableFrontier.Meta
	r.durableMetaPageID = target
	r.metaSlotSeq[target] = durableFrontier.CommitSeq
	r.metaSlotValid[target] = true
	r.metaSlotMeta[target] = durableFrontier.Meta
	r.recoveryClosureGeneration++
	r.durableCommitSeq = durableFrontier.CommitSeq
	if r.durabilityDemandSeq <= r.durableCommitSeq {
		r.durabilityDemandSeq = 0
	}
	r.recomputeOldestRecoverableLocked()
	r.stall = nil
	r.stallCommitSeq = 0
	r.metaSyncs++
	r.mu.Unlock()
	r.db.mu.Lock()
	r.db.meta = durableFrontier.Meta
	r.db.metaPageID = target
	r.db.mu.Unlock()
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
	for _, c := range completed {
		if c.holdsPreparePin {
			c.holdsPreparePin = false
			r.db.publishPrepareMu.RUnlock()
		}
		if c.OldValueLogSet != nil && r.db.valueLogManager != nil {
			_ = r.db.valueLogManager.Release(c.OldValueLogSet)
		}
	}
}

// stabilizeRecoveryWindow makes both selectable meta slots name the newest
// durable generation. Destructive maintenance calls this before deciding that
// data absent from the current root is reclaimable: one ordinary publication
// leaves the alternate meta as a valid fallback to the prior closure.
func (r *RootPublicationCoordinator) stabilizeRecoveryWindow(seq uint64) error {
	if r == nil {
		return ErrClosed
	}
	if err := r.retryDurable(seq); err != nil {
		return err
	}

	// Exclude the publisher before attempting the preparation gate. The
	// non-queueing attempt lets an already-admitted builder obtain its read pin,
	// register, and be completed by the publisher instead of forming an RWMutex
	// writer-preference cycle.
	r.lockMaintenancePublication()
	defer r.publishMu.Unlock()
	defer r.db.publishPrepareMu.Unlock()
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()

	r.mu.Lock()
	if r.poison != nil {
		err := r.poison
		r.mu.Unlock()
		return err
	}
	if r.durableCommitSeq < seq {
		r.mu.Unlock()
		return ErrPublicationStalled
	}
	meta := r.durableMeta
	if r.metaSlotValid[0] && r.metaSlotValid[1] && r.metaSlotMeta[0] == meta && r.metaSlotMeta[1] == meta {
		r.mu.Unlock()
		return nil
	}
	target := uint64(0)
	if r.durableMetaPageID == 0 {
		target = 1
	}
	r.attempts++
	r.mu.Unlock()

	if err, postMeta := r.writeAndSyncMeta(target, meta); err != nil {
		r.recordFailure(meta.CommitSeq, err, postMeta)
		if postMeta {
			return errors.Join(err, ErrRecoveryRequired)
		}
		return errors.Join(ErrPublicationStalled, err)
	}
	r.mu.Lock()
	r.durableMetaPageID = target
	r.metaSlotSeq[target] = meta.CommitSeq
	r.metaSlotValid[target] = true
	r.metaSlotMeta[target] = meta
	r.recoveryClosureGeneration++
	r.recomputeOldestRecoverableLocked()
	r.stall = nil
	r.stallCommitSeq = 0
	r.metaSyncs++
	r.cond.Broadcast()
	r.mu.Unlock()
	r.db.mu.Lock()
	r.db.meta = meta
	r.db.metaPageID = target
	r.db.mu.Unlock()
	return nil
}

// adoptDurableGeneration records a maintenance cutover whose replacement
// index already contains fully-synced redundant meta pages. The caller must
// exclude candidate registration with publishPrepareMu.Lock.
func (r *RootPublicationCoordinator) adoptDurableGeneration(meta page.MetaPageBody, metaPageID uint64) error {
	if r == nil {
		return ErrClosed
	}
	r.recoveryMu.Lock()
	defer r.recoveryMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) != 0 {
		return errors.New("root publication: maintenance cutover with pending candidates")
	}
	if r.poison != nil {
		return r.poison
	}
	r.durableMeta = meta
	r.durableMetaPageID = metaPageID
	r.metaSlotMeta[0] = meta
	r.metaSlotMeta[1] = meta
	r.recoveryClosureGeneration++
	r.metaSlotSeq[0] = meta.CommitSeq
	r.metaSlotSeq[1] = meta.CommitSeq
	r.metaSlotValid[0] = true
	r.metaSlotValid[1] = true
	r.visibleCommitSeq = meta.CommitSeq
	r.durableCommitSeq = meta.CommitSeq
	r.oldestRecoverableCommitSeq = meta.CommitSeq
	r.stall = nil
	r.stallCommitSeq = 0
	r.cond.Broadcast()
	return nil
}

func (r *RootPublicationCoordinator) recordFailure(commitSeq uint64, err error, postMeta bool) {
	r.mu.Lock()
	if postMeta {
		r.poison = errors.Join(err, ErrRecoveryRequired)
		r.poisonEvents++
		r.db.publicationPoisoned.Store(true)
	} else {
		r.stall = errors.Join(ErrPublicationStalled, err)
		r.stallCommitSeq = commitSeq
		r.stallAttempt = r.attempts
		r.publicationStalls++
	}
	r.cond.Broadcast()
	r.mu.Unlock()
}

func (r *RootPublicationCoordinator) waitDurable(seq uint64) error {
	return r.waitDurableMode(seq, false)
}

// retryDurable establishes an explicit durability boundary that is permitted
// to retry a previously stalled frontier once. Candidate-specific WriteSync
// waits use waitDurable so the failure of their own publication is not hidden
// by an immediate retry race.
func (r *RootPublicationCoordinator) retryDurable(seq uint64) error {
	return r.waitDurableMode(seq, true)
}

func (r *RootPublicationCoordinator) waitDurableMode(seq uint64, retryStall bool) error {
	if r == nil {
		return ErrClosed
	}
	start := time.Now()
	if hook := r.db.testRootPublicationBeforeWait; hook != nil {
		hook()
	}
	r.mu.Lock()
	seenStallAttempt := r.stallAttempt
	if r.durableCommitSeq < seq && r.stall != nil && seq <= r.stallCommitSeq && !retryStall {
		err := r.stall
		r.recordWaitLocked(time.Since(start))
		r.mu.Unlock()
		return err
	}
	if r.durableCommitSeq < seq && r.poison == nil {
		if seq > r.durabilityDemandSeq {
			r.durabilityDemandSeq = seq
		}
		r.mu.Unlock()
		r.signal()
		r.mu.Lock()
	}
	for r.durableCommitSeq < seq && r.poison == nil {
		if r.stall != nil && r.stallAttempt > seenStallAttempt {
			err := r.stall
			r.recordWaitLocked(time.Since(start))
			r.mu.Unlock()
			return err
		}
		r.cond.Wait()
	}
	r.recordWaitLocked(time.Since(start))
	if r.poison != nil {
		err := r.poison
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
	return nil
}

func (r *RootPublicationCoordinator) recordWaitLocked(d time.Duration) {
	r.waiterCount++
	r.waiterLatency += d
	if d > r.waiterLatencyMax {
		r.waiterLatencyMax = d
	}
}

func (r *RootPublicationCoordinator) snapshot() rootPublicationSnapshot {
	if r == nil {
		return rootPublicationSnapshot{}
	}
	r.mu.Lock()
	s := rootPublicationSnapshot{
		visibleCommitSeq: r.visibleCommitSeq, durableCommitSeq: r.durableCommitSeq,
		oldestRecoverableCommitSeq: r.oldestRecoverableCommitSeq,
		pendingCandidates:          uint64(len(r.pending)), pendingBytes: r.pendingBytes,
		candidatesCoalesced: r.candidatesCoalesced, dependencySyncs: r.dependencySyncs,
		metaSyncs: r.metaSyncs, publicationStalls: r.publicationStalls,
		poisonEvents: r.poisonEvents, waiterCount: r.waiterCount,
		waiterLatency: r.waiterLatency, waiterLatencyMax: r.waiterLatencyMax,
	}
	r.mu.Unlock()
	return s
}

func (r *RootPublicationCoordinator) stopPublisher() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stop)
	pending := append([]*PreparedRootCandidate(nil), r.pending...)
	r.pending = nil
	r.pendingBytes = 0
	r.mu.Unlock()
	<-r.done
	for _, candidate := range pending {
		if candidate.holdsPreparePin {
			candidate.holdsPreparePin = false
			r.db.publishPrepareMu.RUnlock()
		}
		if candidate.OldValueLogSet != nil && r.db.valueLogManager != nil {
			_ = r.db.valueLogManager.Release(candidate.OldValueLogSet)
		}
	}
}

func (r *RootPublicationCoordinator) durableError() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.poison != nil {
		return r.poison
	}
	if r.stall != nil {
		return r.stall
	}
	return nil
}

func (c *PreparedRootCandidate) String() string {
	return fmt.Sprintf("root-candidate(seq=%d roots=%d/%d)", c.CommitSeq, c.UserRootPageID, c.SystemRootPageID)
}
