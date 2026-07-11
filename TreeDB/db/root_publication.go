package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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

	publishMu  sync.Mutex
	recoveryMu sync.RWMutex
	mu         sync.Mutex
	cond       *sync.Cond
	wake       chan struct{}
	stop       chan struct{}
	done       chan struct{}

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
	if hadStall && (len(r.pending) >= rootPublicationMaxPendingCommits || debtExceeded) {
		err := r.stall
		r.mu.Unlock()
		releaseOnError()
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
				r.publishMu.Lock()
				r.mu.Lock()
				if len(r.pending) == 0 || r.poison != nil {
					r.mu.Unlock()
					r.publishMu.Unlock()
					break
				}
				r.mu.Unlock()
				// Registration happens inside visible-root serialization. Do not
				// begin dependency/index/meta I/O until the registering writer (and
				// any overlapping builder) has released every root lock. TryLock
				// keeps the coordinator from participating in lock-order waits.
				if !r.rootSerializationLocksAvailable() {
					r.publishMu.Unlock()
					time.Sleep(50 * time.Microsecond)
					continue
				}
				r.mu.Lock()
				if len(r.pending) == 0 || r.poison != nil {
					r.mu.Unlock()
					r.publishMu.Unlock()
					break
				}
				frontier := r.pending[len(r.pending)-1]
				closure := r.closureThroughLocked(frontier)
				r.attempts++
				r.mu.Unlock()
				r.recoveryMu.Lock()
				if err, postMeta := r.publish(frontier, closure); err != nil {
					r.recordFailure(frontier.CommitSeq, err, postMeta)
					r.recoveryMu.Unlock()
					r.publishMu.Unlock()
					break
				}
				r.completeLocked(frontier)
				r.recoveryMu.Unlock()
				r.publishMu.Unlock()
			}
		case <-r.stop:
			return
		}
	}
}

func (r *RootPublicationCoordinator) rootSerializationLocksAvailable() bool {
	if !r.db.mu.TryLock() {
		return false
	}
	defer r.db.mu.Unlock()
	if !r.db.writeMu.TryLock() {
		return false
	}
	defer r.db.writeMu.Unlock()
	if !r.db.commitMu.TryLock() {
		return false
	}
	r.db.commitMu.Unlock()
	return true
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

func (r *RootPublicationCoordinator) publish(candidate *PreparedRootCandidate, closure []*PreparedRootCandidate) (error, bool) {
	if hook := r.db.testRootPublicationBeforeDependencySync; hook != nil {
		hook()
	}
	idx := r.db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing index"), false
	}
	if err := r.db.flushRootPublicationClosureDurability(idx, closure); err != nil {
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
	if err := idx.pager.SyncPages([]uint64{target}); err != nil {
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
	r.completeLocked(frontier)
}

func (r *RootPublicationCoordinator) completeLocked(frontier *PreparedRootCandidate) {
	r.mu.Lock()
	count := 0
	bytes := uint64(0)
	var completed []*PreparedRootCandidate
	for count < len(r.pending) {
		c := r.pending[count]
		completed = append(completed, c)
		bytes += c.OwnedBytes
		count++
		if c == frontier {
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
	r.durableMeta = frontier.Meta
	r.durableMetaPageID = target
	r.metaSlotSeq[target] = frontier.CommitSeq
	r.metaSlotValid[target] = true
	r.metaSlotMeta[target] = frontier.Meta
	r.recoveryClosureGeneration++
	r.durableCommitSeq = frontier.CommitSeq
	r.recomputeOldestRecoverableLocked()
	r.stall = nil
	r.stallCommitSeq = 0
	r.metaSyncs++
	r.mu.Unlock()
	r.db.mu.Lock()
	r.db.meta = frontier.Meta
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

	r.db.publishPrepareMu.Lock()
	defer r.db.publishPrepareMu.Unlock()
	// Exclude new candidate registration before taking the publisher mutex.
	// An already-registered candidate owns a publishPrepareMu read pin until the
	// publisher completes it; taking publishMu first would prevent that completion
	// while waiting for the pin and deadlock maintenance.
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
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
