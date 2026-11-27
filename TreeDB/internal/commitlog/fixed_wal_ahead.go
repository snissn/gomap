package commitlog

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	fixedWALAheadPrefix      = ".wal-ahead-"
	fixedWALAheadWaitTimeout = 150 * time.Millisecond
)

// One slot is owned by one journal, never by a Writer or resource token. All
// mutable fields below are protected by that journal's mu. Until initDone is
// closed, only the worker touches reserve and initErr. After claimed, only the
// append owner touches reserve. Unclaimed slots remain occupied until the
// worker has joined.
type fixedWALAheadSlot struct {
	generation, sequence, namespace, parentGeneration uint64
	currentInfo                                       os.FileInfo
	identity                                          [16]byte
	capacity, plannedCapacity, budget, target         int64
	reserve                                           *fixedWALAheadReserve
	signal                                            chan struct{}
	initDone                                          chan struct{}
	initErr                                           error
	cancelledAtomic                                   atomic.Bool
	ready, cancelled, claimed, done                   bool
}

type fixedWALAheadReserve struct {
	parent, file *os.File
	name         string
	state        *fixedWAL
}

// dispose never reopens the file by its diagnostic path. Missing private name
// after adoption is expected; a substituted link must not be unlinked.
func (r *fixedWALAheadReserve) dispose() error {
	if r == nil {
		return nil
	}
	var errs []error
	if r.file != nil {
		err := removeExactFixedWALAhead(r.parent, r.file, r.name)
		errs = append(errs, err)
		errs = append(errs, r.file.Close())
		r.file = nil
	}
	if r.parent != nil {
		errs = append(errs, r.parent.Close())
		r.parent = nil
	}
	return errors.Join(errs...)
}

// Open directly here because ValidateStableChildLink intentionally translates
// ENOENT into a conflict. Rename success legitimately removes this private link.
func removeExactFixedWALAhead(parent, file *os.File, name string) error {
	linked, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := file.Stat()
	linkedInfo, linkedErr := linked.Stat()
	closeErr := linked.Close()
	if err != nil || linkedErr != nil || closeErr != nil {
		return errors.Join(err, linkedErr, closeErr)
	}
	if !os.SameFile(info, linkedInfo) {
		return rootpublication.ErrResourceConflict
	}
	if err = rootpublication.RemoveStableChildFile(parent, name); err != nil {
		return err
	}
	return rootpublication.SyncStableNamespace(parent)
}

func isFixedWALAheadName(name string) bool {
	if !strings.HasPrefix(name, fixedWALAheadPrefix) || len(name) != len(fixedWALAheadPrefix)+32 {
		return false
	}
	_, err := hex.DecodeString(name[len(fixedWALAheadPrefix):])
	return err == nil && strings.ToLower(name) == name
}

// Called only under the exclusive directory JournalOwner, before a new slot
// can be created. Exact grammar excludes both canonical and low-level Writer
// private files. Refuse multiplicity rather than grow/sweep an orphan backlog.
func cleanupFixedWALAheadOrphan(walDir string) error {
	if !FixedWALCreationSupported() {
		return nil
	}
	parent, err := rootpublication.OpenStableParent(walDir)
	if err != nil {
		return err
	}
	defer parent.Close()
	entries, err := parent.ReadDir(-1)
	if err != nil {
		return err
	}
	var name string
	for _, entry := range entries {
		if !isFixedWALAheadName(entry.Name()) {
			continue
		}
		if name != "" {
			return fmt.Errorf("%w: multiple private WAL ahead reserves", rootpublication.ErrResourceOwnership)
		}
		name = entry.Name()
	}
	if name == "" {
		return nil
	}
	file, err := rootpublication.OpenStableChildFile(parent, name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > fixedWALDefaultPhysicalBudget {
		return fmt.Errorf("%w: invalid private WAL ahead reserve", rootpublication.ErrResourceConflict)
	}
	if err = rootpublication.ValidateStableChildLink(parent, file, name); err != nil {
		return err
	}
	if err = rootpublication.RemoveStableChildFile(parent, name); err != nil {
		return err
	}
	return rootpublication.SyncStableNamespace(parent)
}

func (j *CommandJournal) cancelFixedWALAheadLocked() {
	if slot := j.fixedAhead; slot != nil && !slot.claimed && !slot.cancelled {
		slot.cancelledAtomic.Store(true)
		slot.cancelled = true
		close(slot.signal)
	}
}

func (j *CommandJournal) fixedWALAheadErrorLocked() error {
	if j.fixedAhead != nil && j.fixedAhead.done {
		j.fixedAhead = nil
	}
	if j.fixedAheadErr != nil && j.writer != nil {
		return j.writer.poisonCommandBuffer(j.fixedAheadErr)
	}
	return j.fixedAheadErr
}

func waitFixedWALAheadInit(done <-chan struct{}, timeout time.Duration) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
	}
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// This is reached before an append that still fits, after a rotation's first
// append drains pending stable rotations, or after a capture flush. Start
// preparing the successor once occupancy (flushed physical bytes plus any
// buffered unflushed group-commit bytes) has reached half capacity, or when
// fewer than 3 additional frames of the incoming size will fit after this append.
// The worker sees only immutable scalars and a duplicated retained parent.
func (j *CommandJournal) maybeStartFixedWALAheadLocked(incoming int64) error {
	if j.fixedAhead != nil && j.fixedAhead.done {
		if err := j.fixedWALAheadErrorLocked(); err != nil {
			return err
		}
	}
	w := j.writer
	if j.fixedAhead != nil || j.closing || w == nil || w.fixed == nil || w.containerSelection.ext4Stream || j.stableParent == nil || !FixedWALCreationSupported() ||
		j.pendingStableSuccessor != nil || len(j.pendingStableRotations) != 0 || j.segmentSeq == ^uint64(0) || j.fixedAheadGeneration == ^uint64(0) {
		return nil
	}
	current := w.fixed
	occupiedPhysical := current.physical
	if buffered := w.ActiveBytes() - current.logical; buffered > 0 {
		delta := buffered
		if current.pendingLogical == int64(len(current.pending)) && current.pendingPhysical > delta {
			delta = current.pendingPhysical
		}
		if occupiedPhysical <= current.capacity-delta {
			occupiedPhysical += delta
		} else {
			occupiedPhysical = current.capacity
		}
	}
	halfReached := occupiedPhysical-fixedWALHeaderSize >= (current.capacity-fixedWALHeaderSize+1)/2
	nextFrameOverflows := false
	if !halfReached && incoming > 0 {
		if projected, err := fixedWALExtentEnd(occupiedPhysical, incoming); err == nil {
			extent := projected - occupiedPhysical
			// Start ahead-reserve when fewer than 3 additional frames of the
			// same size will fit after this append (4 frames for >=16 MiB
			// containers preparing 32-64 MiB reserves), giving background
			// zero-fill and fsync lead time without shifting small-segment tests.
			lookahead := int64(3)
			if current.capacity >= 16<<20 {
				lookahead = 4
			}
			remaining := current.capacity - projected
			if remaining < 0 {
				remaining = 0
			}
			if extent > 0 && remaining/extent < lookahead {
				nextFrameOverflows = true
			}
		}
	}
	if !halfReached && !nextFrameOverflows {
		return nil
	}
	plan, err := fixedSuccessorSizePlan(current, w.fixedBudget, fixedWALPressureRotation, 0)
	if err != nil {
		return err
	}
	if plan.capacity <= 0 || plan.capacity > fixedWALDefaultPhysicalBudget {
		return nil
	}
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(j.stableParent)
	if err != nil {
		return err
	}
	parent, err := rootpublication.DuplicateStableFile(j.stableParent)
	if err != nil {
		return err
	}
	j.fixedAheadGeneration++
	slot := &fixedWALAheadSlot{
		generation: j.fixedAheadGeneration, sequence: j.segmentSeq, namespace: j.namespaceGeneration, parentGeneration: parentGeneration,
		currentInfo: info, identity: current.identity, capacity: current.capacity, plannedCapacity: plan.capacity, budget: w.fixedBudget, target: j.segmentTargetBytes,
		reserve: &fixedWALAheadReserve{parent: parent, state: &fixedWAL{capacity: plan.capacity}},
		signal:  make(chan struct{}), initDone: make(chan struct{}),
	}
	j.fixedAhead = slot
	j.fixedAheadWorkers++
	go j.runFixedWALAhead(slot)
	return nil
}

func (j *CommandJournal) runFixedWALAhead(slot *fixedWALAheadSlot) {
	r := slot.reserve
	var nonce [16]byte
	_, err := rand.Read(nonce[:])
	if err == nil {
		r.name = fmt.Sprintf("%s%x", fixedWALAheadPrefix, nonce)
		r.file, err = rootpublication.OpenStableChildFile(r.parent, r.name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	}
	if err == nil {
		err = durabilitycut.EmitStableNamespace(durabilitycut.NamespaceCreate, durabilitycut.ResourceCommandWAL, r.parent.Name(), "", r.file.Name(), r.parent, r.file, "", r.name)
	}
	if err == nil {
		r.state, err = writeInitializedFixedWALCancelable(r.file, r.state.capacity, slot.cancelledAtomic.Load)
	}
	if errors.Is(err, errFixedWALAheadCancelled) {
		err = nil
	}
	slot.initErr = err
	close(slot.initDone)

	j.mu.Lock()
	if err == nil && !slot.cancelled && !slot.claimed && !j.closing {
		slot.ready = true
		j.mu.Unlock()
		<-slot.signal
		j.mu.Lock()
	}
	claimed := slot.claimed
	if !claimed {
		j.mu.Unlock()
		err = errors.Join(err, r.dispose())
		j.mu.Lock()
	}
	if err != nil {
		j.fixedAheadErr = errors.Join(j.fixedAheadErr, err)
	}
	slot.done = true
	j.fixedAheadWorkers--
	if j.syncCond != nil {
		j.syncCond.Broadcast()
	}
	j.mu.Unlock()
}

// Claim a matching exact pressure plan, waiting briefly when an identical
// ahead reserve is already finishing initialization so rotation does not cancel
// it mid-flight and duplicate the full zero-fill + fsync synchronously.
// Non-matching rotations or a stalled private sync cancel and fall back
// synchronously without blocking indefinitely. Caller owns dispose after claim,
// even if canonical creation/publication fails.
func (j *CommandJournal) takeFixedWALAheadLocked(plan fixedWALSizePlan) (*fixedWALAheadReserve, error) {
	slot := j.fixedAhead
	if slot == nil {
		return nil, nil
	}
	if slot.cancelled || slot.claimed || slot.done || j.writer.fixedRotationReason != fixedWALPressureRotation ||
		slot.generation != j.fixedAheadGeneration || slot.sequence != j.segmentSeq || slot.namespace != j.namespaceGeneration ||
		slot.budget != j.writer.fixedBudget || slot.target != j.segmentTargetBytes || j.writer.fixed == nil ||
		slot.identity != j.writer.fixed.identity || slot.capacity != j.writer.fixed.capacity || slot.plannedCapacity != plan.capacity ||
		!fixedWALCapacityFitsEmptyFrame(slot.plannedCapacity, plan.incoming) {
		j.cancelFixedWALAheadLocked()
		return nil, nil
	}
	if !slot.ready {
		if !waitFixedWALAheadInit(slot.initDone, fixedWALAheadWaitTimeout) {
			j.cancelFixedWALAheadLocked()
			return nil, nil
		}
		if slot.initErr != nil {
			j.cancelFixedWALAheadLocked()
			return nil, slot.initErr
		}
		if slot.reserve == nil || slot.reserve.state == nil || slot.reserve.state.capacity != plan.capacity || !slot.reserve.state.fitsEmptyFrame(plan.incoming) {
			j.cancelFixedWALAheadLocked()
			return nil, nil
		}
		slot.ready = true
	}
	info, err := j.writer.f.Stat()
	if err == nil && !os.SameFile(info, slot.currentInfo) {
		err = rootpublication.ErrResourceConflict
	}
	var generation uint64
	if err == nil {
		generation, err = rootpublication.StableNamespaceParentGeneration(j.stableParent)
	}
	if err == nil && generation != slot.parentGeneration {
		err = rootpublication.ErrResourceConflict
	}
	if err == nil {
		err = rootpublication.ValidateStableChildLink(slot.reserve.parent, j.writer.f, filepath.Base(j.path))
	}
	if err != nil {
		j.cancelFixedWALAheadLocked()
		return nil, err
	}
	slot.claimed = true
	j.fixedAhead = nil
	close(slot.signal)
	return slot.reserve, nil
}

// Called with j.mu held after a claim. Cleanup errors are not optional just
// because a synchronous operation or publication happened to succeed.
func (j *CommandJournal) disposeFixedWALAheadLocked(r *fixedWALAheadReserve) error {
	if r == nil {
		return nil
	}
	err := r.dispose()
	if err != nil {
		j.fixedAheadErr = errors.Join(j.fixedAheadErr, err)
		if j.writer != nil {
			return j.writer.poisonCommandBuffer(err)
		}
	}
	return err
}
