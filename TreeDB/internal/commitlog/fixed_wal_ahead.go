package commitlog

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const fixedWALAheadPrefix = ".wal-ahead-"

// One slot is owned by one journal, never by a Writer or resource token. All
// mutable fields below are protected by that journal's mu. Until ready, only
// the worker touches reserve. After claimed, only the append owner touches it.
// The slot remains occupied until the worker has joined, including on a miss.
type fixedWALAheadSlot struct {
	generation, sequence, namespace, parentGeneration uint64
	currentInfo                                       os.FileInfo
	identity                                          [16]byte
	capacity, budget, target                          int64
	reserve                                           *fixedWALAheadReserve
	started                                           time.Time
	signal                                            chan struct{}
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

// This is reached before an append that still fits, using physical demand.
// The worker sees only immutable scalars and a duplicated retained parent.
func (j *CommandJournal) maybeStartFixedWALAheadLocked(incoming int64) error {
	w := j.writer
	if j.fixedAhead != nil || j.closing || w == nil || w.fixed == nil || j.stableParent == nil || !FixedWALCreationSupported() ||
		j.pendingStableSuccessor != nil || len(j.pendingStableRotations) != 0 || j.segmentSeq == ^uint64(0) || j.fixedAheadGeneration == ^uint64(0) {
		return nil
	}
	current := w.fixed
	plan, err := fixedSuccessorSizePlan(current, w.fixedBudget, fixedWALPressureRotation, 0)
	if err != nil {
		return err
	}
	buffered := w.ActiveBytes() - current.logical
	if !j.fixedAheadPolicy.shouldStart(current, buffered, incoming, plan.capacity, time.Now) {
		return nil
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
		currentInfo: info, identity: current.identity, capacity: current.capacity, budget: w.fixedBudget, target: j.segmentTargetBytes,
		reserve: &fixedWALAheadReserve{parent: parent, state: &fixedWAL{capacity: plan.capacity}}, signal: make(chan struct{}), started: time.Now(),
	}
	j.fixedAhead = slot
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
		r.state, err = writeInitializedFixedWAL(r.file, r.state.capacity)
	}
	elapsed := time.Since(slot.started)
	// Learn private preparation work, not contention for the append owner's
	// mutex after that work has completed.
	j.mu.Lock()
	if err == nil {
		j.fixedAheadPolicy.prepared(r.state.capacity, elapsed)
	}
	if err == nil && !slot.cancelled && !j.closing {
		slot.ready = true
		j.mu.Unlock()
		<-slot.signal
		j.mu.Lock()
	}
	claimed := slot.claimed
	j.mu.Unlock()
	if !claimed {
		err = errors.Join(err, r.dispose())
	}
	j.mu.Lock()
	if err != nil {
		j.fixedAheadErr = errors.Join(j.fixedAheadErr, err)
	}
	slot.done = true
	if j.syncCond != nil {
		j.syncCond.Broadcast()
	}
	j.mu.Unlock()
}

// Claim only a ready exact pressure plan. Every rotation invalidates anything
// else and proceeds synchronously without waiting for uncancellable worker I/O.
// Caller owns dispose after claim, even if canonical creation/publication fails.
func (j *CommandJournal) takeFixedWALAheadLocked(plan fixedWALSizePlan) (*fixedWALAheadReserve, error) {
	slot := j.fixedAhead
	if slot == nil {
		return nil, nil
	}
	if slot.cancelled || slot.claimed || !slot.ready || j.writer.fixedRotationReason != fixedWALPressureRotation ||
		slot.generation != j.fixedAheadGeneration || slot.sequence != j.segmentSeq || slot.namespace != j.namespaceGeneration ||
		slot.budget != j.writer.fixedBudget || slot.target != j.segmentTargetBytes || j.writer.fixed == nil ||
		slot.identity != j.writer.fixed.identity || slot.capacity != j.writer.fixed.capacity || slot.reserve.state.capacity != plan.capacity ||
		!slot.reserve.state.fitsEmptyFrame(plan.incoming) {
		j.cancelFixedWALAheadLocked()
		return nil, nil
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
