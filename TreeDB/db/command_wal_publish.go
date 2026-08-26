package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	ErrCommandWALSplitPublish         = errors.New("treedb: command wal roots changed without advancing applied lsn")
	ErrCommandWALAppliedLSNRegression = errors.New("treedb: command wal applied lsn regression")
	ErrCommandWALAppliedLSNNonContig  = errors.New("treedb: command wal applied lsn non-contiguous")
)

type CommandWALLSNRange struct {
	First uint64
	Last  uint64
}

type conditionalCommitMutation struct {
	entries    []batchpkg.Entry
	ranges     []batchpkg.DeleteRange
	ownerTxnID uint64
}

func (mutation conditionalCommitMutation) record(db *DB, commitSeq uint64) {
	db.conditionalRecordCommittedEntries(mutation.entries, mutation.ranges, mutation.ownerTxnID, commitSeq)
}

type finalizeCommitOptions struct {
	commandWALPublish           bool
	appliedCommandLSN           uint64
	appliedRanges               []CommandWALLSNRange
	skipPrePublishFlush         bool
	syncMetaPageOnly            bool
	skipConditionalRootConflict bool
	maxEntryRevision            page.EntryRevision
	durablePublishLocked        bool
	// durablePublishRelease transfers a caller-owned durablePublishMu lease to
	// the finalizer and clears the caller's ownership flag exactly once. It is
	// required whenever durablePublishLocked is true.
	durablePublishRelease func()
	// rootPublicationBuilder is an admitted builder lease acquired before any
	// caller-owned durable publication lock. The finalizer consumes its final
	// handle atomically with the visible candidate handoff.
	rootPublicationBuilder *rootpublication.BuilderToken
	// closeTeardownPinned proves the caller acquired teardownMu for reading
	// before Close could revoke write admission and will retain that lease
	// through commit post-work. It permits an already-admitted publication to
	// finish capturing its stable index identity after Close has set closing.
	closeTeardownPinned bool
	// durableIndex is the exact index generation against which the caller built
	// its roots. Root-releasing callers capture it before dropping construction
	// serialization so an online vacuum cannot redirect publication to a
	// replacement file with coincident page IDs.
	durableIndex *indexGen
	// expectedBaseCommitSeq binds a constructed root to the visible generation
	// it was derived from. Page IDs alone are ABA-unsafe once COW reuse begins.
	expectedBaseCommitSeq    uint64
	hasExpectedBaseCommitSeq bool
	// releaseRootSerialization transfers an already-prepared candidate from
	// root construction to the synchronous durability transaction. The callback
	// must release every DB/write/commit/root-build lock held by the caller and
	// is invoked exactly once, immediately before external/index/meta I/O.
	releaseRootSerialization func()
	// recordVacuumMutation transfers a successful primary-tree mutation into the
	// online-vacuum tail before durablePublishMu can pass to cutover. Cutover
	// acquires that same fence before stopping and draining the recorder, so a
	// visible old-generation commit cannot fall between publication and capture.
	recordVacuumMutation func()
	// conditionalMutation installs exact point/range conflict evidence at
	// the same visibility cut as the new root. It runs while db.mu still excludes
	// readers from sampling that root, so a conditional transaction cannot
	// validate in the gap between root visibility and oracle publication.
	conditionalMutation conditionalCommitMutation
	// durableResources transfers producer-owned exact external handles into the
	// candidate root's dependency manifest. The finalizer consumes the set.
	durableResources *rootpublication.StableResourceSet
	// leafManifestAlreadyPersistent marks an exact immutable manifest revision
	// already carried by durableResources. Post-work may build rebuildable
	// record-length indexes, but must not create a second manifest revision after
	// the meta has selected the producer's exact one.
	leafManifestAlreadyPersistent bool
	// durableResourceRequirements scopes exact logical obligations for external
	// resources whose physical files can outlive several root generations.
	durableResourceRequirements rootpublication.StableLogicalObligationRequirements
	// durableResourceMutation is exact root-local addition/removal evidence used
	// to derive inherited resource pins without replaying retained history.
	durableResourceMutation rootpublication.StableLogicalObligationMutation
	// durableResourceAppendMutation is collections-owned root-validated append
	// evidence whose exact requirement oracle remains lazy until capture cannot
	// certify it against the visible base and producer commitments.
	durableResourceAppendMutation       rootpublication.StableLogicalObligationMutation
	durableResourceRequirementWork      rootpublication.StableResourceClosureWork
	durableResourceRequirementsFallback func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)
	// valueLogPublicationLocked proves the caller already owns the exclusive
	// valueLogPublicationMu lease. Candidate dependency capture must reuse that
	// lease instead of recursively acquiring its read side.
	valueLogPublicationLocked bool
	// publishTiming is optional request-scoped diagnostics owned by the
	// higher-level command-WAL publisher. It subdivides queued root
	// finalization without affecting publication ordering or durability.
	publishTiming *CommandWALPublishTiming
}

func (db *DB) publishCommandWALRoots(newRootID uint64, sysRootID uint64, appliedLSN uint64, covered []CommandWALLSNRange, sync bool) error {
	return db.publishCommandWALRootsWithMode(newRootID, sysRootID, appliedLSN, covered, sync, false, false)
}

func (db *DB) publishCurrentCommandWALRoots(appliedLSN uint64, covered []CommandWALLSNRange, sync bool) error {
	return db.publishCommandWALRootsWithMode(0, 0, appliedLSN, covered, sync, true, false)
}

func (db *DB) publishCurrentCommandWALRootsTeardownPinned(appliedLSN uint64, covered []CommandWALLSNRange, sync bool) error {
	return db.publishCommandWALRootsWithMode(0, 0, appliedLSN, covered, sync, true, true)
}

func (db *DB) publishCommandWALRootsWithMode(newRootID uint64, sysRootID uint64, appliedLSN uint64, covered []CommandWALLSNRange, sync bool, currentRoots bool, teardownPinned bool) error {
	if db == nil {
		return ErrClosed
	}
	if !teardownPinned {
		db.teardownMu.RLock()
		defer db.teardownMu.RUnlock()
	}
	builder, err := db.acquireCommandWALPublicationBuilderV1()
	if err != nil {
		return err
	}
	if builder != nil {
		defer builder.Release()
	}
	durablePublishLocked := false
	releaseDurablePublish := func() {
		if durablePublishLocked {
			db.durablePublishMu.Unlock()
			durablePublishLocked = false
		}
	}
	defer releaseDurablePublish()
	var (
		baseSeq        uint64
		rootsUnchanged bool
	)
	if !currentRoots {
		db.mu.RLock()
		baseSeq = db.meta.CommitSeq
		rootsUnchanged = newRootID == db.meta.UserRootPageID && sysRootID == db.meta.SystemRootPageID
		db.mu.RUnlock()
	}
	if hook := db.testCommandWALBeforeDurablePublishLockHook; hook != nil {
		hook()
	}
	db.durablePublishMu.Lock()
	durablePublishLocked = true
	if currentRoots {
		// Applied-LSN-only publications carry no constructed root candidate.
		// Bind them to the latest roots only after entering the durable publish
		// gate so a maintenance/manual publication that won the gate cannot be
		// overwritten by roots captured before the wait.
		db.mu.RLock()
		baseSeq = db.meta.CommitSeq
		newRootID = db.meta.UserRootPageID
		sysRootID = db.meta.SystemRootPageID
		db.mu.RUnlock()
		rootsUnchanged = true
	}
	var vlogRefDelta *valueLogRefDelta
	if rootsUnchanged {
		vlogRefDelta = db.newNoopValueLogRefDeltaIfTrackable(baseSeq)
	}
	db.writeMu.Unlock()
	publishPrepareGuard, err := db.prepareFlushApplyPublish(sync)
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		return err
	}
	defer publishPrepareGuard.Release()

	finalizeOpts := finalizeCommitOptions{
		commandWALPublish:        true,
		appliedCommandLSN:        appliedLSN,
		appliedRanges:            covered,
		skipPrePublishFlush:      true,
		durablePublishLocked:     true,
		durablePublishRelease:    releaseDurablePublish,
		rootPublicationBuilder:   builder,
		closeTeardownPinned:      true,
		releaseRootSerialization: func() {},
		expectedBaseCommitSeq:    baseSeq,
		hasExpectedBaseCommitSeq: true,
	}
	post, err := db.finalizeCommitLockedWithOptions(
		newRootID,
		sysRootID,
		nil,
		sync,
		adaptive.Metrics{},
		nil,
		false,
		vlogRefDelta,
		nil,
		nil,
		finalizeOpts,
	)
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		return err
	}
	db.finalizeCommitPostWork(post)
	return nil
}

// PublishCommandWALAppliedLSN publishes the current roots with an advanced
// AppliedCommandLSN. Callers must only pass ranges for command frames already
// reflected in the current roots.
func (db *DB) PublishCommandWALAppliedLSN(appliedLSN uint64, covered []CommandWALLSNRange, sync bool) error {
	if db == nil {
		return ErrClosed
	}
	unlockCommandWALPublish := db.lockCommandWALRawPublishWithTeardown()
	defer unlockCommandWALPublish()
	return db.publishCurrentCommandWALRootsTeardownPinned(appliedLSN, covered, sync)
}

// RefreshCommandWALCheckpointFallback republishes the current durable root at
// a checkpoint only when the recovery fallback slot still names an older
// AppliedCommandLSN. It does not append a command frame or advance the LSN.
//
// This is intentionally a checkpoint-maintenance seam for the cached public
// command-WAL owner. Callers must exclude new public command frames while
// deciding whether a refresh is needed.
func (db *DB) RefreshCommandWALCheckpointFallback() error {
	if db == nil || !db.commandWAL {
		return nil
	}
	// Keep the predicate and same-LSN publication in the raw-publish domain.
	// The public cached owner holds its operation gate as well, but backend
	// command-WAL publishers do not use that gate. Holding this existing
	// backend serialization prevents an intervening command frame from making
	// the sampled AppliedCommandLSN stale before the fallback is refreshed.
	unlockCommandWALPublish := db.lockCommandWALRawPublishWithTeardown()
	defer unlockCommandWALPublish()
	db.durablePublishMu.Lock()
	if db.durableRoot.slot > 1 {
		db.durablePublishMu.Unlock()
		return fmt.Errorf("command WAL checkpoint fallback: invalid selected root slot %d", db.durableRoot.slot)
	}
	selected := db.durableRoot.slotRecord[db.durableRoot.slot]
	fallback := db.durableRoot.slotRecord[db.durableRoot.slot^1]
	state := db.state.Load()
	needsRefresh := state != nil && state.AppliedCommandLSN != 0 &&
		selected.AppliedCommandLSN == state.AppliedCommandLSN &&
		fallback.AppliedCommandLSN < selected.AppliedCommandLSN
	db.durablePublishMu.Unlock()
	if !needsRefresh {
		return nil
	}
	// Call the inner publisher while retaining the raw-publish guard above;
	// PublishCommandWALAppliedLSN would otherwise reacquire it. This remains a
	// same-root, same-LSN metadata publication and cannot append a WAL frame.
	if err := db.publishCurrentCommandWALRootsTeardownPinned(state.AppliedCommandLSN, nil, true); err != nil {
		return err
	}
	if runtime := db.rootPublication; runtime != nil && runtime.coordinator != nil {
		refreshed := db.state.Load()
		if refreshed == nil {
			return ErrClosed
		}
		if err := runtime.coordinator.WaitThrough(context.Background(), refreshed.CommitSeq); err != nil {
			return publicRootPublicationErrorV1(err)
		}
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	if db.durableRoot.slot > 1 {
		return fmt.Errorf("command WAL checkpoint fallback: invalid refreshed root slot %d", db.durableRoot.slot)
	}
	selected = db.durableRoot.slotRecord[db.durableRoot.slot]
	fallback = db.durableRoot.slotRecord[db.durableRoot.slot^1]
	if selected.AppliedCommandLSN != state.AppliedCommandLSN || fallback.AppliedCommandLSN != state.AppliedCommandLSN {
		return fmt.Errorf("command WAL checkpoint fallback refresh did not converge: selected=%d fallback=%d want=%d", selected.AppliedCommandLSN, fallback.AppliedCommandLSN, state.AppliedCommandLSN)
	}
	return nil
}

func validateCommandWALPublishLocked(current page.MetaPageBody, newRootID uint64, sysRootID uint64, opts finalizeCommitOptions) error {
	if !opts.commandWALPublish {
		return nil
	}
	rootsChanged := newRootID != current.UserRootPageID || sysRootID != current.SystemRootPageID
	if rootsChanged && opts.appliedCommandLSN == current.AppliedCommandLSN {
		return ErrCommandWALSplitPublish
	}
	return validateContiguousAppliedCommandLSN(current.AppliedCommandLSN, opts.appliedCommandLSN, opts.appliedRanges)
}

// validateContiguousAppliedCommandLSN verifies that covered spans advance the
// applied LSN without gaps. It never mutates the caller-owned covered slice.
func validateContiguousAppliedCommandLSN(current, next uint64, covered []CommandWALLSNRange) error {
	if next < current {
		return fmt.Errorf("%w: current=%d next=%d", ErrCommandWALAppliedLSNRegression, current, next)
	}
	if next == current {
		if len(covered) != 0 {
			return fmt.Errorf("%w: no-op publish with stale coverage ranges", ErrCommandWALAppliedLSNNonContig)
		}
		return nil
	}
	if current == ^uint64(0) {
		return fmt.Errorf("%w: current lsn exhausted", ErrCommandWALAppliedLSNNonContig)
	}
	if len(covered) == 0 {
		return fmt.Errorf("%w: missing coverage for [%d,%d]", ErrCommandWALAppliedLSNNonContig, current+1, next)
	}
	ranges := covered
	sorted := true
	for i := 1; i < len(ranges); i++ {
		prev, cur := ranges[i-1], ranges[i]
		if prev.First > cur.First || (prev.First == cur.First && prev.Last > cur.Last) {
			sorted = false
			break
		}
	}
	if !sorted {
		ranges = append([]CommandWALLSNRange(nil), covered...)
		sort.Slice(ranges, func(i, j int) bool {
			if ranges[i].First != ranges[j].First {
				return ranges[i].First < ranges[j].First
			}
			return ranges[i].Last < ranges[j].Last
		})
	}
	cursor := current + 1
	for _, r := range ranges {
		if r.First == 0 || r.Last < r.First {
			return fmt.Errorf("%w: invalid coverage range [%d,%d]", ErrCommandWALAppliedLSNNonContig, r.First, r.Last)
		}
		if r.Last < cursor {
			continue
		}
		if r.First > cursor {
			return fmt.Errorf("%w: gap before %d", ErrCommandWALAppliedLSNNonContig, cursor)
		}
		if r.Last >= next {
			return nil
		}
		if r.Last == ^uint64(0) {
			return fmt.Errorf("%w: lsn range exhausted", ErrCommandWALAppliedLSNNonContig)
		}
		nextCursor := r.Last + 1
		if nextCursor <= r.Last {
			return fmt.Errorf("%w: lsn range exhausted", ErrCommandWALAppliedLSNNonContig)
		}
		cursor = nextCursor
	}
	return fmt.Errorf("%w: gap before %d", ErrCommandWALAppliedLSNNonContig, cursor)
}

type commandWALSegmentCleanupDecision struct {
	Path            string
	Size            int64
	ScannedBytes    int64
	Frames          uint64
	MinLSN          uint64
	MaxLSN          uint64
	Active          bool
	Covered         bool
	Removed         bool
	Pinned          bool
	Error           string
	identity        rootpublication.StableIdentity
	file            *os.File
	lane            int
	seq             uint64
	generationKnown bool
}

type commandWALSegmentScanResult struct {
	maxLSN       uint64
	minLSN       uint64
	scannedBytes int64
	frames       uint64
	typed        bool
	terminalTail bool
}

type commandWALSegmentScanOptions struct {
	seenLSNs          map[uint64]struct{}
	seenLSNAppliedLSN uint64
	seenLSNMax        uint64
}

func requireNoUnappliedCommandWALFrames(dir string, appliedLSN uint64, maxSegmentBytes int64) error {
	segments, err := listWALSegments(dir)
	if err != nil {
		return err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		scan, err := scanCommandWALSegmentWithSeen(seg.path, maxSegmentBytes, seg.seq == activeByLane[seg.lane], seenLSNs, appliedLSN)
		if err != nil {
			return err
		}
		if scan.typed && scan.maxLSN > appliedLSN {
			return fmt.Errorf("%w: command WAL segment %s max LSN %d exceeds applied LSN %d", ErrRecoveryRequired, filepath.Base(seg.path), scan.maxLSN, appliedLSN)
		}
	}
	return nil
}

func commandWALActiveSeqByLane(segments []logSegment) map[int]uint64 {
	activeByLane := make(map[int]uint64)
	for _, seg := range segments {
		if seg.valueLog {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		if seg.seq > activeByLane[seg.lane] {
			activeByLane[seg.lane] = seg.seq
		}
	}
	return activeByLane
}

func isCommandWALLaneSegment(seg logSegment) bool {
	return commitlog.IsCommandSegmentName(filepath.Base(seg.path))
}

func commandWALSegmentMaxLSN(path string, maxSegmentBytes int64, allowTerminalTail bool) (maxLSN uint64, typed bool, err error) {
	scan, err := scanCommandWALSegment(path, maxSegmentBytes, allowTerminalTail)
	return scan.maxLSN, scan.typed, err
}

func scanCommandWALSegment(path string, maxSegmentBytes int64, allowTerminalTail bool) (commandWALSegmentScanResult, error) {
	return scanCommandWALSegmentWithOptions(path, maxSegmentBytes, allowTerminalTail, commandWALSegmentScanOptions{})
}

func scanCommandWALSegmentWithSeen(path string, maxSegmentBytes int64, allowTerminalTail bool, seenLSNs map[uint64]struct{}, appliedLSN uint64) (commandWALSegmentScanResult, error) {
	return scanCommandWALSegmentWithOptions(path, maxSegmentBytes, allowTerminalTail, commandWALSegmentScanOptions{seenLSNs: seenLSNs, seenLSNAppliedLSN: appliedLSN})
}

func scanCommandWALSegmentWithOptions(path string, maxSegmentBytes int64, allowTerminalTail bool, opts commandWALSegmentScanOptions) (commandWALSegmentScanResult, error) {
	// PR2 has no durable per-segment max-LSN catalog yet, so open/cleanup
	// paths derive classification by streaming the segment without retaining
	// payloads. TODO(command-wal): cache validated per-segment min/max LSN
	// summaries once the cleanup manifest/catalog is introduced so restart
	// does not rescan old covered typed bytes.
	r, err := commitlog.NewReaderWithOptions(path, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return commandWALSegmentScanResult{}, err
	}
	defer r.Close()
	return scanCommandWALSegmentReader(r, allowTerminalTail, opts)
}

func scanCommandWALSegmentFileWithOptions(file *os.File, maxSegmentBytes int64, allowTerminalTail bool, opts commandWALSegmentScanOptions) (commandWALSegmentScanResult, error) {
	r, err := commitlog.NewReaderFromFileWithOptions(file, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
	if err != nil {
		return commandWALSegmentScanResult{}, err
	}
	defer r.Close()
	return scanCommandWALSegmentReader(r, allowTerminalTail, opts)
}

func scanCommandWALSegmentReader(r *commitlog.Reader, allowTerminalTail bool, opts commandWALSegmentScanOptions) (commandWALSegmentScanResult, error) {
	var lastLSN uint64
	var scan commandWALSegmentScanResult
	for {
		lsn, err := r.ReadValidatedCommandFrameLSN()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return scan, nil
			}
			if errors.Is(err, commitlog.ErrCommandWALTerminalTail) {
				scan.terminalTail = true
				if allowTerminalTail {
					return scan, nil
				}
				return scan, err
			}
			if errors.Is(err, commitlog.ErrCommandWALLegacyPayload) && !scan.typed {
				return commandWALSegmentScanResult{}, nil
			}
			return scan, err
		}
		scannedBytes, err := r.Offset()
		if err != nil {
			return scan, err
		}
		scan.scannedBytes = scannedBytes
		if lastLSN != 0 && lsn <= lastLSN {
			scan.typed = true
			return scan, commitlog.ErrCommandWALDuplicateLSN
		}
		if opts.seenLSNs != nil &&
			(opts.seenLSNAppliedLSN == 0 || lsn > opts.seenLSNAppliedLSN) &&
			(opts.seenLSNMax == 0 || lsn <= opts.seenLSNMax) {
			if _, ok := opts.seenLSNs[lsn]; ok {
				scan.typed = true
				return scan, commitlog.ErrCommandWALDuplicateLSN
			}
			opts.seenLSNs[lsn] = struct{}{}
		}
		lastLSN = lsn
		scan.typed = true
		scan.frames++
		if scan.minLSN == 0 || lsn < scan.minLSN {
			scan.minLSN = lsn
		}
		if lsn > scan.maxLSN {
			scan.maxLSN = lsn
		}
	}
}

func filterCommandWALSegmentsForLegacyReplay(segments []logSegment, appliedLSN uint64, maxSegmentBytes int64) ([]logSegment, error) {
	var filtered []logSegment
	skipped := false
	activeByLane := commandWALActiveSeqByLane(segments)
	seenLSNs := make(map[uint64]struct{})
	for i, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		active := seg.seq == activeByLane[seg.lane]
		scan, err := scanCommandWALSegmentWithSeen(seg.path, maxSegmentBytes, active, seenLSNs, appliedLSN)
		if err != nil {
			return nil, err
		}
		if !scan.typed {
			if skipped {
				filtered = append(filtered, seg)
			}
			continue
		}
		// Covered typed segments are skipped by the maxLSN check below. This
		// branch is only the crossing case where part of the segment is already
		// published and part would still need command replay. With appliedLSN=0
		// no command frame can be partly covered, so the first typed frame falls
		// through to the fully-unapplied branch below.
		if scan.maxLSN > appliedLSN && scan.minLSN <= appliedLSN {
			return nil, fmt.Errorf("%w: command WAL segment %s partially applied range [%d,%d] over applied LSN %d", ErrRecoveryRequired, filepath.Base(seg.path), scan.minLSN, scan.maxLSN, appliedLSN)
		}
		if scan.maxLSN <= appliedLSN {
			if !skipped {
				filtered = make([]logSegment, 0, len(segments)-1)
				filtered = append(filtered, segments[:i]...)
			}
			skipped = true
			continue
		}
		// scan.typed && scan.maxLSN > appliedLSN: the entire segment is
		// unapplied (partial-application and covered cases handled above).
		return nil, fmt.Errorf("%w: command WAL frame LSN %d exceeds applied LSN %d", ErrRecoveryRequired, scan.maxLSN, appliedLSN)
	}
	if !skipped {
		return segments, nil
	}
	return filtered, nil
}

func scanCommandWALSegmentsCoveredByAppliedLSN(dir string, appliedLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	return scanCommandWALSegmentsForCleanupProof(dir, appliedLSN, appliedLSN, maxSegmentBytes)
}

func scanCommandWALSegmentsForCleanupProof(dir string, cleanupThrough uint64, durableWALLSN uint64, maxSegmentBytes int64) ([]commandWALSegmentCleanupDecision, error) {
	if durableWALLSN < cleanupThrough {
		return nil, fmt.Errorf("%w: durable WAL LSN %d is behind cleanup frontier %d", ErrCommandWALAppliedLSNNonContig, durableWALLSN, cleanupThrough)
	}
	// PR2 cleanup deliberately streams segments on demand. It is intended for
	// checkpoint/maintenance boundaries; a later manifest/catalog can cache
	// results if this moves onto a hotter path.
	segments, err := listWALSegments(dir)
	if err != nil {
		return nil, err
	}
	activeByLane := commandWALActiveSeqByLane(segments)
	decisions := make([]commandWALSegmentCleanupDecision, 0, len(segments))
	var scanErr error
	seenLSNs := make(map[uint64]struct{})
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		if !isCommandWALLaneSegment(seg) {
			continue
		}
		active := seg.seq == activeByLane[seg.lane]
		file, err := os.Open(seg.path)
		if err != nil {
			scanErr = errors.Join(scanErr, fmt.Errorf("open command WAL segment %s: %w", filepath.Base(seg.path), err))
			continue
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(file)
		if identityErr != nil {
			_ = file.Close()
			scanErr = errors.Join(scanErr, fmt.Errorf("identify command WAL segment %s: %w", filepath.Base(seg.path), identityErr))
			continue
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			scanErr = errors.Join(scanErr, fmt.Errorf("stat command WAL segment %s: %w", filepath.Base(seg.path), statErr))
			continue
		}
		scan, err := scanCommandWALSegmentFileWithOptions(file, maxSegmentBytes, active, commandWALSegmentScanOptions{
			seenLSNs:          seenLSNs,
			seenLSNAppliedLSN: cleanupThrough,
			seenLSNMax:        durableWALLSN,
		})
		if err != nil {
			_ = file.Close()
			decisions = append(decisions, commandWALSegmentCleanupDecision{
				Path:         seg.path,
				Size:         info.Size(),
				ScannedBytes: scan.scannedBytes,
				Frames:       scan.frames,
				MinLSN:       scan.minLSN,
				MaxLSN:       scan.maxLSN,
				Active:       active,
				Error:        err.Error(),
			})
			scanErr = errors.Join(scanErr, fmt.Errorf("scan command WAL segment %s: %w", filepath.Base(seg.path), err))
			continue
		}
		if !scan.typed {
			_ = file.Close()
			continue
		}
		decision := commandWALSegmentCleanupDecision{
			Path:            seg.path,
			Size:            info.Size(),
			ScannedBytes:    scan.scannedBytes,
			Frames:          scan.frames,
			MinLSN:          scan.minLSN,
			MaxLSN:          scan.maxLSN,
			Active:          active,
			Covered:         scan.maxLSN <= cleanupThrough,
			identity:        identity,
			lane:            seg.lane,
			seq:             seg.seq,
			generationKnown: true,
		}
		// Only deletion candidates need their discovery handles retained until
		// exact identity leases are acquired. Keeping handles for active or
		// uncovered replay segments makes cleanup descriptor usage grow with the
		// entire retained WAL lineage even though those objects cannot be unlinked.
		if decision.Covered && !decision.Active {
			decision.file = file
		} else if closeErr := file.Close(); closeErr != nil {
			decision.Error = closeErr.Error()
			decisions = append(decisions, decision)
			scanErr = errors.Join(scanErr, fmt.Errorf("close retained command WAL segment %s: %w", filepath.Base(seg.path), closeErr))
			continue
		}
		decisions = append(decisions, decision)
	}
	if lineageErr := validateCommandWALCleanupReplayLineage(seenLSNs, cleanupThrough, durableWALLSN); lineageErr != nil {
		scanErr = errors.Join(scanErr, lineageErr)
	}
	if scanErr != nil {
		closeCommandWALCleanupDecisions(decisions)
		return decisions, scanErr
	}
	return decisions, nil
}

// validateCommandWALCleanupReplayLineage proves that every LSN which the
// oldest recoverable root may need is still present exactly once. Frames at
// or below cleanupThrough are already represented by every durable root and
// may legitimately be sparse after earlier multi-lane cleanup batches.
func validateCommandWALCleanupReplayLineage(seenLSNs map[uint64]struct{}, cleanupThrough uint64, durableWALLSN uint64) error {
	if durableWALLSN < cleanupThrough {
		return fmt.Errorf("%w: durable WAL LSN %d is behind cleanup frontier %d", ErrCommandWALAppliedLSNNonContig, durableWALLSN, cleanupThrough)
	}
	if durableWALLSN == cleanupThrough {
		return nil
	}
	want := durableWALLSN - cleanupThrough
	if uint64(len(seenLSNs)) != want {
		return fmt.Errorf("%w: retained replay range [%d,%d] has %d complete frames, want %d", ErrCommandWALAppliedLSNNonContig, cleanupThrough+1, durableWALLSN, len(seenLSNs), want)
	}
	return nil
}

func removeCoveredCommandWALSegments(decisions []commandWALSegmentCleanupDecision) ([]commandWALSegmentCleanupDecision, error) {
	return removeCoveredCommandWALSegmentsWithRegistry(decisions, nil)
}

func removeCoveredCommandWALSegmentsWithRegistry(decisions []commandWALSegmentCleanupDecision, registry *rootpublication.IdentityPinRegistry) ([]commandWALSegmentCleanupDecision, error) {
	defer closeCommandWALCleanupDecisions(decisions)
	type cleanupLease struct {
		decision int
		lease    *rootpublication.IdentityDeleteLease
	}
	leases := make([]cleanupLease, 0, len(decisions))
	abortLeases := func() {
		for i := range leases {
			if leases[i].lease != nil {
				leases[i].lease.Abort()
			}
		}
	}
	for i := range decisions {
		decision := &decisions[i]
		if !decision.Covered || decision.Active {
			continue
		}
		current, err := os.Open(decision.Path)
		if err != nil {
			decision.Error = err.Error()
			abortLeases()
			return decisions, errors.Join(ErrRecoveryRequired, fmt.Errorf("reopen command WAL cleanup target %s: %w", filepath.Base(decision.Path), err))
		}
		currentIdentity, identityErr := rootpublication.StableIdentityFromFile(current)
		closeErr := current.Close()
		if identityErr != nil {
			decision.Error = identityErr.Error()
			abortLeases()
			return decisions, identityErr
		}
		if closeErr != nil {
			decision.Error = closeErr.Error()
			abortLeases()
			return decisions, closeErr
		}
		if decision.file == nil || !rootpublication.SamePhysicalIdentity(decision.identity, currentIdentity) {
			err := errors.Join(ErrRecoveryRequired, fmt.Errorf("command WAL cleanup target identity changed: %s", filepath.Base(decision.Path)))
			decision.Error = err.Error()
			abortLeases()
			return decisions, err
		}
		var lease *rootpublication.IdentityDeleteLease
		if registry != nil {
			lease, err = registry.BeginDeleteAt(decision.identity, filepath.Clean(decision.Path))
			if err != nil {
				decision.Pinned = errors.Is(err, rootpublication.ErrResourcePinned)
				decision.Error = err.Error()
				abortLeases()
				return decisions, err
			}
		}
		leases = append(leases, cleanupLease{decision: i, lease: lease})
	}
	// The scan handles keep the discovered physical objects alive while every
	// deletion lease is acquired. Close those handles before the final pathname
	// revalidation and unlink: Windows forbids removing an otherwise-closed WAL
	// segment while our own scan handle remains open. Closing the handle here is
	// safe because the next loop reopens the pathname and compares its physical
	// identity immediately before each namespace mutation.
	for _, entry := range leases {
		decision := &decisions[entry.decision]
		file := decision.file
		decision.file = nil
		if file == nil {
			err := errors.Join(ErrRecoveryRequired, fmt.Errorf("command WAL cleanup target lost scan handle: %s", filepath.Base(decision.Path)))
			decision.Error = err.Error()
			abortLeases()
			return decisions, err
		}
		if err := file.Close(); err != nil {
			decision.Error = err.Error()
			abortLeases()
			return decisions, err
		}
	}
	for leaseIndex := range leases {
		entry := &leases[leaseIndex]
		decision := &decisions[entry.decision]
		if err := durabilitycut.EmitPath(durabilitycut.BeforeWALOrAssetUnlink, durabilitycut.ResourceCommandWAL, "", decision.Path); err != nil {
			decision.Error = err.Error()
			for i := leaseIndex; i < len(leases); i++ {
				if leases[i].lease != nil {
					leases[i].lease.Abort()
				}
			}
			return decisions, err
		}
		current, openErr := os.Open(decision.Path)
		if openErr != nil {
			decision.Error = openErr.Error()
			for i := leaseIndex; i < len(leases); i++ {
				if leases[i].lease != nil {
					leases[i].lease.Abort()
				}
			}
			return decisions, errors.Join(ErrRecoveryRequired, fmt.Errorf("revalidate command WAL cleanup target %s: %w", filepath.Base(decision.Path), openErr))
		}
		currentIdentity, identityErr := rootpublication.StableIdentityFromFile(current)
		closeErr := current.Close()
		if identityErr != nil || closeErr != nil || !rootpublication.SamePhysicalIdentity(decision.identity, currentIdentity) {
			revalidateErr := errors.Join(identityErr, closeErr)
			if revalidateErr == nil {
				revalidateErr = fmt.Errorf("command WAL cleanup target identity changed: %s", filepath.Base(decision.Path))
			}
			decision.Error = revalidateErr.Error()
			for i := leaseIndex; i < len(leases); i++ {
				if leases[i].lease != nil {
					leases[i].lease.Abort()
				}
			}
			return decisions, errors.Join(ErrRecoveryRequired, revalidateErr)
		}
		removeErr := os.Remove(decision.Path)
		if removeErr != nil && !os.IsNotExist(removeErr) {
			decision.Error = removeErr.Error()
			for i := leaseIndex; i < len(leases); i++ {
				if leases[i].lease != nil {
					leases[i].lease.Abort()
				}
			}
			return decisions, removeErr
		}
		if entry.lease != nil {
			entry.lease.CommitDeleted()
			entry.lease = nil
		}
		after := durabilitycut.Event{
			Point:    durabilitycut.AfterWALOrAssetUnlink,
			Resource: durabilitycut.ResourceCommandWAL,
			Path:     decision.Path,
		}
		if removeErr == nil {
			after.Namespace = durabilitycut.NamespaceUnlink
			after.OldPath = decision.Path
		}
		// Preserve the completed namespace mutation in the returned decisions
		// even when the post-unlink observer reports a simulated crash cut.
		decision.Removed = true
		if err := durabilitycut.Emit(after); err != nil {
			decision.Error = err.Error()
			for i := leaseIndex + 1; i < len(leases); i++ {
				if leases[i].lease != nil {
					leases[i].lease.Abort()
				}
			}
			if removeErr == nil {
				return decisions, errors.Join(err, ErrRecoveryRequired)
			}
			return decisions, err
		}
	}
	return decisions, nil
}

func closeCommandWALCleanupDecisions(decisions []commandWALSegmentCleanupDecision) {
	for i := range decisions {
		if decisions[i].file != nil {
			_ = decisions[i].file.Close()
			decisions[i].file = nil
		}
	}
}
