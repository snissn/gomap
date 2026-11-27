package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"syscall"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Only preopen a bounded number of immutable predecessors. Other predecessors
// and the mutable suffix are scanned under the original append exclusion.
const maxOpenClosedWALCleanupSegments = 8

// commandWALCleanupTestHooks gives cleanup's per-handle observation points one
// slot in DB. These callbacks are installed only by tests before starting cleanup.
type commandWALCleanupTestHooks struct {
	beforeSegment func(string)
	afterScan     func()
	openPrefix    func(string) (*os.File, error)
}

type cleanupClosedWALFile struct {
	segment  logSegment
	file     *os.File
	info     os.FileInfo
	identity rootpublication.StableIdentity
	kept     bool
}

// simpleCommandWALCleanupInventory recognizes a single canonical journal lane.
// The locked scanner handles old or mixed layouts and unusual directory entries.
func simpleCommandWALCleanupInventory(dir string, current commitlog.CommandJournalCleanupSnapshot) ([]logSegment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segments []logSegment
	for _, entry := range entries {
		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok || entry.IsDir() {
			continue
		}
		if valueLog || !entry.Type().IsRegular() || lane != current.Lane || seq == 0 || seq > current.SegmentSeq ||
			name != commitlog.CommandSegmentName(lane, seq) {
			return nil, nil
		}
		segments = append(segments, logSegment{seq: seq, lane: lane, path: filepath.Join(dir, name)})
	}
	slices.SortFunc(segments, func(a, b logSegment) int {
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
	if len(segments) == 0 || !samePath(segments[len(segments)-1].path, current.ActivePath) {
		return nil, nil
	}
	return segments, nil
}

func commandWALCleanupDescriptorExhausted(err error) bool {
	// os.Open on Windows reports Win32 ERROR_TOO_MANY_OPEN_FILES directly.
	const windowsTooManyOpenFiles syscall.Errno = 4
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		(runtime.GOOS == "windows" && errors.Is(err, windowsTooManyOpenFiles))
}

// A first capture pins its active WAL, and cleanup must separately reopen a
// covered pathname while still holding the scanned inode. Let both operations
// make progress before detaching optional readers.
func commandWALCleanupHasDescriptorHeadroom(dir string) (room bool, err error) {
	retirement, err := os.Open(dir)
	if err != nil {
		if commandWALCleanupDescriptorExhausted(err) {
			return false, nil
		}
		return false, err
	}
	capture, err := os.Open(dir)
	if err != nil {
		if commandWALCleanupDescriptorExhausted(err) {
			return false, retirement.Close()
		}
		return false, errors.Join(err, retirement.Close())
	}
	return true, errors.Join(capture.Close(), retirement.Close())
}

type preScannedClosedWALEntry struct {
	segment      logSegment
	file         *os.File
	info         os.FileInfo
	identity     rootpublication.StableIdentity
	logicalSize  int64
	scannedBytes int64
	frames       uint64
	minLSN       uint64
	maxLSN       uint64
	typed        bool
	emptyFixed   bool
	extraLSNs    []uint64
	err          error
}

// CommandWALClosedSegmentPreScan holds an opportunistic background read-only
// validation of already-closed command-WAL segments started before checkpoint
// fallback convergence so WAL scanning overlaps with fallback index.db fsyncs.
// Every pre-scanned segment is strictly revalidated by identity, stat, and
// inventory under WithCleanupScanSnapshot before any deletion decision uses it.
type CommandWALClosedSegmentPreScan struct {
	done         chan struct{}
	closeOnce    sync.Once
	fixed        bool
	seenLSNFloor uint64
	entries      []preScannedClosedWALEntry
	usable       bool
}

// Close waits for the background pre-scan to finish and closes any unconsumed
// file descriptors.
func (p *CommandWALClosedSegmentPreScan) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		<-p.done
		for i := range p.entries {
			if p.entries[i].file != nil {
				_ = p.entries[i].file.Close()
				p.entries[i].file = nil
			}
		}
	})
}

// StartCommandWALClosedSegmentPreScan opportunistically starts validating
// already-closed immutable command-WAL segments in the background so the scan
// overlaps with checkpoint fallback convergence I/O. Returns nil when not
// applicable. Callers must pass the result to
// CleanupCommandWALCoveredSegmentsAtCheckpointWithPreScan or call Close().
func (db *DB) StartCommandWALClosedSegmentPreScan() *CommandWALClosedSegmentPreScan {
	if db == nil || !db.commandWAL || db.readOnly || durabilitycut.Enabled() || db.testCommandWALCleanupHooks != nil {
		return nil
	}
	db.teardownMu.RLock()
	if db.closing.Load() || db.commandJournal == nil || db.commandWALPoisonedError() != nil {
		db.teardownMu.RUnlock()
		return nil
	}
	journal := db.commandJournal
	snapshot, err := journal.CaptureCleanupSnapshot()
	if err != nil || snapshot.SegmentSeq <= 1 || snapshot.PendingStableRotation != 0 || snapshot.PendingSuccessor {
		db.teardownMu.RUnlock()
		return nil
	}
	db.durablePublishMu.Lock()
	var seenLSNFloor uint64
	if db.durableRoot.slot <= 1 {
		seenLSNFloor = db.durableRoot.slotRecord[db.durableRoot.slot].AppliedCommandLSN
	}
	db.durablePublishMu.Unlock()
	walDir := resolveStorageLayout(db.dir).walDir
	maxSegmentBytes := db.walMaxSegmentBytes
	pre := &CommandWALClosedSegmentPreScan{
		done:         make(chan struct{}),
		fixed:        snapshot.FixedContainer,
		seenLSNFloor: seenLSNFloor,
	}
	go func() {
		defer db.teardownMu.RUnlock()
		defer close(pre.done)
		var staged []preScannedClosedWALEntry
		stageErr := journal.WithCleanupScanSnapshot(snapshot, func(current commitlog.CommandJournalCleanupSnapshot) error {
			if current.FixedContainer != pre.fixed || current.SegmentSeq <= 1 {
				return nil
			}
			segments, err := simpleCommandWALCleanupInventory(walDir, current)
			if err != nil || len(segments) < 2 {
				return err
			}
			wanted := min(len(segments)-1, maxOpenClosedWALCleanupSegments)
			if room, err := commandWALCleanupHasDescriptorHeadroom(walDir); err != nil || !room {
				return err
			}
			staged = make([]preScannedClosedWALEntry, 0, wanted)
			for _, seg := range segments[:wanted] {
				file, err := os.Open(seg.path)
				if err != nil {
					return err
				}
				info, statErr := file.Stat()
				identity, idErr := rootpublication.StableIdentityFromFile(file)
				fixed, fmtErr := commitlog.HasFixedWALMagic(file)
				if err := errors.Join(statErr, idErr, fmtErr); err != nil {
					_ = file.Close()
					return err
				}
				if fixed != current.FixedContainer || !info.Mode().IsRegular() || rootpublication.SamePhysicalIdentity(identity, current.ActiveIdentity) {
					_ = file.Close()
					return errors.New("pre-scan segment mismatch")
				}
				staged = append(staged, preScannedClosedWALEntry{
					segment:  seg,
					file:     file,
					info:     info,
					identity: identity,
				})
			}
			return nil
		})
		if stageErr != nil || len(staged) == 0 {
			for i := range staged {
				if staged[i].file != nil {
					_ = staged[i].file.Close()
					staged[i].file = nil
				}
			}
			return
		}
		scanOne := func(entry *preScannedClosedWALEntry) {
			var readBuffer *commitlog.CleanupReadBuffer
			if !pre.fixed {
				readBuffer = new(commitlog.CleanupReadBuffer)
			}
			r, err := readBuffer.Open(entry.file, commitlog.Options{MaxSegmentSize: maxSegmentBytes})
			if err != nil {
				entry.err = err
				return
			}
			defer r.Close()
			logicalSize, fixed := r.FixedLogicalSize()
			if !fixed {
				logicalSize = entry.info.Size()
			}
			localSeen := make(map[uint64]struct{})
			res, err := scanCommandWALSegmentReader(r, false, commandWALSegmentScanOptions{
				seenLSNs:          localSeen,
				seenLSNAppliedLSN: pre.seenLSNFloor,
			})
			if err != nil {
				entry.err = err
				return
			}
			entry.logicalSize = logicalSize
			entry.scannedBytes = res.scannedBytes
			entry.frames = res.frames
			entry.minLSN = res.minLSN
			entry.maxLSN = res.maxLSN
			entry.typed = res.typed
			entry.emptyFixed = res.emptyFixed
			if len(localSeen) > 0 {
				entry.extraLSNs = make([]uint64, 0, len(localSeen))
				for lsn := range localSeen {
					entry.extraLSNs = append(entry.extraLSNs, lsn)
				}
			}
		}
		if len(staged) == 1 {
			scanOne(&staged[0])
		} else {
			var wg sync.WaitGroup
			wg.Add(len(staged))
			for i := range staged {
				go func(idx int) {
					defer wg.Done()
					scanOne(&staged[idx])
				}(i)
			}
			wg.Wait()
		}
		allOK := true
		for i := range staged {
			if staged[i].err != nil {
				allOK = false
				break
			}
		}
		if !allOK {
			for i := range staged {
				if staged[i].file != nil {
					_ = staged[i].file.Close()
					staged[i].file = nil
				}
			}
			return
		}
		pre.entries = staged
		pre.usable = true
	}()
	return pre
}

func (db *DB) scanCommandWALCleanupProof(proof durableWALCleanupProofV1, scanSealedPrefix bool) ([]commandWALSegmentCleanupDecision, error) {
	return db.scanCommandWALCleanupProofWithPreScan(proof, scanSealedPrefix, nil)
}

func (db *DB) scanCommandWALCleanupProofWithPreScan(proof durableWALCleanupProofV1, scanSealedPrefix bool, preScan *CommandWALClosedSegmentPreScan) ([]commandWALSegmentCleanupDecision, error) {
	if preScan != nil && !durabilitycut.Enabled() && db.testCommandWALCleanupHooks == nil {
		<-preScan.done
		if preScan.usable && len(preScan.entries) > 0 && proof.cleanupThrough >= preScan.seenLSNFloor && proof.journal.FixedContainer == preScan.fixed {
			if decisions, ok, err := db.finishCommandWALCleanupPreScan(proof, preScan); ok {
				return decisions, err
			}
		}
	}
	var beforeSegment func(string)
	openPrefix := os.Open
	if hooks := db.testCommandWALCleanupHooks; hooks != nil {
		beforeSegment = hooks.beforeSegment
		if hooks.openPrefix != nil {
			openPrefix = hooks.openPrefix
		}
	}
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment, &proof.journal)
	if !scanSealedPrefix {
		// Checkpoint, Close, and explicit cleanup use the original locked scan.
		// Reuse one read-ahead buffer across closed stream segments (fixed
		// containers bypass buffering inside CleanupReadBuffer.Open).
		if !proof.journal.FixedContainer {
			scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
		}
		return scan.finish(proof.journalOwner.WithCleanupScan(proof.journal, func() error {
			return scan.scanLockedDirectory(db.dir)
		}))
	}
	dir := resolveStorageLayout(db.dir).walDir
	var prefix []cleanupClosedWALFile
	var stagedFixed bool
	closePrefix := func() error {
		var err error
		for i := range prefix {
			file := &prefix[i]
			if file.file != nil && !file.kept {
				err = errors.Join(err, file.file.Close())
				file.file = nil
			}
		}
		return err
	}

	err := proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		if !current.FixedContainer && current.SegmentSeq > 1 {
			scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
		}
		if current.SegmentSeq <= 2 {
			return scan.scanLockedDirectory(db.dir)
		}
		segments, err := simpleCommandWALCleanupInventory(dir, current)
		if err != nil {
			return err
		}
		if len(segments) < 3 {
			// Require multiple sealed generations. Keep small inventories on
			// one lock acquisition and use the original full validation.
			return scan.scanLockedDirectory(db.dir)
		}
		wanted := min(len(segments)-1, maxOpenClosedWALCleanupSegments)
		var stageErr error
		for _, seg := range segments[:wanted] {
			file, err := openPrefix(seg.path)
			if err != nil {
				if !commandWALCleanupDescriptorExhausted(err) {
					return fmt.Errorf("open closed command WAL segment %s: %w", filepath.Base(seg.path), err)
				}
				stageErr = err
				break
			}
			info, statErr := file.Stat()
			identity, identityErr := rootpublication.StableIdentityFromFile(file)
			fixed, formatErr := commitlog.HasFixedWALMagic(file)
			if err := errors.Join(statErr, identityErr, formatErr); err != nil {
				return errors.Join(err, file.Close())
			}
			if fixed != current.FixedContainer || !info.Mode().IsRegular() || rootpublication.SamePhysicalIdentity(identity, current.ActiveIdentity) {
				if err := errors.Join(file.Close(), closePrefix()); err != nil {
					return err
				}
				prefix = nil
				return scan.scanLockedDirectory(db.dir)
			}
			prefix = append(prefix, cleanupClosedWALFile{segment: seg, file: file, info: info, identity: identity})
		}
		room, err := commandWALCleanupHasDescriptorHeadroom(dir)
		if err != nil {
			return err
		}
		if room && len(prefix) == wanted {
			stagedFixed = current.FixedContainer
			return nil
		}
		if err := closePrefix(); err != nil {
			return errors.Join(stageErr, err)
		}
		prefix = nil
		if !room {
			// No retirement has started. A later maintenance event retries the
			// debt after the optional staging descriptors have been released.
			return errDurableWALCleanupProofUnavailable
		}
		// A transient staging refusal still permits the original locked scan.
		return scan.scanLockedDirectory(db.dir)
	})
	if err != nil || len(prefix) == 0 {
		scan.hardErr = errors.Join(scan.hardErr, closePrefix())
		return scan.finish(err)
	}

	// The database maintenance owner serializes deletes and journal rotation
	// never writes an earlier generation. Exact open files protect the scanned
	// objects until the second inventory check. No extent summary survives this
	// cleanup invocation.
	for i := range prefix {
		closed := &prefix[i]
		closed.kept = scan.scanFile(closed.segment, closed.file, false, nil)
	}
	if scan.hardErr != nil {
		return scan.finish(closePrefix())
	}

	var suffixDescriptorErr error
	err = proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		segments, inventoryErr := simpleCommandWALCleanupInventory(dir, current)
		if inventoryErr != nil && !commandWALCleanupDescriptorExhausted(inventoryErr) {
			return inventoryErr
		}
		if inventoryErr != nil || current.FixedContainer != stagedFixed || len(segments) <= len(prefix) || !slices.EqualFunc(prefix, segments[:len(prefix)], func(a cleanupClosedWALFile, b logSegment) bool {
			return a.segment.path == b.path
		}) {
			// Drop staged handles before one locked full scan under the same
			// captured root bounds. The locked scan opens uncovered files one at a time.
			if err := errors.Join(closePrefix(), closeCommandWALCleanupDecisions(scan.decisions)); err != nil {
				return errors.Join(inventoryErr, err)
			}
			scan = newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment, &proof.journal)
			if !current.FixedContainer && current.SegmentSeq > 1 {
				scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
			}
			return scan.scanLockedDirectory(db.dir)
		}
		for _, file := range prefix {
			linked, err := os.Lstat(file.segment.path)
			if err != nil {
				return errors.Join(ErrRecoveryRequired, err)
			}
			if !linked.Mode().IsRegular() || !os.SameFile(file.info, linked) ||
				rootpublication.SamePhysicalIdentity(file.identity, current.ActiveIdentity) {
				return fmt.Errorf("%w: closed command WAL segment changed: %s", ErrRecoveryRequired, filepath.Base(file.segment.path))
			}
			if !stagedFixed && (linked.Size() != file.info.Size() || !linked.ModTime().Equal(file.info.ModTime())) {
				return fmt.Errorf("%w: closed command WAL stream changed during scan: %s", ErrRecoveryRequired, filepath.Base(file.segment.path))
			}
		}
		if err := closePrefix(); err != nil {
			return err
		}
		suffix := segments[len(prefix):]
		for i, seg := range suffix {
			active := i == len(suffix)-1
			var expected *rootpublication.StableIdentity
			if active {
				expected = &current.ActiveIdentity
			}
			if openErr := scan.scanSegment(seg, active, expected); openErr != nil {
				if commandWALCleanupDescriptorExhausted(openErr) {
					suffixDescriptorErr = errors.Join(suffixDescriptorErr, openErr)
				} else {
					scan.hardErr = errors.Join(scan.hardErr, openErr)
				}
			}
		}
		return nil
	})
	scan.hardErr = errors.Join(scan.hardErr, closePrefix())
	if suffixDescriptorErr == nil {
		return scan.finish(err)
	}
	// Incomplete validation prohibits retirement. Finish closes the retained
	// scan handles. Defer debt only if descriptor acquisition was the sole
	// failure, keeping independent scan, identity, and close errors visible.
	decisions, scanErr := scan.finish(errors.Join(err, suffixDescriptorErr))
	if err == nil && scan.hardErr == nil && scan.lineageErr == nil {
		return decisions, errDurableWALCleanupProofUnavailable
	}
	return decisions, scanErr
}

// hasZeroFrontierReclaimableCommandWALCandidate inspects only closed non-active
// command-WAL segment headers under the cleanup scan lock when the cleanup
// frontier is 0. At frontier 0, only a positively validated empty fixed
// container can ever be reclaimed; stream segments and non-empty fixed
// containers (whose first extent witness at offset 4096 is non-zero) are never
// deletable at frontier 0. Returning (false, nil) lets opportunistic
// checkpoint/close cleanup avoid scanning retained segments before fallback
// convergence advances the cleanup frontier.
func (db *DB) hasZeroFrontierReclaimableCommandWALCandidate(proof durableWALCleanupProofV1) (bool, error) {
	const (
		fixedWALHeaderSize  = 4096
		fixedWALWitnessSize = 96
	)
	var hasCandidate bool
	var probe [fixedWALWitnessSize]byte
	err := proof.journalOwner.WithCleanupScan(proof.journal, func() error {
		segments, err := listSegments(resolveStorageLayout(db.dir).walDir, true)
		if err != nil {
			return err
		}
		activeByLane := commandWALActiveSeqByLane(segments)
		for _, seg := range segments {
			if seg.valueLog || !isCommandWALLaneSegment(seg) {
				continue
			}
			if seg.seq == activeByLane[seg.lane] {
				continue
			}
			file, err := os.Open(seg.path)
			if err != nil {
				return fmt.Errorf("open command WAL segment %s: %w", filepath.Base(seg.path), err)
			}
			fixed, magicErr := commitlog.HasFixedWALMagic(file)
			var maybeEmptyFixed bool
			var probeErr error
			if magicErr == nil && fixed {
				clear(probe[:])
				n, readErr := file.ReadAt(probe[:], fixedWALHeaderSize)
				if readErr != nil && !errors.Is(readErr, io.EOF) {
					probeErr = readErr
				} else if n < fixedWALWitnessSize {
					maybeEmptyFixed = true
				} else {
					allZero := true
					for _, b := range probe {
						if b != 0 {
							allZero = false
							break
						}
					}
					maybeEmptyFixed = allZero
				}
			}
			closeErr := file.Close()
			if err := errors.Join(magicErr, probeErr, closeErr); err != nil {
				return fmt.Errorf("classify command WAL segment %s: %w", filepath.Base(seg.path), err)
			}
			if maybeEmptyFixed {
				hasCandidate = true
				return nil
			}
		}
		return nil
	})
	return hasCandidate, err
}

// hasClosedNonActiveCommandWALSegment reports whether the WAL directory
// contains at least one closed non-active command-WAL segment under the cleanup
// scan lock. Opportunistic checkpoint cleanup can never unlink an active
// segment, so returning (false, nil) avoids scanning the active segment on
// no-op checkpoints when nothing is reclaimable.
func (db *DB) hasClosedNonActiveCommandWALSegment(proof durableWALCleanupProofV1) (bool, error) {
	var hasClosed bool
	err := proof.journalOwner.WithCleanupScan(proof.journal, func() error {
		segments, err := listSegments(resolveStorageLayout(db.dir).walDir, true)
		if err != nil {
			return err
		}
		activeByLane := commandWALActiveSeqByLane(segments)
		for _, seg := range segments {
			if seg.valueLog || !isCommandWALLaneSegment(seg) {
				continue
			}
			if seg.seq != activeByLane[seg.lane] {
				hasClosed = true
				return nil
			}
		}
		return nil
	})
	return hasClosed, err
}

func (db *DB) finishCommandWALCleanupPreScan(proof durableWALCleanupProofV1, preScan *CommandWALClosedSegmentPreScan) ([]commandWALSegmentCleanupDecision, bool, error) {
	dir := resolveStorageLayout(db.dir).walDir
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, nil, &proof.journal)
	usedPreScan := false
	err := proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		if current.FixedContainer != preScan.fixed {
			return nil
		}
		segments, invErr := simpleCommandWALCleanupInventory(dir, current)
		if invErr != nil || len(segments) <= len(preScan.entries) {
			return nil
		}
		for i, entry := range preScan.entries {
			if entry.segment.path != segments[i].path || entry.file == nil {
				return nil
			}
			linked, statErr := os.Lstat(entry.segment.path)
			if statErr != nil || !linked.Mode().IsRegular() || !os.SameFile(entry.info, linked) ||
				rootpublication.SamePhysicalIdentity(entry.identity, current.ActiveIdentity) {
				return nil
			}
			if !preScan.fixed && (linked.Size() != entry.info.Size() || !linked.ModTime().Equal(entry.info.ModTime())) {
				return nil
			}
		}
		// All pre-scanned closed segments are strictly verified as immutable and
		// identical under the live cleanup snapshot. Transfer their decisions and
		// scan only the remaining suffix (typically the empty fresh active segment).
		usedPreScan = true
		scan.decisions = make([]commandWALSegmentCleanupDecision, 0, len(segments))
		for i := range preScan.entries {
			entry := &preScan.entries[i]
			for _, lsn := range entry.extraLSNs {
				if (scan.options.seenLSNAppliedLSN == 0 || lsn > scan.options.seenLSNAppliedLSN) &&
					(scan.options.seenLSNMax == 0 || lsn <= scan.options.seenLSNMax) {
					if _, dup := scan.options.seenLSNs[lsn]; dup {
						scan.lineageErr = errors.Join(scan.lineageErr, fmt.Errorf("scan command WAL segment %s: %w", filepath.Base(entry.segment.path), commitlog.ErrCommandWALDuplicateLSN))
					} else {
						scan.options.seenLSNs[lsn] = struct{}{}
					}
				}
			}
			if !entry.typed && !entry.emptyFixed {
				continue
			}
			decision := commandWALSegmentCleanupDecision{
				Path: entry.segment.path, Size: entry.info.Size(), LogicalSize: entry.logicalSize,
				ScannedBytes: entry.scannedBytes, Frames: entry.frames, MinLSN: entry.minLSN, MaxLSN: entry.maxLSN,
				Active: false, identity: entry.identity, lane: entry.segment.lane, seq: entry.segment.seq, generationKnown: true,
			}
			decision.Covered = entry.maxLSN <= scan.options.seenLSNAppliedLSN
			if decision.Covered {
				if decision.protectedByCapture(&proof.journal) {
					decision.Active = true
				} else {
					decision.file = entry.file
					entry.file = nil
				}
			}
			scan.decisions = append(scan.decisions, decision)
		}
		if !current.FixedContainer && len(segments)-len(preScan.entries) > 1 {
			scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
		}
		suffix := segments[len(preScan.entries):]
		for i, seg := range suffix {
			active := i == len(suffix)-1
			var expected *rootpublication.StableIdentity
			if active {
				expected = &current.ActiveIdentity
			}
			if openErr := scan.scanSegment(seg, active, expected); openErr != nil {
				scan.hardErr = errors.Join(scan.hardErr, openErr)
			}
		}
		return nil
	})
	if err != nil {
		decisions, finishErr := scan.finish(err)
		return decisions, true, finishErr
	}
	if !usedPreScan {
		return nil, false, nil
	}
	decisions, finishErr := scan.finish(nil)
	return decisions, true, finishErr
}
