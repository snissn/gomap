package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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

const (
	minDetachedClosedWALBytes = 96 << 20
	maxDetachedActiveWALBytes = 1 << 20
)

// commandWALStreamCleanupCut captures all extents from one canonical journal
// lane. The active stream is scanned under append ownership when this cut is
// taken. Only its immutable predecessors are read outside that ownership.
type commandWALStreamCleanupCut struct {
	journal commitlog.CommandJournalCleanupSnapshot
	sealed  []cleanupClosedWALFile
	active  cleanupClosedWALFile
}

func closeUnkeptCommandWALFiles(files []cleanupClosedWALFile) error {
	var err error
	for i := range files {
		file := &files[i]
		if file.file != nil && !file.kept {
			err = errors.Join(err, file.file.Close())
			file.file = nil
		}
	}
	return err
}

func (cut *commandWALStreamCleanupCut) closeUnkept() error {
	err := closeUnkeptCommandWALFiles(cut.sealed)
	if cut.active.file != nil {
		err = errors.Join(err, cut.active.file.Close())
		cut.active.file = nil
	}
	return err
}

func sameSealedWALStreamExtent(captured, linked os.FileInfo) bool {
	return linked.Mode().IsRegular() && os.SameFile(captured, linked) &&
		linked.Size() == captured.Size() && linked.ModTime().Equal(captured.ModTime())
}

func (cut *commandWALStreamCleanupCut) validateLinks(dir string, current commitlog.CommandJournalCleanupSnapshot) error {
	segments, err := simpleCommandWALCleanupInventory(dir, current)
	if commandWALCleanupDescriptorExhausted(err) {
		// Validate the captured names against their still-open objects before
		// releasing handles the locked scanner would already have closed. A
		// covered deletion target keeps its scan handle throughout.
		if linkErr := cut.validateCapturedLinks(); linkErr != nil {
			return linkErr
		}
		if closeErr := cut.closeUnkept(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		// The final journal owner and durable root remain locked. Retry only
		// the directory read with the same descriptor budget as the locked
		// scanner, then recheck every captured name before any unlink.
		segments, err = simpleCommandWALCleanupInventory(dir, current)
	}
	if err != nil {
		return err
	}
	if len(segments) <= len(cut.sealed) || segments[len(cut.sealed)].path != cut.active.segment.path {
		return fmt.Errorf("%w: command WAL stream cleanup inventory changed", ErrRecoveryRequired)
	}
	for i, sealed := range cut.sealed {
		if segments[i].path != sealed.segment.path {
			return fmt.Errorf("%w: command WAL stream cleanup inventory changed", ErrRecoveryRequired)
		}
	}
	return cut.validateCapturedLinks()
}

func (cut *commandWALStreamCleanupCut) validateCapturedLinks() error {
	for _, sealed := range cut.sealed {
		linked, err := os.Lstat(sealed.segment.path)
		if err != nil {
			return errors.Join(ErrRecoveryRequired, err)
		}
		if !sameSealedWALStreamExtent(sealed.info, linked) {
			return fmt.Errorf("%w: closed command WAL stream changed: %s", ErrRecoveryRequired, filepath.Base(sealed.segment.path))
		}
	}
	linked, err := os.Lstat(cut.active.segment.path)
	if err != nil {
		return errors.Join(ErrRecoveryRequired, err)
	}
	// The original active stream can gain bytes after it was scanned. It
	// remains ineligible for retirement, including after a rotation.
	if !linked.Mode().IsRegular() || !os.SameFile(cut.active.info, linked) || linked.Size() < cut.active.info.Size() {
		return fmt.Errorf("%w: captured active command WAL stream changed: %s", ErrRecoveryRequired, filepath.Base(cut.active.segment.path))
	}
	return nil
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

func (db *DB) scanCommandWALCleanupProof(proof *durableWALCleanupProofV1, scanSealedPrefix bool) ([]commandWALSegmentCleanupDecision, error) {
	var beforeSegment func(string)
	openPrefix := os.Open
	if hooks := db.testCommandWALCleanupHooks; hooks != nil {
		beforeSegment = hooks.beforeSegment
		if hooks.openPrefix != nil {
			openPrefix = hooks.openPrefix
		}
	}
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment)
	if !scanSealedPrefix {
		// Checkpoint, Close, and explicit cleanup use the original locked scan.
		return scan.finish(proof.journalOwner.WithCleanupScan(proof.journal, func() error {
			return scan.scanLockedDirectory(db.dir)
		}))
	}
	dir := resolveStorageLayout(db.dir).walDir
	var prefix []cleanupClosedWALFile
	closePrefix := func() error { return closeUnkeptCommandWALFiles(prefix) }

	err := proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		if !current.FixedContainer {
			// Read ahead only on closed streams, using their original validator.
			if current.SegmentSeq > 1 {
				scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
			}
			if scanSealedPrefix && current.Ext4Stream && current.ActiveBytes <= maxDetachedActiveWALBytes &&
				db.commandWALClosedBytes.Load() >= minDetachedClosedWALBytes {
				cut, err := captureCommandWALStreamCleanupCut(dir, current, scan, openPrefix)
				if cut != nil || err != nil {
					proof.streamCut = cut
					return err
				}
			}
			return scan.scanLockedDirectory(db.dir)
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
		for _, seg := range segments[:min(len(segments)-1, maxOpenClosedWALCleanupSegments)] {
			file, err := openPrefix(seg.path)
			if err != nil {
				if !commandWALCleanupDescriptorExhausted(err) {
					return fmt.Errorf("open closed command WAL segment %s: %w", filepath.Base(seg.path), err)
				}
				// The locked scan opens uncovered files one at a time. Drop the
				// staged handles and try it once with the same replay bounds.
				if closeErr := closePrefix(); closeErr != nil {
					return errors.Join(err, closeErr)
				}
				prefix = nil
				return scan.scanLockedDirectory(db.dir)
			}
			info, statErr := file.Stat()
			identity, identityErr := rootpublication.StableIdentityFromFile(file)
			fixed, formatErr := commitlog.HasFixedWALMagic(file)
			if err := errors.Join(statErr, identityErr, formatErr); err != nil {
				return errors.Join(err, file.Close())
			}
			if !fixed || !info.Mode().IsRegular() || rootpublication.SamePhysicalIdentity(identity, current.ActiveIdentity) {
				if err := errors.Join(file.Close(), closePrefix()); err != nil {
					return err
				}
				prefix = nil
				return scan.scanLockedDirectory(db.dir)
			}
			prefix = append(prefix, cleanupClosedWALFile{segment: seg, file: file, info: info, identity: identity})
		}
		return nil
	})
	if cut := proof.streamCut; cut != nil {
		if err == nil {
			// The active stream has already been scanned at the same cut.
			// Later appends and rotations are outside its replay proof.
			for i := range cut.sealed {
				sealed := &cut.sealed[i]
				sealed.kept = scan.scanFile(sealed.segment, sealed.file, false, false, nil)
				if sealed.kept {
					scan.decisions[len(scan.decisions)-1].sealedStreamInfo = sealed.info
				}
			}
		}
		if err != nil || scan.hardErr != nil {
			scan.hardErr = errors.Join(scan.hardErr, cut.closeUnkept())
			proof.streamCut = nil
		}
		return scan.finish(err)
	}
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
		closed.kept = scan.scanFile(closed.segment, closed.file, false, true, nil)
	}
	if scan.hardErr != nil {
		return scan.finish(closePrefix())
	}

	err = proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		segments, inventoryErr := simpleCommandWALCleanupInventory(dir, current)
		if inventoryErr != nil && !commandWALCleanupDescriptorExhausted(inventoryErr) {
			return inventoryErr
		}
		if inventoryErr != nil || !current.FixedContainer || len(segments) <= len(prefix) || !slices.EqualFunc(prefix, segments[:len(prefix)], func(a cleanupClosedWALFile, b logSegment) bool {
			return a.segment.path == b.path
		}) {
			// Drop staged handles before one locked full scan under the same
			// captured root bounds. The locked scan opens uncovered files one at a time.
			if err := errors.Join(closePrefix(), closeCommandWALCleanupDecisions(scan.decisions)); err != nil {
				return errors.Join(inventoryErr, err)
			}
			scan = newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment)
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
		}
		if err := closePrefix(); err != nil {
			return err
		}
		for _, seg := range segments[len(prefix) : len(segments)-1] {
			scan.scanSegment(seg, false, true, nil)
		}
		scan.scanSegment(segments[len(segments)-1], true, true, &current.ActiveIdentity)
		return nil
	})
	scan.hardErr = errors.Join(scan.hardErr, closePrefix())
	return scan.finish(err)
}

// captureCommandWALStreamCleanupCut uses the existing append authority to
// identify the whole bounded stream inventory and read its live active member.
// A nil successful result leaves the original locked path in charge.
func captureCommandWALStreamCleanupCut(dir string, current commitlog.CommandJournalCleanupSnapshot, scan *commandWALCleanupScan, openPrefix func(string) (*os.File, error)) (*commandWALStreamCleanupCut, error) {
	segments, err := simpleCommandWALCleanupInventory(dir, current)
	if err != nil || len(segments) < 2 || len(segments)-1 > maxOpenClosedWALCleanupSegments {
		return nil, err
	}
	cut := &commandWALStreamCleanupCut{
		journal: current,
		sealed:  make([]cleanupClosedWALFile, 0, len(segments)-1),
	}
	large := 0
	for _, seg := range segments[:len(segments)-1] {
		file, err := openPrefix(seg.path)
		if err != nil {
			if commandWALCleanupDescriptorExhausted(err) {
				return nil, cut.closeUnkept()
			}
			return nil, errors.Join(err, cut.closeUnkept())
		}
		info, statErr := file.Stat()
		identity, identityErr := rootpublication.StableIdentityFromFile(file)
		fixed, formatErr := commitlog.HasFixedWALMagic(file)
		if err := errors.Join(statErr, identityErr, formatErr); err != nil {
			return nil, errors.Join(err, file.Close(), cut.closeUnkept())
		}
		if fixed || !info.Mode().IsRegular() || rootpublication.SamePhysicalIdentity(identity, current.ActiveIdentity) {
			return nil, errors.Join(file.Close(), cut.closeUnkept())
		}
		seg.size = info.Size()
		cut.sealed = append(cut.sealed, cleanupClosedWALFile{segment: seg, file: file, info: info, identity: identity})
		if seg.size >= minDetachedClosedWALBytes {
			large++
		}
	}
	if large != 1 {
		return nil, cut.closeUnkept()
	}
	active := segments[len(segments)-1]
	file, err := os.Open(active.path)
	if err != nil {
		if commandWALCleanupDescriptorExhausted(err) {
			return nil, cut.closeUnkept()
		}
		return nil, errors.Join(err, cut.closeUnkept())
	}
	info, statErr := file.Stat()
	if statErr != nil {
		return nil, errors.Join(statErr, file.Close(), cut.closeUnkept())
	}
	if !info.Mode().IsRegular() || info.Size() > maxDetachedActiveWALBytes {
		return nil, errors.Join(file.Close(), cut.closeUnkept())
	}
	active.size = info.Size()
	cut.active = cleanupClosedWALFile{segment: active, file: file, info: info}
	scan.scanFile(active, file, true, false, &current.ActiveIdentity)
	return cut, nil
}
