package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Only preopen a bounded number of immutable predecessors. Other predecessors
// and the mutable suffix are scanned under the original append exclusion.
const maxOpenClosedWALCleanupSegments = 8

const (
	minSplitStreamCleanupSegmentBytes = 96 << 20
	maxSplitStreamCleanupActiveBytes  = 1 << 20
)

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
	return simpleCommandWALCleanupEntries(dir, entries, current), nil
}

func simpleCommandWALCleanupEntries(dir string, entries []os.DirEntry, current commitlog.CommandJournalCleanupSnapshot) []logSegment {
	var segments []logSegment
	for _, entry := range entries {
		name := entry.Name()
		lane, seq, valueLog, ok := parseLogSeq(name)
		if !ok || entry.IsDir() {
			continue
		}
		if valueLog || !entry.Type().IsRegular() || lane != current.Lane || seq == 0 || seq > current.SegmentSeq ||
			name != commitlog.CommandSegmentName(lane, seq) {
			return nil
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
		return nil
	}
	return segments
}

func commandWALCleanupDescriptorExhausted(err error) bool {
	// os.Open on Windows reports Win32 ERROR_TOO_MANY_OPEN_FILES directly.
	const windowsTooManyOpenFiles syscall.Errno = 4
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		(runtime.GOOS == "windows" && errors.Is(err, windowsTooManyOpenFiles))
}

func (db *DB) scanCommandWALCleanupProof(proof durableWALCleanupProofV1, scanSealedPrefix bool) ([]commandWALSegmentCleanupDecision, error) {
	var beforeSegment func(string)
	var openSplitPrefix func(string) (*os.File, error)
	if hooks := db.testCommandWALCleanupHooks; hooks != nil {
		beforeSegment = hooks.beforeSegment
		openSplitPrefix = hooks.openPrefix
	}
	openPrefix := os.Open
	if openSplitPrefix != nil {
		openPrefix = openSplitPrefix
	}
	scan := newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment, &proof.journal)
	if !scanSealedPrefix {
		// Checkpoint, Close, and explicit cleanup use the original locked scan.
		return scan.finish(proof.journalOwner.WithCleanupScan(proof.journal, func() error {
			return scan.scanLockedDirectory(db.dir)
		}))
	}
	dir := resolveStorageLayout(db.dir).walDir
	if proof.journal.Ext4Stream && proof.journal.ActiveBytes <= maxSplitStreamCleanupActiveBytes &&
		db.commandWALClosedBytes.Load() >= 2*minSplitStreamCleanupSegmentBytes &&
		db.commandWALCleanupSplitValueLogVolume(proof.journal) {
		if attempted, err := db.scanSplitCommandWALCleanupPrefix(proof, scan, openSplitPrefix, minSplitStreamCleanupSegmentBytes); attempted {
			return scan.finish(err)
		}
	}
	var prefix []cleanupClosedWALFile
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
		if !current.FixedContainer {
			// Stream cleanup retains append exclusion for the full scan.
			// Read ahead only on closed streams, using their original validator.
			if current.SegmentSeq > 1 {
				scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
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
			scan = newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, db.walMaxSegmentBytes, beforeSegment, &proof.journal)
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

// The split placement is a performance selector. Exact journal identity,
// lineage, and namespace checks still govern cleanup on this placement.
func (db *DB) commandWALCleanupSplitValueLogVolume(snapshot commitlog.CommandJournalCleanupSnapshot) bool {
	valueDir, err := os.Open(resolveStorageLayout(db.dir).valueVLogDir)
	if err != nil {
		return false
	}
	identity, identityErr := rootpublication.StableIdentityFromFile(valueDir)
	defer valueDir.Close()
	return identityErr == nil && identity.VolumeID != snapshot.ActiveIdentity.VolumeID && commitlog.IsExt4WALFilesystem(valueDir)
}

// The existing proof capture already holds the append lock and names the active
// generation. The cleanup owner serializes deletion. Rotation releases the
// detached handles before allocating any of its own and forces a locked retry.
// Otherwise refresh the inventory and validate the one active stream under
// append exclusion.
func (db *DB) scanSplitCommandWALCleanupPrefix(proof durableWALCleanupProofV1, scan *commandWALCleanupScan, openPrefix func(string) (*os.File, error), minClosedBytes int64) (bool, error) {
	dir := resolveStorageLayout(db.dir).walDir
	segments, err := simpleCommandWALCleanupInventory(dir, proof.journal)
	if commandWALCleanupDescriptorExhausted(err) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if len(segments) < 3 || len(segments)-1 > maxOpenClosedWALCleanupSegments {
		return false, nil
	}
	sealed := segments[:len(segments)-1]
	prefix := make([]cleanupClosedWALFile, len(sealed))
	large := 0
	for i, seg := range sealed {
		info, err := os.Lstat(seg.path)
		if err != nil || !info.Mode().IsRegular() {
			// Use the locked directory path to handle an unusual inventory.
			return false, nil
		}
		seg.size = info.Size()
		prefix[i] = cleanupClosedWALFile{segment: seg, info: info}
		if info.Size() >= minClosedBytes {
			large++
		}
	}
	if large < 2 {
		return false, nil
	}

	var refreshDir *os.File
	var dirInfo os.FileInfo
	closeReader := func() error {
		if refreshDir == nil {
			return nil
		}
		err := refreshDir.Close()
		refreshDir = nil
		return err
	}
	closeFiles := func(onlyUnkept bool) error {
		var err error
		for i := range prefix {
			file := &prefix[i]
			if file.file != nil && (!onlyUnkept || !file.kept) {
				err = errors.Join(err, file.file.Close())
				file.file = nil
			}
		}
		return err
	}

	var yieldCloseErr error
	var released atomic.Bool
	var fileIO sync.Mutex
	lease, err := proof.journalOwner.StageDetachedCleanupScan(proof.journal, func() (func(), error) {
		// Reserve a directory stream for refresh and open predecessors
		// relative to this exact directory without following symlinks or
		// blocking if a pathname was replaced with a FIFO.
		var err error
		refreshDir, err = os.Open(dir)
		if commandWALCleanupDescriptorExhausted(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		dirInfo, err = refreshDir.Stat()
		if err != nil {
			return nil, errors.Join(err, closeReader())
		}
		discard := func(err error) (func(), error) {
			closeErr := errors.Join(closeFiles(false), closeReader())
			if commandWALCleanupDescriptorExhausted(err) && closeErr == nil {
				// No frames have been scanned. Let the stock scan open
				// uncovered files one at a time.
				return nil, nil
			}
			return nil, errors.Join(err, closeErr)
		}
		for i := range prefix {
			closed := &prefix[i]
			path := closed.segment.path
			var file *os.File
			if openPrefix != nil {
				file, err = openPrefix(path)
			} else {
				file, err = rootpublication.OpenStableChildFile(refreshDir, filepath.Base(path), os.O_RDONLY, 0)
			}
			if err != nil {
				return discard(fmt.Errorf("open closed command WAL segment %s: %w", filepath.Base(path), err))
			}
			closed.file = file
		}
		// The file pointers remain immutable until FinishDetachedCleanupScan
		// even if rotation closes them during an independent Read.
		return func() {
			released.Store(true)
			// A regular file's Close can return while an in-flight Read
			// still owns the descriptor. Drain that one syscall before
			// returning descriptor capacity to rotation.
			fileIO.Lock()
			defer fileIO.Unlock()
			yieldCloseErr = refreshDir.Close()
			for i := range prefix {
				yieldCloseErr = errors.Join(yieldCloseErr, prefix[i].file.Close())
			}
		}, nil
	})
	if lease == nil || err != nil {
		return err != nil, err
	}

	var mixedFormat bool
	for i := range prefix {
		closed := &prefix[i]
		fileIO.Lock()
		if released.Load() {
			fileIO.Unlock()
			break
		}
		info, statErr := closed.file.Stat()
		if statErr != nil || !sameSplitCommandWALStreamExtent(closed.info, info) {
			fileIO.Unlock()
			scan.hardErr = errors.Join(splitCommandWALStreamChanged(closed.segment.path), statErr)
			break
		}
		identity, err := rootpublication.StableIdentityFromFile(closed.file)
		if err != nil || rootpublication.SamePhysicalIdentity(identity, proof.journal.ActiveIdentity) {
			fileIO.Unlock()
			scan.hardErr = errors.Join(splitCommandWALStreamChanged(closed.segment.path), err)
			break
		}
		fixed, err := commitlog.HasFixedWALMagic(closed.file)
		fileIO.Unlock()
		if err != nil {
			scan.hardErr = fmt.Errorf("inspect closed command WAL segment %s: %w", filepath.Base(closed.segment.path), err)
			break
		}
		closed.identity = identity
		if fixed {
			mixedFormat = true
			break
		}
	}
	scan.closedStreamBuffer = commitlog.NewDetachedStreamCleanupReadBuffer(&fileIO, &released)
	for i := 0; !mixedFormat && scan.hardErr == nil && i < len(prefix); i++ {
		closed := &prefix[i]
		if scan.beforeSegment != nil {
			scan.beforeSegment(closed.segment.path)
		}
		if released.Load() {
			break
		}
		closed.kept = scan.scanIdentifiedFile(closed.segment, closed.file, closed.info, closed.identity, false)
		if closed.kept {
			scan.decisions[len(scan.decisions)-1].validatedExtent = closed.info
		}
	}

	finish := func(current commitlog.CommandJournalCleanupSnapshot, snapshotErr error) (err error) {
		defer func() {
			if err != nil || mixedFormat || scan.hardErr != nil {
				// No retirement follows this attempt. Relinquish even covered
				// handles before rotation can start, and give finish no file
				// handles to close again.
				err = errors.Join(err, closeReader(), closeFiles(false))
				for i := range scan.decisions {
					scan.decisions[i].file = nil
				}
			}
		}()
		if snapshotErr != nil || mixedFormat || scan.hardErr != nil {
			return snapshotErr
		}
		if err := closeFiles(true); err != nil {
			return err
		}
		linkedDir, err := os.Stat(dir)
		if err != nil || !os.SameFile(dirInfo, linkedDir) {
			return errors.Join(fmt.Errorf("%w: command WAL directory changed", ErrRecoveryRequired), err)
		}
		entries, readErr := refreshDir.ReadDir(-1)
		if err := errors.Join(readErr, closeReader()); err != nil {
			return err
		}
		live := simpleCommandWALCleanupEntries(dir, entries, current)
		if len(live) != len(segments) || live[len(sealed)].path != proof.journal.ActivePath ||
			!slices.EqualFunc(sealed, live[:len(sealed)], func(a, b logSegment) bool { return a.path == b.path }) {
			return fmt.Errorf("%w: command WAL cleanup inventory changed", ErrRecoveryRequired)
		}
		for i, seg := range sealed {
			linked, err := os.Lstat(seg.path)
			if err != nil || !sameSplitCommandWALStreamExtent(prefix[i].info, linked) {
				return errors.Join(splitCommandWALStreamChanged(seg.path), err)
			}
		}
		scan.scanSegment(live[len(live)-1], true, true, &current.ActiveIdentity)
		return nil
	}
	yielded, err := proof.journalOwner.FinishDetachedCleanupScan(proof.journal, lease, finish)
	if !yielded && (!mixedFormat || err != nil) {
		return true, err
	}

	// Rotation closed every handle before allocating. Preserve independently
	// observed validation and close errors, and never use partial decisions.
	// Otherwise restart the stock scan under the original replay bounds.
	var hardErr error
	if splitCleanupNonCloseError(scan.hardErr) {
		hardErr = scan.hardErr
	}
	err = errors.Join(yieldCloseErr, hardErr)
	lineageErr := scan.lineageErr
	*scan = *newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, scan.maxSegmentBytes, scan.beforeSegment, &proof.journal)
	scan.lineageErr = lineageErr
	return err != nil, err
}

func splitCleanupNonCloseError(err error) bool {
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(group.Unwrap(), splitCleanupNonCloseError)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return splitCleanupNonCloseError(cause)
	}
	return err != nil && !errors.Is(err, os.ErrClosed)
}

func sameSplitCommandWALStreamExtent(captured, linked os.FileInfo) bool {
	return linked.Mode().IsRegular() && os.SameFile(captured, linked) && linked.Size() == captured.Size() && linked.ModTime().Equal(captured.ModTime())
}

func splitCommandWALStreamChanged(path string) error {
	return fmt.Errorf("%w: closed command WAL extent changed: %s", ErrRecoveryRequired, filepath.Base(path))
}
