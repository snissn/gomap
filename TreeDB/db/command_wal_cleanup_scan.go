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

func (db *DB) scanCommandWALCleanupProof(proof durableWALCleanupProofV1, scanSealedPrefix bool) ([]commandWALSegmentCleanupDecision, error) {
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
	if proof.journal.Ext4Stream && proof.journal.ActiveBytes <= maxSplitStreamCleanupActiveBytes &&
		db.commandWALClosedBytes.Load() >= 2*minSplitStreamCleanupSegmentBytes &&
		db.commandWALCleanupSplitValueLogVolume(proof.journal) {
		if attempted, err := db.scanSplitCommandWALCleanupPrefix(proof, scan, openPrefix); attempted {
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
// generation. The cleanup owner serializes deletion. Files strictly before that
// generation cannot be appended to, even if the writer rotates during this scan.
// Refresh the inventory under the original scan lock before reading the suffix.
func (db *DB) scanSplitCommandWALCleanupPrefix(proof durableWALCleanupProofV1, scan *commandWALCleanupScan, openPrefix func(string) (*os.File, error)) (bool, error) {
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
	infos := make([]os.FileInfo, len(sealed))
	large := 0
	for i, seg := range sealed {
		info, err := os.Lstat(seg.path)
		if err != nil || !info.Mode().IsRegular() {
			// Use the locked directory path to handle an unusual inventory.
			return false, nil
		}
		infos[i] = info
		if info.Size() >= minSplitStreamCleanupSegmentBytes {
			large++
		}
	}
	if large < 2 {
		return false, nil
	}

	files := make([]*os.File, 0, len(sealed))
	closeFiles := func() error {
		var err error
		for _, file := range files {
			err = errors.Join(err, file.Close())
		}
		return err
	}
	for i, seg := range sealed {
		file, err := openPrefix(seg.path)
		if commandWALCleanupDescriptorExhausted(err) {
			// No bytes have been scanned. Release staged descriptors before
			// the stock scanner opens one file at a time.
			if closeErr := closeFiles(); closeErr != nil {
				return true, errors.Join(err, closeErr)
			}
			return false, nil
		}
		if err != nil {
			return true, errors.Join(fmt.Errorf("open closed command WAL segment %s: %w", filepath.Base(seg.path), err), closeFiles())
		}
		files = append(files, file)
		info, statErr := file.Stat()
		identity, identityErr := rootpublication.StableIdentityFromFile(file)
		if err := errors.Join(statErr, identityErr); err != nil {
			return true, errors.Join(err, closeFiles())
		}
		if !sameSplitCommandWALStreamExtent(infos[i], info) || rootpublication.SamePhysicalIdentity(identity, proof.journal.ActiveIdentity) {
			return true, errors.Join(splitCommandWALStreamChanged(seg.path), closeFiles())
		}
		fixed, err := commitlog.HasFixedWALMagic(file)
		if err != nil {
			return true, errors.Join(err, closeFiles())
		}
		if fixed {
			if err := closeFiles(); err != nil {
				return true, err
			}
			return false, nil
		}
	}

	scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
	for i, seg := range sealed {
		file := files[i]
		// The stream's logical extent is its physical file size.
		seg.size = infos[i].Size()
		if scan.scanFile(seg, file, false, false, nil) {
			scan.decisions[len(scan.decisions)-1].validatedExtent = infos[i]
		} else {
			scan.hardErr = errors.Join(scan.hardErr, file.Close())
		}
	}

	err = proof.journalOwner.WithCleanupScanSnapshot(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) error {
		live, err := simpleCommandWALCleanupInventory(dir, current)
		if err != nil {
			return err
		}
		if len(live) <= len(sealed) || live[len(sealed)].path != proof.journal.ActivePath ||
			!slices.EqualFunc(sealed, live[:len(sealed)], func(a, b logSegment) bool { return a.path == b.path }) {
			return fmt.Errorf("%w: command WAL cleanup inventory changed", ErrRecoveryRequired)
		}
		for i, seg := range sealed {
			linked, err := os.Lstat(seg.path)
			if err != nil || !sameSplitCommandWALStreamExtent(infos[i], linked) {
				return errors.Join(splitCommandWALStreamChanged(seg.path), err)
			}
		}
		for i, seg := range live[len(sealed) : len(live)-1] {
			var expected *rootpublication.StableIdentity
			if i == 0 {
				expected = &proof.journal.ActiveIdentity
			}
			scan.scanSegment(seg, false, true, expected)
		}
		scan.scanSegment(live[len(live)-1], true, true, &current.ActiveIdentity)
		return nil
	})
	return true, err
}

func sameSplitCommandWALStreamExtent(captured, linked os.FileInfo) bool {
	return linked.Mode().IsRegular() && os.SameFile(captured, linked) && linked.Size() == captured.Size() && linked.ModTime().Equal(captured.ModTime())
}

func splitCommandWALStreamChanged(path string) error {
	return fmt.Errorf("%w: closed command WAL extent changed: %s", ErrRecoveryRequired, filepath.Base(path))
}
