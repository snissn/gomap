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
		if !current.FixedContainer || current.SegmentSeq <= 2 {
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
		segments, err := simpleCommandWALCleanupInventory(dir, current)
		if err != nil {
			return err
		}
		if !current.FixedContainer || len(segments) <= len(prefix) || !slices.EqualFunc(prefix, segments[:len(prefix)], func(a cleanupClosedWALFile, b logSegment) bool {
			return a.segment.path == b.path
		}) {
			// An unsupported inventory gets a single locked full scan. Keep
			// the same captured root bounds. There is no retry loop.
			if err := errors.Join(closePrefix(), closeCommandWALCleanupDecisions(scan.decisions)); err != nil {
				return err
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
