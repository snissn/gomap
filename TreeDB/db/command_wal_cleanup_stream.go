package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Only the ordinary shared-volume workload selected this execution path.
// Exact journal identity, lineage, and namespace checks govern retirement.
func (db *DB) commandWALCleanupSharesValueLogVolume(snapshot commitlog.CommandJournalCleanupSnapshot) bool {
	valueDir, err := rootpublication.OpenStableParent(resolveStorageLayout(db.dir).valueVLogDir)
	if err != nil {
		return false
	}
	identity, identityErr := rootpublication.StableIdentityFromFile(valueDir)
	closeErr := valueDir.Close()
	return identityErr == nil && closeErr == nil && identity.VolumeID == snapshot.ActiveIdentity.VolumeID
}

// Existing storage layouts may point walDir at an alternate directory. Keep
// the common open directory-only, and resolve a configured final symlink before
// retrying the exact directory open. The refresh still checks the configured
// pathname against the directory this call captured.
func openStreamCleanupDirectory(path string) (*os.File, error) {
	dir, err := rootpublication.OpenStableParent(path)
	if err == nil || commandWALCleanupDescriptorExhausted(err) {
		return dir, err
	}
	info, statErr := os.Lstat(path)
	if statErr != nil || info.Mode()&os.ModeSymlink == 0 {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	return rootpublication.OpenStableParent(resolved)
}

// The existing proof capture already holds the append lock and names the active
// generation. The cleanup owner serializes deletion. Rotation releases the
// detached handles before allocating any of its own and forces a locked retry.
// Otherwise refresh the inventory and validate the one active stream under
// append exclusion.
func (db *DB) scanDetachedCommandWALCleanupPrefix(proof durableWALCleanupProofV1, scan *commandWALCleanupScan, openPrefix func(string) (*os.File, error), minClosedBytes int64) (bool, error) {
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
		refreshDir, err = openStreamCleanupDirectory(dir)
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
		if statErr != nil || !sameStreamCleanupExtent(closed.info, info) {
			fileIO.Unlock()
			scan.hardErr = errors.Join(streamCleanupExtentChanged(closed.segment.path), statErr)
			break
		}
		identity, err := rootpublication.StableIdentityFromFile(closed.file)
		if err != nil || rootpublication.SamePhysicalIdentity(identity, proof.journal.ActiveIdentity) {
			fileIO.Unlock()
			scan.hardErr = errors.Join(streamCleanupExtentChanged(closed.segment.path), err)
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
			if err != nil || !sameStreamCleanupExtent(prefix[i].info, linked) {
				return errors.Join(streamCleanupExtentChanged(seg.path), err)
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
	if cleanupHasNonCloseError(scan.hardErr) {
		hardErr = scan.hardErr
	}
	err = errors.Join(yieldCloseErr, hardErr)
	lineageErr := scan.lineageErr
	*scan = *newCommandWALCleanupScan(proof.cleanupThrough, proof.durableWALLSN, scan.maxSegmentBytes, scan.beforeSegment, &proof.journal)
	scan.lineageErr = lineageErr
	return err != nil, err
}

func cleanupHasNonCloseError(err error) bool {
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(group.Unwrap(), cleanupHasNonCloseError)
	}
	if cause := errors.Unwrap(err); cause != nil {
		return cleanupHasNonCloseError(cause)
	}
	return err != nil && !errors.Is(err, os.ErrClosed)
}

func sameStreamCleanupExtent(captured, linked os.FileInfo) bool {
	return linked.Mode().IsRegular() && os.SameFile(captured, linked) && linked.Size() == captured.Size() && linked.ModTime().Equal(captured.ModTime())
}

func streamCleanupExtentChanged(path string) error {
	return fmt.Errorf("%w: closed command WAL extent changed: %s", ErrRecoveryRequired, filepath.Base(path))
}
