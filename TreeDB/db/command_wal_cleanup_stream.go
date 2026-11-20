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

// Detach stream validation only for a WAL on its own ext4 volume. Inspect
// the current pager descriptor and resolved storage directories so separate
// value or leaf mounts do not select this path.
func (db *DB) commandWALCleanupHasDedicatedVolume(snapshot commitlog.CommandJournalCleanupSnapshot) (bool, error) {
	if !snapshot.Ext4Stream {
		return false, nil
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return false, nil
	}
	var indexIdentity rootpublication.StableIdentity
	if err := idx.pager.WithStableResourceFile(func(file *os.File) error {
		var err error
		indexIdentity, err = rootpublication.StableIdentityFromFile(file)
		return err
	}); err != nil || indexIdentity.Platform != snapshot.ActiveIdentity.Platform ||
		indexIdentity.VolumeID == snapshot.ActiveIdentity.VolumeID {
		return false, nil
	}
	if idx.primary != nil {
		var primaryIdentity rootpublication.StableIdentity
		err := idx.primary.Pager().WithStableResourceFile(func(file *os.File) error {
			var err error
			primaryIdentity, err = rootpublication.StableIdentityFromFile(file)
			return err
		})
		if err != nil || primaryIdentity.Platform != indexIdentity.Platform || primaryIdentity.VolumeID != indexIdentity.VolumeID {
			return false, nil
		}
	}
	layout := resolveStorageLayout(db.dir)
	for _, path := range [...]string{layout.rootDir, layout.valueVLogDir, layout.leafVLogDir} {
		dir, err := openStreamCleanupDirectory(path)
		if err != nil {
			return false, nil
		}
		identity, identityErr := rootpublication.StableIdentityFromFile(dir)
		if err := dir.Close(); err != nil {
			return false, errors.Join(identityErr, err)
		}
		if identityErr != nil || identity.Platform != indexIdentity.Platform || identity.VolumeID != indexIdentity.VolumeID {
			return false, nil
		}
	}
	return true, nil
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
// Capture the bounded visible active bytes under append exclusion, then
// validate sealed and copied active bytes in the original order off-lock.
func (db *DB) scanDetachedCommandWALCleanupPrefix(proof durableWALCleanupProofV1, scan *commandWALCleanupScan, openPrefix func(string) (*os.File, error), minClosedBytes int64) (bool, error) {
	dir := resolveStorageLayout(db.dir).walDir
	segments, err := simpleCommandWALCleanupInventory(dir, proof.journal)
	if streamCleanupOnlyDescriptorPressure(err) {
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
	remainingClosedBytes := minClosedBytes
	for i, seg := range sealed {
		info, err := os.Lstat(seg.path)
		if err != nil || !info.Mode().IsRegular() {
			// Use the locked directory path to handle an unusual inventory.
			return false, nil
		}
		seg.size = info.Size()
		prefix[i] = cleanupClosedWALFile{segment: seg, info: info}
		remainingClosedBytes -= min(remainingClosedBytes, info.Size())
	}
	if remainingClosedBytes != 0 {
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
	var stageCut commitlog.CommandJournalCleanupSnapshot
	var stageActiveInfo os.FileInfo
	var activeBytes []byte
	lease, err := proof.journalOwner.StageDetachedCleanupScan(proof.journal, func(current commitlog.CommandJournalCleanupSnapshot) (func(), error) {
		if !current.Ext4Stream {
			return nil, nil
		}
		if current.ActiveBytes > maxDetachedStreamCleanupActiveBytes {
			return nil, nil
		}
		// Reserve a directory stream for refresh and open predecessors
		// relative to this exact directory without following symlinks or
		// blocking if a pathname was replaced with a FIFO.
		var err error
		refreshDir, err = openStreamCleanupDirectory(dir)
		if streamCleanupOnlyDescriptorPressure(err) {
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
			return nil, errors.Join(err, closeFiles(false), closeReader())
		}
		discardOpen := func(err error) (func(), error) {
			if streamCleanupOnlyDescriptorPressure(err) {
				// No frames have been scanned. Release acquired handles
				// and let the locked scan open files one at a time.
				return discard(nil)
			}
			return discard(err)
		}
		active := segments[len(sealed)]
		activeFile, err := rootpublication.OpenStableChildFile(refreshDir, filepath.Base(active.path), os.O_RDONLY, 0)
		if err != nil {
			return discardOpen(err)
		}
		info, statErr := activeFile.Stat()
		identity, identityErr := rootpublication.StableIdentityFromFile(activeFile)
		if err := errors.Join(statErr, identityErr); err != nil {
			return discard(errors.Join(err, activeFile.Close()))
		}
		if !info.Mode().IsRegular() || !rootpublication.SamePhysicalIdentity(identity, current.ActiveIdentity) {
			return discard(errors.Join(ErrRecoveryRequired, activeFile.Close()))
		}
		if info.Size() > maxDetachedStreamCleanupActiveBytes {
			if err := errors.Join(activeFile.Close(), closeFiles(false), closeReader()); err != nil {
				return nil, err
			}
			return nil, nil
		}
		logical, sizeErr := commitlog.LogicalFileSize(activeFile)
		if sizeErr != nil {
			return discard(errors.Join(sizeErr, activeFile.Close()))
		}
		if logical != info.Size() || logical > current.ActiveBytes {
			return discard(errors.Join(ErrRecoveryRequired, activeFile.Close()))
		}

		if logical < current.ActiveBytes {
			if err := errors.Join(activeFile.Close(), closeReader()); err != nil {
				return nil, err
			}
			return nil, nil
		}
		activeBytes = make([]byte, logical)
		_, readErr := activeFile.ReadAt(activeBytes, 0)
		if err := errors.Join(readErr, activeFile.Close()); err != nil {
			return discard(err)
		}
		stageCut, stageActiveInfo = current, info
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
				return discardOpen(fmt.Errorf("open closed command WAL segment %s: %w", filepath.Base(path), err))
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

	var canonicalFallback bool
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
			canonicalFallback = true
			break
		}
	}
	scan.closedStreamBuffer = commitlog.NewDetachedStreamCleanupReadBuffer(&fileIO, &released)
	for i := 0; !canonicalFallback && scan.hardErr == nil && i < len(prefix); i++ {
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

	if !canonicalFallback && scan.hardErr == nil && !released.Load() {
		active := segments[len(sealed)]
		active.size = int64(len(activeBytes))
		if scan.beforeSegment != nil {
			scan.beforeSegment(active.path)
		}
		reader := commitlog.NewReaderFromCapturedStream(activeBytes, commitlog.Options{MaxSegmentSize: scan.maxSegmentBytes})
		result, readErr := scanCommandWALSegmentReader(reader, true, scan.options)
		readErr = errors.Join(readErr, reader.Close())
		scan.recordSegmentScan(active, nil, stageActiveInfo, stageCut.ActiveIdentity, true, result, readErr)
		if readErr == nil &&
			(result.scannedBytes != active.size || result.maxLSN != stageCut.ActiveSegmentMaxLSN || result.terminalTail) {
			// The captured bytes cannot supply a complete stream proof.
			// Close pins and let canonical validation apply its existing policy.
			canonicalFallback = true
		}
	}

	finish := func(current commitlog.CommandJournalCleanupSnapshot, snapshotErr error) (err error) {
		defer func() {
			if err != nil || canonicalFallback || scan.hardErr != nil {
				// No retirement follows this attempt. Relinquish even covered
				// handles before rotation can start, and give finish no file
				// handles to close again.
				// An independent Close error cannot make a stale cut
				// look like a pure retry to CleanupCommandWALCoveredPrefix.
				scan.hardErr = errors.Join(scan.hardErr, closeReader(), closeFiles(false))
				for i := range scan.decisions {
					scan.decisions[i].file = nil
				}
			}
		}()
		if snapshotErr != nil || canonicalFallback || scan.hardErr != nil {
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
		linkedActive, err := os.Lstat(stageCut.ActivePath)
		if err != nil || !linkedActive.Mode().IsRegular() || !os.SameFile(stageActiveInfo, linkedActive) || linkedActive.Size() < stageActiveInfo.Size() {
			return errors.Join(fmt.Errorf("%w: active command WAL stream changed", ErrRecoveryRequired), err)
		}
		for i, seg := range sealed {
			linked, err := os.Lstat(seg.path)
			if err != nil || !sameStreamCleanupExtent(prefix[i].info, linked) {
				return errors.Join(streamCleanupExtentChanged(seg.path), err)
			}
		}
		return nil
	}
	yielded, err := proof.journalOwner.FinishDetachedCleanupScan(stageCut, lease, finish)
	if !yielded && (!canonicalFallback || err != nil || scan.hardErr != nil) {
		return true, err
	}

	// The journal closed every handle before allocating. Preserve independently
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

func streamCleanupOnlyDescriptorPressure(err error) bool {
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		causes := group.Unwrap()
		return len(causes) != 0 && !slices.ContainsFunc(causes, func(cause error) bool {
			return !streamCleanupOnlyDescriptorPressure(cause)
		})
	}
	if cause := errors.Unwrap(err); cause != nil {
		return streamCleanupOnlyDescriptorPressure(cause)
	}
	return commandWALCleanupDescriptorExhausted(err)
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
