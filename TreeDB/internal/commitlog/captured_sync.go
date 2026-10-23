package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

// One exact handle per active segment is shared by captures. Its reference
// count is protected by journal.mu; each capture retains it through Sync.
// Frontier authority belongs to the immutable capture, not this handle owner.
type capturedJournalFile struct {
	token *rootpublication.StableResourceToken
	seq   uint64
	refs  int
}

// CapturedSync owns a complete flushed segment prefix independently of the
// writer's mutable buffers. Appends and rotations may proceed while Sync runs.
// The owner must call Release, even after a failed Sync. Close waits for release.
// Sync and Release require exclusive caller ownership. This object is
// single-use; it is not a concurrent mutable Writer.Sync.
type CapturedSync struct {
	journal            *CommandJournal
	writer             *Writer // Only atomic I/O counters are accessed after capture.
	active             *capturedJournalFile
	rotated            []*rootpublication.StableResourceToken
	root, path, walDir string
	LSN                uint64
	Bytes              uint64
	once               sync.Once
}

// CaptureSync flushes under append authority and pins the exact active handle
// and every earlier rotation dependency before relinquishing that authority.
func (j *CommandJournal) CaptureSync(rotated []*rootpublication.StableResourceToken) (_ *CapturedSync, err error) {
	return j.CaptureSyncInto(new(CapturedSync), rotated)
}

// CaptureSyncInto initializes unused caller-owned storage. A target may not be
// reused: its captured identity/frontier must remain immutable through release.
func (j *CommandJournal) CaptureSyncInto(target *CapturedSync, rotated []*rootpublication.StableResourceToken) (_ *CapturedSync, err error) {
	if j == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return nil, err
	}
	if j.closing || j.writer == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	if target == nil || target.journal != nil {
		return nil, errors.New("commitlog: capture target must be unused")
	}
	if err := j.flushDirectCommandWALLocked(false, durabilitycut.Enabled()); err != nil {
		return nil, err
	}
	*target = CapturedSync{journal: j, writer: j.writer, root: filepath.Dir(j.walDir), path: j.path, walDir: j.walDir, LSN: j.activeSegmentMaxLSN, Bytes: uint64(j.writer.Size())}
	defer func() {
		if err != nil {
			for _, token := range target.rotated {
				token.Release()
			}
			if target.active != nil {
				j.releaseCapturedFileLocked(target.active)
			}
		}
	}()
	if j.capturedFile == nil || j.capturedFile.seq != j.segmentSeq {
		token, tokenErr := j.stableSegmentToken(j.writer.f, j.path, j.segmentSeq, rootpublication.ReachabilityCommandWALActive, rootpublication.DurableFrontier{}, nil, false)
		if tokenErr != nil {
			return nil, tokenErr
		}
		if j.capturedFile != nil {
			j.releaseCapturedFileLocked(j.capturedFile)
		}
		j.capturedFile = &capturedJournalFile{token: token, seq: j.segmentSeq, refs: 1}
	}
	target.active = j.capturedFile
	target.active.refs++
	for _, original := range rotated {
		var pinned *rootpublication.StableResourceToken
		err = original.WithPinnedFile(func(file *os.File) error {
			var pinErr error
			pinned, pinErr = j.stableSegmentToken(file, original.DiagnosticPath(), original.Generation(), original.Reachability(), original.Frontier(), original.Namespace(), false)
			return pinErr
		})
		if err != nil {
			return nil, err
		}
		target.rotated = append(target.rotated, pinned)
	}
	if j.syncCond == nil {
		j.syncCond = sync.NewCond(&j.mu)
	}
	j.syncPins++
	return target, nil
}

// Sync completes only the captured physical prefix. Bytes appended after the
// capture may become durable incidentally; that does not extend its LSN.
func (target *CapturedSync) Sync() error {
	for _, token := range target.rotated {
		path := filepath.Join(target.walDir, filepath.Base(token.DiagnosticPath()))
		if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, path); err != nil {
			return err
		}
		if err := token.SyncThrough(); err != nil {
			return err
		}
		if err := durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, path); err != nil {
			return err
		}
	}
	// Namespace tokens retain their exact parent handles across rotation/Close.
	for _, token := range target.rotated {
		if namespace := token.Namespace(); namespace != nil {
			if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, target.root, target.walDir); err != nil {
				return err
			}
			if err := rootpublication.StabilizeStableNamespaceTokens(namespace); err != nil {
				return err
			}
			if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, target.root, target.walDir); err != nil {
				return err
			}
		}
	}
	if err := durabilitycut.EmitPathLSN(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, target.path, target.LSN); err != nil {
		return err
	}
	started := time.Now()
	// Always perform a physical sync. The shared handle's zero frontier is only
	// a retention pin and must never cache a later capture's durability result.
	err := target.active.token.WithPinnedFile(func(file *os.File) error { return stableio.SyncFile(file) })
	target.writer.fileSyncCalls.Add(1)
	target.writer.fileSyncNs.Add(uint64(time.Since(started)))
	if err != nil {
		target.writer.fileSyncErrors.Add(1)
		return err
	}
	return durabilitycut.EmitPathLSN(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, target.path, target.LSN)
}

func (j *CommandJournal) releaseCapturedFileLocked(file *capturedJournalFile) {
	file.refs--
	if file.refs == 0 {
		file.token.Release()
	}
}

func (target *CapturedSync) Release() {
	if target == nil {
		return
	}
	target.once.Do(func() {
		for _, token := range target.rotated {
			token.Release()
		}
		j := target.journal
		j.mu.Lock()
		j.releaseCapturedFileLocked(target.active)
		j.syncPins--
		j.syncCond.Broadcast()
		j.mu.Unlock()
	})
}
