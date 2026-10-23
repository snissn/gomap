package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// CapturedSync owns a complete flushed segment prefix independently of the
// writer's mutable buffers. Appends and rotations may proceed while Sync runs.
// The owner must call Release, even after a failed Sync. Close waits for release.
// Sync and Release require exclusive caller ownership. This object is
// single-use; it is not a concurrent mutable Writer.Sync.
type CapturedSync struct {
	journal            *CommandJournal
	writer             *Writer // Only atomic I/O counters are accessed after capture.
	active             *rootpublication.StableResourceToken
	rotated            []*rootpublication.StableResourceToken
	root, path, walDir string
	LSN                uint64
	Bytes              uint64
	once               sync.Once
}

// CaptureSync flushes under append authority and pins the exact active handle
// and every earlier rotation dependency before relinquishing that authority.
func (j *CommandJournal) CaptureSync(rotated []*rootpublication.StableResourceToken) (_ *CapturedSync, err error) {
	if j == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.stableRotationFailStopErrorLocked(); err != nil {
		return nil, err
	}
	if j.writer == nil {
		return nil, errors.New("commitlog: command journal is closed")
	}
	if err := j.flushDirectCommandWALLocked(false, durabilitycut.Enabled()); err != nil {
		return nil, err
	}
	target := &CapturedSync{journal: j, writer: j.writer, root: filepath.Dir(j.walDir), path: j.path, walDir: j.walDir, LSN: j.activeSegmentMaxLSN, Bytes: uint64(j.writer.Size())}
	defer func() {
		if err != nil {
			target.releaseResources()
		}
	}()
	target.active, err = j.stableSegmentToken(j.writer.f, j.path, j.segmentSeq, rootpublication.ReachabilityCommandWALActive, rootpublication.DurableFrontier{Bytes: target.Bytes, MaxLSN: target.LSN}, nil, false)
	if err != nil {
		return nil, err
	}
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
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, target.path); err != nil {
		return err
	}
	started := time.Now()
	err := target.active.SyncThrough()
	target.writer.fileSyncCalls.Add(1)
	target.writer.fileSyncNs.Add(uint64(time.Since(started)))
	if err != nil {
		target.writer.fileSyncErrors.Add(1)
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, target.root, target.path)
}

func (target *CapturedSync) releaseResources() {
	target.active.Release()
	for _, token := range target.rotated {
		token.Release()
	}
}

func (target *CapturedSync) Release() {
	if target == nil {
		return
	}
	target.once.Do(func() {
		target.releaseResources()
		j := target.journal
		j.mu.Lock()
		j.syncPins--
		j.syncCond.Broadcast()
		j.mu.Unlock()
	})
}
