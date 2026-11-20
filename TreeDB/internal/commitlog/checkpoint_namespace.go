package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The authoritative writer and parent retain their existing FDs until this
// once completes. A durable waiter can run the I/O while holding journal.mu.
// The completion and fail-stop notification therefore never acquire journal.mu.
type checkpointNamespace struct {
	finish func() error
	done   atomic.Bool
}

func checkpointParentsMatch(old, prepared *os.File) error {
	first, err := old.Stat()
	if err != nil {
		return err
	}
	second, err := prepared.Stat()
	if err != nil {
		return err
	}
	if !first.IsDir() || !os.SameFile(first, second) {
		return rootpublication.ErrResourceConflict
	}
	return nil
}

func checkpointParentCurrent(parent *os.File, successor string) error {
	current, err := os.Stat(filepath.Dir(successor))
	if err != nil {
		return err
	}
	original, err := parent.Stat()
	if err != nil {
		return err
	}
	if !current.IsDir() || !os.SameFile(original, current) {
		return rootpublication.ErrResourceConflict
	}
	return nil
}

func (j *CommandJournal) newCheckpointNamespace(parent, child *os.File) *checkpointNamespace {
	p := new(checkpointNamespace)
	w, root, walDir := j.writer, j.root, j.walDir
	validate := func() error {
		return errors.Join(checkpointParentCurrent(parent, child.Name()), checkpointChildLink(parent, child))
	}
	syncNamespace := func() error {
		if err := durabilitycut.EmitStablePath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, walDir, parent); err != nil {
			return err
		}
		if err := validate(); err != nil {
			return err
		}
		start := time.Now()
		err := rootpublication.SyncStableNamespace(parent)
		w.directorySyncCalls.Add(1)
		w.directorySyncNs.Add(uint64(time.Since(start)))
		if err != nil {
			w.directorySyncErrors.Add(1)
			return err
		}
		if err := validate(); err != nil {
			return err
		}
		return durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceCommandWAL, root, walDir, parent)
	}
	p.finish = sync.OnceValue(func() error {
		// This parent was opened afresh during rotation. Older captures have
		// their own descriptors. Only successor consumers join this obligation.
		if err := j.fileSync.err(); err != nil {
			return err
		}
		if err := syncNamespace(); err != nil {
			return j.fileSync.fail(err)
		}
		if err := j.fileSync.err(); err != nil {
			return err
		}
		p.done.Store(true)
		return nil
	})
	return p
}

func (j *CommandJournal) finishCheckpointNamespaceLocked() error {
	if j == nil || j.checkpointNamespace == nil {
		return nil
	}
	if err := j.checkpointNamespace.finish(); err != nil {
		return err
	}
	j.checkpointNamespace = nil
	return nil
}

func (j *CommandJournal) RotateAtAutomaticCOWCut() (func() error, error) {
	if j == nil {
		return nil, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.rotateActiveSegmentForCheckpointLocked(true, durabilitycut.Enabled(), true)
	p := j.checkpointNamespace
	if err != nil {
		if p != nil {
			// Failure after installation (including old-file/parent close)
			// still owes the namespace and forbids subsequent successful sync.
			err = errors.Join(err, j.finishCheckpointNamespaceLocked())
			return nil, j.fileSync.fail(err)
		}
		return nil, err
	}
	if p == nil {
		return nil, nil
	}
	return p.finish, nil
}
