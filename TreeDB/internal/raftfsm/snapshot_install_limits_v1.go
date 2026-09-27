package raftfsm

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The caller's reader can block inside Read. Cancellation takes effect between
// reads; the cleanup carrier retains scratch/admission until the actual return.
type snapshotInstallContextReaderV1 struct {
	ctx    context.Context
	reader io.Reader
}

func (r snapshotInstallContextReaderV1) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *raftSnapshotScratchDirsV1) close() error {
	var err error
	for _, target := range []*string{&s.main, &s.side, &s.apply} {
		if *target == "" {
			continue
		}
		var cleanupErr error
		if raftSnapshotBeforeCleanupForTest != nil {
			cleanupErr = raftSnapshotBeforeCleanupForTest(*target)
		}
		if cleanupErr == nil {
			cleanupErr = os.RemoveAll(*target)
		}
		if cleanupErr == nil {
			// Keep the path until its parent sync succeeds, even if unlink has
			// already finished. A cleanup retry must not discard that debt.
			parent, openErr := rootpublication.OpenStableParent(filepath.Dir(*target))
			cleanupErr = openErr
			if openErr == nil {
				cleanupErr = errors.Join(rootpublication.SyncStableNamespace(parent), parent.Close())
			}
		}
		if cleanupErr == nil {
			*target = ""
		} else {
			err = errors.Join(err, cleanupErr)
		}
	}
	return err
}
