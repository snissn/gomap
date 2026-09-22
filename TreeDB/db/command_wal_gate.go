package db

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
)

var ErrCommandWALDirtyActivation = errors.New("treedb: command_wal_v2 requires clean legacy WAL before activation")

// ValidateCommandWALActivationClean enforces the activation precondition:
// command_wal_v2 can only be advertised after legacy commit-log debt has been
// drained by checkpoint/rebuild, so tooling cannot silently enable mixed modes.
func ValidateCommandWALActivationClean(dir string) error {
	if err := collectionwal.RequireCleanForOfflineMaintenance(dir); err != nil {
		return fmt.Errorf("%w: legacy collection WAL: %w", ErrCommandWALDirtyActivation, err)
	}
	segments, err := listWALSegments(dir)
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if seg.size == 0 {
			continue
		}
		return fmt.Errorf("%w: found %s", ErrCommandWALDirtyActivation, filepath.Base(seg.path))
	}
	return nil
}
