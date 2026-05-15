package db

import (
	"fmt"
	"path/filepath"
)

// ValidateCommandWALActivationClean enforces the PR1 activation precondition:
// command_wal_v1 can only be advertised after legacy commit-log debt has been
// drained by checkpoint/rebuild. Later PRs will add the activator; this guard is
// production code now so tests and tooling cannot silently enable mixed modes.
func ValidateCommandWALActivationClean(dir string) error {
	segments, err := listWALSegments(dir)
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if seg.valueLog || seg.size == 0 {
			continue
		}
		return fmt.Errorf("treedb: command_wal_v1 requires clean legacy WAL before activation; found %s", filepath.Base(seg.path))
	}
	return nil
}
