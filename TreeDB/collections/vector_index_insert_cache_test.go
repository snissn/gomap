package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func currentDBSystemRootForTest4243(d *backenddb.DB) uint64 {
	snap := d.AcquireSnapshot()
	if snap == nil {
		return 0
	}
	defer func() { _ = snap.Close() }()
	return snapshotSystemRoot(snap)
}
