//go:build treedb_test

package treedb

import backenddb "github.com/snissn/gomap/TreeDB/db"

// NativeMembershipSnapshotForTest retains the ordinary backend generation for
// the producer-incorporation diagnostic. It is not a composite MVCC read cut.
func (d *DB) NativeMembershipSnapshotForTest() *backenddb.Snapshot {
	return d.backend.AcquireSnapshot()
}
