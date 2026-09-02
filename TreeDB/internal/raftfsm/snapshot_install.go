package raftfsm

import (
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// VerifyInstalledSnapshotManifestV1 verifies that this FSM already contains
// the exact logical state described by manifest. It does not copy snapshot
// bytes, truncate logs, or make the node safe to serve reads.
func (f *FSM) VerifyInstalledSnapshotManifestV1(manifest raftcluster.SnapshotManifestV1) error {
	if f == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.verifyInstalledSnapshotManifestV1Locked(manifest)
}

func (f *FSM) verifyInstalledSnapshotManifestV1Locked(manifest raftcluster.SnapshotManifestV1) error {
	if f == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if f.closed {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	if f.db == nil || f.progress == nil {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if err := manifest.Validate(f.snapshotScopeIdentityV1()); err != nil {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest is not valid for FSM scope: %v", err)
	}
	if manifest.GroupID != f.cluster.GroupID {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest group %q does not match FSM group %q", manifest.GroupID, f.cluster.GroupID)
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return err
	}
	if !ok {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM has no durable applied progress for installed snapshot")
	}
	if record.EntryID.Term != manifest.LastIncludedTerm || record.EntryID.Index != manifest.LastIncludedIndex {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest last included %d/%d does not match durable progress %d/%d", manifest.LastIncludedTerm, manifest.LastIncludedIndex, record.EntryID.Term, record.EntryID.Index)
	}
	if record.AppliedCommandLSN != manifest.AppliedCommandLSN {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest AppliedCommandLSN %d does not match durable progress %d", manifest.AppliedCommandLSN, record.AppliedCommandLSN)
	}
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return err
	}
	if localLSN != manifest.AppliedCommandLSN {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest AppliedCommandLSN %d does not match local coverage %d", manifest.AppliedCommandLSN, localLSN)
	}
	digest, err := f.logicalDigestV1Locked(raftapply.LogicalDigestOptionsV1{
		ScopeRule:     f.scopeRule,
		DatabaseScope: f.database,
		CatalogScope:  f.catalog,
	})
	if err != nil {
		return err
	}
	if digest.Hex() != manifest.LogicalDigestV1 {
		return codedError(raftentry.ErrorRejectedConflictV1, "snapshot manifest logical digest %s does not match FSM digest %s", manifest.LogicalDigestV1, digest.Hex())
	}
	return nil
}
