package raftfsm

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

type SnapshotManifestExportOptionsV1 struct {
	CreatedAt time.Time
}

// ExportSnapshotManifestV1 builds a metadata-only snapshot manifest for the
// current durable FSM apply point. It does not copy snapshot files, install
// state, replay log tails, truncate Raft logs, rejoin nodes, or claim that the
// exported metadata can serve reads.
func (f *FSM) ExportSnapshotManifestV1(opts SnapshotManifestExportOptionsV1) (raftcluster.SnapshotManifestV1, error) {
	if f == nil {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.exportSnapshotManifestV1Locked(opts)
}

func (f *FSM) exportSnapshotManifestV1Locked(opts SnapshotManifestExportOptionsV1) (raftcluster.SnapshotManifestV1, error) {
	if f == nil {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if f.closed {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is closed")
	}
	if f.db == nil || f.progress == nil {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	record, ok, err := f.lastAppliedProgressRecord()
	if err != nil {
		return raftcluster.SnapshotManifestV1{}, err
	}
	if !ok {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM has no durable applied progress")
	}
	localLSN, err := localAppliedCommandLSN(f.db)
	if err != nil {
		return raftcluster.SnapshotManifestV1{}, err
	}
	if localLSN != record.AppliedCommandLSN {
		return raftcluster.SnapshotManifestV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "local AppliedCommandLSN coverage %d does not match snapshot progress metadata %d", localLSN, record.AppliedCommandLSN)
	}
	digest, err := f.logicalDigestV1Locked(raftapply.LogicalDigestOptionsV1{
		ScopeRule:     f.scopeRule,
		DatabaseScope: f.database,
		CatalogScope:  f.catalog,
	})
	if err != nil {
		return raftcluster.SnapshotManifestV1{}, err
	}
	createdAt := opts.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	manifest := raftcluster.SnapshotManifestV1{
		Format:            raftcluster.SnapshotManifestFormatV1,
		Version:           raftcluster.SnapshotManifestVersion1,
		GroupID:           f.cluster.GroupID,
		NodeID:            f.cluster.NodeID,
		LastIncludedTerm:  record.EntryID.Term,
		LastIncludedIndex: record.EntryID.Index,
		AppliedCommandLSN: record.AppliedCommandLSN,
		LogicalDigestV1:   digest.Hex(),
		Scope:             f.snapshotScopeIdentityV1(),
		CreatedAt:         createdAt.UTC(),
	}
	if err := manifest.Validate(manifest.Scope); err != nil {
		return raftcluster.SnapshotManifestV1{}, fmt.Errorf("raftfsm: build snapshot manifest: %w", err)
	}
	return manifest, nil
}

func (f *FSM) snapshotScopeIdentityV1() raftcluster.SnapshotScopeIdentityV1 {
	scope := f.scopeRule
	if scope == "" {
		scope = raftentry.ScopeRuleSingleGroupV1
	}
	database := f.database
	if database == "" {
		database = raftentry.DatabaseScopeDefaultV1
	}
	catalog := f.catalog
	if catalog == "" {
		catalog = raftentry.CatalogScopeDefaultV1
	}
	return raftcluster.SnapshotScopeIdentityV1{
		ScopeRule:     string(scope),
		DatabaseScope: database,
		CatalogScope:  catalog,
	}
}
