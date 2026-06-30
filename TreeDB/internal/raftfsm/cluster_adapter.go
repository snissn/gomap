package raftfsm

import (
	"context"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// ApplyCommittedCommandEntryV1 adapts the raftcluster provider-delivered
// committed command shape into the local FSM committed-entry shape.
func (f *FSM) ApplyCommittedCommandEntryV1(ctx context.Context, entry raftcluster.CommittedCommandEntryV1) (raftentry.ApplyResultV1, error) {
	return f.ApplyCommittedEntryV1(CommittedEntryFromClusterV1(entry))
}

func CommittedEntryFromClusterV1(entry raftcluster.CommittedCommandEntryV1) CommittedEntryV1 {
	return CommittedEntryV1{
		Type:                     EntryTypeCommandEntryV1,
		Term:                     entry.Term,
		Index:                    entry.Index,
		Bytes:                    append([]byte(nil), entry.Bytes...),
		CurrentCatalogVersion:    entry.CurrentCatalogVersion,
		HasCurrentCatalogVersion: entry.HasCurrentCatalogVersion,
		SyncLocalCommandWAL:      entry.SyncLocalCommandWAL,
		RequestMetadata:          cloneRequestMetadataV1(entry.RequestMetadata),
		ExpectedTarget:           cloneExpectedTargetV1(entry.ExpectedTarget),
	}
}
