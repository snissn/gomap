package db

import (
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// StageCompositeRawReplay uses the existing raw replay materializer and the
// original physical batch. Selected composite frames carry only inline Sets.
func (db *DB) StageCompositeRawReplay(intent *CommandWALIntent, b *Batch, payload []byte) error {
	if err := db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	if b == nil || b.db != db || !b.physicalOnly {
		return ErrCommandWALRejected
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(payload)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.Op != commitlog.RawKVOpSet {
			return ErrNativeSemanticUnsupported
		}
	}
	return db.stageRawKVReplayBatch(b, commitlog.CommandEnvelope{PayloadFormat: commitlog.PayloadFormatRawKVBatchV1}, ops, nil, nil, nil, nil)
}

// AttachCompositeReplayGroup preserves the DB-minted callback-scoped replay
// token through the final group's single publication. No new append occurs.
func (b *Batch) AttachCompositeReplayGroup(intent *CommandWALIntent, group *RootPublicationBuildGroup) error {
	return b.attachNativeReplayGroup(intent, group, commitlog.CommandKindCompositeMutation)
}

// AttachCatalogIndexReplayGroup uses the same original callback-scoped replay
// authority for one logical catalog index transition and its complete roots.
func (b *Batch) AttachCatalogIndexReplayGroup(intent *CommandWALIntent, group *RootPublicationBuildGroup) error {
	return b.attachNativeReplayGroup(intent, group, commitlog.CommandKindCatalogIndexMutation)
}

// AttachVectorGraphReplayGroup preserves the original assigned command103 authority.
func (b *Batch) AttachVectorGraphReplayGroup(intent *CommandWALIntent, group *RootPublicationBuildGroup) error {
	return b.attachNativeReplayGroup(intent, group, commitlog.CommandKindCollectionRebuildVectorIndex)
}

func (b *Batch) attachNativeReplayGroup(intent *CommandWALIntent, group *RootPublicationBuildGroup, kind commitlog.CommandKind) error {
	if b == nil || b.db == nil {
		return ErrClosed
	}
	if err := b.db.ValidateCommandWALReplayOperationV1(intent); err != nil {
		return err
	}
	if intent.inner.kind != kind {
		return ErrCommandWALRejected
	}
	if err := b.SetRootPublicationBuildGroup(group, true); err != nil {
		return err
	}
	b.commandWALPublishIntent = &intent.inner
	return nil
}
