package raftapply

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// Native apply keeps the frame unassigned through original OCC validation. The
// legacy append seam cannot assign for this route; the same COW owner must do so.
func (h *Harness) nativeApplyReady() error {
	if _, ok := h.walApply.(defaultCommandWALApplySeam); !ok {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: native collection apply requires the original native append owner")
	}
	return nil
}

func (h *Harness) applyNativeCreateCollectionV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1, collection collections.CollectionMeta) (raftentry.ApplyResultV1, error) {
	if err := h.nativeApplyReady(); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	result, err := h.replayCollectionManager().ApplyNativeCommandWALCreate(collection, meta.SyncLocalCommandWAL, func(uint64) error {
		return h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest)
	})
	return h.nativeCollectionApplyResult(entry, meta, result, err, true)
}

func (h *Harness) applyNativeCollectionMutationV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1, mutation collectionMutationV1) (raftentry.ApplyResultV1, error) {
	if err := h.nativeApplyReady(); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if mutation.command == nativewire.CommandUpdateBSONSet {
		result, err := h.replayCollectionManager().ApplyNativeCommandWALBSONSet(mutation.collection, mutation.bsonSetItems, meta.SyncLocalCommandWAL, func(uint64) error {
			return h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest)
		})
		return h.nativeCollectionApplyResult(entry, meta, result, err, false)
	}
	var kind commitlog.CommandKind
	switch mutation.command {
	case nativewire.CommandInsertBatch:
		kind = commitlog.CommandKindCollectionInsertBatchByID
	case nativewire.CommandReplaceBatch:
		kind = commitlog.CommandKindCollectionUpdateBatchByID
	case nativewire.CommandDeleteBatch:
		kind = commitlog.CommandKindCollectionDeleteBatchByID
	default:
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: native deterministic mutation is not ordinary document insert/replace/delete/BSON-set")
	}
	if kind != commitlog.CommandKindCollectionDeleteBatchByID {
		if mutation.documentFormat != collections.DocumentFormatJSON && mutation.documentFormat != collections.DocumentFormatBSON {
			return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorUnsupportedFeatureV1, "raftapply: native deterministic mutation requires JSON or BSON")
		}
		if err := validateMutationDocumentsV1(mutation.documentFormat, mutation.documents); err != nil {
			return raftentry.ApplyResultV1{}, err
		}
	}
	result, err := h.replayCollectionManager().ApplyNativeCommandWALMutation(mutation.collection, mutation.documentFormat, kind, mutation.ids, mutation.documents, meta.SyncLocalCommandWAL, func(uint64) error {
		return h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest)
	})
	return h.nativeCollectionApplyResult(entry, meta, result, err, false)
}

func (h *Harness) nativeCollectionApplyResult(entry raftentry.CommandEntryV1, meta ApplyMetadataV1, native collections.NativeCommandWALApplyResult, err error, create bool) (raftentry.ApplyResultV1, error) {
	publication := native.Publication
	if err != nil {
		if publication.Assigned || publication.Uncertain || publication.LogicalInstalled {
			return recoveryRequired(entry.Digest, raftentry.ErrorUnsafeDurabilityModeV1, err)
		}
		return raftentry.ApplyResultV1{}, codeCollectionApplyError(err)
	}
	if !publication.Assigned || publication.AssignedLSN == 0 || !publication.LogicalInstalled || !publication.Seal.Valid() || !publication.Seal.Sealed() || h.db.State().AppliedCommandLSN < publication.AssignedLSN {
		return recoveryRequired(entry.Digest, raftentry.ErrorUnsafeDurabilityModeV1, errors.Join(backenddb.ErrRecoveryRequired, errors.New("raftapply: native apply lacks its completed exact physical seal")))
	}
	logical, err := h.logicalDigestV1(LogicalDigestOptionsV1{ScopeRule: meta.ScopeRule, DatabaseScope: meta.DatabaseScope, CatalogScope: meta.CatalogScope})
	if err != nil {
		return recoveryRequired(entry.Digest, raftentry.ErrorUnsafeDurabilityModeV1, err)
	}
	status := raftentry.ApplyStatusApplied
	if create && native.AlreadyApplied {
		status = raftentry.ApplyStatusAlreadyApplied
	}
	return raftentry.ApplyResultV1{Status: status, CommandDigest: entry.Digest, DeterministicErrorCode: raftentry.ErrorNoneV1, AffectedCount: int64(native.Affected), MatchedCount: int64(native.Matched), ResultDigest: raftentry.CommandDigestV1(logical)}, nil
}
