package raftapply

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// applySplitVectorInsertV1 uses the same append/publication/finalize/result
// boundary as ordinary deterministic collection mutations. Actual fixed group
// and semantic consensus positions come from the applying FSM, not the payload.
func (h *Harness) applySplitVectorInsertV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	guard, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if err := checkCatalogVersionGuardV1(meta, guard); err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	raw, err := requiredDeterministicSectionV1(entry, nativewire.SectionSplitVectorInsertV1, "split_vector_insert")
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	v, err := commitlog.DecodeSplitVectorInsertPayloadV1(raw)
	if err != nil {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: split vector insert: %v", err)
	}
	collectionName, err := lowerCollectionNameV1(entry)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	if collectionName != v.Collection {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorTargetMismatchV1, "raftapply: split insert collection target mismatch")
	}
	expectedGroup := v.SourceGroup
	if v.Operation == "project" {
		expectedGroup = v.TargetGroup
	}
	if meta.GroupID == "" || meta.GroupID != expectedGroup {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorTargetMismatchV1, "raftapply: split insert actual FSM group mismatch")
	}
	switch v.Operation {
	case "source":
		if v.SourceTerm != 0 || v.SourceIndex != 0 {
			return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: source positions must be assigned by FSM")
		}
		v.SourceTerm, v.SourceIndex = meta.EntryID.Term, meta.EntryID.Index
	case "project":
		if v.TargetTerm != 0 || v.TargetIndex != 0 {
			return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: target positions must be assigned by FSM")
		}
		v.TargetTerm, v.TargetIndex = meta.EntryID.Term, meta.EntryID.Index
	}
	collection, err := h.replayCollectionManager().OpenCollection(v.Collection)
	if err != nil {
		return raftentry.ApplyResultV1{}, codeCollectionApplyError(err)
	}
	if err := collection.PreflightVectorPartitionSplitInsertV1(context.Background(), v); err != nil {
		return raftentry.ApplyResultV1{}, codeCollectionApplyError(err)
	}
	payload, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: split WAL payload: %v", err)
	}
	frame := commandwalapply.LoweredFrame{Class: commandwalapply.LoweredFrameClassCollectionSplitVectorInsertV1, Kind: commitlog.CommandKindCollectionSplitVectorInsertV1, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionSplitVectorInsertV1, Payload: payload}
	handle, _, err := h.walApply.Append(h.db, frame, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL})
	if err != nil {
		return raftentry.ApplyResultV1{}, codeCommandWALApplyError(err)
	}
	finalized := false
	defer func() {
		if !finalized {
			h.walApply.Abort(h.db, handle)
		}
	}()
	if err := h.injectFault(FaultAfterLocalWALAppendBeforeVisibleV1, meta.EntryID, entry.Digest); err != nil {
		return commandWALPostAppendRecoveryRequired(entry, err)
	}
	intent := handle.CommandWALIntent()
	if intent == nil || handle.LSN() == 0 {
		return commandWALPostAppendRecoveryRequired(entry, errors.New("split WAL intent unavailable"))
	}
	switch v.Operation {
	case "source":
		err = collection.InsertVectorPartitionSplitSourceWithCommandWALIntentV1(context.Background(), v, intent)
	case "project":
		_, err = collection.ProjectVectorPartitionSplitInsertWithCommandWALIntentV1(context.Background(), v, v.TargetTerm, v.TargetIndex, intent)
	case "clear":
		err = collection.CompleteVectorPartitionSplitInsertWithCommandWALIntentV1(context.Background(), v, intent)
	}
	if err != nil {
		return h.collectionMutationApplyError(entry, handle, err)
	}
	if _, err := h.walApply.Finalize(h.db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL}); err != nil {
		return commandWALFinalizeRecoveryRequired(entry, err)
	}
	finalized = true
	logical, err := h.logicalDigestV1(LogicalDigestOptionsV1{ScopeRule: meta.ScopeRule, DatabaseScope: meta.DatabaseScope, CatalogScope: meta.CatalogScope})
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	affected := int64(0)
	if v.Operation == "source" {
		affected = 1
	}
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, CommandDigest: entry.Digest, DeterministicErrorCode: raftentry.ErrorNoneV1, AffectedCount: affected, ResultDigest: raftentry.CommandDigestV1(logical)}, nil
}
