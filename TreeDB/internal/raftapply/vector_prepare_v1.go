package raftapply

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// The applying FSM owns all semantic positions; clients supply only the
// frozen source tuple and fixed single-group layout.
func (h *Harness) preflightVectorPrepareV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (commitlog.VectorPrepareV1, *collections.Collection, error) {
	guard, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return commitlog.VectorPrepareV1{}, nil, err
	}
	if err := checkCatalogVersionGuardV1(meta, guard); err != nil {
		return commitlog.VectorPrepareV1{}, nil, err
	}
	raw, err := requiredDeterministicSectionV1(entry, nativewire.SectionVectorPrepareV1, "vector_prepare")
	if err != nil {
		return commitlog.VectorPrepareV1{}, nil, err
	}
	v, err := commitlog.DecodeVectorPreparePayloadV1(raw)
	if err != nil {
		return v, nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: vector prepare: %v", err)
	}
	name, err := lowerCollectionNameV1(entry)
	if err != nil {
		return v, nil, err
	}
	if name != v.Collection || meta.GroupID == "" || meta.GroupID != v.Group {
		return v, nil, codedError(raftentry.ErrorTargetMismatchV1, "raftapply: vector prepare actual collection/FSM group mismatch")
	}
	if v.Term != 0 || v.IndexPosition != 0 || v.CommandDigest != "" {
		return v, nil, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: vector prepare positions must be assigned by FSM")
	}
	c, err := h.replayCollectionManager().OpenCollection(name)
	if err != nil {
		return v, nil, codeCollectionApplyError(err)
	}
	if err := h.withVectorPrepareOwnerV1(c, v, func(*collections.CommandWALAdmittedCollection) error { return nil }); err != nil {
		return v, nil, codeCollectionApplyError(err)
	}
	return v, c, nil
}

func (h *Harness) applyVectorPrepareV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	v, c, err := h.preflightVectorPrepareV1(entry, meta)
	if err != nil {
		return raftentry.ApplyResultV1{}, err
	}
	v.Term, v.IndexPosition, v.CommandDigest = meta.EntryID.Term, meta.EntryID.Index, entry.Digest.Hex()
	var result raftentry.ApplyResultV1
	err = h.withVectorPrepareOwnerV1(c, v, func(owner *collections.CommandWALAdmittedCollection) error {
		var applyErr error
		result, applyErr = func() (raftentry.ApplyResultV1, error) {
			payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
			if err != nil {
				return raftentry.ApplyResultV1{}, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: vector prepare WAL payload: %v", err)
			}
			frame := commandwalapply.LoweredFrame{Class: commandwalapply.LoweredFrameClassCollectionVectorPrepareV1, Kind: commitlog.CommandKindCollectionVectorPrepareV1, Scope: commitlog.CommandScopeCollection, PayloadFormat: commitlog.PayloadFormatCollectionVectorPrepareV1, Payload: payload}
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
			capture, err := handle.BorrowStableResourceCaptureLeaseV1()
			if err != nil {
				return commandWALPostAppendRecoveryRequired(entry, err)
			}
			defer capture.Release()
			if err := owner.SetVectorPrepareCaptureLeaseV1(capture); err != nil {
				return commandWALPostAppendRecoveryRequired(entry, err)
			}
			intent := handle.CommandWALIntent()
			if intent == nil || handle.LSN() == 0 {
				return commandWALPostAppendRecoveryRequired(entry, errors.New("vector prepare WAL intent unavailable"))
			}
			if err := owner.ApplyVectorPrepareWithCommandWALIntentV1(context.Background(), v, intent, false); err != nil {
				return h.collectionMutationApplyError(entry, handle, err)
			}
			if _, err := h.walApply.Finalize(h.db, handle, commandwalapply.ApplyMetadata{}, commandwalapply.Options{Sync: meta.SyncLocalCommandWAL}); err != nil {
				return commandWALFinalizeRecoveryRequired(entry, err)
			}
			finalized = true
			return raftentry.ApplyResultV1{}, nil
		}()
		return applyErr
	})
	if err != nil {
		if _, coded := ErrorCodeOf(err); !coded {
			err = codeCollectionApplyError(err)
		}
		return result, err
	}
	logical, err := h.logicalDigestV1(LogicalDigestOptionsV1{ScopeRule: meta.ScopeRule, DatabaseScope: meta.DatabaseScope, CatalogScope: meta.CatalogScope})
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusApplied, CommandDigest: entry.Digest, DeterministicErrorCode: raftentry.ErrorNoneV1, AffectedCount: 1, ResultDigest: raftentry.CommandDigestV1(logical)}, nil
}

func (h *Harness) withVectorPrepareOwnerV1(c *collections.Collection, v commitlog.VectorPrepareV1, apply func(*collections.CommandWALAdmittedCollection) error) error {
	if storage := h.opts.VectorPrepareStorageOwner; storage != nil {
		return c.WithPreparedCommandWALVectorPrepareOwnedV1(context.Background(), v, storage, apply)
	}
	return c.WithPreparedCommandWALVectorPrepareV1(context.Background(), v, apply)
}
