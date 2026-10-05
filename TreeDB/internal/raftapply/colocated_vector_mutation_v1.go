package raftapply

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// Scope has no publication authority. The applying FSM supplies group and
// term/index; the prepared source owner supplies original operation counts.
func colocatedVectorMutationInputsV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (commitlog.ColocatedVectorMutationWALV1, bool, error) {
	var v commitlog.ColocatedVectorMutationWALV1
	var raw []byte
	for _, section := range entry.Decoded.Sections {
		if section.ID == nativewire.SectionColocatedVectorMutationScopeV1 {
			raw = section.Bytes
		}
	}
	if raw == nil {
		return v, false, nil
	}
	scope, err := commitlog.DecodeColocatedVectorMutationScopeV1(raw)
	if err != nil {
		return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated scope: %v", err)
	}
	if meta.GroupID == "" || meta.GroupID != scope.OwnerGroup {
		return v, true, codedError(raftentry.ErrorTargetMismatchV1, "raftapply: colocated actual FSM group mismatch")
	}
	command := entry.Decoded.CommandID
	if command != nativewire.CommandReplaceBatch && command != nativewire.CommandDeleteBatch {
		return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated exact-ID replace/delete required")
	}
	collection, err := lowerCollectionNameV1(entry)
	if err != nil {
		return v, true, err
	}
	rawIDs, err := requiredDeterministicSectionV1(entry, nativewire.SectionDocumentIDs, "ids")
	if err != nil {
		return v, true, err
	}
	var idScratch [1][]byte
	ids, err := nativewire.DecodeByteVectorItemsInto(idScratch[:0], rawIDs, nativewire.Limits{MaxByteVectorItems: 1, MaxByteVectorBytes: commitlog.ColocatedVectorMutationMaxIdentityBytesV1})
	if err != nil || len(ids) != 1 {
		return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated exact ID: %v", err)
	}
	guard, err := decodeExpectedCatalogVersionV1(entry.Target.ExpectedCatalogVersion)
	if err != nil {
		return v, true, err
	}
	// These views borrow the already-owned entry only for synchronous preflight
	// and apply. WAL encoding and publication copy the bytes they retain.
	v = commitlog.ColocatedVectorMutationWALV1{Scope: scope, Collection: collection, Delete: command == nativewire.CommandDeleteBatch, ID: ids[0], Attempt: entry.IdempotencyKey, CommandDigest: [32]byte(entry.Digest), ExpectedCatalogVersion: guard, Term: meta.EntryID.Term, Index: meta.EntryID.Index}
	if !v.Delete {
		format, err := lowerDocumentFormatV1(entry)
		if err != nil || format != collections.DocumentFormatJSON {
			return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated replacement requires JSON: %v", err)
		}
		if err := requireExistingReplacementModeV1(entry); err != nil {
			return v, true, err
		}
		rawDocuments, err := requiredDeterministicSectionV1(entry, nativewire.SectionDocuments, "documents")
		if err != nil {
			return v, true, err
		}
		var documentScratch [1][]byte
		documents, err := nativewire.DecodeByteVectorItemsInto(documentScratch[:0], rawDocuments, nativewire.Limits{MaxByteVectorItems: 1, MaxByteVectorBytes: commitlog.ColocatedVectorMutationMaxDocumentBytesV1})
		if err != nil || len(documents) != 1 {
			return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated exact document: %v", err)
		}
		v.Document = documents[0]
	}
	if err := v.ValidateInputsV1(); err != nil {
		return v, true, codedError(raftentry.ErrorMalformedEntryV1, "raftapply: colocated inputs: %v", err)
	}
	return v, true, nil
}

func (h *Harness) colocatedVectorMutationOutcomeV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (bool, bool, error) {
	v, scoped, err := colocatedVectorMutationInputsV1(entry, meta)
	if err != nil || !scoped {
		return scoped, false, err
	}
	c, err := h.replayCollectionManager().OpenCollection(v.Collection)
	if err != nil {
		return true, false, codeCollectionApplyError(err)
	}
	_, known, err := c.ReadVectorPartitionColocatedOutcomeV1(v.Scope, v.Attempt, v.CommandDigest)
	if err != nil {
		return true, false, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: covered colocated outcome: %v", err)
	}
	return true, known, nil
}

func (h *Harness) preflightColocatedVectorMutationV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) error {
	v, _, err := colocatedVectorMutationInputsV1(entry, meta)
	if err != nil {
		return err
	}
	c, err := h.replayCollectionManager().OpenCollection(v.Collection)
	if err != nil {
		return codeCollectionApplyError(err)
	}
	err = c.WithPreparedCommandWALMutation(func(owner *collections.CommandWALAdmittedCollection) error {
		return owner.PreflightVectorPartitionColocatedMutationV1(context.Background(), v)
	})
	if err != nil {
		return codeCollectionApplyError(err)
	}
	return nil
}

func (h *Harness) recoveredColocatedVectorMutationResultV1(entry raftentry.CommandEntryV1, meta ApplyMetadataV1) (raftentry.ApplyResultV1, error) {
	logical, err := h.logicalDigestForProgressV1(meta)
	if err != nil {
		code, _ := ErrorCodeOf(err)
		return recoveryRequired(entry.Digest, code, err)
	}
	// Ordinary duplicate counts remain zero. Public original counts/position are
	// read only from the covered atomic witness, never this external result store.
	return raftentry.ApplyResultV1{Status: raftentry.ApplyStatusAlreadyApplied, CommandDigest: entry.Digest, ResultDigest: raftentry.CommandDigestV1(logical)}, nil
}

func requireColocatedRecordedOutcomeV1(scoped, known bool) error {
	if scoped && !known {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "raftapply: external colocated result has no covered atomic outcome")
	}
	return nil
}

func compareColocatedProofV1(proof collections.VectorIndexPartitionLiveStatusV1, v commitlog.ColocatedVectorMutationWALV1, outcome commitlog.ColocatedVectorMutationOutcomeV1) error {
	if proof.Generation != v.Scope.Generation || proof.Coverage != outcome.Coverage || proof.Revision != outcome.Revision || outcome.Matched != v.Matched || outcome.Affected != v.Affected || outcome.Term != v.Term || outcome.Index != v.Index {
		return errors.New("raftapply: atomic colocated outcome and live proof disagree")
	}
	return nil
}

// CoveredColocatedVectorMutationOutcomeV1 is read-only recovery admission for the
// production FSM's existing local-coverage gap fence. It decodes exact entry
// bytes and validates actual FSM group metadata; it never records a result.
func CoveredColocatedVectorMutationOutcomeV1(db *backenddb.DB, src []byte, meta ApplyMetadataV1, decode raftentry.DecodeOptions) (commitlog.ColocatedVectorMutationOutcomeV1, bool, error) {
	var zero commitlog.ColocatedVectorMutationOutcomeV1
	entry, err := raftentry.DecodeCommandEntryV1(src, decode)
	if err != nil {
		return zero, false, err
	}
	v, scoped, err := colocatedVectorMutationInputsV1(entry, meta)
	if err != nil || !scoped {
		return zero, false, err
	}
	c, err := collections.NewCommandWALReplayCollectionManager(db).OpenCollection(v.Collection)
	if err != nil {
		return zero, false, err
	}
	return c.ReadVectorPartitionColocatedOutcomeV1(v.Scope, v.Attempt, v.CommandDigest)
}
