package raftplacement

import (
	"context"
	"errors"
	"fmt"
)

// VectorPartitionLifecycleCommitterV1 is the narrow meta-Raft submission
// seam used by the lifecycle coordinator. raftcluster's catalog-meta provider
// implements this method directly; keeping the interface here avoids a
// reverse dependency from the catalog schema to a particular Raft provider.
type VectorPartitionLifecycleCommitterV1 interface {
	SubmitCatalogMetaCommandV1(context.Context, []byte) (term uint64, index uint64, err error)
}

// VectorPartitionLifecycleCoordinatorV1 submits lifecycle transitions through
// the sole catalog/meta owner and reads the resulting applied record. It is
// intentionally not a local state owner: a successful method return always
// corresponds to a record published by the configured replicated authority.
type VectorPartitionLifecycleCoordinatorV1 struct {
	Authority *CatalogMetaAuthorityV1
	Committer VectorPartitionLifecycleCommitterV1
}

func (c VectorPartitionLifecycleCoordinatorV1) validateConfiguredV1() error {
	if c.Authority == nil || c.Committer == nil {
		return ErrCatalogMetaUnavailable
	}
	return nil
}

// Submit commits one already-guarded deterministic lifecycle command and
// returns its locally applied record. The meta-Raft provider only returns after
// its Apply callback, so a missing record after a successful commit is treated
// as a fail-closed configuration error rather than silently trusting a local
// cache.
func (c VectorPartitionLifecycleCoordinatorV1) Submit(ctx context.Context, command VectorPartitionLifecycleCommandV1) (VectorPartitionLifecycleRecordV1, error) {
	if err := c.validateConfiguredV1(); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	raw, err := EncodeVectorPartitionLifecycleCommandV1(command)
	if err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	if _, _, err := c.Committer.SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
		return VectorPartitionLifecycleRecordV1{}, err
	}
	record, ok := c.Authority.VectorPartitionLifecycleRecordV1(command.Identity)
	if !ok {
		return VectorPartitionLifecycleRecordV1{}, errors.Join(ErrCatalogMetaUnavailable, fmt.Errorf("lifecycle command committed without locally applied record"))
	}
	return record, nil
}

func (c VectorPartitionLifecycleCoordinatorV1) validateConfirmedMutationProofV1(identity VectorPartitionLifecycleIndexIdentityV1, proof VectorPartitionLifecycleMutationProofV1, label string) error {
	if proof.ActiveGeneration == 0 {
		return nil
	}
	record, ok := c.Authority.lifecycleRecordForIndexGenerationV1(identity, proof.ActiveGeneration)
	if !ok || !record.MutationConfirmed {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("%s relevant mutation is pending outcome confirmation", label))
	}
	return nil
}

// MutationProofV1 returns a durable proof for a relevant mutation. It fails
// closed for a build in progress or an identity that no longer names the
// active generation. If the exact generation is active, callers must first
// submit InvalidateBeforeRelevantMutationV1.
func (a *CatalogMetaAuthorityV1) MutationProofV1(identity VectorPartitionLifecycleIndexIdentityV1) (VectorPartitionLifecycleMutationProofV1, bool, error) {
	if a == nil {
		return VectorPartitionLifecycleMutationProofV1{}, false, ErrCatalogMetaUnavailable
	}
	if err := validateVectorPartitionLifecycleIndexIdentityV1(identity); err != nil {
		return VectorPartitionLifecycleMutationProofV1{}, false, err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.record.Epoch == 0 {
		return VectorPartitionLifecycleMutationProofV1{}, false, ErrCatalogMetaUnavailable
	}
	servingKey := vectorPartitionLifecycleServingKeyV1{Collection: identity.Collection, IndexName: identity.IndexName}
	active, hasActive := a.activeNames[servingKey]
	if hasActive && active.Index != identity {
		// A DDL/recreate caller with a stale identity must not bypass the
		// active generation merely because the names still match.
		return VectorPartitionLifecycleMutationProofV1{}, false, ErrVectorPartitionLifecycleIdentity
	}
	if !hasActive {
		candidate, record, found := selectMutationProofRecordLockedV1(a.lifecycle, identity)
		if found {
			proof := VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity, ActiveGeneration: candidate.Generation, InvalidationEpoch: record.InvalidationEpoch}
			if err := record.CanCommitRelevantMutation(proof); err != nil {
				return VectorPartitionLifecycleMutationProofV1{}, false, err
			}
			return proof, false, nil
		}
		return VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity}, false, nil
	}
	record, ok := a.lifecycle[active]
	if !ok || record.State != VectorPartitionLifecycleActiveV1 {
		return VectorPartitionLifecycleMutationProofV1{}, false, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active lifecycle record is unavailable"))
	}
	return VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity, ActiveGeneration: active.Generation}, true, nil
}

// selectMutationProofRecordLockedV1 gives no-active mutation admission a
// deterministic, safety-first meaning. A pending invalidation wins over every
// other retained generation; otherwise the newest exact generation wins. Map
// iteration must never choose which generation blocks or authorizes a write.
func selectMutationProofRecordLockedV1(records map[VectorPartitionLifecycleIdentityV1]VectorPartitionLifecycleRecordV1, index VectorPartitionLifecycleIndexIdentityV1) (VectorPartitionLifecycleIdentityV1, VectorPartitionLifecycleRecordV1, bool) {
	var selectedIdentity VectorPartitionLifecycleIdentityV1
	var selectedRecord VectorPartitionLifecycleRecordV1
	found := false
	rank := func(record VectorPartitionLifecycleRecordV1) int {
		if record.State == VectorPartitionLifecycleInvalidatedV1 && !record.MutationConfirmed {
			return 3
		}
		switch record.State {
		case VectorPartitionLifecycleBuildingV1, VectorPartitionLifecycleStagedV1, VectorPartitionLifecyclePreparedV1:
			return 2
		case VectorPartitionLifecycleInvalidatedV1, VectorPartitionLifecycleRetiredV1, VectorPartitionLifecycleCleanableV1:
			return 1
		default:
			return 0
		}
	}
	for candidate, record := range records {
		if candidate.Index != index || record.State == VectorPartitionLifecycleAbsentV1 {
			continue
		}
		if !found || rank(record) > rank(selectedRecord) ||
			(rank(record) == rank(selectedRecord) && (candidate.Generation > selectedIdentity.Generation ||
				(candidate.Generation == selectedIdentity.Generation && record.Revision > selectedRecord.Revision))) {
			selectedIdentity, selectedRecord, found = candidate, record, true
		}
	}
	return selectedIdentity, selectedRecord, found
}

func (a *CatalogMetaAuthorityV1) lifecycleRecordForIndexGenerationV1(index VectorPartitionLifecycleIndexIdentityV1, generation uint64) (VectorPartitionLifecycleRecordV1, bool) {
	if a == nil {
		return VectorPartitionLifecycleRecordV1{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, record, ok := findVectorPartitionLifecycleGenerationLockedV1(a.lifecycle, index, generation)
	return record, ok
}

// InvalidateBeforeRelevantMutationV1 is the common admission point for every
// nativewire, Mongo, shared-submit, replay, and retry path that can classify a
// mutation as relevant. It never permits a local mutation while a matching
// active generation exists: it first commits invalidation and only then returns
// the proof that downstream submit code must carry to commit the mutation.
//
// A retry that races the first invalidation recomputes the proof after a stale
// command refusal. This makes retry idempotent without weakening the ordering
// invariant.
func (c VectorPartitionLifecycleCoordinatorV1) InvalidateBeforeRelevantMutationV1(ctx context.Context, identity VectorPartitionLifecycleIndexIdentityV1, reason string) (VectorPartitionLifecycleMutationProofV1, error) {
	return c.invalidateBeforeRelevantMutationV1(ctx, identity, reason, 0, 0)
}

// InvalidateGenerationBeforeRelevantMutationV1 refuses to invalidate a successor that replaced the generation named by identity.
func (c VectorPartitionLifecycleCoordinatorV1) InvalidateGenerationBeforeRelevantMutationV1(ctx context.Context, identity VectorPartitionLifecycleIdentityV1, reason string) (VectorPartitionLifecycleMutationProofV1, error) {
	if err := validateVectorPartitionLifecycleIdentityV1(identity); err != nil {
		return VectorPartitionLifecycleMutationProofV1{}, err
	}
	return c.invalidateBeforeRelevantMutationV1(ctx, identity.Index, reason, 0, identity.Generation)
}

// InvalidateBeforeRelevantMutationAtEpochV1 binds per-index invalidation to
// the already-open collection mutation barrier. This prevents independent
// index fences from being confirmed by different concurrent data mutations.
func (c VectorPartitionLifecycleCoordinatorV1) InvalidateBeforeRelevantMutationAtEpochV1(ctx context.Context, identity VectorPartitionLifecycleIndexIdentityV1, reason string, mutationEpoch uint64) (VectorPartitionLifecycleMutationProofV1, error) {
	if mutationEpoch == 0 {
		return VectorPartitionLifecycleMutationProofV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation epoch is required"))
	}
	return c.invalidateBeforeRelevantMutationV1(ctx, identity, reason, mutationEpoch, 0)
}

func (c VectorPartitionLifecycleCoordinatorV1) invalidateBeforeRelevantMutationV1(ctx context.Context, identity VectorPartitionLifecycleIndexIdentityV1, reason string, mutationEpoch, expectedGeneration uint64) (VectorPartitionLifecycleMutationProofV1, error) {
	if c.Authority == nil || c.Committer == nil {
		return VectorPartitionLifecycleMutationProofV1{}, ErrCatalogMetaUnavailable
	}
	if err := validateVectorPartitionLifecycleReasonV1(reason); err != nil {
		return VectorPartitionLifecycleMutationProofV1{}, err
	}
	proof, active, err := c.Authority.MutationProofV1(identity)
	if err != nil {
		return proof, err
	}
	if active && expectedGeneration != 0 && proof.ActiveGeneration != expectedGeneration {
		return VectorPartitionLifecycleMutationProofV1{}, ErrVectorPartitionLifecycleIdentity
	}
	if !active {
		if err := c.validateConfirmedMutationProofV1(identity, proof, "prior"); err != nil {
			return VectorPartitionLifecycleMutationProofV1{}, err
		}
		return proof, err
	}
	record, ok := c.Authority.lifecycleRecordForIndexGenerationV1(identity, proof.ActiveGeneration)
	if !ok {
		return VectorPartitionLifecycleMutationProofV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active generation %d disappeared before invalidation", proof.ActiveGeneration))
	}
	invalidationEpoch := record.MutationEpoch + 1
	if mutationEpoch != 0 {
		if mutationEpoch <= record.MutationEpoch {
			return VectorPartitionLifecycleMutationProofV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("collection mutation epoch %d does not follow source epoch %d", mutationEpoch, record.MutationEpoch))
		}
		invalidationEpoch = mutationEpoch
	}
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleInvalidateV1, ExpectedRevision: record.Revision,
		ExpectedState: VectorPartitionLifecycleActiveV1, Identity: record.Identity,
		Reason: reason, InvalidationEpoch: invalidationEpoch,
	}
	if _, err := c.Submit(ctx, command); err != nil {
		// A concurrent/replayed invalidation is safe only if the newly applied
		// record independently proves the ordering for this exact identity.
		if retry, retryActive, retryErr := c.Authority.MutationProofV1(identity); retryErr == nil && !retryActive {
			if confirmErr := c.validateConfirmedMutationProofV1(identity, retry, "concurrent"); confirmErr != nil {
				return VectorPartitionLifecycleMutationProofV1{}, confirmErr
			}
			return retry, nil
		}
		return VectorPartitionLifecycleMutationProofV1{}, err
	}
	proof, active, err = c.Authority.MutationProofV1(identity)
	if err != nil {
		return VectorPartitionLifecycleMutationProofV1{}, err
	}
	if active {
		return VectorPartitionLifecycleMutationProofV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("invalidation did not remove active generation"))
	}
	return proof, nil
}

// ConfirmRelevantMutationV1 clears the durable build/activation freeze only
// after the caller has a definitive committed-data result.  If a process
// crashes or a submit is ambiguous before this call, the fence intentionally
// remains pending and recovery must prove the data outcome before confirming.
func (c VectorPartitionLifecycleCoordinatorV1) ConfirmRelevantMutationV1(ctx context.Context, proof VectorPartitionLifecycleMutationProofV1) error {
	if c.Authority == nil || c.Committer == nil {
		return ErrCatalogMetaUnavailable
	}
	if proof.ActiveGeneration == 0 || proof.InvalidationEpoch == 0 {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("mutation confirmation requires exact invalidation proof"))
	}
	record, ok := c.Authority.lifecycleRecordForIndexGenerationV1(proof.IndexIdentity, proof.ActiveGeneration)
	if !ok || record.State != VectorPartitionLifecycleInvalidatedV1 || record.InvalidationEpoch != proof.InvalidationEpoch {
		return errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("mutation confirmation has no matching invalidated generation"))
	}
	_, err := c.Submit(ctx, VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleConfirmMutationV1, ExpectedRevision: record.Revision,
		ExpectedState: VectorPartitionLifecycleInvalidatedV1, Identity: record.Identity,
		MutationEpoch: proof.InvalidationEpoch,
	})
	return err
}
