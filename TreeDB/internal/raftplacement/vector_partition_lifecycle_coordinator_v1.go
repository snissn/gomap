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

// Submit commits one already-guarded deterministic lifecycle command and
// returns its locally applied record. The meta-Raft provider only returns after
// its Apply callback, so a missing record after a successful commit is treated
// as a fail-closed configuration error rather than silently trusting a local
// cache.
func (c VectorPartitionLifecycleCoordinatorV1) Submit(ctx context.Context, command VectorPartitionLifecycleCommandV1) (VectorPartitionLifecycleRecordV1, error) {
	if c.Authority == nil || c.Committer == nil {
		return VectorPartitionLifecycleRecordV1{}, ErrCatalogMetaUnavailable
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
		for candidate, record := range a.lifecycle {
			if candidate.Index == identity && record.State != VectorPartitionLifecycleAbsentV1 {
				proof := VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity, ActiveGeneration: candidate.Generation, InvalidationEpoch: record.InvalidationEpoch}
				if err := record.CanCommitRelevantMutation(proof); err != nil {
					return VectorPartitionLifecycleMutationProofV1{}, false, err
				}
				return proof, false, nil
			}
		}
		return VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity}, false, nil
	}
	record, ok := a.lifecycle[active]
	if !ok || record.State != VectorPartitionLifecycleActiveV1 {
		return VectorPartitionLifecycleMutationProofV1{}, false, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active lifecycle record is unavailable"))
	}
	return VectorPartitionLifecycleMutationProofV1{IndexIdentity: identity, ActiveGeneration: active.Generation}, true, nil
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
	if c.Authority == nil || c.Committer == nil {
		return VectorPartitionLifecycleMutationProofV1{}, ErrCatalogMetaUnavailable
	}
	if err := validateVectorPartitionLifecycleReasonV1(reason); err != nil {
		return VectorPartitionLifecycleMutationProofV1{}, err
	}
	proof, active, err := c.Authority.MutationProofV1(identity)
	if err != nil || !active {
		return proof, err
	}
	record, ok := c.Authority.lifecycleRecordForIndexGenerationV1(identity, proof.ActiveGeneration)
	if !ok {
		return VectorPartitionLifecycleMutationProofV1{}, errors.Join(ErrVectorPartitionLifecycleGuard, fmt.Errorf("active generation %d disappeared before invalidation", proof.ActiveGeneration))
	}
	command := VectorPartitionLifecycleCommandV1{
		Kind: VectorPartitionLifecycleInvalidateV1, ExpectedRevision: record.Revision,
		ExpectedState: VectorPartitionLifecycleActiveV1, Identity: record.Identity,
		Reason: reason, InvalidationEpoch: record.MutationEpoch + 1,
	}
	if _, err := c.Submit(ctx, command); err != nil {
		// A concurrent/replayed invalidation is safe only if the newly applied
		// record independently proves the ordering for this exact identity.
		if retry, retryActive, retryErr := c.Authority.MutationProofV1(identity); retryErr == nil && !retryActive {
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
