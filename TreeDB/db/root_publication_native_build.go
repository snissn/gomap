package db

import (
	"errors"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// MarkNativeSemanticBuildRecoveryRequired fences the original backend before
// abandoning a failed materialization of an already installed logical plan.
// Original native/group owners retain their own failure custody; these atomic
// flags neither destroy resources nor invoke callbacks under build locks.
func (group *RootPublicationBuildGroup) MarkNativeSemanticBuildRecoveryRequired() {
	group.db.MarkNativeSemanticRecoveryRequired()
}

// MarkNativeSemanticRecoveryRequired fences all publication on this original
// backend after accepted native materialization or handoff failure. The original
// sparse Work retains the exact failure resources and accounting authority.
func (db *DB) MarkNativeSemanticRecoveryRequired() {
	db.publicationPoisoned.Store(true)
	db.MarkCommandWALRecoveryRequired()
}

// BuildNativeOrderedRoot builds a captured native projection within the SAME
// physical build group as raw effects. Input owns its immutable base root and
// bounded delta; this never samples a live collection catalog or publishes.
func (group *RootPublicationBuildGroup) BuildNativeOrderedRoot(input OrderedRootDeltaBatchPublishInput) (uint64, error) {
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.buildNativeOrderedRootLocked(input)
}
func (group *RootPublicationBuildGroup) buildNativeOrderedRootLocked(input OrderedRootDeltaBatchPublishInput) (uint64, error) {
	if group == nil || group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil || input.Delta == nil {
		return 0, errors.New("invalid captured native root build")
	}
	opts, err := group.db.orderedRootPublishOptionsForPolicy(input.StoragePolicy)
	if err != nil {
		return 0, err
	}
	// Native roots use the existing ordered-root DATA builder. PRIMARY private
	// construction belongs only to the group's raw user-root path.
	root, retired, metrics, result, err := group.db.publishOrderedRootDeltaBatchWithAllocatorResult(group.idx, input.BaseRoot, input.Delta, opts, group.tracker, group.tracker, input.IncludeDeletedOnColdBuild, group.db.shouldCollectValueLogRefDelta(group.baseSeq))
	if err != nil {
		group.failed = true
		return 0, err
	}
	delta, err := group.db.buildAppliedOrderedRootDeltaBatchValueLogRefDelta(group.idx, group.baseSeq, input, orderedRootDeltaBatchGroupApplyResult{rootID: root, retired: retired, metrics: metrics, applyResult: result})
	if err != nil {
		group.failed = true
		return 0, err
	}
	group.mergeValueLogRefDeltaLocked(delta)
	releaseValueLogRefDelta(delta)
	group.pinBatchValueLogSegmentsLocked(input.Delta)
	group.retired = append(group.retired, retired...)
	mergeOrderedRootPublishMetrics(&group.metrics, metrics)
	return root, nil
}

// BuildNativeSystemDelta applies captured logical catalog/schema/outcome
// effects to the exact captured system basis. The caller supplies only those
// effects covered by this finite T, after obtaining its private domain roots.
func (group *RootPublicationBuildGroup) BuildNativeSystemDelta(delta *batchpkg.Batch) error {
	group.mu.Lock()
	defer group.mu.Unlock()
	root, err := group.buildNativeOrderedRootLocked(OrderedRootDeltaBatchPublishInput{BaseRoot: group.systemRoot, Delta: delta})
	if err == nil {
		group.systemRoot = root
	}
	return err
}

// SetNativeDurableResources transfers one already prepared complete native
// closure to this original build group. The final candidate adopts that exact
// closure; an abandoned group releases it through existing failure custody.
func (group *RootPublicationBuildGroup) SetNativeDurableResources(resources *rootpublication.StableResourceSet, requirements rootpublication.StableLogicalObligationRequirements) error {
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.closed || group.failed || group.nativeResources != nil {
		return errors.New("invalid captured native resource transfer")
	}
	group.nativeResources = resources
	group.nativeRequirements = requirements
	return nil
}

// SetNativeDurableResourceAppend transfers the same native closure together
// with collections' root-validated append proof. Its exact fallback borrows the
// group's captured basis only until synchronous finalization/cleanup. Callers
// preadmit the fallback cursor/decoder and bounded fresh records before any
// assets or closure are adopted; retained desired history stays streamed.
// Ordinary complete-closure callers retain SetNativeDurableResources.
func (group *RootPublicationBuildGroup) SetNativeDurableResourceAppend(resources *rootpublication.StableResourceSet, requirements rootpublication.StableLogicalObligationRequirements, mutation rootpublication.StableLogicalObligationMutation, work rootpublication.StableResourceClosureWork, fallback func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error)) error {
	if group == nil {
		return rootpublication.ErrResourceOwnership
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.db == nil || group.closed || group.failed || group.nativeResources != nil || !group.writeLocked || !group.durablePublishLocked {
		return rootpublication.ErrResourceOwnership
	}
	ctx := CommandWALPublishContext{durableResourceAppendMutation: &group.nativeAppend, durableResourceRequirementWork: &group.nativeRequirementWork, durableResourceRequirementsFallback: &group.nativeRequirementsFallback}
	if err := ctx.RegisterDurableLogicalObligationAppendMutation(mutation, work, fallback); err != nil {
		return err
	}
	group.nativeResources, group.nativeRequirements = resources, requirements
	return nil
}

// NativeColumnAssetBuildContext borrows the original build group's physical
// namespace and pin authority for synchronous captured typed materialization.
// Neither capability may be retained in logical deltas or used to publish.
func (group *RootPublicationBuildGroup) NativeColumnAssetBuildContext() (string, *rootpublication.IdentityPinRegistry, error) {
	if group == nil {
		return "", nil, ErrNativeSemanticUnsupported
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil || group.db.publicationPoisoned.Load() {
		return "", nil, ErrNativeSemanticUnsupported
	}
	root := group.db.ColumnAssetRootDir()
	registry := group.db.StableResourceIdentityPinRegistry()
	if root == "" || registry == nil {
		return "", nil, ErrNativeSemanticUnsupported
	}
	return root, registry, nil
}
