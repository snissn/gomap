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
	group.db.publicationPoisoned.Store(true)
	group.db.MarkCommandWALRecoveryRequired()
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
