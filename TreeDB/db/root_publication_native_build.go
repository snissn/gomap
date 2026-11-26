package db

import (
	"context"
	"errors"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
	return group.buildNativeOrderedRootLocked(input, false)
}

// BuildNativeOrderedRootColdScan builds a cold (BaseRoot == 0) native ordered
// root directly from a push-based sorted entry scan, avoiding intermediate
// batch.Batch materialization.
func (group *RootPublicationBuildGroup) BuildNativeOrderedRootColdScan(input OrderedRootColdScanPublishInput) (uint64, error) {
	group.mu.Lock()
	defer group.mu.Unlock()
	if group == nil || group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil || input.Scan == nil {
		return 0, errors.New("invalid captured native root cold build")
	}
	if input.EntryCount <= 0 {
		return 0, nil
	}
	opts, err := group.db.orderedRootPublishOptionsForPolicy(input.StoragePolicy)
	if err != nil {
		return 0, err
	}
	if opts.outerLeavesInValueLog && opts.leafPageLog == nil {
		return 0, errors.New("ordered root value-log leaf storage requires a leaf page log")
	}
	group.db.observeOrderedRootSpanNativeEligibility(group.db.orderedRootSpanNativeEligibility(orderedRootSpanNativeEligibilityRequest{
		Route:          OrderedRootSpanNativeRouteOverlayColdBuild,
		Context:        "ordered-root cold scan build",
		DeltaOps:       input.EntryCount,
		ForceColdBuild: true,
	}))
	var leafPageLog bulk.LeafPageAppender
	if opts.outerLeavesInValueLog {
		leafPageLog = opts.leafPageLog
	}
	collectRefDelta := opts.outerLeavesInValueLog || group.db.shouldCollectValueLogRefDelta(group.baseSeq)
	var refDelta *valueLogRefDelta
	if collectRefDelta {
		refDelta = newValueLogRefDelta()
		refDelta.allowEmptyDependencyReuse = true
		refDelta.outerLeafDependencyReuse = opts.outerLeavesInValueLog
		refDelta.requiresCandidateProjection = false
	}
	root, err := bulk.BuildWithScan(func(emit bulk.EntryEmitter) error {
		var emptyBytes [0]byte
		return input.Scan(func(key, val []byte, ptr page.ValuePtr, flags byte, rev page.EntryRevision) error {
			if len(key) == 0 {
				key = emptyBytes[:]
			}
			if flags&node.FlagPointer != 0 {
				if !page.IsValueLogFileID(ptr.FileID) {
					return ErrNativeSemanticUnsupported
				}
				group.pinValueLogSegmentLocked(ptr.FileID)
				if refDelta != nil {
					refDelta.add(ptr.FileID, 1)
				}
				return emit(key, nil, ptr, node.FlagPointer, rev)
			}
			if len(val) == 0 {
				val = emptyBytes[:]
			}
			threshold := max(0, page.PageSize-node.NodeHeaderSize-node.DirectoryEntrySize-7-page.EntryRevisionSize-len(key))
			if input.CheckInlineThreshold {
				threshold = min(threshold, group.db.InlineThresholdForKey(key))
			}
			if len(val) > threshold {
				var values [1][]byte
				values[0] = val
				ptrs, appendErr := group.appendNativeValueLogValuesLocked(values[:])
				if appendErr != nil {
					return appendErr
				}
				ptr = ptrs[0]
				if refDelta != nil {
					refDelta.add(ptr.FileID, 1)
				}
				return emit(key, nil, ptr, node.FlagPointer, rev)
			}
			return emit(key, val, page.ValuePtr{}, node.FlagInline, rev)
		})
	}, group.tracker, group.idx.pager, bulk.BuildOptions{
		LeafPrefixCompression: opts.leafPrefixCompression,
		LeafColumnar:          opts.leafColumnar,
		PackedValuePtr:        opts.packedValuePtr,
		InternalBaseDelta:     opts.internalBaseDelta && !opts.outerLeavesInValueLog,
		LeafPageLog:           leafPageLog,
		MaxOutputPages:        opts.maxOutputPages,
	})
	if err != nil {
		releaseValueLogRefDelta(refDelta)
		group.failed = true
		return 0, err
	}
	group.nativeRootTransitions = append(group.nativeRootTransitions, nativeOrderedRootTransition{
		base: 0,
		next: root,
	})
	group.mergeValueLogRefDeltaLocked(refDelta)
	releaseValueLogRefDelta(refDelta)
	if root != 0 {
		count := int(group.nativeBuiltRootCount)
		if count == len(group.nativeBuiltRoots) {
			copy(group.nativeBuiltRoots[:count-1], group.nativeBuiltRoots[1:count])
			count--
		}
		group.nativeBuiltRoots[count] = root
		count++
		group.nativeBuiltRootCount = uint8(count)
	}
	return root, nil
}
func (group *RootPublicationBuildGroup) buildNativeOrderedRootLocked(input OrderedRootDeltaBatchPublishInput, systemRoot bool) (uint64, error) {
	if group == nil || group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil || input.Delta == nil {
		return 0, errors.New("invalid captured native root build")
	}
	opts, err := group.db.orderedRootPublishOptionsForPolicy(input.StoragePolicy)
	if err != nil {
		return 0, err
	}
	if systemRoot {
		opts = systemRootOrderedPublishOptions(group.db)
	}
	// Pin submitted pointers before fallible physical construction. The original
	// stream needs no sort/range plan; superseded puts conservatively retain a
	// group-lifetime GC pin without changing the exact logical reference delta.
	if input.Delta.HasValueLogPointers() {
		for _, entry := range input.Delta.OrderedEntries() {
			if entry.Type == batchpkg.OpPut && entry.IsPtr && page.IsValueLogFileID(entry.ValuePtr.FileID) {
				group.pinValueLogSegmentLocked(entry.ValuePtr.FileID)
			}
		}
	}
	// Metadata may name a file absent from the submitted pointer entries.
	group.pinBatchValueLogSegmentsLocked(input.Delta)
	var (
		baseDescriptorEntries []collectionEntry
		baseDescriptorErr     error
		mayChangeDescriptors  bool
	)
	collectRefDelta := group.db.shouldCollectValueLogRefDelta(group.baseSeq)
	if systemRoot && collectRefDelta {
		mayChangeDescriptors = orderedRootDeltaMayChangeCollectionRootDescriptors(input.Delta)
		if mayChangeDescriptors {
			baseDescriptorEntries, baseDescriptorErr = vacuumCollectCollectionEntriesFromRoot(context.Background(), group.idx.pager, group.db.valueLogManager, input.BaseRoot)
		}
	}
	// Native roots use the existing ordered-root DATA builder. PRIMARY private
	// construction belongs only to the group's raw user-root path.
	root, retired, metrics, result, err := group.db.publishOrderedRootDeltaBatchWithAllocatorResult(group.idx, input.BaseRoot, input.Delta, opts, group.tracker, group.tracker, input.IncludeDeletedOnColdBuild, collectRefDelta)
	if err != nil {
		group.failed = true
		return 0, err
	}
	delta, err := group.db.buildAppliedOrderedRootDeltaBatchValueLogRefDeltaWithOuterLeaves(group.idx, group.baseSeq, input, orderedRootDeltaBatchGroupApplyResult{rootID: root, retired: retired, metrics: metrics, applyResult: result}, opts.outerLeavesInValueLog)
	if err != nil {
		group.failed = true
		return 0, err
	}
	if !systemRoot {
		chained := false
		if input.BaseRoot != 0 {
			for i := range group.nativeRootTransitions {
				if group.nativeRootTransitions[i].next == input.BaseRoot {
					group.nativeRootTransitions[i].next = root
					chained = true
					break
				}
			}
		}
		if !chained {
			group.nativeRootTransitions = append(group.nativeRootTransitions, nativeOrderedRootTransition{
				base: input.BaseRoot,
				next: root,
			})
		}
		if delta != nil {
			delta.requiresCandidateProjection = false
		} else if collectRefDelta {
			group.vlogRefDeltaUncovered = true
		}
	} else {
		baseRoots := make([]uint64, len(group.nativeRootTransitions))
		newRoots := make([]uint64, len(group.nativeRootTransitions))
		for i, transition := range group.nativeRootTransitions {
			baseRoots[i] = transition.base
			newRoots[i] = transition.next
		}
		transitionsChanged := orderedRootTransitionsChanged(baseRoots, newRoots)
		covered := false
		if group.currentRoot == group.baseRoot {
			if !mayChangeDescriptors && !transitionsChanged {
				covered = true
			} else if baseDescriptorErr == nil && orderedRootCollectionDescriptorTransitionsCoveredByDelta(
				baseDescriptorEntries,
				input.Delta,
				group.currentRoot,
				input.BaseRoot,
				root,
				baseRoots,
				newRoots,
			) {
				covered = true
			}
		}
		if delta == nil {
			if collectRefDelta {
				group.vlogRefDeltaUncovered = true
			}
		} else if covered {
			delta.requiresCandidateProjection = false
			group.nativeRootTransitions = group.nativeRootTransitions[:0]
		} else {
			delta.requiresCandidateProjection = true
			group.vlogRefDeltaUncovered = true
		}
	}
	group.mergeValueLogRefDeltaLocked(delta)
	releaseValueLogRefDelta(delta)
	// Replace superseded outputs in-place. A fifth distinct current output
	// evicts only the oldest READ capability; original construction is unchanged.
	if root != input.BaseRoot {
		count := int(group.nativeBuiltRootCount)
		for i := 0; i < count; i++ {
			if group.nativeBuiltRoots[i] != input.BaseRoot {
				continue
			}
			copy(group.nativeBuiltRoots[i:count-1], group.nativeBuiltRoots[i+1:count])
			count--
			group.nativeBuiltRoots[count] = 0
			break
		}
		if root != 0 {
			if count == len(group.nativeBuiltRoots) {
				copy(group.nativeBuiltRoots[:count-1], group.nativeBuiltRoots[1:count])
				count--
			}
			group.nativeBuiltRoots[count] = root
			count++
		}
		group.nativeBuiltRootCount = uint8(count)
	}
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
	root, err := group.buildNativeOrderedRootLocked(OrderedRootDeltaBatchPublishInput{BaseRoot: group.systemRoot, Delta: delta}, true)
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

// NativeInlineThresholdForKey returns the maintained physical value placement
// policy. It is independent of the inline logical BSON/WAL representation.
func (group *RootPublicationBuildGroup) NativeInlineThresholdForKey(key []byte) int {
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.db.InlineThresholdForKey(key)
}

// AppendNativeValueLogValues prepares physical projection values. The existing
// append owner fences GC until these per-pointer pins have been transferred to
// this group's segment ledger. Finalize includes those segments in its ordinary
// dependency flush/sync; Close releases them on success and abandon alike.
func (group *RootPublicationBuildGroup) AppendNativeValueLogValues(values [][]byte) ([]page.ValuePtr, error) {
	if group == nil {
		return nil, ErrNativeSemanticUnsupported
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.appendNativeValueLogValuesLocked(values)
}

func (group *RootPublicationBuildGroup) appendNativeValueLogValuesLocked(values [][]byte) ([]page.ValuePtr, error) {
	if group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil {
		return nil, ErrNativeSemanticUnsupported
	}
	ptrs, err := group.db.AppendValueLogValues(values)
	if err != nil {
		group.failed = true
		return nil, err
	}
	if len(ptrs) != len(values) {
		group.db.ReleaseValueLogValues(ptrs)
		group.failed = true
		return nil, errors.New("native physical value append returned an invalid pointer count")
	}
	for _, ptr := range ptrs {
		if !page.IsValueLogFileID(ptr.FileID) {
			group.db.ReleaseValueLogValues(ptrs)
			group.failed = true
			return nil, errors.New("native physical value append returned an invalid value-log pointer")
		}
	}
	for _, ptr := range ptrs {
		group.pinValueLogSegmentLocked(ptr.FileID)
	}
	group.db.ReleaseValueLogValues(ptrs)
	return ptrs, nil
}

// WithNativeConstructionRoots borrows current roots produced by this same
// group synchronously, restricted to its four newest current outputs. Evicted,
// superseded and foreign roots refuse; zero denotes an empty tree.
// The original basis owns old files; durablePublishMu
// excludes reclamation and the maintained Manager reads newly registered leaf
// segments through its writable-read barrier. No Snapshot is acquired.
// visit must not retain an iterator or its borrowed key/value views.
func (group *RootPublicationBuildGroup) WithNativeConstructionRoots(roots []uint64, visit func([]iterator.UnsafeIterator) error) (resultErr error) {
	if group == nil {
		return ErrNativeSemanticUnsupported
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	if group.db == nil || group.closed || group.failed || !group.writeLocked || !group.durablePublishLocked || group.tracker == nil || group.idx == nil || group.capturedBasis == nil || visit == nil || len(roots) > len(group.nativeBuiltRoots) {
		return ErrNativeSemanticUnsupported
	}
	for _, root := range roots {
		if root == 0 {
			continue
		}
		found := false
		for _, owned := range group.nativeBuiltRoots[:group.nativeBuiltRootCount] {
			if root == owned {
				found = true
				break
			}
		}
		if !found {
			return ErrRootPublicationBasisMismatch
		}
	}
	// Make freshly appended leaf segments available through the SAME maintained
	// Manager/barrier; this registers physical files without selecting any catalog.
	if _, err := group.db.registerLeafPageLogSegmentsForPublish(); err != nil {
		return err
	}
	var readerStorage [4]iterator.UnsafeIterator
	readers := readerStorage[:len(roots)]
	defer func() {
		for _, it := range readers {
			if it != nil {
				resultErr = errors.Join(resultErr, it.Close())
			}
		}
		clear(readers)
	}()
	for i, root := range roots {
		if root == 0 {
			continue
		}
		tr := tree.New(group.idx.pager, newValueReader(group.db.valueLogManager), root)
		readers[i] = tr.Iterator(nil, nil)
		if err := readers[i].Error(); err != nil {
			return err
		}
	}
	return visit(readers)
}
