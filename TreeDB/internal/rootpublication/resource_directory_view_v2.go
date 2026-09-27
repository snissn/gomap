package rootpublication

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

var errStopDependencyDirectoryWalk = errors.New("stop dependency directory walk")

// DependencyDirectoryV2 returns a borrowed directory from a fully bound set.
// The caller must retain the set until it has finished using the directory.
func (set *StableResourceSet) DependencyDirectoryV2() (*DependencyDirectoryV2, error) {
	if set == nil {
		return nil, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.physicalOnly {
		return nil, ErrResourceOwnership
	}
	if owner := ResourceOwnerState(set.owner.Load()); owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, ErrResourceOwnership
	}
	directory := set.emptyDependencyDirectoryLocked()
	var err error
	var entries, bound uint64
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		entries++
		view := entry.logicalObligations
		if view.directory == nil {
			return true
		}
		bound++
		if view.tail != nil || view.index != nil || len(view.removed) != 0 || (directory != nil && directory != view.directory) {
			err = fmt.Errorf("%w: dependency directory has an unsealed delta", ErrResourceConflict)
			return false
		}
		directory = view.directory
		return true
	})
	if directory != nil && entries != bound {
		err = fmt.Errorf("%w: partially bound dependency directory", ErrResourceConflict)
	}
	return directory, err
}

// BindDependencyDirectoryV2 replaces an admitted candidate's in-memory logical
// history with its newly built directory and bounded per-physical summaries.
// The returned set independently owns physical pins and the supplied root lease.
// The caller retains ownership of source and its original directory reference.
func BindDependencyDirectoryV2(source *StableResourceSet, directory *DependencyDirectoryV2) (*StableResourceSet, error) {
	if source == nil || directory == nil || source.physicalOnly {
		return nil, ErrResourceOwnership
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, ErrResourceOwnership
	}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	var bindErr error
	var physical, logical uint64
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		physical++
		if entry.logicalObligations.count < 0 || uint64(entry.logicalObligations.count) > ^uint64(0)-logical {
			bindErr = ErrDependencyManifestFormat
			return false
		}
		logical += uint64(entry.logicalObligations.count)
		// Encoding a physical record must not materialize the logical view.
		physicalEntry := *entry
		physicalEntry.logicalObligations = stableLogicalObligationView{}
		descriptor := dependencyManifestEntryV1FromStableResourceEntry(physicalEntry)
		key := DependencyPhysicalKeyV2(descriptor)
		wantBytes, err := EncodeDependencyPhysicalV2(descriptor)
		if err != nil {
			bindErr = err
			return false
		}
		matched, err := directory.matchesPhysicalRecordV2(key, wantBytes)
		if err != nil {
			bindErr = err
			return false
		}
		if !matched {
			bindErr = fmt.Errorf("%w: directory physical descriptor differs from candidate", ErrResourceConflict)
			return false
		}
		cloned, err := cloneStableResourceEntryDirectoryV2(entry, directory)
		if err != nil {
			bindErr = err
			return false
		}
		cloned.logicalObligations = stableLogicalObligationView{
			directory: directory, owner: key, count: entry.logicalObligations.count,
			commitments: cloneStableLogicalObligationCommitments(entry.logicalObligations.commitments),
		}
		builder.entries = append(builder.entries, cloned)
		return true
	})
	if bindErr != nil {
		return nil, bindErr
	}
	if physical != directory.ref.PhysicalCount || logical != directory.ref.LogicalCount {
		return nil, ErrDependencyManifestFormat
	}
	bound, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	if physical == 0 {
		if err := directory.Walk(nil); err != nil {
			bound.Release()
			return nil, err
		}
		if err := directory.Retain(); err != nil {
			bound.Release()
			return nil, ErrResourceOwnership
		}
		bound.extras = &stableResourceSetExtras{empty: directory}
	}
	return bound, nil
}

func cloneStableResourceEntryDirectoryV2(entry *stableResourceEntry, directory *DependencyDirectoryV2) (stableResourceEntry, error) {
	token := activeEntryToken(*entry)
	if token == nil || token.released.Load() {
		return stableResourceEntry{}, ErrResourceOwnership
	}
	if err := token.namespace.validateStable(); err != nil {
		return stableResourceEntry{}, err
	}
	fields := make([]ReachabilityField, 0, len(entry.reachability))
	for field := range entry.reachability {
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return stableResourceEntry{}, ErrUnresolvedResource
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	var registry *IdentityPinRegistry
	identity := token.identity
	if token.identityPin != nil {
		registry = token.identityPin.registry
		if err := registry.Observe(identity); err != nil {
			return stableResourceEntry{}, err
		}
	}
	cloned, err := token.cloneSharedPinnedDirectory(entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, fields[0], nil, directory, func() {
		if registry != nil {
			_ = registry.Unobserve(identity)
		}
	})
	if err != nil {
		if registry != nil {
			_ = registry.Unobserve(identity)
		}
		return stableResourceEntry{}, err
	}
	if err := cloned.claim(ResourceOwnerBuilder); err != nil {
		cloned.Release()
		return stableResourceEntry{}, err
	}
	return stableResourceEntry{
		token: cloned, logicalLane: entry.logicalLane, resourceID: entry.resourceID,
		diagnosticPath: entry.diagnosticPath, frontier: cloneDurableFrontier(entry.frontier),
		reachability: cloneReachabilityUnion(nil, entry.reachability), logicalObligations: entry.logicalObligations,
		dependencyManifestV1: &dependencyManifestEntryCacheV1{},
	}, nil
}

func lookupStableLogicalMembershipV2(index *stableLogicalObligationIndexNode, directory *DependencyDirectoryV2, owners *stableResourceLogicalIndexNode, kind ResourceKind, obligation StableLogicalObligation, work *StableResourceClosureWork) (StableLogicalObligation, bool, error) {
	if existing, found := findStableLogicalObligationIndex(index, obligation, work); found {
		return existing, true, nil
	}
	if directory == nil {
		return StableLogicalObligation{}, false, nil
	}
	if work != nil {
		work.AggregateMembershipProbes++
	}
	owner, existing, found, err := directory.LookupLogical(obligation)
	if err != nil || !found {
		return StableLogicalObligation{}, false, err
	}
	reader := manifestReaderV1{data: owner, offset: 1}
	ownerKind, ok := reader.str()
	if !ok {
		return StableLogicalObligation{}, false, ErrDependencyManifestFormat
	}
	if ResourceKind(ownerKind) != kind {
		return StableLogicalObligation{}, false, nil
	}
	lane, laneOK := reader.str()
	resourceID, resourceOK := reader.str()
	generation, generationOK := reader.u64()
	if !laneOK || !resourceOK || !generationOK || reader.remaining() != 0 {
		return StableLogicalObligation{}, false, ErrDependencyManifestFormat
	}
	entry := findStableResourceLogical(owners, stableLogicalResourceKey{kind: kind, lane: lane, resourceID: resourceID, generation: generation})
	if entry == nil || entry.logicalObligations.directory != directory {
		return StableLogicalObligation{}, false, nil
	}
	if _, removed := entry.logicalObligations.removed[stableLogicalObligationKey(obligation)]; removed {
		return StableLogicalObligation{}, false, nil
	}
	return existing, true, nil
}

// walk streams a directory-backed entry and then its small admitted delta.
// The directory lease is owned by the containing entry's physical token.
func (view stableLogicalObligationView) walk(visit func(StableLogicalObligation) bool) error {
	if visit == nil {
		return nil
	}
	if view.directory != nil {
		err := view.directory.Walk(func(key, value []byte) error {
			if key[0] != dependencyLogicalKeyV2 {
				return nil
			}
			owner, obligation, err := DecodeDependencyLogicalV2(key, value)
			if err != nil {
				return err
			}
			_, removed := view.removed[stableLogicalObligationKey(obligation)]
			if !removed && bytes.Equal(owner, view.owner) && !visit(obligation) {
				return errStopDependencyDirectoryWalk
			}
			return nil
		})
		if errors.Is(err, errStopDependencyDirectoryWalk) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	view.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
		if _, removed := view.removed[stableLogicalObligationKey(obligation)]; removed {
			return true
		}
		return visit(obligation)
	})
	return nil
}

// lookup checks a small unsealed producer delta before the admitted directory.
// A directory lookup error cannot be interpreted as permission to add a key.
func (view stableLogicalObligationView) lookup(obligation StableLogicalObligation, work *StableResourceClosureWork) (StableLogicalObligation, bool, error) {
	if _, removed := view.removed[stableLogicalObligationKey(obligation)]; removed {
		return StableLogicalObligation{}, false, nil
	}
	if existing, found := findStableLogicalObligationIndex(view.index, obligation, work); found {
		return existing, true, nil
	}
	if view.directory == nil {
		return StableLogicalObligation{}, false, nil
	}
	if work != nil {
		work.AggregateMembershipProbes++
	}
	owner, existing, found, err := view.directory.LookupLogical(obligation)
	if err != nil || !found {
		return StableLogicalObligation{}, false, err
	}
	if !bytes.Equal(owner, view.owner) {
		return StableLogicalObligation{}, false, fmt.Errorf("%w: logical directory key has a different physical owner", ErrResourceConflict)
	}
	return existing, true, nil
}

// appendDirectoryDelta retains only additions since the pinned directory. The
// publication builder replaces this delta with its successor root before the
// resulting resource set becomes visible; no directory predecessor is retained.
func (view stableLogicalObligationView) appendDirectoryDelta(values []StableLogicalObligation, work *StableResourceClosureWork) (stableLogicalObligationView, error) {
	index := view.index
	added := make([]StableLogicalObligation, 0, len(values))
	for _, obligation := range values {
		existing, found, err := view.lookup(obligation, work)
		if err != nil {
			return stableLogicalObligationView{}, err
		}
		if found {
			if existing != obligation {
				return stableLogicalObligationView{}, fmt.Errorf("%w: logical directory key has conflicting integrity metadata", ErrResourceConflict)
			}
			continue
		}
		// Values in the same producer batch must also resolve against additions
		// already admitted during this call.
		next, err := insertStableLogicalObligationIndex(index, obligation, work)
		if err != nil {
			return stableLogicalObligationView{}, err
		}
		if next != index {
			added = append(added, obligation)
			index = next
		}
	}
	if len(added) == 0 {
		return view, nil
	}
	commitments := cloneStableLogicalObligationCommitments(view.commitments)
	if commitments == nil {
		commitments = make(map[ReachabilityField]stableLogicalObligationCommitment)
	}
	for _, obligation := range added {
		commitment := commitments[obligation.Reachability]
		commitment.addObligation(obligation)
		commitments[obligation.Reachability] = commitment
	}
	view.tail = &stableLogicalObligationNode{parent: view.tail, values: added}
	view.index = index
	view.count += len(added)
	view.commitments = commitments
	return view, nil
}

// DependencyDirectoryBaseV2 returns the borrowed inherited root for a rebuild,
// including closures with unsealed admitted deltas. It does not certify that
// this root represents the closure; only DependencyDirectoryV2 does that.
func (set *StableResourceSet) DependencyDirectoryBaseV2() (*DependencyDirectoryV2, error) {
	if set == nil {
		return nil, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.physicalOnly || set.Owner() == ResourceOwnerReleased || set.Owner() == ResourceOwnerTransferred {
		return nil, ErrResourceOwnership
	}
	directory := set.emptyDependencyDirectoryLocked()
	var err error
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if inherited := entry.logicalObligations.directory; inherited != nil {
			if directory != nil && directory != inherited {
				err = ErrResourceConflict
				return false
			}
			directory = inherited
		}
		return true
	})
	return directory, err
}
