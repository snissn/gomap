package rootpublication

import (
	"bytes"
	"os"
)

// RecoverDependencyDirectoryV2 admits physical handles before streaming their
// logical records. Only physical entries and per-field counts/commitments are
// retained; the directory's global logical keys enforce uniqueness. Callbacks
// validate host storage, while this function binds exact directory ownership.
func RecoverDependencyDirectoryV2(directory *DependencyDirectoryV2, admit func(DependencyManifestEntryV1) (*StableResourceSet, error), validate func(*os.File, DependencyManifestEntryV1, StableLogicalObligation) error) (*StableResourceSet, error) {
	if directory == nil || admit == nil || validate == nil {
		return nil, ErrResourceOwnership
	}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	type recoveredEntry struct {
		index    int
		physical DependencyManifestEntryV1
	}
	owners := make(map[string]recoveredEntry)
	err := directory.Walk(func(key, value []byte) error {
		if key[0] == dependencyPhysicalKeyV2 {
			physical, err := DecodeDependencyPhysicalV2(key, value)
			if err != nil {
				return err
			}
			admitted, err := admit(physical)
			if err != nil {
				admitted.Release()
				return err
			}
			if admitted == nil {
				return ErrUnresolvedResource
			}
			defer admitted.Release()
			admitted.mu.Lock()
			defer admitted.mu.Unlock()
			if admitted.physicalOnly || admitted.Owner() == ResourceOwnerReleased || admitted.Owner() == ResourceOwnerTransferred {
				return ErrResourceOwnership
			}
			var cloned stableResourceEntry
			count := 0
			admitted.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
				count++
				if count != 1 || entry.logicalObligations.count != 0 || entry.frontier.Bytes < physical.Frontier.Bytes || entry.frontier.MaxLSN < physical.Frontier.MaxLSN {
					err = ErrResourceConflict
					return false
				}
				exact := *entry
				exact.frontier = cloneDurableFrontier(physical.Frontier)
				actual := dependencyManifestEntryV1FromStableResourceEntry(exact)
				encoded, encodeErr := EncodeDependencyPhysicalV2(actual)
				if encodeErr != nil || !bytes.Equal(encoded, value) {
					err = ErrResourceConflict
					return false
				}
				cloned, err = cloneStableResourceEntryDirectoryV2(&exact, directory)
				return err == nil
			})
			if err != nil || count != 1 {
				if cloned.token != nil {
					cloned.token.releaseFrom(ResourceOwnerBuilder)
				}
				if err != nil {
					return err
				}
				return ErrResourceConflict
			}
			cloned.logicalObligations = stableLogicalObligationView{directory: directory, owner: bytes.Clone(key), commitments: make(map[ReachabilityField]stableLogicalObligationCommitment)}
			owners[string(key)] = recoveredEntry{index: len(builder.entries), physical: physical}
			builder.entries = append(builder.entries, cloned)
			return nil
		}
		owner, obligation, err := DecodeDependencyLogicalV2(key, value)
		if err != nil {
			return err
		}
		item, found := owners[string(owner)]
		if !found {
			return ErrUnresolvedResource
		}
		entry := &builder.entries[item.index]
		if err := entry.token.WithPinnedFile(func(file *os.File) error { return validate(file, item.physical, obligation) }); err != nil {
			return err
		}
		view := &entry.logicalObligations
		if view.count == int(^uint(0)>>1) {
			return ErrDependencyManifestFormat
		}
		view.count++
		commitment := view.commitments[obligation.Reachability]
		commitment.addObligation(obligation)
		view.commitments[obligation.Reachability] = commitment
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, entry := range builder.entries {
		switch entry.token.kind {
		case ResourceColumnAsset, ResourceTypedColumnAsset, ResourceVectorGraphPack, ResourceDictionary, ResourceTemplate:
			if entry.logicalObligations.count == 0 {
				return nil, ErrUnresolvedResource
			}
		}
	}
	resources, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	if len(owners) == 0 {
		if err := directory.Retain(); err != nil {
			resources.Release()
			return nil, err
		}
		resources.emptyDirectory = directory
	}
	return resources, nil
}
