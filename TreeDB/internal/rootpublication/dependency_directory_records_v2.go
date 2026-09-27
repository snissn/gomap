package rootpublication

import (
	"bytes"
	"sort"
)

// WalkDependencyDirectoryRecordsV2 streams a complete directory in encoded key
// order, including admitted additions, removals and physical descriptor changes.
// Rebuilds use this instead of materializing inherited logical obligations. Only
// physical descriptors and the current producer delta are retained as scratch.
func WalkDependencyDirectoryRecordsV2(source *StableResourceSet, visit func(key, value []byte) error) error {
	if source == nil || source.physicalOnly || visit == nil {
		return ErrResourceOwnership
	}
	owned, _, err := cloneStableResourceSetKindView(source)
	if err != nil {
		return err
	}
	defer owned.Release()
	type record struct{ key, value []byte }
	type ownerView struct {
		physical DependencyManifestEntryV1
		logical  stableLogicalObligationView
	}
	owners := make(map[string]ownerView)
	var physicalRecords, additions []record
	var expectedLogical uint64
	directory := owned.emptyDirectory
	owned.rangeEntries(func(entry *stableResourceEntry) bool {
		if inherited := entry.logicalObligations.directory; inherited != nil {
			if directory != nil && directory != inherited {
				err = ErrResourceConflict
				return false
			}
			directory = inherited
		}
		physical := *entry
		physical.logicalObligations = stableLogicalObligationView{}
		descriptor := dependencyManifestEntryV1FromStableResourceEntry(physical)
		key := DependencyPhysicalKeyV2(descriptor)
		if _, duplicate := owners[string(key)]; duplicate {
			err = ErrResourceConflict
			return false
		}
		value, encodeErr := EncodeDependencyPhysicalV2(descriptor)
		if encodeErr != nil {
			err = encodeErr
			return false
		}
		owners[string(key)] = ownerView{descriptor, entry.logicalObligations}
		physicalRecords = append(physicalRecords, record{key, value})
		if entry.logicalObligations.count < 0 || uint64(entry.logicalObligations.count) > ^uint64(0)-expectedLogical {
			err = ErrDependencyManifestFormat
			return false
		}
		expectedLogical += uint64(entry.logicalObligations.count)
		entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			if err = dependencyLogicalOwnerV2(descriptor, obligation); err != nil {
				return false
			}
			value, encodeErr := EncodeDependencyLogicalV2(key, obligation)
			if encodeErr != nil {
				err = encodeErr
				return false
			}
			additions = append(additions, record{DependencyLogicalKeyV2(obligation), value})
			return true
		})
		return err == nil
	})
	if err != nil {
		return err
	}
	if _, _, err := WalkDependencyDirectoryChangesV2(owned, directory, func(_, _ []byte, _ bool) error { return nil }); err != nil {
		return err
	}
	sort.Slice(physicalRecords, func(i, j int) bool { return bytes.Compare(physicalRecords[i].key, physicalRecords[j].key) < 0 })
	sort.Slice(additions, func(i, j int) bool { return bytes.Compare(additions[i].key, additions[j].key) < 0 })
	for i := 1; i < len(additions); i++ {
		if bytes.Equal(additions[i-1].key, additions[i].key) {
			return ErrResourceConflict
		}
	}
	for _, physical := range physicalRecords {
		if err := visit(physical.key, physical.value); err != nil {
			return err
		}
	}
	var emitted uint64
	emit := func(key, value []byte) error {
		if emitted >= expectedLogical {
			return ErrDependencyManifestFormat
		}
		emitted++
		return visit(key, value)
	}
	position := 0
	if directory != nil {
		err = directory.Walk(func(key, value []byte) error {
			if key[0] == dependencyPhysicalKeyV2 {
				return nil
			}
			owner, obligation, err := DecodeDependencyLogicalV2(key, value)
			if err != nil {
				return err
			}
			view, retained := owners[string(owner)]
			if !retained {
				return nil
			}
			if removed, exists := view.logical.removed[stableLogicalObligationKey(obligation)]; exists {
				if removed != obligation {
					return ErrResourceConflict
				}
				return nil
			}
			if view.logical.directory != directory {
				return ErrResourceConflict
			}
			if err := dependencyLogicalOwnerV2(view.physical, obligation); err != nil {
				return err
			}
			for position < len(additions) && bytes.Compare(additions[position].key, key) < 0 {
				if err := emit(additions[position].key, additions[position].value); err != nil {
					return err
				}
				position++
			}
			if position < len(additions) && bytes.Equal(additions[position].key, key) {
				if !bytes.Equal(additions[position].value, value) {
					return ErrResourceConflict
				}
				position++
			}
			return emit(key, value)
		})
		if err != nil {
			return err
		}
	}
	for position < len(additions) {
		if err := emit(additions[position].key, additions[position].value); err != nil {
			return err
		}
		position++
	}
	if emitted != expectedLogical {
		return ErrDependencyManifestFormat
	}
	return nil
}
