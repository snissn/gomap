package rootpublication

import (
	"bytes"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

type ownedDependencyDirectoryRecordV2 struct{ key, value []byte }

// Full directory serialization is a real materialization boundary. Reuse the
// constructor-owned canonical manifest/history rather than rebuilding a map of
// inherited owners. Every output allocation remains admitted through the last
// synchronous visitor (including pull-iterator suspension).
func (set *StableResourceSet) walkOwnedDependencyDirectoryRecordsV2(visit func([]byte, []byte) error) error {
	owner := set.MetadataOwner()
	if owner == nil {
		return ErrResourceOwnership
	}
	if diagnostic, handled, err := set.acquireEmptyDirectoryDiagnostics(); handled {
		if err != nil {
			return err
		}
		defer diagnostic.Close()
		// Validate the genuine empty physical directory under the same retained
		// lease. Empty encoded output must not silently bypass its integrity.
		return diagnostic.emptyDirectory.walkBorrowed(owner, nil)
	}
	if set.hasDependencyDirectoryV2() {
		// Full replacement is the actual flatten boundary. The same import core
		// validates each shared directory once and admits the independent full
		// logical history, while retaining its original physical/operation owner.
		flat, err := importStableResourceSetMetadata(owner, set, true)
		if err != nil {
			return err
		}
		defer flat.Release()
		set = flat
	}
	manifest, _, err := set.DependencyManifestV1()
	if err != nil {
		return err
	}
	defer manifest.ReleaseOwnedMetadataV1()
	return manifest.WithEntriesV1(func(entries []DependencyManifestEntryV1) error {
		count := uint64(len(entries))
		charge := uint64(0)
		add := func(n uint64) error { return diagnosticChargeAdd(&charge, n, 1) }
		for _, entry := range entries {
			if uint64(len(entry.LogicalObligations)) > ^uint64(0)-count {
				return retainedalloc.ErrCapacity
			}
			count += uint64(len(entry.LogicalObligations))
			physical := entry
			physical.LogicalObligations = nil
			keySize := uint64(21 + len(entry.Kind) + len(entry.LogicalLane) + len(entry.ResourceID))
			var rids []uint64
			if entry.Frontier.exactRIDs != nil {
				rids = entry.Frontier.exactRIDs.values
			}
			if err := add(keySize); err != nil {
				return err
			}
			if err := add(uint64(dependencyManifestEntrySizeV1(physical, rids))); err != nil {
				return err
			}
			for _, obligation := range entry.LogicalObligations {
				if err := dependencyLogicalOwnerV2(physical, obligation); err != nil {
					return err
				}
				logicalKeySize := uint64(57 + len(obligation.Class) + len(obligation.Kind) + len(obligation.Namespace) + len(obligation.Reachability))
				if err := add(logicalKeySize); err != nil {
					return err
				}
				if err := add(40 + keySize); err != nil {
					return err
				}
			}
		}
		if count > uint64(^uint(0)>>1) {
			return retainedalloc.ErrCapacity
		}
		if err := diagnosticChargeAdd(&charge, count, uint64(unsafe.Sizeof(ownedDependencyDirectoryRecordV2{}))); err != nil {
			return err
		}
		if err := owner.AddPending(charge); err != nil {
			return err
		}
		records := make([]ownedDependencyDirectoryRecordV2, 0, int(count))
		defer func() { clear(records); records = nil; owner.RemovePending(charge) }()
		for _, entry := range entries {
			physical := entry
			physical.LogicalObligations = nil
			key := DependencyPhysicalKeyV2(physical)
			var rids []uint64
			if physical.Frontier.exactRIDs != nil {
				rids = physical.Frontier.exactRIDs.values
			}
			// Entries are already normalized by the retained immutable manifest.
			// Consume its exact RID slice; public RIDs() would allocate another copy.
			value := appendDependencyManifestEntryV1(make([]byte, 0, dependencyManifestEntrySizeV1(physical, rids)), physical, rids)
			records = append(records, ownedDependencyDirectoryRecordV2{key, value})
			for _, obligation := range entry.LogicalObligations {
				value, err := EncodeDependencyLogicalV2(key, obligation)
				if err != nil {
					return err
				}
				records = append(records, ownedDependencyDirectoryRecordV2{DependencyLogicalKeyV2(obligation), value})
			}
		}
		slices.SortFunc(records, func(a, b ownedDependencyDirectoryRecordV2) int { return bytes.Compare(a.key, b.key) })
		for i, record := range records {
			if i != 0 && bytes.Equal(records[i-1].key, record.key) {
				return ErrResourceConflict
			}
		}
		for _, record := range records {
			if err := visit(record.key, record.value); err != nil {
				return err
			}
		}
		return nil
	})
}
