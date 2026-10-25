package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Qualified key bytes are copied once from the captured schema. They and their
// result-array value cells belong to the original build floor before any root
// materializes. No current catalog/name or physical CommitSeq is sampled.
func nativeEngineRootDescriptorKeyCharge(captured *nativeEngineVersion) (uint64, error) {
	return nativeEngineRootDescriptorKeyChargeForCompaction(captured, 0)
}
func nativeEngineRootDescriptorKeyChargeForCompaction(captured *nativeEngineVersion, target uint64) (total uint64, err error) {
	scanNativeBuildCollections(captured, target, func(id uint64, c *nativeCollectionState) bool {
		domainSet := c.pending
		if id == target {
			domainSet = c.domains
		}
		domainSet.Scan(func(name string, _ *nativeDomainState) bool {
			maxInt := int(^uint(0) >> 1)
			if len(c.schema.meta.Name) > maxInt-len(systemCollectionRootPrefix)-1 || len(name) > maxInt-len(systemCollectionRootPrefix)-1-len(c.schema.meta.Name) {
				err = memtable.ErrCOWCapacity
				return false
			}
			charge := memtable.COWAllocationCharge(uint64(len(systemCollectionRootPrefix) + len(c.schema.meta.Name) + 1 + len(name)))
			if charge > ^uint64(0)-total {
				err = memtable.ErrCOWCapacity
				return false
			}
			total += charge
			if name == "primary" {
				charge = memtable.COWAllocationCharge(uint64(len(systemCollectionDocumentGenerationPrefix) + len(c.schema.meta.Name)))
				if charge > ^uint64(0)-total {
					err = memtable.ErrCOWCapacity
					return false
				}
				total += charge
			}
			return true
		})
		return err == nil
	})
	return total, err
}

// Incarnation order need not equal physical key order. Each collection's
// relative domain names are already ordered; flushing at collection boundaries
// preserves sorted bounded deltas without a catalog-sized sort or index. Later
// chunks update the SAME group's private system root. The first domain's
// preadmitted scratch is reused only after all domain materialization returns.
func (b *nativeEngineBuild) materializeRootDescriptors(group nativeOrderedRootBuilder) error {
	if len(b.built) == 0 {
		return nil
	}
	system, ok := group.(nativeSystemRootBuilder)
	if !ok || len(b.domains) != len(b.built) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	delta := b.domains[0].delta
	defer delta.Reset() // scrub all borrowed result aliases, including on error
	flush := func() error {
		if delta.IsEmpty() {
			return nil
		}
		err := system.BuildNativeSystemDelta(delta)
		delta.Reset()
		return err
	}
	for i := range b.built {
		result := &b.built[i]
		if i != 0 && result.collection != b.built[i-1].collection {
			if err := flush(); err != nil {
				return err
			}
		}
		if err := delta.AppendViewTrustedSortedUnique(nativeBorrowedStringBytes(result.rootKey), result.rootValue[:]); err != nil {
			return err
		}
		if result.generationKey != "" {
			if err := flush(); err != nil {
				return err
			}
			if err := delta.AppendViewTrustedSortedUnique(nativeBorrowedStringBytes(result.generationKey), result.generationValue[:]); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
		}
		if delta.Len() == nativeBuildBatchEntries {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}
