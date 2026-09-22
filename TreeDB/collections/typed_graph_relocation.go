package collections

import (
	"maps"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Relocation joins existing admission only at the committed index cutover. Busy
// typed-publication work defers vacuum; it is never cancelled by this hook.
func (d *collectionDBSchemaCoordinators) prepareRootRelocation(root string) backenddb.CollectionRootRelocationPrepare {
	return func(snap *backenddb.Snapshot, nextPager *pager.Pager, roots map[uint64]uint64) (func(bool), error) {
		var releases []func()
		release := func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
		busy := func() (func(bool), error) { release(); return nil, rootpublication.ErrResourcePinned }
		// This also pairs read-owner snapshots with immutable publication state and
		// excludes Ensure's final readiness CAS after its potentially slow warmup.
		unlockStorage, ok := tryVectorPartitionStorageBarrier(root)
		if !ok {
			return busy()
		}
		releases = append(releases, unlockStorage)
		// Coordinator membership cannot grow while its publication owners are gated.
		if !d.mu.TryLock() {
			return busy()
		}
		releases = append(releases, d.mu.Unlock)
		for _, coord := range d.collections {
			// Ordinary schema backfills have no derived typed authority to rebind;
			// they already retry captured plans after a real pager replacement.
			// Holding the storage barrier first prevents a nil publication from
			// becoming healthy while this cutover skips its admission gates.
			if coord.typedPublication.Load() == nil {
				continue
			}
			// Schema admission spans synchronous prepare through publication install.
			if !coord.schemaMu.TryLock() {
				return busy()
			}
			releases = append(releases, coord.schemaMu.Unlock)
			// Background buffered publication instead holds native admission.
			if !coord.nativeVectorAdmissionMu.TryLock() {
				return busy()
			}
			releases = append(releases, coord.nativeVectorAdmissionMu.Unlock)
			// Receipt invalidation is allowed outside schema admission.
			if !coord.typedPublicationDebtMu.TryLock() {
				return busy()
			}
			releases = append(releases, coord.typedPublicationDebtMu.Unlock)
			if !coord.domainsMu.TryLock() {
				return busy()
			}
			releases = append(releases, coord.domainsMu.Unlock)
			for domain := range coord.domains {
				// A queued async candidate can own captured coordinates before entering
				// native admission. Preserve that accepted work and defer this cutover.
				if !domain.indexedAsyncMu.TryLock() {
					return busy()
				}
				releases = append(releases, domain.indexedAsyncMu.Unlock)
				if domain.indexedAsyncRun {
					return busy()
				}
			}
		}
		replacements := make(map[*collectionSchemaCoordinator]*typedGraphPublicationState)
		for name, coord := range d.collections {
			before := coord.typedPublication.Load()
			if before == nil || before.invalid {
				continue
			}
			catalog, err := loadCollectionCatalog(snap, name)
			if err != nil {
				release()
				return nil, err
			}
			// Mapping a still-reachable obsolete root does not certify logical currency.
			// Leave stale authority untouched so the ordinary mismatch guard rejects it.
			if !before.matches(catalog) {
				continue
			}
			next := *before
			next.catalog, err = relocateTypedGraphCatalog(before.catalog, nextPager, roots)
			if err != nil {
				release()
				return nil, err
			}
			if before.servingBase != nil {
				base := *before.servingBase
				alias := catalog.typedGraphBase
				if alias == nil || base.view.Catalog == nil ||
					!collectionMetaValuesEqual(base.view.Catalog.meta, alias.meta) ||
					!maps.Equal(base.view.Catalog.roots, alias.roots) ||
					base.materializerView.Catalog == nil ||
					!collectionMetaValuesEqual(base.materializerView.Catalog.meta, alias.meta) ||
					!maps.Equal(base.materializerView.Catalog.roots, alias.roots) {
					continue
				}
				base.view.Catalog, err = relocateTypedGraphCatalog(base.view.Catalog, nextPager, roots)
				if err != nil {
					release()
					return nil, err
				}
				base.materializerView.Catalog, err = relocateTypedGraphCatalog(base.materializerView.Catalog, nextPager, roots)
				if err != nil {
					release()
					return nil, err
				}
				for _, view := range []*columnPhysicalScanSnapshotView{&base.view, &base.materializerView} {
					if view.Diagnostics.ManifestRoot != 0 {
						relocated, ok := roots[view.Diagnostics.ManifestRoot]
						if !ok {
							release()
							return nil, ErrVectorIndexSnapshotMismatch
						}
						view.Diagnostics.ManifestRoot = relocated
					}
				}
				next.servingBase = &base
			}
			replacements[coord] = &next
		}
		return func(committed bool) {
			defer release()
			if committed {
				// Every installer is excluded until the backend snapshot and these derived
				// coordinates agree. Existing readers retain the untouched old objects.
				for coord, next := range replacements {
					coord.typedPublication.Store(next)
				}
			}
		}, nil
	}
}

func relocateTypedGraphCatalog(old *collectionCatalog, nextPager *pager.Pager, roots map[uint64]uint64) (*collectionCatalog, error) {
	if old == nil || len(old.rootOverlays) != 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	remap := func(oldRoots map[string]uint64) (map[string]uint64, error) {
		out := make(map[string]uint64, len(oldRoots))
		for name, oldRoot := range oldRoots {
			if oldRoot == 0 {
				out[name] = 0
				continue
			}
			next, ok := roots[oldRoot]
			if !ok {
				return nil, ErrVectorIndexSnapshotMismatch
			}
			out[name] = next
		}
		return out, nil
	}
	next := *old
	var err error
	next.roots, err = remap(old.roots)
	if err != nil {
		return nil, err
	}
	next.pager = nextPager
	if old.typedGraphBase != nil {
		base := *old.typedGraphBase
		base.roots, err = remap(base.roots)
		if err != nil {
			return nil, err
		}
		next.typedGraphBase = &base
	}
	return &next, nil
}
