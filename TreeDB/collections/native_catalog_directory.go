package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// newNativeEmptyEngine is the empty physical-catalog constructor. It does not
// select a backend snapshot or publish. The opening complete-cut owner must
// prove the captured physical catalog is empty before choosing this path.
// A partial result belongs to Release on the existing worker, even on error.
func newNativeEmptyEngine(allocator, maintenance backenddb.NativeSemanticAllocator) (*nativeEngineVersion, error) {
	if allocator == nil || maintenance == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	charge, err := nativeEnginePreparationCharge(0, 0)
	if err != nil {
		return nil, err
	}
	lease, err := allocator.AdmitNativeBytes(charge)
	if err != nil {
		return nil, err
	}
	e := &nativeEngineVersion{nextIncarnation: 1, allocator: allocator, maintenance: maintenance, refund: &nativeEngineRefund{lease: lease}, preparation: &nativeEnginePreparation{}}
	e.refs.Store(1)
	e.cleanup.Work = e
	p := e.preparation
	defer p.sealPrivateBuilders(e)
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return allocator.AdmitNativeBytes(n) }
	p.names, err = cowbtree.NewOwnedMapBuilder[string, *nativeCollectionState](nil, 0, nativeCollectionLifetime{}, admit)
	if err != nil {
		return e, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder[uint64, *nativeCollectionState](nil, 0, nativeCollectionLifetime{}, admit)
	if err != nil {
		return e, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder[uint64, *nativeCollectionState](nil, 0, nativeCollectionLifetime{}, admit)
	return e, err
}

// prepareNativeCatalogReplacement applies the original planner's complete
// catalog effects and directory replacement to one private successor. It is
// shared by create/drop/rename/schema plans. Effects include all actual system
// keys (metadata, identity/counter and removed/renamed descriptors); this helper
// does not infer them from a live manager or perform singleton publication.
//
// old is the exact captured descriptor, or nil for create. replacement is
// borrowed from that original plan and retained into this Work before the next
// fallible step. Create consumes exactly the current counter; rename/schema
// replacement preserves incarnation, and drop never decrements the counter.
func prepareNativeCatalogReplacement(base *nativeEngineVersion, old, replacement *nativeCollectionState, effects []memtable.COWMutation) (*nativeEngineVersion, error) {
	if base == nil || base.names == nil || base.collections == nil || base.pending == nil || len(effects) == 0 || (old == nil && replacement == nil) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	nextIncarnation := base.nextIncarnation
	if old != nil {
		byName, named := base.names.Get(old.schema.meta.Name)
		byID, identified := base.collections.Get(old.incarnation)
		if !named || !identified || byName != old || byID != old {
			return nil, backenddb.ErrRootPublicationBasisMismatch
		}
	}
	if replacement != nil {
		if replacement.schema == nil || replacement.schema.meta == nil || replacement.domains == nil || replacement.pending == nil {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if c, exists := base.names.Get(replacement.schema.meta.Name); exists && c != old {
			return nil, backenddb.ErrRootPublicationBasisMismatch
		}
		if old == nil {
			if nextIncarnation == 0 || nextIncarnation == ^uint64(0) || replacement.incarnation != nextIncarnation {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
			if _, exists := base.collections.Get(replacement.incarnation); exists {
				return nil, backenddb.ErrRootPublicationBasisMismatch
			}
			nextIncarnation++
		} else if old.incarnation != replacement.incarnation {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
	}
	if !nativeCatalogReplacementEffectsValid(old, replacement, nextIncarnation, effects) {
		return nil, errNativeCatalogIdentity
	}
	next, err := newNativeEnginePreparation(base, 1, 0, base.allocator)
	if err != nil {
		return nil, err
	}
	p := next.preparation
	defer p.sealPrivateBuilders(next)
	if replacement != nil {
		(nativeCollectionLifetime{}).Retain(replacement)
		p.ownedCollections = append(p.ownedCollections, replacement)
	}
	next.nextIncarnation = nextIncarnation
	next.catalog, err = prepareNativeCatalogUpdate(base.catalog, effects, base.allocator)
	if err != nil {
		return next, err
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	// Rename removes one name and adds one. The other directories mutate one
	// incarnation, so their cohorts need only one actual mutation each.
	names := 1
	if old != nil && replacement != nil && old.schema.meta.Name != replacement.schema.meta.Name {
		names = 2
	}
	p.names, err = cowbtree.NewOwnedMapBuilder(base.names, names, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder(base.collections, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder(base.pending, 1, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	if old != nil && (replacement == nil || old.schema.meta.Name != replacement.schema.meta.Name) {
		if err = p.names.Delete(old.schema.meta.Name); err != nil {
			return next, err
		}
	}
	if replacement == nil {
		if err = p.collections.Delete(old.incarnation); err != nil {
			return next, err
		}
		err = p.pending.Delete(old.incarnation)
		return next, err
	}
	if err = p.names.Set(replacement.schema.meta.Name, replacement); err != nil {
		return next, err
	}
	if err = p.collections.Set(replacement.incarnation, replacement); err != nil {
		return next, err
	}
	if replacement.pending.Len() == 0 {
		err = p.pending.Delete(replacement.incarnation)
	} else {
		err = p.pending.Set(replacement.incarnation, replacement)
	}
	return next, err
}

// The original planner supplies validated schema JSON and exact root effects.
// This final identity check is independent of that planner: directory identity
// and its durable name/counter records cannot diverge before allocation. The
// source constructor preserves the ordered effects' latest value and owns all
// input bytes. The checks below follow that same order for repeated name keys.
func nativeCatalogReplacementEffectsValid(old, replacement *nativeCollectionState, nextIncarnation uint64, effects []memtable.COWMutation) bool {
	oldRemoved := old == nil || (replacement != nil && old.schema.meta.Name == replacement.schema.meta.Name)
	oldMeta, oldIdentity := oldRemoved, oldRemoved
	newMeta, newIdentity := replacement == nil, replacement == nil
	counter := false
	for _, effect := range effects {
		if effect.Remove || effect.Flags&node.FlagPointer != 0 {
			return false
		}
		deleted := effect.Flags&node.FlagTombstone != 0
		if string(effect.Key) == nativeCatalogNextIncarnationKey {
			value, err := nativeCatalogIdentityValue(effect.Value, true)
			if deleted || err != nil || value != nextIncarnation {
				return false
			}
			counter = true
		}
		if !oldRemoved {
			if nativeCatalogNameKey(effect.Key, systemCollectionMetaPrefix, old.schema.meta.Name) {
				oldMeta = deleted
			}
			if nativeCatalogNameKey(effect.Key, nativeCatalogIncarnationPrefix, old.schema.meta.Name) {
				oldIdentity = deleted
			}
		}
		if replacement != nil {
			if nativeCatalogNameKey(effect.Key, systemCollectionMetaPrefix, replacement.schema.meta.Name) {
				newMeta = !deleted && len(effect.Value) != 0
			}
			if nativeCatalogNameKey(effect.Key, nativeCatalogIncarnationPrefix, replacement.schema.meta.Name) {
				value, err := nativeCatalogIdentityValue(effect.Value, false)
				newIdentity = !deleted && err == nil && value == replacement.incarnation
			}
		}
	}
	return counter && oldMeta && oldIdentity && newMeta && newIdentity
}

func nativeCatalogNameKey(key []byte, prefix, name string) bool {
	return len(key) >= len(prefix) && len(key)-len(prefix) == len(name) && string(key[:len(prefix)]) == prefix && string(key[len(prefix):]) == name
}
