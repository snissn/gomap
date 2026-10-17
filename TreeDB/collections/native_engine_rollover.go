package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// prepareNativeEngineRollover prepares one complete immutable successor. The
// caller adopts its returned edge BEFORE checking err. Partial failure belongs
// only to Release on the original worker, after publication gates are released.
// Catalog/logical identities and history sequence are unchanged.
func prepareNativeEngineRollover(base *nativeEngineVersion) (*nativeEngineVersion, error) {
	if base == nil || base.pending == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	collections, domains := 0, 0
	base.pending.Scan(func(_ uint64, c *nativeCollectionState) bool {
		touched := 0
		c.pending.Scan(func(_ string, d *nativeDomainState) bool {
			if d.pending != nil && d.pending.Len() != 0 {
				touched++
			}
			return true
		})
		if touched != 0 {
			collections++
			if touched > int(^uint(0)>>1)-domains {
				domains = -1
				return false
			}
			domains += touched
		}
		return true
	})
	if domains < 0 {
		return nil, memtable.ErrCOWCapacity
	}
	if domains == 0 {
		base.Retain()
		return base, nil
	}
	next, err := newNativeEnginePreparation(base, collections, domains, base.maintenance)
	if err != nil {
		return nil, err
	}
	p := next.preparation
	defer p.sealPrivateBuilders(next)
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.maintenance.AdmitNativeBytes(n) }
	p.names, err = cowbtree.NewOwnedMapBuilder(base.names, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder(base.collections, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder(base.pending, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	base.pending.Scan(func(id uint64, c *nativeCollectionState) bool {
		touched := 0
		c.pending.Scan(func(_ string, d *nativeDomainState) bool {
			if d.pending != nil && d.pending.Len() != 0 {
				touched++
			}
			return true
		})
		if touched == 0 {
			return true
		}
		p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(c.domains, touched, nativeDomainLifetime{}, admit)
		if err != nil {
			return false
		}
		p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(c.pending, touched, nativeDomainLifetime{}, admit)
		if err != nil {
			return false
		}
		c.pending.Scan(func(name string, d *nativeDomainState) bool {
			if d.pending == nil || d.pending.Len() == 0 {
				return true
			}
			var successor *nativeDomainState
			successor, err = prepareNativeDomainRollover(d, admit)
			// Original sparse planner edge is attached before inspecting the result.
			if successor != nil {
				p.ownedDomains = append(p.ownedDomains, successor)
			}
			if err != nil {
				return false
			}
			if err = p.domainBuilder.Set(name, successor); err != nil {
				return false
			}
			err = p.pendingDomainBuilder.Set(name, successor)
			return err == nil
		})
		if err != nil {
			return false
		}
		p.domains = p.domainBuilder.Seal()
		p.domainBuilder = nil
		p.pendingDomains = p.pendingDomainBuilder.Seal()
		p.pendingDomainBuilder = nil
		c.schema.retain()
		p.schema = c.schema
		var successor *nativeCollectionState
		successor, err = newNativeCollectionState(id, p.schema, p.domains, p.pendingDomains, admit)
		if err != nil {
			return false
		}
		p.schema, p.domains, p.pendingDomains = nil, nil, nil
		p.ownedCollections = append(p.ownedCollections, successor)
		if err = p.names.Set(c.schema.meta.Name, successor); err != nil {
			return false
		}
		if err = p.collections.Set(id, successor); err != nil {
			return false
		}
		err = p.pending.Set(id, successor)
		return err == nil
	})
	if err != nil {
		return next, err
	}
	next.names = p.names.Seal()
	p.names = nil
	next.collections = p.collections.Seal()
	p.collections = nil
	next.pending = p.pending.Seal()
	p.pending = nil
	return next, nil
}
