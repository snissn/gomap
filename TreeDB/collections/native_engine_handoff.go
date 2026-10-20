package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeBuiltDomain is an immutable result in the original captured build
// ledger. name and boundary borrow that ledger's own captured descriptor/root,
// never current metadata. Results are grouped by collection incarnation.
type nativeBuiltDomain struct {
	collection, interpretation uint64
	name                       string
	oldBaseRoot, root          uint64
	boundary                   *memtable.COWRoot
	typedBoundary              *nativeTypedDelta
	typedMeta                  *CollectionMeta
	// empty proves the captured descriptor had no pending physical source.
	// Its unchanged root cannot consume any newer source in latest.
	empty bool
	// physicalOnly is explicit maintenance of an exact captured base; it
	// consumes no source when the captured domain has no pending boundary.
	physicalOnly bool
	// Build-only qualified key/value storage belongs to the original admitted
	// result array. Handoff never samples a current collection name.
	rootKey   string
	rootValue [8]byte
}

// prepareNativeEngineHandoff rebases only captured touched domains onto their
// accepted physical roots. Newer pending effects and source positions survive;
// catalog removal/recreation and changed domain interpretation are preserved.
// allocation spends the original preaccepted maintenance promise. Every partial
// result already belongs to next's original Work, including on error. The lower
// NativeSemanticState wrapper must retain that partial on its original build
// ledger and return nil/error; unlike rollover, the handoff ABI transfers no
// partial state to its caller.
func prepareNativeEngineHandoff(latest *nativeEngineVersion, built []nativeBuiltDomain, collections int, allocation backenddb.NativeSemanticAllocator) (*nativeEngineVersion, error) {
	return prepareNativeEngineHandoffWithCatalog(latest, built, collections, nil, allocation)
}

func prepareNativeEngineHandoffWithCatalog(latest *nativeEngineVersion, built []nativeBuiltDomain, collections int, catalog *nativeBuiltDomain, allocation backenddb.NativeSemanticAllocator) (*nativeEngineVersion, error) {
	if latest == nil || allocation == nil || collections < 0 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	groups := 0
	for i := range built {
		if built[i].collection == 0 || built[i].interpretation == 0 || built[i].name == "" ||
			(built[i].empty && (built[i].boundary != nil || (!built[i].physicalOnly && built[i].root != built[i].oldBaseRoot))) ||
			(!built[i].empty && built[i].boundary == nil) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if i == 0 || built[i-1].collection != built[i].collection {
			groups++
			if i != 0 && built[i-1].collection > built[i].collection {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		} else if built[i-1].name >= built[i].name {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
	}
	if groups > collections {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if catalog != nil && (catalog.collection != 0 || catalog.name != nativeCatalogDomainName || catalog.interpretation != 1 || catalog.root != 0 || catalog.oldBaseRoot != 0 || catalog.boundary == nil || catalog.empty) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if catalog != nil && latest.catalog == nil {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	// Even an empty physical frontier consumes the captured state's original
	// build ledger. A fresh successor shares only immutable directories/catalog,
	// so the next idle checkpoint cannot replay that single-use ledger.
	next, err := newNativeEnginePreparation(latest, collections, len(built), allocation)
	if err != nil {
		return nil, err
	}
	p := next.preparation
	defer p.sealPrivateBuilders(next)
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return allocation.AdmitNativeBytes(n) }
	if latest.catalog != nil {
		if catalog == nil {
			(nativeDomainLifetime{}).Retain(latest.catalog)
			next.catalog = latest.catalog
		} else {
			var scratch cowbtree.OwnedMapCompletion
			next.catalog, scratch, err = prepareNativeDomainHandoff(latest.catalog, catalog, admit)
			next.refund.children.Append(scratch)
			if err != nil {
				return next, err
			}
		}
	}
	if len(built) == 0 {
		retainNativeEngineDirectories(next, latest)
		return next, nil
	}
	p.names, err = cowbtree.NewOwnedMapBuilder(latest.names, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder(latest.collections, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder(latest.pending, collections, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	for start := 0; start < len(built); {
		end := start + 1
		for end < len(built) && built[end].collection == built[start].collection {
			end++
		}
		id := built[start].collection
		c, exists := latest.collections.Get(id)
		if !exists {
			start = end // dropped or recreated names retain their newer incarnation
			continue
		}
		p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(c.domains, end-start, nativeDomainLifetime{}, admit)
		if err != nil {
			return next, err
		}
		p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(c.pending, end-start, nativeDomainLifetime{}, admit)
		if err != nil {
			return next, err
		}
		pendingCount := c.pending.Len()
		for i := start; i < end; i++ {
			result := &built[i]
			current, ok := c.domains.Get(result.name)
			if !ok || current.interpretation != result.interpretation {
				continue
			}
			domain, scratch, e := prepareNativeDomainHandoff(current, result, admit)
			if domain != nil {
				p.ownedDomains = append(p.ownedDomains, domain)
			}
			next.refund.children.Append(scratch)
			if e != nil {
				return next, e
			}
			if err = p.domainBuilder.Set(result.name, domain); err != nil {
				return next, err
			}
			_, wasPending := p.pendingDomainBuilder.Get(result.name)
			if domain.pending != nil || domain.captured != nil {
				err = p.pendingDomainBuilder.Set(result.name, domain)
				if !wasPending {
					pendingCount++
				}
			} else {
				err = p.pendingDomainBuilder.Delete(result.name)
				if wasPending {
					pendingCount--
				}
			}
			if err != nil {
				return next, err
			}
		}
		p.domains = p.domainBuilder.Seal()
		p.domainBuilder = nil
		p.pendingDomains = p.pendingDomainBuilder.Seal()
		p.pendingDomainBuilder = nil
		if built[start].typedMeta != nil {
			physical := built[start].typedMeta
			logical := c.schema.meta
			if !nativeIndexLogicalMetaEqual(*logical, *physical) {
				// The collection lookup already fixed the incarnation. A later
				// logical cut may only have removed an ordered subset of these
				// exact declarations; every other nonphysical field must match.
				compatible := *physical
				compatible.Indexes = logical.Indexes
				compatible = normalizeCollectionIndexedBufferOptions(compatible)
				if !nativeIndexSubsetRemoval(physical.Indexes, logical.Indexes) ||
					!nativeIndexLogicalMetaEqual(*logical, compatible) {
					return next, backenddb.ErrRootPublicationBasisMismatch
				}
			}
			// Admit and deep-copy the captured schema exactly as promised for
			// both handoff successors. Compact only this private owned copy:
			// after the proof above it is the latest logical configuration with
			// exactly the build's four physical metadata outputs. No borrowed
			// latest-schema aliases or additional scratch backing escape here.
			p.schema, err = newNativeSchemaState(physical, admit)
			if err != nil {
				return next, err
			}
			owned := p.schema.meta.Indexes
			n := 0
			for i := range owned {
				if n < len(logical.Indexes) && owned[i].Name == logical.Indexes[n].Name {
					owned[n] = owned[i]
					n++
				}
			}
			clear(owned[n:])
			p.schema.meta.Indexes = owned[:n]
			*p.schema.meta = normalizeCollectionIndexedBufferOptions(*p.schema.meta)
		} else {
			c.schema.retain()
			p.schema = c.schema
		}
		successor, e := newNativeCollectionState(id, p.schema, p.domains, p.pendingDomains, admit)
		if e != nil {
			return next, e
		}
		p.schema, p.domains, p.pendingDomains = nil, nil, nil
		p.ownedCollections = append(p.ownedCollections, successor)
		if err = p.names.Set(c.schema.meta.Name, successor); err != nil {
			return next, err
		}
		if err = p.collections.Set(id, successor); err != nil {
			return next, err
		}
		if pendingCount == 0 {
			err = p.pending.Delete(id)
		} else {
			err = p.pending.Set(id, successor)
		}
		if err != nil {
			return next, err
		}
		start = end
	}
	next.names = p.names.Seal()
	p.names = nil
	next.collections = p.collections.Seal()
	p.collections = nil
	next.pending = p.pending.Seal()
	p.pending = nil
	return next, nil
}

// prepareNativeDomainHandoff adopts every partial into its returned descriptor
// before any later fallible step. The caller must attach it to its original
// Work before inspecting err. The exact captured boundary alone removes prefix
// sources; newer active and captured sources remain independently owned.
func prepareNativeDomainHandoff(current *nativeDomainState, result *nativeBuiltDomain, admit cowbtree.OwnedMapAdmit) (domain *nativeDomainState, scratch cowbtree.OwnedMapCompletion, err error) {
	if current.baseRoot != result.oldBaseRoot {
		return nil, scratch, backenddb.ErrRootPublicationBasisMismatch
	}
	domain, err = newNativeDomainState(current.name, result.root, current.interpretation, current.storagePolicy, nil, admit)
	if err != nil {
		return domain, scratch, err
	}
	domain.typed, err = cloneNativeTypedSuffix(current.typed, result.typedBoundary, admit)
	if err != nil {
		return domain, scratch, err
	}
	if result.empty || current.pending != result.boundary {
		if current.pending != nil && !current.pending.RetainOwned() {
			return domain, scratch, memtable.ErrCOWClosed
		}
		domain.pending = current.pending
		domain.captured, scratch, err = prepareNativeSourceSuffix(current.captured, result.boundary, admit)
	}
	return domain, scratch, err
}
