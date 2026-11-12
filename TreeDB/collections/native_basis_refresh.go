package collections

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This synchronous scope borrows both original Snapshots. Separate point slots
// preserve old descriptor aliases while the corresponding new descriptor is
// read. No row scan, snapshot capture, build ledger or accepted receipt exists.
type nativeBasisRefreshScratch struct {
	before, after             *backenddb.Snapshot
	allocator                 backenddb.NativeSemanticAllocator
	leaves                    backenddb.NativeMaintenanceLeafReader
	beforeReader, afterReader backenddb.SnapshotRootReader
	beforeRoot, afterRoot     bool
	key                       [page.PageSize]byte
	beforeKey, afterKey       [page.PageSize]byte
	beforeLeaf, afterLeaf     [page.PageSize]byte
}

func (s *nativeBasisRefreshScratch) readBeforeLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeMaintenanceSnapshotLeaf(s.before, s.allocator, ptr, dst, s.leaves)
}
func (s *nativeBasisRefreshScratch) readAfterLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeMaintenanceSnapshotLeaf(s.after, s.allocator, ptr, dst, s.leaves)
}
func (s *nativeBasisRefreshScratch) get(key []byte, after bool) ([]byte, bool, error) {
	var entry node.LeafEntry
	var err error
	if after {
		if !s.afterRoot {
			return nil, false, nil
		}
		entry, err = s.afterReader.GetEntryExactWithFixedScratch(key, s.afterKey[:], s.afterLeaf[:], s.readAfterLeaf)
	} else {
		if !s.beforeRoot {
			return nil, false, nil
		}
		entry, err = s.beforeReader.GetEntryExactWithFixedScratch(key, s.beforeKey[:], s.beforeLeaf[:], s.readBeforeLeaf)
	}
	if errors.Is(err, tree.ErrKeyNotFound) || (err == nil && entry.Flags&node.FlagTombstone != 0) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("basis-refresh guard=leaf-read after=%t: %w", after, err)
	}
	if entry.Flags&node.FlagPointer != 0 {
		return nil, false, backenddb.ErrNativeSemanticUnsupported
	}
	return entry.Value, true, nil
}
func (s *nativeBasisRefreshScratch) makeKey(prefix, collection, domain string) ([]byte, error) {
	n := len(prefix)
	if n > len(s.key) || len(collection) > len(s.key)-n {
		return nil, memtable.ErrCOWCapacity
	}
	n += len(collection)
	if domain != "" {
		if n == len(s.key) || len(domain) > len(s.key)-n-1 {
			return nil, memtable.ErrCOWCapacity
		}
		n += 1 + len(domain)
	}
	if n > len(s.key) {
		return nil, memtable.ErrCOWCapacity
	}
	key := append(s.key[:0], prefix...)
	key = append(key, collection...)
	if domain != "" {
		key = append(key, '/')
		key = append(key, domain...)
	}
	return key, nil
}
func (s *nativeBasisRefreshScratch) unchanged(prefix, collection string) (raw []byte, found bool, err error) {
	key, err := s.makeKey(prefix, collection, "")
	if err != nil {
		return nil, false, err
	}
	old, oldFound, err := s.get(key, false)
	if err != nil {
		return nil, false, err
	}
	current, currentFound, err := s.get(key, true)
	if err != nil {
		return nil, false, err
	}
	presenceChanged := oldFound != currentFound
	if presenceChanged || !bytes.Equal(old, current) {
		return nil, false, fmt.Errorf("basis-refresh guard=catalog-equivalence key_class=%q presence_changed=%t before_found=%t after_found=%t before_bytes=%d after_bytes=%d: %w", prefix, presenceChanged, oldFound, currentFound, len(old), len(current), backenddb.ErrRootPublicationBasisMismatch)
	}
	return current, currentFound, nil
}
func (s *nativeBasisRefreshScratch) collection(c *nativeCollectionState) error {
	_, metadataFound, err := s.unchanged(systemCollectionMetaPrefix, c.schema.meta.Name)
	if err != nil {
		return err
	}
	raw, identityFound, err := s.unchanged(nativeCatalogIncarnationPrefix, c.schema.meta.Name)
	if err != nil {
		return err
	}
	if identityFound != metadataFound {
		return fmt.Errorf("basis-refresh guard=metadata-incarnation-presence metadata_found=%t identity_found=%t: %w", metadataFound, identityFound, backenddb.ErrRootPublicationBasisMismatch)
	}
	if identityFound {
		id, err := nativeCatalogIdentityValue(raw, false)
		if err != nil {
			return fmt.Errorf("basis-refresh guard=incarnation-decode bytes=%d: %w", len(raw), err)
		}
		incarnation := c.incarnation
		if id != incarnation {
			return fmt.Errorf("basis-refresh guard=incarnation-identity decoded=%d semantic=%d: %w", id, incarnation, backenddb.ErrRootPublicationBasisMismatch)
		}
	}
	return nil
}
func (s *nativeBasisRefreshScratch) root(c *nativeCollectionState, d *nativeDomainState) (uint64, error) {
	key, err := s.makeKey(systemCollectionRootOverlayPrefix, c.schema.meta.Name, d.name)
	if err != nil {
		return 0, err
	}
	for _, after := range [2]bool{false, true} {
		if _, found, err := s.get(key, after); err != nil {
			return 0, err
		} else if found {
			return 0, fmt.Errorf("basis-refresh guard=domain-overlay-presence after=%t: %w", after, backenddb.ErrNativeSemanticUnsupported)
		}
	}
	key, err = s.makeKey(systemCollectionRootPrefix, c.schema.meta.Name, d.name)
	if err != nil {
		return 0, err
	}
	old, found, err := s.get(key, false)
	if err != nil {
		return 0, err
	}
	oldRoot := uint64(0)
	if found {
		oldRoot, err = decodeRootID(old)
		if err != nil {
			return 0, err
		}
	}
	catalogRoot := d.catalogRoot
	if oldRoot != catalogRoot {
		return 0, fmt.Errorf("basis-refresh guard=old-domain-root decoded=%d semantic_catalog=%d: %w", oldRoot, catalogRoot, backenddb.ErrRootPublicationBasisMismatch)
	}
	current, found, err := s.get(key, true)
	if err != nil {
		return 0, err
	}
	if !found {
		if oldRoot != 0 {
			return 0, fmt.Errorf("basis-refresh guard=disappearing-domain-root before=%d after_found=%t: %w", oldRoot, found, backenddb.ErrRootPublicationBasisMismatch)
		}
		return 0, nil // a purely unsealed collection/domain remains unsealed
	}
	root, err := decodeRootID(current)
	if err != nil {
		return 0, err
	}
	if oldRoot == 0 && root != 0 || oldRoot != 0 && root == 0 {
		return 0, fmt.Errorf("basis-refresh guard=zero-nonzero-domain-root before=%d after=%d: %w", oldRoot, root, backenddb.ErrRootPublicationBasisMismatch)
	}
	return root, nil
}

// prepareNativeEngineBasisRefresh preserves the logical engine, including all
// active/captured sources and catalog deltas, while rebinding moved physical
// roots using their original qualified descriptors. Count and construction use
// the SAME immutable paired Snapshots; no historical rows are scanned. Only
// producer-qualified leaf packing additionally walks pending typed headers.
// Only changed domains/collections get new immutable headers.
func prepareNativeEngineBasisRefresh(base *nativeEngineVersion, before, after *backenddb.Snapshot) (*nativeEngineVersion, error) {
	return prepareNativeEngineBasisRefreshWithMaintenanceLeaves(base, before, after, nil)
}

func prepareNativeEngineBasisRefreshWithMaintenanceLeaves(base *nativeEngineVersion, before, after *backenddb.Snapshot, leaves backenddb.NativeMaintenanceLeafReader) (*nativeEngineVersion, error) {
	return prepareNativeEnginePhysicalBasisRefresh(base, before, after, leaves, false)
}

func prepareNativeEnginePhysicalBasisRefresh(base *nativeEngineVersion, before, after *backenddb.Snapshot, leaves backenddb.NativeMaintenanceLeafReader, leafPack bool) (*nativeEngineVersion, error) {
	if leaves != nil && !leaves.MatchesNativeMaintenanceSnapshots(before, after) {
		return nil, fmt.Errorf("basis-refresh guard=snapshot-pair before=%p after=%p: %w", before, after, backenddb.ErrRootPublicationBasisMismatch)
	}
	if base == nil || base.collections == nil || before == nil || after == nil || base.maintenance == nil || base.inventory != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	oldToken, oldOK := before.StateToken()
	newToken, newOK := after.StateToken()
	if !oldOK || !newOK {
		return nil, fmt.Errorf("basis-refresh guard=snapshot-liveness before_live=%t after_live=%t: %w", oldOK, newOK, backenddb.ErrClosed)
	}
	if oldToken.AppliedCommandLSN != newToken.AppliedCommandLSN {
		return nil, fmt.Errorf("basis-refresh guard=command-lsn before=%d after=%d before_system=%d after_system=%d: %w", oldToken.AppliedCommandLSN, newToken.AppliedCommandLSN, oldToken.SystemRootPageID, newToken.SystemRootPageID, backenddb.ErrRootPublicationBasisMismatch)
	}
	lease, err := base.maintenance.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeBasisRefreshScratch{}))))
	if err != nil {
		return nil, err
	}
	s := &nativeBasisRefreshScratch{before: before, after: after, allocator: base.maintenance, leaves: leaves}
	defer func() {
		*s = nativeBasisRefreshScratch{}
		s = nil
		lease.Close()
	}()
	if oldToken.SystemRootPageID != 0 {
		s.beforeReader, err = before.NativeReaderAtRoot(oldToken.SystemRootPageID)
		if err != nil {
			return nil, err
		}
		s.beforeRoot = true
	}
	if newToken.SystemRootPageID != 0 {
		s.afterReader, err = after.NativeReaderAtRoot(newToken.SystemRootPageID)
		if err != nil {
			return nil, err
		}
		s.afterRoot = true
	}
	if _, _, err = s.unchanged(nativeCatalogNextIncarnationKey, ""); err != nil {
		return nil, nativeBasisRefreshDiagnosticError("next-incarnation", oldToken, newToken, err)
	}
	collections, domains := 0, 0
	base.collections.Scan(func(_ uint64, c *nativeCollectionState) bool {
		if err = s.collection(c); err != nil {
			return false
		}
		graph, e := s.leafPackGraphRefresh(c, leafPack)
		if e != nil {
			err = e
			return false
		}
		touched := false
		c.domains.Scan(func(_ string, d *nativeDomainState) bool {
			var root uint64
			root, err = s.root(c, d)
			if err != nil {
				return false
			}
			if root != d.catalogRoot || graph.touches(d) {
				if domains == int(^uint(0)>>1) {
					err = memtable.ErrCOWCapacity
					return false
				}
				domains++
				touched = true
			}
			return true
		})
		if touched {
			collections++ // bounded by the original map's int cardinality
		}
		return err == nil
	})
	if err != nil {
		return nil, nativeBasisRefreshDiagnosticError("count", oldToken, newToken, err)
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
	if base.catalog != nil {
		(nativeDomainLifetime{}).Retain(base.catalog)
		next.catalog = base.catalog
	}
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
	base.collections.Scan(func(id uint64, c *nativeCollectionState) bool {
		graph, e := s.leafPackGraphRefresh(c, leafPack)
		if e != nil {
			err = e
			return false
		}
		touched := 0
		c.domains.Scan(func(_ string, d *nativeDomainState) bool {
			root, e := s.root(c, d)
			if e != nil {
				err = e
				return false
			}
			if root != d.catalogRoot || graph.touches(d) {
				touched++
			}
			return true
		})
		if err != nil || touched == 0 {
			return err == nil
		}
		p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(c.domains, touched, nativeDomainLifetime{}, admit)
		if err != nil {
			return false
		}
		p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(c.pending, touched, nativeDomainLifetime{}, admit)
		if err != nil {
			return false
		}
		c.domains.Scan(func(name string, d *nativeDomainState) bool {
			root, e := s.root(c, d)
			if e != nil {
				err = e
				return false
			}
			if root == d.catalogRoot && !graph.touches(d) {
				return true
			}
			var domain *nativeDomainState
			baseRoot := d.baseRoot
			// A full replacement deliberately has no physical read base, even
			// while the authenticated old catalog descriptor moves underneath it.
			if baseRoot != 0 {
				baseRoot = root
			}
			domain, err = newNativeDomainState(d.name, baseRoot, d.interpretation, d.storagePolicy, nil, admit)
			if domain != nil {
				p.ownedDomains = append(p.ownedDomains, domain)
			}
			if err != nil {
				return false
			}
			domain.catalogRoot = root
			domain.sourceGeneration = d.sourceGeneration
			if d.pending != nil {
				if !d.pending.RetainOwned() {
					err = memtable.ErrCOWClosed
					return false
				}
				domain.pending = d.pending
			}
			if graph.touches(d) {
				domain.typed, err = graph.clone(d.typed, admit)
				if err != nil {
					return false // original private domain owns partial headers
				}
			} else {
				d.typed.retain()
				domain.typed = d.typed
			}
			d.captured.retain()
			domain.captured = d.captured
			if err = p.domainBuilder.Set(name, domain); err != nil {
				return false
			}
			if _, pending := c.pending.Get(name); pending {
				err = p.pendingDomainBuilder.Set(name, domain)
			}
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
		successor, err = newNativeCollectionState(id, c.documentGeneration, p.schema, p.domains, p.pendingDomains, admit)
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
		if _, pending := base.pending.Get(id); pending {
			err = p.pending.Set(id, successor)
		}
		return err == nil
	})
	if err != nil {
		return next, nativeBasisRefreshDiagnosticError("construct", oldToken, newToken, err)
	}
	next.names = p.names.Seal()
	p.names = nil
	next.collections = p.collections.Seal()
	p.collections = nil
	next.pending = p.pending.Seal()
	p.pending = nil
	return next, nil
}

// BEGIN source-only native refresh diagnostic context.
// This failure-only helper reuses the ORIGINAL StateToken results. It performs
// no new snapshot/catalog read and adds nothing to the sizeof-accounted scratch.
// Guard/context labels identify this invocation, not global causal ordering.
func nativeBasisRefreshDiagnosticError(phase string, before, after backenddb.StateToken, err error) error {
	return fmt.Errorf("basis-refresh phase=%s before=(seq=%d root=%d system=%d lsn=%d revision=%d leaf_version=%d) after=(seq=%d root=%d system=%d lsn=%d revision=%d leaf_version=%d): %w", phase,
		before.CommitSeq, before.RootPageID, before.SystemRootPageID, before.AppliedCommandLSN, before.MaxEntryRevision, before.LeafGenerationStateVersion,
		after.CommitSeq, after.RootPageID, after.SystemRootPageID, after.AppliedCommandLSN, after.MaxEntryRevision, after.LeafGenerationStateVersion, err)
}

// END source-only native refresh diagnostic context.
