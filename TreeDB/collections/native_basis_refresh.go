package collections

import (
	"bytes"
	"errors"
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
	beforeReader, afterReader backenddb.SnapshotRootReader
	beforeRoot, afterRoot     bool
	key                       [page.PageSize]byte
	beforeKey, afterKey       [page.PageSize]byte
	beforeLeaf, afterLeaf     [page.PageSize]byte
}

func (s *nativeBasisRefreshScratch) readBeforeLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeSnapshotLeaf(s.before, s.allocator, ptr, dst)
}
func (s *nativeBasisRefreshScratch) readAfterLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return readNativeSnapshotLeaf(s.after, s.allocator, ptr, dst)
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
		return nil, false, err
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
	if oldFound != currentFound || !bytes.Equal(old, current) {
		return nil, false, backenddb.ErrRootPublicationBasisMismatch
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
		return backenddb.ErrRootPublicationBasisMismatch
	}
	if identityFound {
		id, err := nativeCatalogIdentityValue(raw, false)
		if err != nil {
			return err
		}
		if id != c.incarnation {
			return backenddb.ErrRootPublicationBasisMismatch
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
			return 0, backenddb.ErrNativeSemanticUnsupported
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
	if oldRoot != d.baseRoot {
		return 0, backenddb.ErrRootPublicationBasisMismatch
	}
	current, found, err := s.get(key, true)
	if err != nil {
		return 0, err
	}
	if !found {
		if oldRoot != 0 {
			return 0, backenddb.ErrRootPublicationBasisMismatch
		}
		return 0, nil // a purely unsealed collection/domain remains unsealed
	}
	root, err := decodeRootID(current)
	if err != nil {
		return 0, err
	}
	if oldRoot == 0 && root != 0 || oldRoot != 0 && root == 0 {
		return 0, backenddb.ErrRootPublicationBasisMismatch
	}
	return root, nil
}

// prepareNativeEngineBasisRefresh preserves the logical engine, including all
// active/captured sources and catalog deltas, while rebinding moved physical
// roots using their original qualified descriptors. Count and construction use
// the SAME immutable paired Snapshots; no historical rows or source chain are
// scanned. Only changed domains/collections get new immutable headers.
func prepareNativeEngineBasisRefresh(base *nativeEngineVersion, before, after *backenddb.Snapshot) (*nativeEngineVersion, error) {
	if base == nil || base.collections == nil || before == nil || after == nil || base.maintenance == nil || base.inventory != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	oldToken, oldOK := before.StateToken()
	newToken, newOK := after.StateToken()
	if !oldOK || !newOK {
		return nil, backenddb.ErrClosed
	}
	if oldToken.AppliedCommandLSN != newToken.AppliedCommandLSN {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	lease, err := base.maintenance.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeBasisRefreshScratch{}))))
	if err != nil {
		return nil, err
	}
	s := &nativeBasisRefreshScratch{before: before, after: after, allocator: base.maintenance}
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
		return nil, err
	}
	collections, domains := 0, 0
	base.collections.Scan(func(_ uint64, c *nativeCollectionState) bool {
		if err = s.collection(c); err != nil {
			return false
		}
		touched := false
		c.domains.Scan(func(_ string, d *nativeDomainState) bool {
			var root uint64
			root, err = s.root(c, d)
			if err != nil {
				return false
			}
			if root != d.baseRoot {
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
		return nil, err
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
		touched := 0
		c.domains.Scan(func(_ string, d *nativeDomainState) bool {
			root, e := s.root(c, d)
			if e != nil {
				err = e
				return false
			}
			if root != d.baseRoot {
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
			if root == d.baseRoot {
				return true
			}
			var domain *nativeDomainState
			domain, err = newNativeDomainState(d.name, root, d.interpretation, d.storagePolicy, nil, admit)
			if domain != nil {
				p.ownedDomains = append(p.ownedDomains, domain)
			}
			if err != nil {
				return false
			}
			if d.pending != nil {
				if !d.pending.RetainOwned() {
					err = memtable.ErrCOWClosed
					return false
				}
				domain.pending = d.pending
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
		if _, pending := base.pending.Get(id); pending {
			err = p.pending.Set(id, successor)
		}
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
