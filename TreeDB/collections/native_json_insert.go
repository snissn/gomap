package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// InsertBatch is the one-shot form of Begin, staged InsertBatch, and Commit.
func (m *NativeJSONManager) InsertBatch(name string, ids, documents [][]byte) (resultErr error) {
	tx, err := m.Begin(name)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	if err = tx.InsertBatch(ids, documents); err != nil {
		return err
	}
	return tx.Commit()
}

func nativeJSONInsertCharge(name string, ids, documents [][]byte) (uint64, error) {
	var total uint64
	add := func(bytes uint64) bool {
		n := memtable.COWAllocationCharge(bytes)
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	for _, n := range []uint64{uint64(unsafe.Sizeof(insertBatchPlan{})), uint64(unsafe.Sizeof(directBufferedInsertPlan{})), uint64(unsafe.Sizeof(backenddb.NativePreparedPublication{})), uint64(unsafe.Sizeof(backenddb.CommandWALIntent{})), uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})), uint64(unsafe.Sizeof(NativeJSONRead{})), uint64(unsafe.Sizeof(string(""))), uint64(unsafe.Sizeof(backenddb.OrderedRootStoragePolicy(0)))} {
		if !add(n) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	// Two ID-header arrays cover resultIDs and the single-row primaryKeys
	// specialization; sorted multi-row plans reuse resultIDs for primaryKeys.
	for _, n := range []uint64{2 * uint64(unsafe.Sizeof([]byte{})), uint64(unsafe.Sizeof(insertBatchItem{})), uint64(unsafe.Sizeof(directBufferedRootEntry{})), uint64(unsafe.Sizeof(memtable.COWMutation{})), uint64(unsafe.Sizeof(txnhistory.Effect{})), uint64(unsafe.Sizeof(commitlog.CollectionDocument{}))} {
		if uint64(len(ids)) > ^uint64(0)/n || !add(uint64(len(ids))*n) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	idBytes, payloadBytes := uint64(0), uint64(10+len(name))
	for i, id := range ids {
		n := uint64(len(id))
		if n > ^uint64(0)-idBytes {
			return 0, memtable.ErrCOWCapacity
		}
		idBytes += n
		n += uint64(len(documents[i])) + 8
		if n > ^uint64(0)-payloadBytes {
			return 0, memtable.ErrCOWCapacity
		}
		payloadBytes += n
	}
	if !add(idBytes) || !add(payloadBytes) {
		return 0, memtable.ErrCOWCapacity
	}
	for _, bytes := range valuelog.COWInspectionMetadataAllocationSizes() {
		if !add(bytes) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

func prepareNativeJSONPrimary(base *nativeEngineVersion, c *nativeCollectionState, entries []memtable.COWMutation) (*nativeEngineVersion, error) {
	if base == nil || base.inventory != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	next, err := newNativeEnginePreparation(base, 1, 1, base.allocator)
	if err != nil {
		return nil, err
	}
	p := next.preparation
	defer p.sealPrivateBuilders(next)
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return base.allocator.AdmitNativeBytes(n) }
	if base.catalog != nil {
		(nativeDomainLifetime{}).Retain(base.catalog)
		next.catalog = base.catalog
	}
	old, _ := c.domains.Get("primary")
	root, policy := uint64(0), backenddb.OrderedRootStoragePolicy(0)
	if old != nil {
		root, policy = old.baseRoot, old.storagePolicy
	}
	domain, err := prepareNativeDomainUpdate(old, "primary", root, 1, policy, entries, base.allocator)
	if domain != nil {
		p.ownedDomains = append(p.ownedDomains, domain)
	}
	if err != nil {
		return next, err
	}
	p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(c.domains, 1, nativeDomainLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(c.pending, 1, nativeDomainLifetime{}, admit)
	if err != nil {
		return next, err
	}
	if err = p.domainBuilder.Set("primary", domain); err != nil {
		return next, err
	}
	if err = p.pendingDomainBuilder.Set("primary", domain); err != nil {
		return next, err
	}
	p.domains, p.pendingDomains = p.domainBuilder.Seal(), p.pendingDomainBuilder.Seal()
	p.domainBuilder, p.pendingDomainBuilder = nil, nil
	c.schema.retain()
	p.schema = c.schema
	replacement, err := newNativeCollectionState(c.incarnation, p.schema, p.domains, p.pendingDomains, admit)
	if err != nil {
		return next, err
	}
	p.schema, p.domains, p.pendingDomains = nil, nil, nil
	p.ownedCollections = append(p.ownedCollections, replacement)
	p.names, err = cowbtree.NewOwnedMapBuilder(base.names, 1, nativeCollectionLifetime{}, admit)
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
	if err = p.names.Set(c.schema.meta.Name, replacement); err != nil {
		return next, err
	}
	if err = p.collections.Set(c.incarnation, replacement); err != nil {
		return next, err
	}
	err = p.pending.Set(c.incarnation, replacement)
	return next, err
}

// The registered singleton preparation shares the same immutable domain and
// directory replacement as the public planner; it never invokes replay install.
func prepareNativeJSONInsertCommand(_ *backenddb.DB, env commitlog.CommandEnvelope, basis backenddb.NativeSemanticState, snapshot *backenddb.Snapshot, _ *backenddb.StableResourceCaptureLease) (publication *backenddb.NativePreparedPublication, resultErr error) {
	base, ok := basis.(*nativeEngineVersion)
	if !ok || base.inventory != nil || snapshot == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	// Canonical decoding's document descriptors are bounded by the payload. This
	// private scope ends before returning; actual rows are copied by COW Prepare.
	lease, err := base.allocator.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(len(env.Payload)) * 8))
	if err != nil {
		return nil, err
	}
	defer lease.Close()
	decoded, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	defer func() { decoded = commitlog.CollectionInsertBatchByIDPayload{} }()
	c, ok := base.names.Get(decoded.Collection)
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	if _, err = nativeJSONMeta(*c.schema.meta); err != nil {
		return nil, err
	}
	mutations := make([]memtable.COWMutation, len(decoded.Documents))
	effects := make([]txnhistory.Effect, len(decoded.Documents))
	ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: c.incarnation, Domain: 1, Interpretation: 1}
	for i, row := range decoded.Documents {
		mutations[i] = memtable.COWMutation{Key: row.ID, Value: row.Document, Flags: node.FlagInline}
		effects[i] = txnhistory.Effect{Namespace: ns, Kind: txnhistory.PointEffect, Key: row.ID}
	}
	next, err := prepareNativeJSONPrimary(base, c, mutations)
	mutations = nil
	if err != nil {
		if next != nil {
			record := next.Release()
			next = nil
			err = errors.Join(err, finishPrivateNative(record))
		}
		return nil, err
	}
	custody, err := newNativeMutationCustody(0, 1, base.allocator)
	if err != nil {
		record := next.Release()
		next = nil
		return nil, errors.Join(err, finishPrivateNative(record))
	}
	replacement, _ := next.collections.Get(c.incarnation)
	(nativeCollectionLifetime{}).Retain(replacement)
	custody.adoptCollection(replacement)
	// Effects own keys independently of decoded input through the retained source.
	domain, _ := replacement.domains.Get("primary")
	for i := range effects {
		row, _ := domain.pending.Get(effects[i].Key)
		effects[i].Key = nativeBorrowedStringBytes(row.Key)
	}
	return &backenddb.NativePreparedPublication{Basis: basis, State: next, Custody: custody, Effects: effects, Dependencies: effects}, nil
}
