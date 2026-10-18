package collections

import (
	"bytes"
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

// InsertBatch is the restricted unindexed JSON operation. IDs must be strictly
// ordered; unsupported shapes refuse before effects or canonical assignment.
func (m *NativeJSONManager) InsertBatch(name string, ids, documents [][]byte) (resultErr error) {
	if err := ValidateCollectionName(name); err != nil {
		return err
	}
	if m == nil || m.db == nil {
		return backenddb.ErrClosed
	}
	if len(ids) != len(documents) || len(ids) == 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	for i := range ids {
		if len(ids[i]) == 0 || documents[i] == nil || (i > 0 && bytes.Compare(ids[i-1], ids[i]) >= 0) {
			return backenddb.ErrNativeSemanticUnsupported
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, capture.Close()) }()
	base, ok := capture.State().(*nativeEngineVersion)
	if capture.State() != nil && !ok {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		base, err = importNativeJSONCatalog(capture)
		if base != nil {
			defer func() {
				record := base.Release()
				base = nil
				resultErr = errors.Join(resultErr, finishPrivateNative(record))
			}()
		}
		if err != nil {
			return err
		}
	}
	c, ok := base.names.Get(name)
	if !ok {
		return tree.ErrKeyNotFound
	}
	if _, err := nativeJSONMeta(*c.schema.meta); err != nil {
		return err
	}
	if base.inventory != nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	// This lease covers the identity-free planner's exact unindexed/sorted
	// branch and command payload. The caller keeps it through synchronous
	// bridge preparation/append; accepted roots and history own independent copies.
	charge, err := nativeJSONInsertCharge(name, ids, documents)
	if err != nil {
		return err
	}
	control, err := base.allocator.AdmitNativeBytes(charge)
	if err != nil {
		return err
	}
	defer control.Close()
	// End every planner/payload/publication alias in this private call before
	// refunding its containing control lease. Nothing from this scratch scope is
	// transferred into the accepted sparse custody.
	return func() (resultErr error) {
		custody, err := newNativeMutationCustody(0, 1, base.allocator)
		if err != nil {
			return err
		}
		var native *backenddb.NativePreparedPublication
		defer func() {
			if custody != nil {
				record := custody.ReleaseNativeCustody()
				custody = nil
				resultErr = errors.Join(resultErr, finishPrivateNative(record))
			}
		}()
		planner := insertBatchPlanner{collection: name, primaryRoot: "primary", templateRoot: "templates", directBufferedRuns: true, options: collectionOptions{documentFormat: DocumentFormatJSON}}
		plan, err := planner.planInsertBatch(ids, documents)
		if err != nil {
			return err
		}
		defer func() { plan = nil }()
		rows := plan.directBufferedInsert.primaryEntries
		old, _ := c.domains.Get("primary")
		if old != nil {
			read, err := newNativeDomainRead(old, capture.Snapshot())
			if err != nil {
				return err
			}
			// Read scratch/file resolution retains the exact opening capture.
			probe := NativeJSONRead{capture: capture}
			for _, row := range rows {
				entry, err := read.getEntry(row.key, &probe.scratch, probe.readLeaf)
				if err == nil && entry.Flags&node.FlagTombstone == 0 {
					return ErrDocumentExists
				}
				if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
					return err
				}
			}
			read = nativeDomainRead{}
			probe.capture, probe.scratch = nil, nativeDomainReadScratch{}
		}
		mutations := make([]memtable.COWMutation, len(rows))
		effects := make([]txnhistory.Effect, len(rows))
		ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: c.incarnation, Domain: 1, Interpretation: 1}
		for i, row := range rows {
			mutations[i] = memtable.COWMutation{Key: row.key, Value: row.value, Flags: node.FlagInline}
			effects[i] = txnhistory.Effect{Namespace: ns, Kind: txnhistory.PointEffect, Key: row.key}
		}
		next, err := prepareNativeJSONPrimary(base, c, mutations)
		mutations = nil
		if err != nil {
			if next != nil {
				record := next.Release()
				next = nil
				err = errors.Join(err, finishPrivateNative(record))
			}
			return err
		}
		replacement, _ := next.collections.Get(c.incarnation)
		(nativeCollectionLifetime{}).Retain(replacement)
		custody.adoptCollection(replacement)
		replacement = nil
		native = &backenddb.NativePreparedPublication{Basis: capture.State(), State: next, Custody: custody, StartSequence: capture.LogicalSequence(), Effects: effects, Dependencies: effects}
		custody, next = nil, nil
		docs := make([]commitlog.CollectionDocument, len(rows))
		for i, row := range rows {
			docs[i] = commitlog.CollectionDocument{ID: row.key, Document: row.value}
		}
		payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(name, docs)
		docs, rows, effects, plan = nil, nil, nil, nil
		if err != nil {
			return errors.Join(err, cancelPrivateNativePublication(native))
		}
		intent, err := m.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)
		if err != nil {
			return errors.Join(err, cancelPrivateNativePublication(native))
		}
		result, err := m.db.WriteNativePublication(native, intent, false)
		plan, payload, intent = nil, nil, nil
		if !result.Consumed {
			err = errors.Join(err, cancelPrivateNativePublication(native))
		}
		return err
	}()
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
