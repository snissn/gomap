package collections

import (
	"bytes"
	"errors"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeJSONTxn uses the existing conditional transaction's single opening cut.
// The selected frontend accepts one insert batch in one unindexed JSON collection.
// Get observes that opening plus its own staged inserts. Commit never reruns the
// body; only the original immutable mutations are composed after validation.
type NativeJSONTxn struct {
	mu                sync.Mutex
	db                *backenddb.DB
	core              backenddb.NativeConditionalTxn
	name              string
	incarnation       uint64
	imported          *nativeEngineVersion
	probe             NativeJSONRead
	dependencies      []txnhistory.Dependency
	plan              *nativeJSONLogicalInsert
	payload           []byte
	effects           []txnhistory.Effect
	lease, stageLease *memtable.COWExternalLease
	charge            uint64
	failure           error
}

// Begin binds the collection's original logical incarnation and interpretation.
// Close must be called on every outcome. Database.Close retains database ownership.
func (m *NativeJSONManager) Begin(name string) (result *NativeJSONTxn, resultErr error) {
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	if m == nil || m.db == nil {
		return nil, backenddb.ErrClosed
	}
	core, err := m.db.BeginNativeConditionalTxn()
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeJSONTxn{}))) + memtable.COWAllocationCharge(uint64(len(name)))*2 + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(txnhistory.Dependency{})))
	for _, n := range valuelog.COWInspectionMetadataAllocationSizes() {
		charge += memtable.COWAllocationCharge(n)
	}
	lease, err := core.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, errors.Join(err, core.Close())
	}
	t := &NativeJSONTxn{db: m.db, core: core, name: string(bytes.Clone([]byte(name))), lease: lease, charge: charge}
	t.probe.capture = core
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, t.Close())
		}
	}()
	base, ok := core.State().(*nativeEngineVersion)
	if core.State() != nil && !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		t.imported, err = importNativeJSONCatalog(core)
		if err != nil {
			return nil, err
		}
		base = t.imported
	}
	if base.inventory != nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	c, ok := base.names.Get(t.name)
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	if _, err = nativeJSONMeta(*c.schema.meta); err != nil {
		return nil, err
	}
	t.incarnation = c.incarnation
	t.dependencies = []txnhistory.Dependency{{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(t.name)}}
	if domain, ok := c.domains.Get("primary"); ok {
		t.probe.domain, err = newNativeDomainRead(domain, core.Snapshot())
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (t *NativeJSONTxn) namespace() txnhistory.Namespace {
	return txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: t.incarnation, Domain: 1, Interpretation: 1}
}

// trackRead admits a replacement dependency array and new key before allocation.
// The coarse lease conservatively retains previous array capacity until Close;
// no dependency backing is refunded while a descriptor can still borrow it.
func (t *NativeJSONTxn) trackRead(id []byte) error {
	size := uint64(unsafe.Sizeof(txnhistory.Dependency{}))
	capacity := cap(t.dependencies)
	var backing uint64
	if len(t.dependencies) == capacity {
		if capacity > int(^uint(0)>>1)/2 {
			return memtable.ErrCOWCapacity
		}
		capacity *= 2
		if uint64(capacity) > ^uint64(0)/size {
			return memtable.ErrCOWCapacity
		}
		backing = memtable.COWAllocationCharge(uint64(capacity) * size)
	}
	key := memtable.COWAllocationCharge(uint64(len(id)))
	if key > ^uint64(0)-backing || backing+key > ^uint64(0)-t.charge {
		return memtable.ErrCOWCapacity
	}
	if err := t.lease.Resize(t.charge + backing + key); err != nil {
		return err
	}
	t.charge += backing + key
	if len(t.dependencies) == cap(t.dependencies) {
		next := make([]txnhistory.Dependency, len(t.dependencies), capacity)
		copy(next, t.dependencies)
		t.dependencies = next
	}
	t.dependencies = append(t.dependencies, txnhistory.Dependency{Namespace: t.namespace(), Kind: txnhistory.PointEffect, Key: bytes.Clone(id)})
	return nil
}

func (t *NativeJSONTxn) Get(id []byte) ([]byte, error) {
	if t == nil {
		return nil, backenddb.ErrClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.core == nil {
		return nil, backenddb.ErrClosed
	}
	if t.failure != nil {
		return nil, t.failure
	}
	if t.plan != nil {
		for _, row := range t.plan.mutations {
			if bytes.Equal(row.Key, id) {
				return bytes.Clone(row.Value), nil
			}
		}
	}
	if err := t.trackRead(id); err != nil {
		t.failure = err
		return nil, err
	}
	return t.probe.Get(id)
}

// InsertBatch stages one strictly sorted batch. A second call refuses without
// changing the first batch. Other staging failures terminally invalidate the txn.
func (t *NativeJSONTxn) InsertBatch(ids, documents [][]byte) (resultErr error) {
	if t == nil {
		return backenddb.ErrClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.core == nil {
		return backenddb.ErrClosed
	}
	if t.failure != nil {
		return t.failure
	}
	if t.plan != nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	defer func() {
		if resultErr != nil {
			t.failure = resultErr
		}
	}()
	if len(ids) == 0 || len(ids) != len(documents) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	var arenaBytes uint64
	for i, id := range ids {
		if len(id) == 0 || documents[i] == nil || (i > 0 && bytes.Compare(ids[i-1], id) >= 0) {
			return backenddb.ErrNativeSemanticUnsupported
		}
		n := uint64(len(id)) + uint64(len(documents[i]))
		if n > ^uint64(0)-arenaBytes {
			return memtable.ErrCOWCapacity
		}
		arenaBytes += n
	}
	charge, err := nativeJSONInsertCharge(t.name, ids, documents)
	if err != nil {
		return err
	}
	// Staged inputs outlive caller buffers, unlike the old synchronous planner.
	extra := memtable.COWAllocationCharge(arenaBytes) + memtable.COWAllocationCharge(uint64(len(ids))*uint64(unsafe.Sizeof([]byte{})))*2 + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{})))
	if extra > ^uint64(0)-charge {
		return memtable.ErrCOWCapacity
	}
	t.stageLease, err = t.core.NativeAllocator().AdmitNativeBytes(charge + extra)
	if err != nil {
		return err
	}
	arena := make([]byte, arenaBytes)
	ownedIDs, ownedDocs := make([][]byte, len(ids)), make([][]byte, len(ids))
	offset := 0
	for i, id := range ids {
		copy(arena[offset:], id)
		ownedIDs[i] = arena[offset : offset+len(id)]
		offset += len(id)
		copy(arena[offset:], documents[i])
		ownedDocs[i] = arena[offset : offset+len(documents[i])]
		offset += len(documents[i])
	}
	planner := insertBatchPlanner{collection: t.name, primaryRoot: "primary", templateRoot: "templates", directBufferedRuns: true, options: collectionOptions{documentFormat: DocumentFormatJSON}}
	prepared, err := planner.planInsertBatch(ownedIDs, ownedDocs)
	if err != nil {
		return err
	}
	rows := prepared.directBufferedInsert.primaryEntries
	// Blind insert intent is a write dependency, including an absent opening key.
	for _, row := range rows {
		if t.probe.domain.domain == nil {
			break
		}
		entry, err := t.probe.domain.getEntry(row.key, &t.probe.scratch, t.probe.readLeaf)
		if err == nil && entry.Flags&node.FlagTombstone == 0 {
			return ErrDocumentExists
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return err
		}
	}
	custody, err := newNativeMutationCustody(0, 1, t.core.NativeAllocator())
	if err != nil {
		return err
	}
	// Adopt all controls before payload encoding or another fallible operation.
	t.plan = &nativeJSONLogicalInsert{name: t.name, incarnation: t.incarnation, bootstrap: t.imported, custody: custody, mutations: make([]memtable.COWMutation, len(rows))}
	t.effects = make([]txnhistory.Effect, len(rows))
	docs := make([]commitlog.CollectionDocument, len(rows))
	for i, row := range rows {
		t.plan.mutations[i] = memtable.COWMutation{Key: row.key, Value: row.value, Flags: node.FlagInline}
		t.effects[i] = txnhistory.Effect{Namespace: t.namespace(), Kind: txnhistory.PointEffect, Key: row.key}
		docs[i] = commitlog.CollectionDocument{ID: row.key, Document: row.value}
	}
	t.payload, err = commitlog.EncodeCollectionInsertBatchByIDPayload(t.name, docs)
	return err
}

// nativeJSONLogicalInsert is fixed internal work, not a body/replay callback.
// Opening identity and all borrowed buffers belong to NativeJSONTxn through the
// synchronous commit. Only sparse collection custody survives acceptance.
type nativeJSONLogicalInsert struct {
	name        string
	incarnation uint64
	bootstrap   *nativeEngineVersion // private complete descriptor import only
	mutations   []memtable.COWMutation
	custody     *nativeMutationCustody
}

func (p *nativeJSONLogicalInsert) PrepareNativePublication(latest backenddb.NativeSemanticState, exact *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	if exact == nil {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	base, ok := latest.(*nativeEngineVersion)
	if latest != nil && !ok {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		base = p.bootstrap
	}
	if base == nil || base.inventory != nil {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	c, ok := base.collections.Get(p.incarnation)
	named, namedOK := base.names.Get(p.name)
	if !ok || !namedOK || named.incarnation != p.incarnation {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrConcurrentModification
	}
	next, err := prepareNativeJSONPrimary(base, c, p.mutations)
	if err != nil {
		if next == nil {
			return backenddb.NativeFinalizedPublication{}, err
		}
		return backenddb.NativeFinalizedPublication{State: next}, err
	}
	replacement, _ := next.collections.Get(p.incarnation)
	(nativeCollectionLifetime{}).Retain(replacement)
	p.custody.adoptCollection(replacement)
	return backenddb.NativeFinalizedPublication{State: next}, nil
}

func (t *NativeJSONTxn) Commit() (resultErr error) {
	if t == nil {
		return backenddb.ErrClosed
	}
	t.mu.Lock()
	if t.core == nil {
		t.mu.Unlock()
		return backenddb.ErrClosed
	}
	defer func() {
		closeErr, stage, lease := t.closeLocked()
		resultErr = errors.Join(resultErr, closeErr)
		t.mu.Unlock()
		t = nil
		if stage != nil {
			stage.Close()
		}
		if lease != nil {
			lease.Close()
		}
	}()
	if t.failure != nil {
		return t.failure
	}
	if t.plan == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	native := &backenddb.NativePreparedPublication{Basis: t.core.State(), StartSequence: t.core.LogicalSequence(), Reprepare: t.plan, Custody: t.plan.custody, Effects: t.effects, Dependencies: t.dependencies}
	// The descriptor owns custody; the internal plan borrows it synchronously.
	intent, err := t.db.NewTrustedCommandWALIntent(commitlog.CommandKindCollectionInsertBatchByID, commitlog.CommandScopeCollection, commitlog.PayloadFormatCollectionInsertBatchByIDV1, t.payload)
	if err != nil {
		t.plan.custody = nil
		return errors.Join(err, cancelPrivateNativePublication(native))
	}
	// End borrowed read aliases before P1 commit closes its opening Snapshot.
	t.probe.capture, t.probe.domain, t.probe.scratch = nil, nativeDomainRead{}, nativeDomainReadScratch{}
	result, err := t.core.CommitNative(native, intent, false)
	t.plan.custody = nil
	if !result.Consumed {
		err = errors.Join(err, cancelPrivateNativePublication(native))
	}
	native, intent = nil, nil
	return err
}

func (t *NativeJSONTxn) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	err, stage, lease := t.closeLocked()
	t.mu.Unlock()
	t = nil
	if stage != nil {
		stage.Close()
	}
	if lease != nil {
		lease.Close()
	}
	return err
}
func (t *NativeJSONTxn) closeLocked() (error, *memtable.COWExternalLease, *memtable.COWExternalLease) {
	var err error
	if t.plan != nil && t.plan.custody != nil {
		record := t.plan.custody.ReleaseNativeCustody()
		t.plan.custody = nil
		err = finishPrivateNative(record)
	}
	t.plan = nil
	t.payload = nil
	t.effects = nil
	t.dependencies = nil
	t.probe.capture, t.probe.domain, t.probe.scratch = nil, nativeDomainRead{}, nativeDomainReadScratch{}
	if t.imported != nil {
		record := t.imported.Release()
		t.imported = nil
		err = errors.Join(err, finishPrivateNative(record))
	}
	if t.core != nil {
		core := t.core
		t.core = nil
		err = errors.Join(err, core.Close())
	}
	t.name = ""
	t.db = nil
	stage, lease := t.stageLease, t.lease
	t.stageLease, t.lease = nil, nil
	return err, stage, lease
}
