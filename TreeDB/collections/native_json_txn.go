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
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeJSONTxn uses the existing conditional transaction's single opening cut.
// The frontend stages ordered row operations per captured scalar-indexed JSON collection
// and one raw point Set in the original P1 batch.
// Get observes that opening plus its own ordered staged row operations. Commit never reruns the
// body; only the original immutable mutations are composed after validation.
type nativeJSONTxnBinding struct {
	name             string
	incarnation      uint64
	probe            NativeJSONRead
	plan             *nativeJSONLogicalInsert
	typed            *nativeTypedDelta
	effects          []txnhistory.Effect
	rows             nativeJSONRows
	stageCharge      uint64
	validation       indexEncodeArena
	validationLease  *memtable.COWExternalLease
	validationCharge uint64
}
type NativeJSONTxn struct {
	mu           sync.Mutex
	db           *backenddb.DB
	core         backenddb.NativeConditionalTxn
	bindings     []nativeJSONTxnBinding
	members      []commitlog.CompositeMutationMember
	count        int
	imported     *nativeEngineVersion
	dependencies []txnhistory.Dependency
	custody      *nativeMutationCustody
	rawStaged    bool
	lease        *memtable.COWExternalLease
	stageLeases  []*memtable.COWExternalLease
	charge       uint64
	failure      error
}

// Begin binds the collection's original logical incarnation and interpretation.
// Close must be called on every outcome. Database.Close retains database ownership.
func (m *NativeJSONManager) Begin(name string, additional ...string) (result *NativeJSONTxn, resultErr error) {
	if len(additional) >= commitlog.MaxCompositeMutationMembersV1 {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	names := additional
	nameAt := func(i int) string {
		if i == 0 {
			return name
		}
		return names[i-1]
	}
	count := 1 + len(additional)
	for i := 0; i < count; i++ {
		if err := ValidateCollectionName(nameAt(i)); err != nil {
			return nil, err
		}
		for j := 0; j < i; j++ {
			if nameAt(i) == nameAt(j) {
				return nil, backenddb.ErrNativeSemanticUnsupported
			}
		}
	}
	if m == nil || m.db == nil {
		return nil, backenddb.ErrClosed
	}
	core, err := m.db.BeginNativeConditionalTxn()
	if err != nil {
		return nil, err
	}
	charge := memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(nativeJSONTxnBinding{}))) + memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof((*memtable.COWExternalLease)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeJSONTxn{}))) + memtable.COWAllocationCharge(uint64(count)*uint64(unsafe.Sizeof(txnhistory.Dependency{})))
	for i := 0; i < count; i++ {
		charge += 2 * memtable.COWAllocationCharge(uint64(len(nameAt(i))))
		for _, n := range valuelog.COWInspectionMetadataAllocationSizes() {
			charge += memtable.COWAllocationCharge(n)
		}
	}
	lease, err := core.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, errors.Join(err, core.Close())
	}
	t := &NativeJSONTxn{db: m.db, core: core, count: count, lease: lease, charge: charge, dependencies: make([]txnhistory.Dependency, 0, count), bindings: make([]nativeJSONTxnBinding, count), stageLeases: make([]*memtable.COWExternalLease, count)}
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
	t.custody, err = newNativeMutationCustody(0, count, core.NativeAllocator())
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		g := &t.bindings[i]
		g.name = string(bytes.Clone([]byte(nameAt(i))))
		g.probe.capture = core
		g.probe.database = m.db
		c, ok := base.names.Get(g.name)
		if !ok {
			return nil, tree.ErrKeyNotFound
		}
		if err = nativeTypedProfile(m.db, *c.schema.meta); err != nil {
			return nil, err
		}
		if _, err = nativeJSONMeta(*c.schema.meta); err != nil {
			return nil, err
		}
		g.incarnation = c.incarnation
		g.probe.collection = c
		g.probe.meta = *c.schema.meta
		t.dependencies = append(t.dependencies, txnhistory.Dependency{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(g.name)})
		if domain, ok := c.domains.Get("primary"); ok {
			g.probe.domain, err = newNativeDomainRead(domain, core.Snapshot())
			if err != nil {
				return nil, err
			}
		}
	}
	return t, nil
}
func (g *nativeJSONTxnBinding) namespace() txnhistory.Namespace {
	return txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: g.incarnation, Domain: 1, Interpretation: 1}
}
func (t *NativeJSONTxn) binding(name string) *nativeJSONTxnBinding {
	for i := 0; i < t.count; i++ {
		if name == "" && i == 0 || t.bindings[i].name == name {
			return &t.bindings[i]
		}
	}
	return nil
}

// trackRead admits a replacement dependency array and new key before allocation.
// The coarse lease conservatively retains previous array capacity until Close;
// no dependency backing is refunded while a descriptor can still borrow it.
func (t *NativeJSONTxn) trackRead(g *nativeJSONTxnBinding, id []byte) error {
	return t.trackDependency(g.namespace(), txnhistory.PointEffect, id, nil)
}
func (t *NativeJSONTxn) trackDependency(namespace txnhistory.Namespace, kind txnhistory.EffectKind, id, end []byte) error {
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
	key := memtable.COWAllocationCharge(uint64(len(id))) + memtable.COWAllocationCharge(uint64(len(end)))
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
	t.dependencies = append(t.dependencies, txnhistory.Dependency{Namespace: namespace, Kind: kind, Key: bytes.Clone(id), End: bytes.Clone(end)})
	return nil
}

func (t *NativeJSONTxn) Get(id []byte) ([]byte, error) { return t.GetFrom("", id) }
func (t *NativeJSONTxn) GetFrom(name string, id []byte) ([]byte, error) {
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
	g := t.binding(name)
	if g == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if g.typed != nil {
		if row, residual, ok := g.typed.find(id); ok {
			return g.probe.reconstructPendingTyped(row, residual)
		}
	}
	if row := g.rows.find(id); row != nil {
		if row.current == nil {
			return nil, tree.ErrKeyNotFound
		}
		return bytes.Clone(row.current), nil
	}
	if err := t.trackRead(g, id); err != nil {
		t.failure = err
		return nil, err
	}
	return g.probe.Get(id)
}

// InsertBatch stages against the same opening and its own prior row operations.
func (t *NativeJSONTxn) InsertBatch(ids, documents [][]byte) error {
	return t.InsertBatchInto("", ids, documents)
}
func (t *NativeJSONTxn) InsertBatchInto(name string, ids, documents [][]byte) (resultErr error) {
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
	defer func() {
		if resultErr != nil {
			t.failure = resultErr
		}
	}()
	g := t.binding(name)
	if g == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if g.probe.meta.Options.ColumnStore != nil {
		return t.stageNativeTypedInsert(g, ids, documents)
	}
	return t.stageNativeInsert(g, ids, documents)
}

// nativeJSONLogicalInsert is fixed internal work, not a body/replay callback.
// Opening identity and all borrowed buffers belong to NativeJSONTxn through the
// synchronous commit. Only sparse collection custody survives acceptance.
type nativeJSONLogicalInsert struct {
	name          string
	incarnation   uint64
	bootstrap     *nativeEngineVersion // private complete descriptor import only
	mutations     []memtable.COWMutation
	additional    []nativeJSONDomainChange
	primaryPolicy backenddb.OrderedRootStoragePolicy
	custody       *nativeMutationCustody
	typed         *nativeTypedDelta
}

type nativeJSONLogicalGroup struct {
	bindings  []nativeJSONTxnBinding
	changes   []nativeJSONPrimaryChange
	count     int
	bootstrap *nativeEngineVersion
	custody   *nativeMutationCustody
	prepared  *nativeEngineVersion
	raw       bool
}

func (p *nativeJSONLogicalGroup) PrepareNativePublication(latest backenddb.NativeSemanticState, exact *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
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
	changes := p.changes
	count := 0
	for i := 0; i < p.count; i++ {
		g := &p.bindings[i]
		if g.plan == nil {
			continue
		}
		c, ok := base.collections.Get(g.incarnation)
		named, nok := base.names.Get(g.name)
		if !ok || !nok || named.incarnation != g.incarnation {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrConcurrentModification
		}
		changes[count] = nativeJSONPrimaryChange{collection: c, entries: g.plan.mutations, additional: g.plan.additional, primaryPolicy: g.plan.primaryPolicy, typed: g.plan.typed}
		count++
	}
	next, err := prepareNativeJSONPrimaries(base, changes[:count])
	if err != nil {
		return backenddb.NativeFinalizedPublication{State: next}, err
	}
	if err = nativeTypedEnsureConstructible(next, exact); err != nil {
		return backenddb.NativeFinalizedPublication{State: next}, err
	}
	for i := 0; i < count; i++ {
		replacement, _ := next.collections.Get(changes[i].collection.incarnation)
		(nativeCollectionLifetime{}).Retain(replacement)
		p.custody.adoptCollection(replacement)
	}
	p.prepared = next
	return backenddb.NativeFinalizedPublication{State: next}, nil
}

func (t *NativeJSONTxn) GetRaw(key []byte) ([]byte, error) {
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
	raw, ok := t.core.(interface{ Get([]byte) ([]byte, error) })
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	return raw.Get(key)
}

// SetRaw stages one raw point write in the SAME original P1 batch.
func (t *NativeJSONTxn) SetRaw(key, value []byte) (err error) {
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
	if t.rawStaged {
		return backenddb.ErrNativeSemanticUnsupported
	}
	raw, ok := t.core.(interface{ Set([]byte, []byte) error })
	if !ok {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err = raw.Set(key, value); err != nil {
		t.failure = err
		return err
	}
	t.rawStaged = true
	return nil
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
		err, stages, lease := t.closeLocked()
		resultErr = errors.Join(resultErr, err)
		t.mu.Unlock()
		t = nil
		for i := range stages {
			if stages[i] != nil {
				stages[i].Close()
				stages[i] = nil
			}
		}
		if lease != nil {
			lease.Close()
		}
	}()
	if t.failure != nil {
		return t.failure
	}
	if err := t.finalizeNativeRows(); err != nil {
		return err
	}
	count, totalEffects := 0, 0
	for i := 0; i < t.count; i++ {
		g := &t.bindings[i]
		if g.plan != nil {
			count++
			totalEffects += len(g.effects)
		}
	}
	if count == 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	rawEntries := t.core.NativeRawEntries()
	// Each distinct allocation is admitted before construction; the outer
	// image is charged from the codec's exact plan after member planning.
	admit := func(bytes uint64) error {
		charge := memtable.COWAllocationCharge(bytes)
		if charge > ^uint64(0)-t.charge {
			return memtable.ErrCOWCapacity
		}
		if err := t.lease.Resize(t.charge + charge); err != nil {
			return err
		}
		t.charge += charge
		return nil
	}
	for _, bytes := range []uint64{
		uint64(unsafe.Sizeof(nativeJSONLogicalGroup{})),
		uint64(totalEffects) * uint64(unsafe.Sizeof(txnhistory.Effect{})),
		uint64(len(t.members)+1) * uint64(unsafe.Sizeof(commitlog.CompositeMutationMember{})),
		uint64(t.count) * uint64(unsafe.Sizeof(nativeJSONPrimaryChange{})),
		uint64(unsafe.Sizeof(backenddb.NativePreparedPublication{})),
		uint64(unsafe.Sizeof(backenddb.CommandWALIntent{})),
	} {
		if err := admit(bytes); err != nil {
			return err
		}
	}
	var rawGrowth int
	effects := make([]txnhistory.Effect, 0, totalEffects)
	members := make([]commitlog.CompositeMutationMember, len(t.members)+1)
	memberCount := 0
	if t.rawStaged {
		if len(rawEntries) != 1 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		op := commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: rawEntries[0].Key, Value: rawEntries[0].Value, Revision: uint64(rawEntries[0].Revision)}
		scan := func(visit func(commitlog.RawKVOperation) error) error { return visit(op) }
		rawPlan, err := commitlog.PlanRawKVBatchPayloadScan(scan)
		if err != nil {
			return err
		}
		if err = admit(uint64(rawPlan.PayloadLen)); err != nil {
			return err
		}
		payload, err := commitlog.EncodeRawKVBatchPayloadPlanned(rawPlan, scan)
		if err != nil {
			return err
		}
		// Actual assignment may add the revision field. Plan its maximum
		// exact shape without mutating the original staged operation.
		op.Revision = ^uint64(0)
		finalPlan, err := commitlog.PlanRawKVBatchPayloadScan(scan)
		if err != nil {
			return err
		}
		rawGrowth = finalPlan.PayloadLen - rawPlan.PayloadLen
		members[memberCount] = commitlog.CompositeMutationMember{Kind: commitlog.CommandKindRawKVBatch, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1, Payload: payload}
		memberCount++
	}
	for i := 0; i < t.count; i++ {
		effects = append(effects, t.bindings[i].effects...)
	}
	for _, member := range t.members {
		members[memberCount] = member
		memberCount++
	}
	kind, scope, format, payload := members[0].Kind, commitlog.CommandScopeCollection, members[0].PayloadFormat, members[0].Payload
	typedMember := false
	for i := 0; i < t.count; i++ {
		typedMember = typedMember || nativeTypedMeta(t.bindings[i].probe.meta)
	}
	if memberCount > 1 || typedMember {
		kind, scope, format = commitlog.CommandKindCompositeMutation, commitlog.CommandScopeSystem, commitlog.PayloadFormatCompositeMutationV1
		var err error
		planned, planErr := commitlog.PlanCompositeMutationPayload(members[:memberCount])
		if planErr != nil {
			return planErr
		}
		capacity := planned + rawGrowth
		if capacity > commitlog.MaxCompositeMutationPayloadBytesV1 {
			return memtable.ErrCOWCapacity
		}
		if err = admit(uint64(capacity)); err != nil {
			return err
		}
		payload, err = commitlog.EncodeCompositeMutationPayloadTo(make([]byte, 0, capacity), members[:memberCount])
		if err != nil {
			return err
		}
	}
	plan := &nativeJSONLogicalGroup{bindings: t.bindings, changes: make([]nativeJSONPrimaryChange, t.count), count: t.count, bootstrap: t.imported, custody: t.custody, raw: t.rawStaged}
	native := &backenddb.NativePreparedPublication{RequireInlineCompositeRaw: t.rawStaged, Basis: t.core.State(), StartSequence: t.core.LogicalSequence(), Reprepare: plan, Custody: t.custody, Effects: effects, Dependencies: t.dependencies}
	for i := range t.bindings {
		if t.bindings[i].typed != nil {
			if err := t.custody.typedResources.Initialize(nil); err != nil {
				return err
			}
			t.custody.hasTypedResources = true
			native.Finalizer = plan
			native.Resources = &t.custody.typedResources
			break
		}
	}
	intent, err := t.db.NewTrustedCommandWALIntent(kind, scope, format, payload)
	if err != nil {
		t.custody = nil
		return errors.Join(err, cancelPrivateNativePublication(native))
	}
	for i := 0; i < t.count; i++ {
		t.bindings[i].probe.capture = nil
		t.bindings[i].probe.domain = nativeDomainRead{}
		t.bindings[i].probe.collection = nil
		t.bindings[i].probe.meta = CollectionMeta{}
		if lease := t.bindings[i].probe.detachScratch(); lease != nil {
			lease.Close()
		}
	}
	members, rawEntries = nil, nil
	result, err := t.core.CommitNative(native, intent, false)
	t.custody = nil
	if !result.Consumed {
		err = errors.Join(err, cancelPrivateNativePublication(native))
	}
	native, intent, plan, payload, effects = nil, nil, nil, nil, nil
	return err
}
func (t *NativeJSONTxn) Close() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	err, stages, lease := t.closeLocked()
	t.mu.Unlock()
	t = nil
	for i := range stages {
		if stages[i] != nil {
			stages[i].Close()
			stages[i] = nil
		}
	}
	if lease != nil {
		lease.Close()
	}
	return err
}
func (t *NativeJSONTxn) closeLocked() (error, []*memtable.COWExternalLease, *memtable.COWExternalLease) {
	var err error
	for i := range t.bindings {
		typed := t.bindings[i].typed
		lease := t.bindings[i].probe.detachScratch()
		validation := t.bindings[i].validationLease
		// Plans and probes may borrow typed row IDs; detach the complete binding
		// before a cancelled delta's original vector receipt can refund.
		t.bindings[i] = nativeJSONTxnBinding{}
		done := typed.release()
		typed = nil
		done.Complete()
		if validation != nil {
			validation.Close()
		}
		if lease != nil {
			lease.Close()
		}
	}
	t.bindings = nil
	t.members = nil
	t.dependencies = nil

	if t.custody != nil {
		record := t.custody.ReleaseNativeCustody()
		t.custody = nil
		err = finishPrivateNative(record)
	}
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
	t.db = nil
	stages, lease := t.stageLeases, t.lease
	t.stageLeases = nil
	t.lease = nil
	return err, stages, lease
}
