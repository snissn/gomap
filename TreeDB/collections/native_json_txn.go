package collections

import (
	"bytes"
	"errors"
	"sync"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// NativeJSONTxn uses the existing conditional transaction's single opening cut.
// The frontend stages ordered row operations per captured scalar-indexed JSON collection
// and one raw point Set in the original P1 batch.
// Get observes that opening plus its own ordered staged row operations. Commit never reruns the
// body; only the original immutable mutations are composed after validation.
type nativeJSONTxnBinding struct {
	name               string
	incarnation        uint64
	probe              NativeJSONRead
	plan               *nativeJSONLogicalInsert
	typed              *nativeTypedDelta
	projection         *trustedFloat32Projection
	effects            []txnhistory.Effect
	rows               nativeJSONRows
	stageCharge        uint64
	validation         indexEncodeArena
	retainedScalar     indexEncodeArena
	validationLease    *memtable.COWExternalLease
	validationCharge   uint64
	resolvedFinal      nativeJSONResolvedFinalRows
	templates          templateV1Resolver
	templateRecords    []templateV1Record
	templateLearned    []templateV1LearnedTemplate
	allowSourceImport  bool
	allowStored        bool
	learnTemplates     bool
	primaryOnlyBSONSet bool
	singlePlan         nativeJSONLogicalInsert
	singleMutation     [1]memtable.COWMutation
	singleEffect       [1]txnhistory.Effect
}
type NativeJSONTxn struct {
	mu                   sync.Mutex
	db                   *backenddb.DB
	core                 backenddb.NativeConditionalTxn
	bindings             []nativeJSONTxnBinding
	singleBinding        [1]nativeJSONTxnBinding
	members              []commitlog.CompositeMutationMember
	singleMember         [1]commitlog.CompositeMutationMember
	count                int
	imported             *nativeEngineVersion
	dependencies         []txnhistory.Dependency
	inlineDeps           [2]txnhistory.Dependency
	custody              *nativeMutationCustody
	rawStaged            bool
	structuredBSONUpdate bool
	lease                *memtable.COWExternalLease
	stageLeases          []*memtable.COWExternalLease
	singleStageLease     [1]*memtable.COWExternalLease
	logicalGroup         nativeJSONLogicalGroup
	preparedPub          backenddb.NativePreparedPublication
	charge               uint64
	failure              error
}

// Begin binds the collection's original logical incarnation and interpretation.
// Close must be called on every outcome. Database.Close retains database ownership.
func (m *NativeJSONManager) Begin(name string, additional ...string) (result *NativeJSONTxn, resultErr error) {
	return m.begin(name, additional, false)
}

// beginPrepared shares the original opening and planner, but never checkpoints
// below an outer collection mutation gate.
func (m *NativeJSONManager) beginPrepared(name string) (*NativeJSONTxn, error) {
	return m.begin(name, nil, true)
}

func (m *NativeJSONManager) begin(name string, additional []string, prepared bool) (result *NativeJSONTxn, resultErr error) {
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
	var core backenddb.NativeConditionalTxn
	var err error
	if prepared {
		core, err = m.db.BeginPreparedNativeConditionalTxn()
	} else {
		core, err = m.db.BeginNativeConditionalTxn()
	}
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
	t := &NativeJSONTxn{db: m.db, core: core, count: count, lease: lease, charge: charge}
	if count == 1 {
		t.bindings = t.singleBinding[:1]
		t.stageLeases = t.singleStageLease[:1]
		t.dependencies = t.inlineDeps[:0]
		t.members = t.singleMember[:0]
	} else {
		t.dependencies = make([]txnhistory.Dependency, 0, count)
		t.bindings = make([]nativeJSONTxnBinding, count)
		t.stageLeases = make([]*memtable.COWExternalLease, count)
	}
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
		nameKey := nameAt(i)
		g.probe.capture = core
		g.probe.database = m.db
		c, ok := base.names.Get(nameKey)
		if !ok {
			return nil, tree.ErrKeyNotFound
		}
		if err = nativeTypedProfile(m.db, *c.schema.meta); err != nil {
			return nil, err
		}
		if !c.schema.canonicalBSON {
			if _, err = nativeJSONMeta(*c.schema.meta); err != nil {
				return nil, err
			}
		}
		g.name = c.schema.meta.Name
		g.incarnation = c.incarnation
		g.probe.collection = c
		g.probe.meta = *c.schema.meta
		t.dependencies = append(t.dependencies, txnhistory.Dependency{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: nativeBorrowedStringBytes(g.name)})
		if domain, ok := c.domains.Get("primary"); ok {
			g.probe.domain, err = newNativeDomainRead(domain, core.Snapshot())
			if err != nil {
				return nil, err
			}
		}
	}
	return t, nil
}

func isCanonicalNativeBSONMeta(meta *CollectionMeta) bool {
	if meta == nil || !nativeBSONSetMeta(meta) ||
		len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 ||
		meta.Options.DocumentFormat != DocumentFormatBSON {
		return false
	}
	if ValidateCollectionName(meta.Name) != nil {
		return false
	}
	if _, err := backendRootStoragePolicy(meta.Options.DataRootStoragePolicy); err != nil {
		return false
	}
	if _, err := backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy); err != nil {
		return false
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments < 0 ||
		meta.Options.BufferedIndexedWriteMaxBytes < 0 ||
		meta.Options.BufferedIndexedWriteMaxRootRuns < 0 ||
		meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits < 0 ||
		(meta.Options.DisableBufferedIndexedAsyncFlush && (meta.Options.BufferedIndexedAsyncFlush || meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits != 0)) {
		return false
	}
	for i := range meta.Indexes {
		idx := &meta.Indexes[i]
		if len(idx.Components) != 0 {
			return false
		}
		switch idx.ValueType {
		case IndexValueString, IndexValueBool, IndexValueInt64, IndexValueDouble, IndexValueBSONOrderedV2:
		default:
			return false
		}
		if i > 0 && meta.Indexes[i-1].Name >= idx.Name {
			return false
		}
		if ValidateIndexName(idx.Name) != nil || ValidateIndexPath(idx.Field) != nil {
			return false
		}
	}
	return true
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
	if g.probe.meta.Options.ColumnStore != nil && !nativeOrdinaryColumnGraphMeta(g.probe.meta) {
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
	bindings                  []nativeJSONTxnBinding
	changes                   []nativeJSONPrimaryChange
	singleChange              [1]nativeJSONPrimaryChange
	count                     int
	bootstrap                 *nativeEngineVersion
	opening                   backenddb.NativeSemanticCapture
	reimported                *nativeEngineVersion // private owning reference, including failed preparation
	custody                   *nativeMutationCustody
	prepared                  *nativeEngineVersion
	raw                       bool
	returnCustodyOnInstall    bool
	reportProjectedGeneration bool
	participants              []*backenddb.CommandWALIntent
}

func (p *nativeJSONLogicalGroup) PrepareNativePublication(latest backenddb.NativeSemanticState, exact *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	custody := p.custody
	if p.returnCustodyOnInstall {
		// The original custody is already owned by the cache. This preparation
		// is the plan's last use of its borrowed ledger.
		p.custody = nil
	}
	if exact == nil {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	base, ok := latest.(*nativeEngineVersion)
	if latest != nil && !ok {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		base = p.bootstrap
		if base == nil || p.opening == nil || p.reimported != nil {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
		}
		// The original dependency check has already passed. A cold import is
		// private to this transaction, so physical maintenance cannot refresh
		// it with the published semantic state. Import the exact current catalog
		// before composing mutations; never change the opening or grant stale
		// staged graph recipes new provenance. An unchanged immutable catalog
		// on the same pinned pager needs no reimport, even if a compatible raw
		// checkpoint advanced applied LSN. Keep its original decoding capability.
		before := p.opening.Snapshot()
		beforeToken, beforeOK := before.StateToken()
		afterToken, afterOK := exact.StateToken()
		beforePager, afterPager := before.Pager(), exact.Pager()
		if !beforeOK || !afterOK || beforePager == nil || afterPager == nil {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrRootPublicationBasisMismatch
		}
		if beforePager != afterPager || beforeToken.SystemRootPageID != afterToken.SystemRootPageID {
			var err error
			p.reimported, err = importNativeJSONCatalogAt(p.opening, exact)
			if err != nil {
				return backenddb.NativeFinalizedPublication{}, err
			}
			base = p.reimported
		}
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
		if err := validateNativeGraphPreparation(c, g.plan.typed); err != nil {
			return backenddb.NativeFinalizedPublication{}, err
		}
		changes[count] = nativeJSONPrimaryChange{collection: c, entries: g.plan.mutations, additional: g.plan.additional, primaryPolicy: g.plan.primaryPolicy, typed: g.plan.typed}
		count++
	}
	var next *nativeEngineVersion
	var err error
	if count == 0 {
		next, err = prepareNativeCommandWALNoop(base)
	} else {
		next, err = prepareNativeCollectionChanges(base, changes[:count], nil, p.reportProjectedGeneration)
	}
	if err != nil {
		return backenddb.NativeFinalizedPublication{State: next}, err
	}
	if err = nativeTypedEnsureConstructible(next, exact); err != nil {
		return backenddb.NativeFinalizedPublication{State: next}, err
	}
	for i := 0; i < count; i++ {
		replacement, _ := next.collections.Get(changes[i].collection.incarnation)
		(nativeCollectionLifetime{}).Retain(replacement)
		custody.adoptCollection(replacement)
	}
	p.prepared = next
	return backenddb.NativeFinalizedPublication{State: next}, nil
}

func (t *NativeJSONTxn) rootCoveredBSONUpdate() bool {
	if !t.structuredBSONUpdate || t.count != 1 || t.rawStaged || len(t.members) != 1 ||
		t.members[0].Kind != commitlog.CommandKindCollectionUpdateBatchByID {
		return false
	}
	g := &t.bindings[0]
	meta := &g.probe.meta
	if !nativeBSONSetMeta(meta) || len(meta.TextIndexes) != 0 || len(meta.VectorIndexes) != 0 ||
		g.typed != nil || g.projection != nil || g.plan == nil || g.plan.typed != nil || len(g.plan.additional) != 0 || len(g.plan.mutations) == 0 {
		return false
	}
	for _, row := range g.plan.mutations {
		if row.Flags != node.FlagInline {
			return false
		}
	}
	return true
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

func (t *NativeJSONTxn) Commit() error { return t.commit(false) }

// Prepared insertion and deterministic apply request physical completion. All
// modes retain the same opening, logical effects and original custody.
func (t *NativeJSONTxn) commit(sealed bool) error { return t.commitNativeApply(sealed, nil) }

// An apply request keeps the original lowered singleton frame even for a
// no-effect command. It borrows this synchronous transaction, never a publisher.
type nativeCommandWALApplyCommit struct {
	frame          commandwalapply.LoweredFrame
	sync           bool
	observe        func(uint64) error
	result         backenddb.NativePublicationResult
	alreadyApplied bool
}

func (t *NativeJSONTxn) commitNativeApply(sealed bool, apply *nativeCommandWALApplyCommit) error {
	return t.commitNativePlan(sealed, apply, false)
}

func (t *NativeJSONTxn) commitNativePlan(sealed bool, apply *nativeCommandWALApplyCommit, typedGroup bool) error {
	return t.commitNativePlanWithStats(sealed, apply, typedGroup, nil)
}

func (t *NativeJSONTxn) commitNativePlanWithStats(sealed bool, apply *nativeCommandWALApplyCommit, typedGroup bool, stats *CollectionInsertStats) error {
	return t.commitNativePlanObserved(sealed, apply, typedGroup, stats, nil, nil, nil)
}

// The optional observations describe clean, preassignment refusals for their
// original one-shot callers. An explicit transaction never rebases itself.
func (t *NativeJSONTxn) commitNativePlanObserved(sealed bool, apply *nativeCommandWALApplyCommit, typedGroup bool, stats *CollectionInsertStats, expired *bool, retry *nativeBSONSetRetry, projected **nativeInsertProjectedRefusal) (resultErr error) {
	return t.commitNativePlanWithJournal(sealed, apply, typedGroup, stats, expired, retry, projected, nil)
}

func (t *NativeJSONTxn) commitNativePlanWithJournal(sealed bool, apply *nativeCommandWALApplyCommit, typedGroup bool, stats *CollectionInsertStats, expired *bool, retry *nativeBSONSetRetry, projected **nativeInsertProjectedRefusal, journal *nativeOperationJournal) (resultErr error) {
	if expired != nil {
		*expired = false
	}
	if retry != nil {
		retry.rowConflict = false
	}
	if t == nil {
		return backenddb.ErrClosed
	}
	t.mu.Lock()
	if t.core == nil {
		t.mu.Unlock()
		return backenddb.ErrClosed
	}
	var committing *backenddb.NativePreparedPublication
	defer func() {
		if committing != nil {
			// Attachment clears Custody before the cache can assign or install
			// a frame. If CommitNative unwinds, only an unaccepted original
			// edge can return to this transaction's cleanup.
			t.custody, _ = committing.Custody.(*nativeMutationCustody)
			committing.Reprepare.(*nativeJSONLogicalGroup).participants = nil
			committing = nil
		}
		err, stages, lease := t.closeLocked()
		if err != nil {
			clearNativeCommitRetry(expired, retry, projected)
		}
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
	// Only the original singleton103 graph apply may have a typed recipe and
	// catalog effect without any CRUD members.
	graphSingleton := apply != nil && apply.frame.Kind == commitlog.CommandKindCollectionRebuildVectorIndex && !typedGroup && t.count == 1 && !t.rawStaged && len(t.members) == 0 && nativeTypedGraphOnly(t.bindings[0].typed)
	if err := t.finalizeNativeRowsWithStats(graphSingleton, stats); err != nil {
		// Final rows (including unique ownership) are private preparation;
		// CommitNative has not assigned or installed this mutation. An apply
		// owner may already have an assigned frame, so never mark that route.
		if apply == nil {
			return mutationNotAcceptedError(err)
		}
		return err
	}
	count, totalEffects := 0, 0
	for i := 0; i < t.count; i++ {
		g := &t.bindings[i]
		if g.plan != nil {
			count++
			if len(g.effects) > int(^uint(0)>>1)-totalEffects {
				return memtable.ErrCOWCapacity
			}
			totalEffects += len(g.effects)
		}
	}
	if count == 0 && apply == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	rawEntries := t.core.NativeRawEntries()
	returnCustodyOnInstall := !sealed && apply == nil && !typedGroup && len(rawEntries) == 0 && t.rootCoveredBSONUpdate()
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
	var commitCharge uint64
	for _, bytes := range [...]uint64{
		uint64(unsafe.Sizeof(nativeJSONLogicalGroup{})),
		uint64(len(t.members)+1) * uint64(unsafe.Sizeof(commitlog.CompositeMutationMember{})),
		uint64(t.count) * uint64(unsafe.Sizeof(nativeJSONPrimaryChange{})),
		uint64(unsafe.Sizeof(backenddb.NativePreparedPublication{})),
		uint64(unsafe.Sizeof(backenddb.CommandWALIntent{})),
	} {
		n := memtable.COWAllocationCharge(bytes)
		if n > ^uint64(0)-commitCharge {
			return memtable.ErrCOWCapacity
		}
		commitCharge += n
	}
	if commitCharge > ^uint64(0)-t.charge {
		return memtable.ErrCOWCapacity
	}
	if err := t.lease.Resize(t.charge + commitCharge); err != nil {
		return err
	}
	t.charge += commitCharge
	// A singleton's original binding receipt stays live through synchronous
	// CommitNative, including cancellation and assigned/installed errors. Borrow
	// that final image; the journal still takes its own retained key ownership.
	// Multiple bindings keep the original ordered, independently owned flattening.
	var effects []txnhistory.Effect
	if t.count == 1 {
		effects = t.bindings[0].effects
	} else {
		if uint64(totalEffects) > ^uint64(0)/uint64(unsafe.Sizeof(txnhistory.Effect{})) {
			return memtable.ErrCOWCapacity
		}
		if err := admit(uint64(totalEffects) * uint64(unsafe.Sizeof(txnhistory.Effect{}))); err != nil {
			return err
		}
		effects = make([]txnhistory.Effect, 0, totalEffects)
		for i := 0; i < t.count; i++ {
			effects = append(effects, t.bindings[i].effects...)
		}
	}
	var rawGrowth int
	var memberBuf [2]commitlog.CompositeMutationMember
	var members []commitlog.CompositeMutationMember
	if len(t.members)+1 <= len(memberBuf) {
		members = memberBuf[:len(t.members)+1]
	} else {
		members = make([]commitlog.CompositeMutationMember, len(t.members)+1)
	}
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
	for _, member := range t.members {
		members[memberCount] = member
		memberCount++
	}
	kind, scope, format, payload := members[0].Kind, commitlog.CommandScopeCollection, members[0].PayloadFormat, members[0].Payload
	if graphSingleton {
		kind, scope, format, payload = apply.frame.Kind, apply.frame.Scope, apply.frame.PayloadFormat, apply.frame.Payload
	}
	typedMember := false
	for i := 0; i < t.count; i++ {
		typedMember = typedMember || nativeTypedMeta(t.bindings[i].probe.meta)
	}
	// The fixed zero-row graph recipe carries its original singleton103 frame;
	// it has no fabricated CRUD member or composite transport representation.
	if !typedGroup && (memberCount > 1 || typedMember) && !graphSingleton {
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
	t.logicalGroup = nativeJSONLogicalGroup{bindings: t.bindings, count: t.count, bootstrap: t.imported, opening: t.core, custody: t.custody, raw: t.rawStaged, returnCustodyOnInstall: returnCustodyOnInstall, reportProjectedGeneration: projected != nil}
	plan := &t.logicalGroup
	if t.count <= len(plan.singleChange) {
		plan.changes = plan.singleChange[:t.count]
	} else {
		plan.changes = make([]nativeJSONPrimaryChange, t.count)
	}
	defer func(p *nativeJSONLogicalGroup) {
		// CommitNative and its finalizer are synchronous. Accepted sparse
		// custody owns its replacement descriptors; this temporary ancestor
		// belongs only to preparation. Drop borrowed aliases before refund,
		// outside the publication gate and before the original import closes.
		reimported := p.reimported
		clear(p.changes)
		*p = nativeJSONLogicalGroup{}
		if reimported != nil {
			cleanupErr := finishPrivateNative(reimported.Release())
			if cleanupErr != nil {
				clearNativeCommitRetry(expired, retry, projected)
			}
			resultErr = errors.Join(resultErr, cleanupErr)
		}
	}(plan)
	t.preparedPub = backenddb.NativePreparedPublication{RequireInlineCompositeRaw: t.rawStaged, Basis: t.core.State(), StartSequence: t.core.LogicalSequence(), Reprepare: plan, Custody: t.custody, Effects: effects, Dependencies: t.dependencies, ReturnCustodyOnInstall: returnCustodyOnInstall}
	native := &t.preparedPub
	if retry != nil && len(t.dependencies) >= 2 {
		// The singleton's original catalog dependency precedes the point read
		// recorded by touchNativeRow. Later unique-key dependencies still apply.
		point := t.dependencies[1]
		if point.Kind == txnhistory.PointEffect && point.Namespace == t.bindings[0].namespace() && bytes.Equal(point.Key, retry.id) {
			native.ObservePointConflict = 2
		}
	}
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
	if apply != nil {
		kind, scope, format, payload = apply.frame.Kind, apply.frame.Scope, apply.frame.PayloadFormat, apply.frame.Payload
		native.ObserveAssigned = apply.observe
	}
	var intent *backenddb.CommandWALIntent
	var err error
	if typedGroup {
		intent, plan.participants, err = t.nativeTypedUpsertIntents(members[:memberCount], admit)
	} else {
		intent, err = t.db.NewTrustedCommandWALIntent(kind, scope, format, payload)
	}
	if err != nil {
		t.custody = nil
		return errors.Join(err, cancelPrivateNativePublication(native))
	}
	if sealed {
		if err = intent.PrepareSealedReceiptWithAdmission(t.core.NativeAllocator()); err != nil {
			t.custody = nil
			return errors.Join(err, cancelPrivateNativePublication(native))
		}
		defer intent.ReleaseNativeSealedReceipt()
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
	native.RequestJournalCompletion = journal != nil && journal.requested && apply == nil && !typedGroup && !sealed
	sync := false
	if apply != nil {
		sync = apply.sync
	}
	if typedGroup {
		sync = true
	}
	var publishStarted time.Time
	if stats != nil {
		publishStarted = time.Now()
	}
	if returnCustodyOnInstall {
		// Clear non-owning caller aliases before the cache can transfer the
		// successful custody to the cleanup worker. If CommitNative declines
		// ownership, the original native descriptor is still cancelled below.
		t.bindings[0].plan.custody = nil
	}
	t.custody = nil
	committing = native
	result, err := t.core.CommitNative(native, intent, sync)
	committing = nil
	if journal != nil && journal.requested {
		journal.completion = result.Journal
	}
	if expired != nil {
		// Exact identity also excludes a lower-layer cleanup error joined to
		// the sentinel. Consumed alone only transfers descriptor ownership.
		*expired = apply == nil && !typedGroup && err == backenddb.ErrTxnTooOld &&
			!result.Assigned && result.AssignedLSN == 0 && !result.Uncertain &&
			!result.LogicalInstalled && !result.WALCompleted && !result.WALStable &&
			!backenddb.CommitPublicationAccepted(err) &&
			!errors.Is(err, ErrCommitAmbiguous)
	}
	if projected != nil && apply == nil && !typedGroup &&
		!result.Assigned && result.AssignedLSN == 0 && !result.Uncertain && !result.LogicalInstalled &&
		!result.WALCompleted && !result.WALStable {
		// The exact type excludes a release failure joined into the refusal.
		*projected, _ = err.(*nativeInsertProjectedRefusal)
	}
	if retry != nil {
		retry.rowConflict = native.ObservePointConflict != 0 && result.OnlyPointConflict && err == backenddb.ErrConcurrentModification &&
			!result.Assigned && result.AssignedLSN == 0 && !result.Uncertain && !result.LogicalInstalled &&
			!result.WALCompleted && !result.WALStable
	}
	if stats != nil {
		stats.Publish = time.Since(publishStarted)
	}
	// The assigned binding borrows these synchronous request intents only.
	// No failed sparse custody may retain them after the input receipt ends.
	plan.participants = nil
	if apply != nil {
		apply.result = result
	}

	if apply == nil && err != nil && (result.Assigned || result.LogicalInstalled) {
		err = commitAmbiguousError("native command publication completion", err)
	}
	if !result.Consumed {
		cleanupErr := cancelPrivateNativePublication(native)
		if cleanupErr != nil {
			clearNativeCommitRetry(expired, retry, projected)
		}
		err = errors.Join(err, cleanupErr)
	}
	native, intent, plan, payload, effects = nil, nil, nil, nil, nil
	return err
}

func clearNativeCommitRetry(expired *bool, retry *nativeBSONSetRetry, projected **nativeInsertProjectedRefusal) {
	if expired != nil {
		*expired = false
	}
	if retry != nil {
		retry.rowConflict = false
	}
	if projected != nil {
		*projected = nil
	}
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
	clear(t.singleMember[:])
	t.dependencies = nil
	clear(t.inlineDeps[:])
	t.preparedPub = backenddb.NativePreparedPublication{}

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
