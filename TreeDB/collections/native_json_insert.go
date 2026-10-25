package collections

import (
	"errors"
	"strings"
	"time"
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
func (m *NativeJSONManager) InsertBatch(name string, ids, documents [][]byte) error {
	tx, err := m.Begin(name)
	_, err = finishNativeJSONInsert(tx, err, ids, documents, nil, false)
	return err
}

func (m *NativeJSONManager) insertPrepared(name string, ids, documents [][]byte, stats *CollectionInsertStats, ordinaryOnly bool) (bool, error) {
	tx, err := m.beginPrepared(name)
	return finishNativeJSONInsert(tx, err, ids, documents, stats, ordinaryOnly)
}

func finishNativeJSONInsert(tx *NativeJSONTxn, err error, ids, documents [][]byte, stats *CollectionInsertStats, ordinaryOnly bool) (recordStats bool, resultErr error) {
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	if tx.bindings[0].probe.meta.Options.ColumnStore != nil {
		if ordinaryOnly {
			return false, backenddb.ErrNativeSemanticUnsupported
		}
		// Plain typed insertion keeps its existing route and Last behavior.
		// Typed diagnostics use their distinct maintained API.
		stats = nil
	}
	var started time.Time
	if stats != nil {
		started = time.Now()
	}
	if err = tx.InsertBatch(ids, documents); err != nil {
		return false, mutationNotAcceptedError(err)
	}
	if stats != nil {
		stats.PrepareDocuments = time.Since(started)
	}
	return stats != nil, tx.commitNativePlanWithStats(false, nil, false, stats)
}

// collectNativeInsertStats observes the original prepared work without another
// capture or document decode. Its one array and cloned names become diagnostic
// output owned by the Collection handle only after successful completion; the
// request gets a separate slice only when WithStats was requested. Until then
// construction is admitted on the original transaction's staging lease.
func (t *NativeJSONTxn) collectNativeInsertStats(g *nativeJSONTxnBinding, direct *directBufferedInsertPlan, stats *CollectionInsertStats) error {
	indexes := g.probe.meta.Indexes
	if uint64(len(indexes)) > ^uint64(0)/uint64(unsafe.Sizeof(CollectionSecondaryRunStats{})) {
		return memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(len(indexes)) * uint64(unsafe.Sizeof(CollectionSecondaryRunStats{})))
	for _, index := range indexes {
		n := memtable.COWAllocationCharge(uint64(len(index.Name)))
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		charge += n
	}
	if charge != 0 {
		if err := t.admitStage(g, charge); err != nil {
			return err
		}
	}
	s := stats
	s.Documents, s.Indexes = len(g.rows.rows), len(indexes)
	// These are prepared logical domain runs, not physical checkpoint roots.
	s.Runs = 1 + len(g.plan.additional)
	if len(indexes) != 0 {
		s.SecondaryRuns = make([]CollectionSecondaryRunStats, len(indexes))
	}
	for i, index := range indexes {
		s.SecondaryRuns[i].IndexName = strings.Clone(index.Name)
	}
	for _, run := range direct.secondaryRootPlans {
		entry := &s.SecondaryRuns[run.runtimeIdx]
		entry.Entries, entry.KeyBytes = len(run.entries), run.keyBytes
		// The maintained native planner has sorted the prepared run already;
		// this does not claim that caller input was originally ordered.
		entry.AlreadySorted = true
		s.SecondaryEntries += entry.Entries
		s.SecondaryKeyBytes += entry.KeyBytes
		if entry.Entries != 0 {
			s.SecondarySortedRuns++
		}
	}
	return nil
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

// nativeJSONDomainChange is one immutable maintained-planner root delta.
// Root names are collection-relative; they are never physical root identities.
type nativeJSONDomainChange struct {
	name    string
	policy  backenddb.OrderedRootStoragePolicy
	entries []memtable.COWMutation
}

type nativeJSONPrimaryChange struct {
	collection    *nativeCollectionState
	entries       []memtable.COWMutation
	primaryPolicy backenddb.OrderedRootStoragePolicy
	additional    []nativeJSONDomainChange
	typed         *nativeTypedDelta
}

// nativeNamedDomainChange keeps replacement-only metadata off the ordinary
// maintained index delta representation and its per-write allocation budget.
type nativeNamedDomainChange struct {
	name    string
	policy  backenddb.OrderedRootStoragePolicy
	entries []memtable.COWMutation
	// Replacement discards all old physical and logical sources. The expected
	// interpretation authenticates the captured domain before issuing a fresh one.
	replace                bool
	expectedInterpretation uint64
}

// nativeNamedCollectionChange borrows an already captured immutable collection.
// Named-only changes never advance documentGeneration. Saved images must cover
// exactly the document generation and prior domain interpretation they read.
type nativeNamedCollectionChange struct {
	collection       *nativeCollectionState
	sourceGeneration uint64
	domains          []nativeNamedDomainChange
}

func prepareNativeNamedDomains(base *nativeEngineVersion, changes []nativeNamedCollectionChange) (*nativeEngineVersion, error) {
	return prepareNativeCollectionChanges(base, nil, changes)
}

func prepareNativeJSONPrimary(base *nativeEngineVersion, c *nativeCollectionState, entries []memtable.COWMutation) (*nativeEngineVersion, error) {
	return prepareNativeJSONPrimaries(base, []nativeJSONPrimaryChange{{collection: c, entries: entries}})
}
func prepareNativeJSONPrimaries(base *nativeEngineVersion, changes []nativeJSONPrimaryChange) (*nativeEngineVersion, error) {
	return prepareNativeCollectionChanges(base, changes, nil)
}

func prepareNativeCollectionChanges(base *nativeEngineVersion, changes []nativeJSONPrimaryChange, named []nativeNamedCollectionChange) (*nativeEngineVersion, error) {
	count := len(changes) + len(named)
	if base == nil || base.inventory != nil || count == 0 || (len(changes) != 0 && len(named) != 0) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	domainCount := len(changes)
	for _, change := range changes {
		if len(change.additional) > int(^uint(0)>>1)-domainCount {
			return nil, memtable.ErrCOWCapacity
		}
		domainCount += len(change.additional)
	}
	for _, change := range named {
		if len(change.domains) > int(^uint(0)>>1)-domainCount {
			return nil, memtable.ErrCOWCapacity
		}
		domainCount += len(change.domains)
	}
	next, err := newNativeEnginePreparation(base, count, domainCount, base.allocator)
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
	p.names, err = cowbtree.NewOwnedMapBuilder(base.names, count, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.collections, err = cowbtree.NewOwnedMapBuilder(base.collections, count, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	p.pending, err = cowbtree.NewOwnedMapBuilder(base.pending, count, nativeCollectionLifetime{}, admit)
	if err != nil {
		return next, err
	}
	for changeIndex := 0; changeIndex < count; changeIndex++ {
		var change nativeJSONPrimaryChange
		var namedChange nativeNamedCollectionChange
		primary := len(changes) != 0
		if primary {
			change = changes[changeIndex]
		} else {
			namedChange = named[changeIndex]
			change.collection = namedChange.collection
		}
		c, entries := change.collection, change.entries
		if c == nil {
			return next, backenddb.ErrNativeSemanticUnsupported
		}
		current, exists := base.collections.Get(c.incarnation)
		if !exists || (primary && current != c) || (!primary && current.schema != c.schema) {
			return next, backenddb.ErrConcurrentModification
		}
		if !primary {
			c = current
		}
		if !primary && c.documentGeneration != namedChange.sourceGeneration {
			return next, backenddb.ErrConcurrentModification
		}
		generation := c.documentGeneration
		if primary && (len(entries) != 0 || change.typed != nil) {
			if generation == ^uint64(0) {
				return next, memtable.ErrCOWCapacity
			}
			generation++
		}
		domainCount := 1 + len(change.additional)
		if !primary {
			domainCount = len(namedChange.domains)
		}
		p.domainBuilder, err = cowbtree.NewOwnedMapBuilder(c.domains, domainCount, nativeDomainLifetime{}, admit)
		if err != nil {
			return next, err
		}
		p.pendingDomainBuilder, err = cowbtree.NewOwnedMapBuilder(c.pending, domainCount, nativeDomainLifetime{}, admit)
		if err != nil {
			return next, err
		}
		for i := 0; i < domainCount; i++ {
			name, policy, rows := "primary", change.primaryPolicy, entries
			if primary && i > 0 {
				delta := change.additional[i-1]
				name, policy, rows = delta.name, delta.policy, delta.entries
			}
			var delta nativeNamedDomainChange
			if !primary {
				delta = namedChange.domains[i]
				name, policy, rows = delta.name, delta.policy, delta.entries
				if name == "primary" || name == nativeCatalogDomainName || name == "" {
					return next, backenddb.ErrNativeSemanticUnsupported
				}
			}
			old, _ := c.domains.Get(name)
			root := uint64(0)
			if old != nil {
				root = old.baseRoot
				// The legacy primary-only adapter did not carry a policy.
				// Preserve its exact original physical-domain policy.
				if primary && i == 0 {
					policy = old.storagePolicy
				}
			}
			interpretation := uint64(1)
			if old != nil {
				interpretation = old.interpretation
			}
			if !primary {
				expected := uint64(0)
				if old != nil {
					expected = old.interpretation
				}
				if delta.expectedInterpretation != expected {
					return next, backenddb.ErrConcurrentModification
				}
				if delta.replace {
					if interpretation == ^uint64(0) {
						return next, memtable.ErrCOWCapacity
					}
					if old != nil {
						interpretation++
					}
					old, root = nil, 0
				}
			}
			domain, err := prepareNativeDomainUpdate(old, name, root, interpretation, policy, rows, base.allocator)
			if domain != nil {
				p.ownedDomains = append(p.ownedDomains, domain)
			}
			if err != nil {
				return next, err
			}
			if !primary && delta.replace {
				domain.sourceGeneration = namedChange.sourceGeneration
			}
			if primary && i == 0 && change.typed != nil {
				// Clone only private headers: their original vector receipts remain
				// shared, while reprepare never grafts into the staged chain itself.
				typed, cloneErr := cloneNativeTypedSuffix(change.typed, nil, admit)
				if typed != nil {
					tail := typed
					for tail.previous != nil {
						tail = tail.previous
					}
					tail.previous = domain.typed // transfer this original inherited edge
					domain.typed = typed         // adopt partial headers before observing error
				}
				if cloneErr != nil {
					return next, cloneErr
				}
			}
			if err = p.domainBuilder.Set(name, domain); err != nil {
				return next, err
			}
			if err = p.pendingDomainBuilder.Set(name, domain); err != nil {
				return next, err
			}
		}
		p.domains, p.pendingDomains = p.domainBuilder.Seal(), p.pendingDomainBuilder.Seal()
		p.domainBuilder, p.pendingDomainBuilder = nil, nil
		c.schema.retain()
		p.schema = c.schema
		replacement, err := newNativeCollectionState(c.incarnation, generation, p.schema, p.domains, p.pendingDomains, admit)
		if err != nil {
			return next, err
		}
		p.schema, p.domains, p.pendingDomains = nil, nil, nil
		p.ownedCollections = append(p.ownedCollections, replacement)
		if err = p.names.Set(c.schema.meta.Name, replacement); err != nil {
			return next, err
		}
		if err = p.collections.Set(c.incarnation, replacement); err != nil {
			return next, err
		}
		if err = p.pending.Set(c.incarnation, replacement); err != nil {
			return next, err
		}
	}
	return next, nil
}

// The registered singleton preparation retains its unindexed capability only.
// Indexed foreground writes use the original ConditionalTxn logical planner;
// startup recovery invokes the registered replay handler, not this callback.
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
	transferred := false
	defer func() {
		if !transferred {
			lease.Close()
		}
	}()
	decoded, err := commitlog.DecodeCollectionInsertBatchByIDPayload(env.Payload)
	if err != nil {
		return nil, err
	}
	defer func() { decoded = commitlog.CollectionInsertBatchByIDPayload{} }()
	c, ok := base.names.Get(decoded.Collection)
	if !ok {
		return nil, tree.ErrKeyNotFound
	}
	if len(c.schema.meta.Indexes) != 0 || c.schema.meta.Options.ColumnStore != nil || normalizedDocumentFormat(c.schema.meta.Options.DocumentFormat) == DocumentFormatTemplateV1 {
		return nil, backenddb.ErrNativeSemanticUnsupported
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
	custody.refund.control = lease
	transferred = true
	// Effects own keys independently of decoded input through the retained source.
	domain, _ := replacement.domains.Get("primary")
	for i := range effects {
		row, _ := domain.pending.Get(effects[i].Key)
		effects[i].Key = nativeBorrowedStringBytes(row.Key)
	}
	return &backenddb.NativePreparedPublication{Basis: basis, State: next, Custody: custody, Effects: effects, Dependencies: effects}, nil
}
