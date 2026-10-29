package collections

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

// A rewrite retains all decoded records until planning ends. Input admission
// bounds its entry slices/maps; this workspace adds the six BTree structures
// and every codec attempt (including discarded smaller-chunk attempts).
type nativeTextRewriteWorkspace struct {
	admit              func(uint64) error
	operations, tables uint64
}

func newNativeTextRewriteWorkspace(admit func(uint64) error) (*nativeTextRewriteWorkspace, error) {
	n, err := nativeTextTableCharge(0, 6)
	if err != nil {
		return nil, err
	}
	if err = admit(n + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextRewriteWorkspace{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textV2RewritePlan{})))); err != nil {
		return nil, err
	}
	return &nativeTextRewriteWorkspace{admit: admit, tables: n}, nil
}

func (w *nativeTextRewriteWorkspace) admitMutation() error {
	if w == nil {
		return nil
	}
	if w.operations == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	n, err := nativeTextTableCharge(w.operations+1, 6)
	if err != nil {
		return err
	}
	if n > w.tables {
		if err = w.admit(n - w.tables); err != nil {
			return err
		}
	}
	w.operations++
	w.tables = n
	return nil
}

func (w *nativeTextRewriteWorkspace) admitCodec(n uint64) error {
	if w == nil {
		return nil
	}
	return w.admit(n)
}

// Only immutable sparse deltas enter the existing publisher. A rewrite does
// not replace a domain, reinterpret its keys, or advance document generation.
type nativeTextRewritePublication struct {
	collection *nativeCollectionState
	bootstrap  *nativeEngineVersion
	domains    []nativeNamedDomainChange
	custody    *nativeMutationCustody
}

func (p *nativeTextRewritePublication) PrepareNativePublication(latest backenddb.NativeSemanticState, _ *backenddb.Snapshot) (backenddb.NativeFinalizedPublication, error) {
	base, ok := latest.(*nativeEngineVersion)
	if latest == nil {
		base, ok = p.bootstrap, p.bootstrap != nil
	}
	if !ok {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: p.collection, sourceGeneration: p.collection.documentGeneration, domains: p.domains}})
	if err == nil {
		c, _ := next.collections.Get(p.collection.incarnation)
		(nativeCollectionLifetime{}).Retain(c)
		p.custody.adoptCollection(c)
	}
	return backenddb.NativeFinalizedPublication{State: next}, err
}

func (c *Collection) nativeRewriteTextIndex(ctx context.Context, indexName string, run textV2RewriteRunOptions) (stats TextIndexRewriteStats, before TextIndexStorageStats, reason string, resultErr error) {
	if c.manager.isClosing() {
		return stats, before, "", backenddb.ErrClosed
	}
	if c.nativeJSON.db.CommandWALEnabled() {
		return stats, before, "", fmt.Errorf("%w: text-v2 rewrite is rejected under command_wal_v2 until collection text maintenance commands are supported", backenddb.ErrCommandWALRejected)
	}
	if err := ctx.Err(); err != nil {
		return stats, before, "", err
	}
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return stats, before, "", err
	}
	unlock := c.lockVectorIndexMutation()
	defer unlock()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return stats, before, "", err
	}
	base := read.engine
	lease, err := base.allocator.AdmitNativeBytes(0)
	if err != nil {
		return stats, before, "", errors.Join(err, read.Close())
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()); lease.Close() }()
	var charge uint64
	admit := func(n uint64) error {
		n = memtable.COWAllocationCharge(n)
		if n > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charge + n); err != nil {
			return err
		}
		charge += n
		return nil
	}
	// The nested scope drops all planner/output aliases before capture and
	// operation receipt release, on success and on every refused preparation.
	return func() (TextIndexRewriteStats, TextIndexStorageStats, string, error) {
		def, found := findTextIndex(read.meta.TextIndexes, indexName)
		if !found {
			return stats, before, "", ErrIndexNotFound
		}
		if def.Version != TextIndexVersionV2 {
			return stats, before, "", fmt.Errorf("%w: text index %q is %q; rewrite/merge is only available for explicit text index v2", ErrTextIndexUnavailable, indexName, def.Version)
		}
		catalog, err := newNativeTextCatalog(read, admit)
		if err != nil {
			return stats, before, "", err
		}
		// Term strings, old-key lists and term/processed-key maps derive from
		// keys, whose size is independent of their often tiny stats values.
		// Eight full key cohorts and 1024 bytes per inspected key cover their
		// map occupancy/growth, slice headers and retained tombstone keys.
		catalog.nativeText.admitKey = func(n uint64) error {
			if n > (^uint64(0)-1024)/8 {
				return memtable.ErrCOWCapacity
			}
			return admit(memtable.COWAllocationCharge(n*8 + 1024))
		}
		plan, before, reason, err := prepareTextV2RewriteRun(ctx, read.capture.Snapshot(), catalog, def, run)
		if err != nil {
			return stats, before, "", err
		}
		defer resetTextV2RewritePlanTables(plan)
		if plan.stats.Noop {
			return plan.stats, before, reason, nil
		}
		// Seven names/dependencies; at most six changed tables. Positions remain
		// immutable but are in the read dependency closure.
		// Actual arrays plus both sets of qualified root-name allocations. Names
		// are validated and bounded by the collection/index name contracts.
		headerBytes := 6*uint64(unsafe.Sizeof(nativeNamedDomainChange{})) + 8*uint64(unsafe.Sizeof(txnhistory.Dependency{})) + 6*uint64(unsafe.Sizeof(txnhistory.Effect{})) + 13*uint64(unsafe.Sizeof(string(""))) + 6*uint64(unsafe.Sizeof(memtable.Table(nil))) + 6*uint64(unsafe.Sizeof(backenddb.OrderedRootStoragePolicy(0))) + uint64(unsafe.Sizeof(nativeTextRewritePublication{})) + uint64(unsafe.Sizeof(backenddb.NativePreparedPublication{}))
		if err = admit(memtable.COWAllocationCharge(headerBytes) + 13*memtable.COWAllocationCharge(uint64(len(c.name)+len(indexName)+48)) + memtable.COWAllocationCharge(uint64(len(c.name)))); err != nil {
			return stats, before, "", err
		}

		names := []string{collectionTextV2PostingBlocksRootName(c.name, indexName), collectionTextV2TermsRootName(c.name, indexName), collectionTextV2DocIDRootName(c.name, indexName), collectionTextV2DocMapRootName(c.name, indexName), collectionTextV2NormBlocksRootName(c.name, indexName), collectionTextV2GenerationsRootName(c.name, indexName)}
		tables := []memtable.Table{plan.postingBlocksTable, plan.termsTable, plan.docIDTable, plan.docMapTable, plan.normTable, plan.generationsTable}
		policies := make([]backenddb.OrderedRootStoragePolicy, len(names))
		for i, name := range names {
			policies[i], err = collectionRootStoragePolicy(read.meta, name)
			if err != nil {
				return stats, before, "", err
			}
			tables[i].Freeze()
		}
		deltas, err := nativeTextTables(names, policies, tables, admit)
		if err != nil {
			return stats, before, "", err
		}
		domains := make([]nativeNamedDomainChange, 0, len(deltas))
		effects := make([]txnhistory.Effect, 0, len(deltas))
		dependencies := make([]txnhistory.Dependency, 0, 8)
		for _, root := range collectionTextRootNamesForDefinition(c.name, def) {
			name := strings.TrimPrefix(root, c.name+"/")
			d, ok := read.collection.domains.Get(name)
			if !ok {
				return stats, before, "", backenddb.ErrNativeSemanticUnsupported
			}
			dependencies = append(dependencies, txnhistory.Dependency{Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: read.collection.incarnation, Domain: nativeJSONDomainNumber(read.meta, name), Interpretation: d.interpretation}, Kind: txnhistory.RangeEffect})
		}
		dependencies = append(dependencies, txnhistory.Dependency{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(c.name)})
		for _, delta := range deltas {
			if len(delta.entries) == 0 {
				continue
			}
			name := strings.TrimPrefix(delta.name, c.name+"/")
			d, _ := read.collection.domains.Get(name)
			domains = append(domains, nativeNamedDomainChange{name: name, policy: delta.policy, entries: delta.entries, expectedInterpretation: d.interpretation})
			effects = append(effects, txnhistory.Effect{Namespace: txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: read.collection.incarnation, Domain: nativeJSONDomainNumber(read.meta, name), Interpretation: d.interpretation}, Kind: txnhistory.RangeEffect})
		}
		custody, err := newNativeMutationCustody(0, 1, base.allocator)
		if err != nil {
			return stats, before, "", err
		}
		logical := &nativeTextRewritePublication{collection: read.collection, bootstrap: base, domains: domains, custody: custody}
		pub := &backenddb.NativePreparedPublication{Basis: read.capture.State(), StartSequence: read.capture.LogicalSequence(), Reprepare: logical, Custody: custody, Effects: effects, Dependencies: dependencies}
		result, err := c.nativeJSON.db.WriteNativePublication(pub, nil, false)
		if err != nil && (result.Assigned || result.LogicalInstalled) {
			err = commitAmbiguousError("native text rewrite completion", err)
		}
		if !result.Consumed {
			err = errors.Join(err, cancelPrivateNativePublication(pub))
		}
		if err != nil {
			return stats, before, "", err
		}
		return plan.stats, before, "", nil
	}()
}
