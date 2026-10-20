package collections

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Existing Collection methods own one original ConditionalTxn. The callback
// scope completes before CommitNative: logical reprepare never invokes a body.
func (c *Collection) nativeUpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	results, _, err := c.nativeUpdateBatchMode(items, updateBatchModeAny, nil)
	return results, err
}

// A conditional decline closes the original private transaction without claiming
// an identity. The same captured metadata and encoded old/new scalar states are
// used by ordinary callbacks and structured BSON updates.
func (c *Collection) nativeUpdateBatchMode(items []UpdateBatchItem, mode updateBatchMode, bsonItems []BSONSetUpdateBatchItem) (results []UpdateBatchResult, complete bool, resultErr error) {
	if c.manager.isClosing() {
		return nil, false, backenddb.ErrClosed
	}
	// Preserve the ordinary empty-batch fast path. Structured batches still
	// validate the captured BSON role, including when the input is empty.
	if bsonItems == nil && len(items) == 0 {
		return nil, true, nil
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	count := len(items)
	if bsonItems != nil {
		count = len(bsonItems)
	}
	g := tx.binding(c.name)
	if bsonItems != nil {
		items, err = tx.prepareNativeBSONSetItems(c.name, bsonItems)
		if err != nil {
			return nil, false, err
		}
	}
	if len(items) == 0 {
		return nil, true, nil
	}
	if mode == updateBatchModeNoSecondaryUniqueIndexes && collectionMetaHasSecondaryUniqueIndex(g.probe.meta) {
		if err = tx.admitRowControls(g, count); err != nil {
			return nil, false, err
		}
		for _, item := range items {
			if err = tx.admitRowDocument(g, item.DocumentID, nil); err != nil {
				return nil, false, err
			}
		}
		if _, err = prepareUpdateBatchItems(items); err != nil {
			return nil, false, err
		}
		return make([]UpdateBatchResult, count), false, nil
	}
	results, err = tx.UpdateBatch(items)
	if err != nil {
		return nil, false, err
	}
	if mode == updateBatchModeNoSecondaryUniqueIndexChanges && collectionMetaHasSecondaryUniqueIndex(g.probe.meta) && len(tx.members) != 0 {
		tx.mu.Lock()
		if g.typed != nil {
			var mutations []memtable.COWMutation
			mutations, err = tx.nativeTypedFinalMutations(g)
			if err == nil {
				_, _, _, err = nativeTypedIndexFinalPlanMode(g.probe.meta, mutations, g.typed, func(id []byte) ([]byte, error) { return tx.typedOpeningIndexState(g, id) }, func(n uint64) error { return tx.admitStage(g, n) }, mode)
			}
		} else {
			_, _, _, err = nativeJSONFinalRowsResolved(g.name, g.probe.meta, &g.rows, func(n uint64) error { return tx.admitStage(g, n) }, func(doc []byte) error { return tx.prepareValidation(g, doc) }, mode, g.templates)
		}
		tx.mu.Unlock()
		if errors.Is(err, errUpdateBatchChangesSecondaryUniqueIndex) {
			clear(results)
			return results, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	if len(tx.members) != 0 {
		if err = tx.Commit(); err != nil {
			return nil, false, err
		}
	}
	return results, true, nil
}
func (c *Collection) nativeDeleteBatch(ids [][]byte) (count int, resultErr error) {
	if c.manager.isClosing() {
		return 0, backenddb.ErrClosed
	}
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	count, err = tx.DeleteBatch(ids)
	if err != nil {
		return 0, err
	}
	if len(tx.members) == 0 {
		return 0, nil
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

// Conditional single deletes use the same opening row and point dependency as
// the existing batch reducer. The callback gets an admitted private copy; its
// decision never becomes a canonical callback or runs during logical reprepare.
func (c *Collection) nativeDeleteDocumentIf(id []byte, predicate func([]byte) (bool, error)) (deleted bool, resultErr error) {
	if c.manager.isClosing() {
		return false, backenddb.ErrClosed
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	accepted, err := func() (bool, error) {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		g := tx.binding(c.name)
		// Typed reconstruction has a separate admitted row source. Preserve
		// its existing refusal rather than pass a residual to the predicate.
		if g.probe.meta.Options.ColumnStore != nil {
			return false, backenddb.ErrNativeSemanticUnsupported
		}
		if err := tx.admitRowControls(g, 1); err != nil {
			return false, err
		}
		if err := tx.admitRowDocument(g, id, nil); err != nil {
			return false, err
		}
		row, err := tx.touchNativeRow(g, id)
		if err != nil || row.current == nil {
			return false, err
		}
		if err = tx.admitRowDocument(g, row.id, row.current); err != nil {
			return false, err
		}
		return predicate(bytes.Clone(row.current))
	}()
	if err != nil || !accepted {
		return false, err
	}
	count, err := tx.DeleteBatch([][]byte{id})
	if err != nil || count == 0 {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// The existing immutable primary/secondary builders are reused here. Index-state
// uses exactly the same per-item encoder as the physical planner. Tombstones
// represent the empty final document; no second mutable collection engine runs.
func buildNativeCRUDFinalPlan(name string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate, stateEntries, roots int) (*directBufferedInsertPlan, error) {
	primary := buildDirectBufferedPrimaryRootEntries(changed, nil)
	for i := range primary {
		if changed[i].document == nil {
			primary[i].value = nil
			primary[i].flags = node.FlagTombstone
		}
	}
	secondary, _, err := buildDirectBufferedSecondaryRootPlans(name, runtimes, changed, nil)
	if err != nil {
		return nil, err
	}
	plan := &directBufferedInsertPlan{
		primaryRootName: collectionPrimaryRootName(name), indexStateRootName: collectionIndexStateRootName(name),
		primaryEntries: primary, secondaryRootPlans: secondary,
		indexStateEntries: make([]directBufferedRootEntry, 0, stateEntries),
		rootNames:         make([]string, 0, roots), policies: make([]backenddb.OrderedRootStoragePolicy, 0, roots),
	}
	plan.rootNames = append(plan.rootNames, plan.primaryRootName)
	plan.policies = append(plan.policies, opts.dataStoragePolicy)
	if len(runtimes) > 0 && persistIndexStateForOptions(opts) {
		for _, item := range changed {
			if !item.indexStateChanged {
				continue
			}
			entry, err := batchUpdateIndexStateEntry(item, runtimes)
			if err != nil {
				return nil, err
			}
			plan.indexStateEntries = append(plan.indexStateEntries, entry)
		}
		if len(plan.indexStateEntries) > 0 {
			plan.rootNames = append(plan.rootNames, plan.indexStateRootName)
			plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
		}
	}
	for _, run := range secondary {
		plan.rootNames = append(plan.rootNames, run.rootName)
		plan.policies = append(plan.policies, runtimes[run.runtimeIdx].def.storagePolicy)
	}
	slices.SortFunc(plan.primaryEntries, func(a, b directBufferedRootEntry) int { return bytes.Compare(a.key, b.key) })
	slices.SortFunc(plan.indexStateEntries, func(a, b directBufferedRootEntry) int { return bytes.Compare(a.key, b.key) })
	for i := range plan.secondaryRootPlans {
		rows := plan.secondaryRootPlans[i].entries
		// The maintained builder emits deletes before sets. Overlapping old/new
		// multikey memberships therefore have equal keys whose final set must
		// remain last when the native writer applies the sorted mutations.
		slices.SortStableFunc(rows, func(a, b directBufferedSecondaryRootEntry) int { return bytes.Compare(a.key, b.key) })
	}
	return plan, nil
}

// Each unique prefix is read from the original captured index domain and is
// recorded in the same history namespace before canonical assignment. A prior
// owner may be replaced only when the maintained whole-batch handoff permits it.
func (t *NativeJSONTxn) checkNativeUpdateUnique(g *nativeJSONTxnBinding, runtimes []indexRuntime, changed []preparedBatchUpdate, replacements batchUniqueReplacementSet) error {
	var cursor [page.PageSize + 1]byte
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		domain, hasDomain := g.probe.collection.domains.Get("index/" + runtime.def.name)
		for _, item := range changed {
			if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encoded)), encoded)
				if err != nil {
					return err
				}
				end := prefixEnd(prefix)
				ns := g.namespace()
				ns.Domain = nativeJSONSecondaryDomainNumber(g.probe.meta, runtimeIdx)
				if err = t.trackDependency(ns, txnhistory.RangeEffect, prefix, end); err != nil {
					return err
				}
				if !hasDomain {
					continue
				}
				read, err := newNativeDomainRead(domain, t.core.Snapshot())
				if err != nil {
					return err
				}
				start := prefix
				for {
					entry, found, err := nativeJSONSeekLive(&read, &g.probe, start, end, cursor[:])
					if err != nil {
						return err
					}
					if !found {
						break
					}
					owner, err := indexKeyDocumentID(runtime.def.valueType, entry.Key)
					if err != nil {
						return err
					}
					if batchUpdateUniqueOwnerConflicts(runtime.def.name, encoded, owner, item.documentID, replacements) {
						return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
					}
					if len(entry.Key) >= len(cursor) {
						return memtable.ErrCOWCapacity
					}
					n := copy(cursor[:], entry.Key)
					cursor[n] = 0
					start = cursor[:n+1]
				}
			}
		}
	}
	return nil
}
