package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Existing Collection methods own one original ConditionalTxn. The callback
// scope completes before CommitNative: logical reprepare never invokes a body.
func (c *Collection) nativeUpdateBatch(items []UpdateBatchItem) (results []UpdateBatchResult, resultErr error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if len(items) == 0 {
		return nil, nil
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	results, _, err = tx.stageNativeCRUD(items, nil, false)
	if err != nil {
		return nil, err
	}
	if tx.bindings[0].plan == nil {
		return results, nil
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
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
	_, count, err = tx.stageNativeCRUD(nil, ids, true)
	if err != nil {
		return 0, err
	}
	if tx.bindings[0].plan == nil {
		return 0, nil
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

// stageNativeCRUD adapts opening reads, not the legacy mutable planner. Its
// final-state/mask, primary, secondary, unique handoff and index-state encoding
// are the maintained planner functions. Only the original stage receipt owns
// copied IDs/documents and all private planning scratch through Commit/Close.
func (t *NativeJSONTxn) stageNativeCRUD(items []UpdateBatchItem, ids [][]byte, deleting bool) (results []UpdateBatchResult, deleted int, resultErr error) {
	g := &t.bindings[0]
	n := len(items)
	if deleting {
		n = len(ids)
	}
	perItem := uint64(unsafe.Sizeof(preparedBatchUpdate{})) + uint64(unsafe.Sizeof(updateBatchItem{})) + 8*uint64(unsafe.Sizeof([]byte{})) + 256
	control := uint64(unsafe.Sizeof(nativeJSONLogicalInsert{})) + uint64(unsafe.Sizeof(directBufferedInsertPlan{}))
	if uint64(n) > (^uint64(0)-control)/perItem {
		return nil, 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(n)*perItem + control)
	if charge == ^uint64(0) {
		return nil, 0, memtable.ErrCOWCapacity
	}
	var err error
	t.stageLeases[0], err = t.core.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, 0, err
	}
	admit := func(bytes uint64) error {
		if bytes > ^uint64(0)-charge {
			return memtable.ErrCOWCapacity
		}
		if err := t.stageLeases[0].Resize(charge + bytes); err != nil {
			return err
		}
		charge += bytes
		return nil
	}
	// Inputs and callback results start as caller-owned. Admit copied retention
	// and the maintained parser/encoder scratch before either is constructed.
	admitDocument := func(id, doc []byte) error {
		base, err := nativeJSONInsertCharge(g.name, [][]byte{id}, [][]byte{doc})
		if err != nil {
			return err
		}
		indexed, err := nativeJSONIndexedPlanCharge(g.probe.meta, [][]byte{id}, [][]byte{doc})
		if err != nil || indexed > ^uint64(0)-base {
			return memtable.ErrCOWCapacity
		}
		return admit(base + indexed)
	}
	var owned []updateBatchItem
	if deleting {
		owned = make([]updateBatchItem, n)
		seen := make(map[string]struct{}, n)
		for i, id := range ids {
			if len(id) == 0 {
				return nil, 0, fmt.Errorf("collections: document id cannot be empty at index %d", i)
			}
			if _, exists := seen[string(id)]; exists {
				return nil, 0, fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
			}
			if err = admitDocument(id, nil); err != nil {
				return nil, 0, err
			}
			owned[i].DocumentID = bytes.Clone(id)
			seen[string(owned[i].DocumentID)] = struct{}{}
		}
	} else {
		for _, item := range items {
			if err = admitDocument(item.DocumentID, nil); err != nil {
				return nil, 0, err
			}
		}
		owned, err = prepareUpdateBatchItems(items)
		if err != nil {
			return nil, 0, err
		}
		// Results are caller-owned API output, not retained native plan memory.
		results = make([]UpdateBatchResult, n)
	}
	planner, err := nativeJSONInsertPlanner(g.probe.meta)
	if err != nil {
		return nil, 0, err
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		return nil, 0, err
	}
	changed := make([]preparedBatchUpdate, 0, n)
	stats := CollectionUpdateStats{}
	for i, item := range owned {
		if err = t.trackRead(g, item.DocumentID); err != nil {
			return nil, 0, err
		}
		current, err := g.probe.Get(item.DocumentID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		if !deleting {
			results[i].Matched = true
		}
		if err = admitDocument(item.DocumentID, current); err != nil {
			return nil, 0, err
		}
		prepared := preparedBatchUpdate{itemIndex: i, documentID: item.DocumentID}
		prepared.oldState, err = orderedIndexStateForDocumentWithArena(current, runtimes, planner.options, nil)
		if err != nil {
			return nil, 0, updateBatchItemError(i, err)
		}
		if deleting {
			// Empty final state uses the same runtime-change/mask computation.
			markBatchUpdateIndexEffects(&prepared, runtimes, &stats)
			deleted++
		} else {
			document, modified, callbackErr := callCollectionUpdateCallback(item.Update, current)
			if callbackErr != nil {
				return nil, 0, updateBatchItemError(i, callbackErr)
			}
			if !modified {
				continue
			}
			if len(document) == 0 {
				return nil, 0, updateBatchItemError(i, errors.New("changed replacement document cannot be empty"))
			}
			if err = admitDocument(item.DocumentID, document); err != nil {
				return nil, 0, err
			}
			prepared.document = bytes.Clone(document)
			// JSON preparation validates the same final document accepted by Insert and
			// the maintained UpdateBatch planner; no callback is stored in the plan/WAL.
			docs, _, _, _, err := prepareInsertDocuments([][]byte{prepared.document}, planner.options)
			if err != nil {
				return nil, 0, updateBatchItemError(i, err)
			}
			if len(docs) != 1 {
				return nil, 0, backenddb.ErrNativeSemanticUnsupported
			}
			prepared.document = docs[0]
			if err = prepareBatchUpdateIndexEffects(&prepared, runtimes, planner.options, nil, &stats, false); err != nil {
				return nil, 0, updateBatchItemError(i, err)
			}
			results[i].Modified = true
		}
		changed = append(changed, prepared)
	}
	if len(changed) == 0 {
		return results, 0, nil
	}
	if !deleting {
		if err = rejectBatchUniqueConflicts(runtimes, changed); err != nil {
			return nil, 0, err
		}
		replacements := batchUniqueReplacementOwners(runtimes, changed)
		if err = t.checkNativeUpdateUnique(g, runtimes, changed, replacements); err != nil {
			return nil, 0, err
		}
	}
	direct, err := buildNativeCRUDFinalPlan(g.name, planner.options, runtimes, changed, deleting)
	if err != nil {
		return nil, 0, err
	}
	rows := direct.primaryEntries
	// The same original sparse mutation custody owns every later private root.
	// All immutable source backing remains on the stage until CommitNative ends.
	g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, bootstrap: t.imported, custody: t.custody, primaryPolicy: planner.options.dataStoragePolicy, mutations: make([]memtable.COWMutation, len(rows))}
	g.plan.additional, g.effects, err = extractNativeInsertEffects(g.name, g.incarnation, direct, g.plan.mutations, admit)
	if err != nil {
		return nil, 0, err
	}
	if deleting {
		deleteIDs := make([][]byte, len(changed))
		for i := range changed {
			deleteIDs[i] = changed[i].documentID
		}
		g.kind, g.format = commitlog.CommandKindCollectionDeleteBatchByID, commitlog.PayloadFormatCollectionDeleteBatchByIDV1
		g.payload, err = commitlog.EncodeCollectionDeleteBatchByIDPayload(g.name, deleteIDs)
	} else {
		g.kind, g.format = commitlog.CommandKindCollectionUpdateBatchByID, commitlog.PayloadFormatCollectionUpdateBatchByIDV1
		g.payload, err = commitlog.EncodeCollectionUpdateBatchByIDPayload(g.name, collectionDocumentsFromPreparedBatchUpdates(changed))
	}
	return results, deleted, err
}

// The existing immutable primary/secondary builders are reused here. Index-state
// uses exactly the same per-item encoder as the physical planner. Tombstones
// represent the empty final document; no second mutable collection engine runs.
func buildNativeCRUDFinalPlan(name string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate, deleting bool) (*directBufferedInsertPlan, error) {
	primary := buildDirectBufferedPrimaryRootEntries(changed, nil)
	if deleting {
		for i := range primary {
			primary[i].value = nil
			primary[i].flags = node.FlagTombstone
		}
	}
	secondary, _, err := buildDirectBufferedSecondaryRootPlans(name, runtimes, changed, nil)
	if err != nil {
		return nil, err
	}
	plan := &directBufferedInsertPlan{primaryRootName: collectionPrimaryRootName(name), indexStateRootName: collectionIndexStateRootName(name), primaryEntries: primary, secondaryRootPlans: secondary}
	plan.rootNames = append(plan.rootNames, plan.primaryRootName)
	plan.policies = append(plan.policies, opts.dataStoragePolicy)
	if len(runtimes) > 0 && persistIndexStateForOptions(opts) {
		for _, item := range changed {
			if !deleting && !item.indexStateChanged {
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
	sort.Slice(plan.primaryEntries, func(i, j int) bool { return bytes.Compare(plan.primaryEntries[i].key, plan.primaryEntries[j].key) < 0 })
	sort.Slice(plan.indexStateEntries, func(i, j int) bool {
		return bytes.Compare(plan.indexStateEntries[i].key, plan.indexStateEntries[j].key) < 0
	})
	for i := range plan.secondaryRootPlans {
		rows := plan.secondaryRootPlans[i].entries
		sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
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
				ns.Domain = uint64(runtimeIdx) + 4
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
