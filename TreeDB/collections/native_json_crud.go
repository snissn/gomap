package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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
	results, err = tx.UpdateBatch(items)
	if err != nil {
		return nil, err
	}
	if len(tx.members) == 0 {
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

// The existing immutable primary/secondary builders are reused here. Index-state
// uses exactly the same per-item encoder as the physical planner. Tombstones
// represent the empty final document; no second mutable collection engine runs.
func buildNativeCRUDFinalPlan(name string, opts collectionOptions, runtimes []indexRuntime, changed []preparedBatchUpdate) (*directBufferedInsertPlan, error) {
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
	plan := &directBufferedInsertPlan{primaryRootName: collectionPrimaryRootName(name), indexStateRootName: collectionIndexStateRootName(name), primaryEntries: primary, secondaryRootPlans: secondary}
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
