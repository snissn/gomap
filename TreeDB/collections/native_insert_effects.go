package collections

import (
	"strings"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// extractNativeInsertEffects adapts the maintained immutable insert plan, not
// the mutable collection publisher. Input backing remains owned by the original
// staged transaction through CommitNative. Only COW Prepare copies final rows.
// Extra backing is admitted on that same original staging receipt before make.
//
// Index eligibility remains fenced until captured probes, unique range absence
// and singleton/composite replay consume these same complete root effects.
func extractNativeInsertEffects(name string, incarnation uint64, plan *directBufferedInsertPlan, primary []memtable.COWMutation, admit func(uint64) error, typedMeta ...CollectionMeta) ([]nativeJSONDomainChange, []txnhistory.Effect, error) {
	if plan == nil || len(plan.rootNames) != len(plan.policies) || len(primary) != len(plan.primaryEntries) || admit == nil {
		return nil, nil, backenddb.ErrNativeSemanticUnsupported
	}
	roots, extra := 0, 0
	count := func(n int) bool {
		if n == 0 {
			return true
		}
		if n > int(^uint(0)>>1)-extra {
			return false
		}
		extra += n
		roots++
		return true
	}
	if !count(len(plan.templateEntries)) || !count(len(plan.indexStateEntries)) {
		return nil, nil, memtable.ErrCOWCapacity
	}
	for _, secondary := range plan.secondaryRootPlans {
		if secondary.runtimeIdx < 0 || !count(len(secondary.entries)) {
			return nil, nil, memtable.ErrCOWCapacity
		}
	}
	if extra > int(^uint(0)>>1)-len(primary) {
		return nil, nil, memtable.ErrCOWCapacity
	}
	// Existing primary mutations/effects were admitted before planner execution.
	// Charge only the newly introduced root and effect backing. Individual root
	// arrays are charged separately, including their allocation rounding.
	if roots > 0 {
		if err := admitNativeInsertArray(roots, uint64(unsafe.Sizeof(nativeJSONDomainChange{})), admit); err != nil {
			return nil, nil, err
		}
		// Admit the entire enlarged backing, not a sum of independently rounded
		// pieces. The existing primary allowance remains conservative overhead.
		if err := admitNativeInsertArray(len(primary)+extra, uint64(unsafe.Sizeof(txnhistory.Effect{})), admit); err != nil {
			return nil, nil, err
		}
		chargeRows := func(n int) error {
			if n == 0 {
				return nil
			}
			return admitNativeInsertArray(n, uint64(unsafe.Sizeof(memtable.COWMutation{})), admit)
		}
		if err := chargeRows(len(plan.templateEntries)); err != nil {
			return nil, nil, err
		}
		if err := chargeRows(len(plan.indexStateEntries)); err != nil {
			return nil, nil, err
		}
		for _, secondary := range plan.secondaryRootPlans {
			if err := chargeRows(len(secondary.entries)); err != nil {
				return nil, nil, err
			}
		}
	}
	var additional []nativeJSONDomainChange
	if roots > 0 {
		additional = make([]nativeJSONDomainChange, 0, roots)
	}
	effects := make([]txnhistory.Effect, len(primary), len(primary)+extra)
	namespace := func(domain uint64) txnhistory.Namespace {
		return txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: incarnation, Domain: domain, Interpretation: 1}
	}
	for i, row := range plan.primaryEntries {
		primary[i] = memtable.COWMutation{Key: row.key, Value: row.value, Ptr: row.ptr, Flags: row.flags}
		effects[i] = txnhistory.Effect{Namespace: namespace(1), Kind: txnhistory.PointEffect, Key: row.key}
	}
	appendRows := func(root string, domain uint64, rows []directBufferedRootEntry) {
		if len(rows) == 0 {
			return
		}
		delta := nativeJSONDomainChange{name: strings.TrimPrefix(root, name+"/"), policy: nativeInsertRootPolicy(plan, root), entries: make([]memtable.COWMutation, len(rows))}
		for i, row := range rows {
			delta.entries[i] = memtable.COWMutation{Key: row.key, Value: row.value, Ptr: row.ptr, Flags: row.flags}
			effects = append(effects, txnhistory.Effect{Namespace: namespace(domain), Kind: txnhistory.PointEffect, Key: row.key})
		}
		additional = append(additional, delta)
	}
	appendRows(plan.templateRootName, 2, plan.templateEntries)
	appendRows(plan.indexStateRootName, 3, plan.indexStateEntries)
	secondaryDomain := func(index int) uint64 {
		if len(typedMeta) != 0 {
			return nativeJSONSecondaryDomainNumber(typedMeta[0], index)
		}
		return uint64(index) + 4
	}
	for _, secondary := range plan.secondaryRootPlans {
		if len(secondary.entries) == 0 {
			continue
		}
		delta := nativeJSONDomainChange{name: strings.TrimPrefix(secondary.rootName, name+"/"), policy: nativeInsertRootPolicy(plan, secondary.rootName), entries: make([]memtable.COWMutation, len(secondary.entries))}
		for i, row := range secondary.entries {
			flags := byte(node.FlagInline)
			if row.tombstone {
				flags = node.FlagTombstone
			}
			delta.entries[i] = memtable.COWMutation{Key: row.key, Flags: flags}
			effects = append(effects, txnhistory.Effect{Namespace: namespace(secondaryDomain(secondary.runtimeIdx)), Kind: txnhistory.PointEffect, Key: row.key})
		}
		additional = append(additional, delta)
	}
	return additional, effects, nil
}

func nativeInsertRootPolicy(plan *directBufferedInsertPlan, root string) backenddb.OrderedRootStoragePolicy {
	for i, candidate := range plan.rootNames {
		if candidate == root && i < len(plan.policies) {
			return plan.policies[i]
		}
	}
	return 0
}

func admitNativeInsertArray(count int, size uint64, admit func(uint64) error) error {
	if count < 0 || size == 0 || uint64(count) > ^uint64(0)/size {
		return memtable.ErrCOWCapacity
	}
	return admit(memtable.COWAllocationCharge(uint64(count) * size))
}
