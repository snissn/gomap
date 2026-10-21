package caching

import (
	"strconv"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// COWMemoryStats returns the existing memory authority's fixed-size snapshot.
// It remains available to lifecycle diagnostics after DB.Close; it does not
// admit a storage read or create a root/resource owner.
func (db *DB) COWMemoryStats() memtable.COWStats {
	if db == nil || db.cow == nil {
		return memtable.COWStats{}
	}
	return db.cow.budget.Stats()
}

func (db *DB) cowStatsInto(stats map[string]string) {
	c := db.cow
	if c == nil {
		return
	}
	s := c.budget.Stats()
	// Parent total includes this fixed reservation once. Child utilization is
	// diagnostic, never added to total/peak or presented as extra capacity.
	if c.maintenance != nil {
		stats["treedb.cache.cow.maintenance_capacity_bytes"] = strconv.FormatUint(c.maintenance.Limits().MaxTotalBytes, 10)
		stats["treedb.cache.cow.maintenance_used_bytes"] = strconv.FormatUint(c.maintenance.Stats().TotalBytes, 10)
	}
	for _, v := range []struct {
		name  string
		value uint64
	}{
		{"total_bytes", s.TotalBytes}, {"history_bytes", s.HistoryBytes}, {"reserved_bytes", s.ReservedBytes},
		{"retired_bytes", s.RetiredBytes}, {"peak_bytes", s.PeakBytes}, {"control_bytes", s.ControlBytes},
		{"deferred_bytes", s.DeferredBytes}, {"external_bytes", s.ExternalBytes},
		{"views", uint64(s.Views)}, {"generations", uint64(s.Generations)}, {"sources", uint64(s.Sources)},
		{"external_leases", uint64(s.ExternalLeases)}, {"active_cuts", uint64(c.activeCuts.Load())},
		{"capture_calls_total", c.captureCalls.Load()}, {"prepare_calls_total", c.prepareCalls.Load()},
		{"publications_total", c.publications.Load()}, {"rollovers_total", c.rollovers.Load()},
		{"handoffs_total", c.handoffs.Load()},
		{"txn.journal_effects", c.journalCount.Load()}, {"txn.journal_bytes", c.journalBytes.Load()}, {"txn.history_floor", c.journalFloor.Load()},
		{"txn.index_points", c.journalPoints.Load()}, {"txn.range_effects", c.journalRanges.Load()}, {"txn.too_old_total", c.tooOld.Load()},
		{"txn.barrier_effect_capacity_total", c.journalBarrierEffects.Load()}, {"txn.barrier_byte_capacity_total", c.journalBarrierBytes.Load()},
		{"txn.barrier_logical_replacement_total", c.journalBarrierReplacement.Load()}, {"txn.barrier_conflicts_total", c.journalBarrierConflicts.Load()},
		{"txn.commit_wait_ns", c.commitWaitNs.Load()}, {"txn.commit_hold_ns", c.commitHoldNs.Load()}, {"txn.validation_ns", c.validationNs.Load()},
	} {
		stats["treedb.cache.cow."+v.name] = strconv.FormatUint(v.value, 10)
	}
	c.cutMu.Lock()
	current, frozen := 0, 0
	logicalSeq := uint64(0)
	orderedKeys, orderedVersions := 0, 0
	if c.cut != nil {
		logicalSeq = c.cut.logicalSeq
		current = len(c.cut.shards)
		frozen = len(c.cut.frozen)
		if c.ordered {
			for _, table := range c.cut.shards {
				keys, versions := table.root.OrderedCounts()
				orderedKeys += keys
				orderedVersions += versions
			}
			for _, table := range c.cut.frozen {
				keys, versions := table.root.OrderedCounts()
				orderedKeys += keys
				orderedVersions += versions
			}
		}
	}
	c.cutMu.Unlock()
	if c.ordered {
		stats["treedb.cache.cow.ordered.logical_keys"] = strconv.Itoa(orderedKeys)
		stats["treedb.cache.cow.ordered.physical_versions"] = strconv.Itoa(orderedVersions)
	}
	stats["treedb.cache.cow.txn.logical_seq"] = strconv.FormatUint(logicalSeq, 10)
	stats["treedb.cache.cow.txn.barrier_active"] = strconv.FormatBool(c.journalBarrierActive.Load())
	stats["treedb.cache.cow.current_roots"] = strconv.Itoa(current)
	stats["treedb.cache.cow.frozen_roots"] = strconv.Itoa(frozen)
	stats["treedb.cache.cow.refresh_required"] = strconv.FormatBool(c.refreshRequired.Load())
}
