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
	} {
		stats["treedb.cache.cow."+v.name] = strconv.FormatUint(v.value, 10)
	}
	c.cutMu.Lock()
	current, frozen := 0, 0
	if c.cut != nil {
		current = len(c.cut.shards)
		frozen = len(c.cut.frozen)
	}
	c.cutMu.Unlock()
	stats["treedb.cache.cow.current_roots"] = strconv.Itoa(current)
	stats["treedb.cache.cow.frozen_roots"] = strconv.Itoa(frozen)
	stats["treedb.cache.cow.refresh_required"] = strconv.FormatBool(c.refreshRequired.Load())
}
