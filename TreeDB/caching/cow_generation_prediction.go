package caching

import "github.com/snissn/gomap/TreeDB/internal/memtable"

// cowPredictionCanSkipEstimate only proves the existing predictor would neither
// request pressure nor rotate. A loose bound, unsupported input, or any doubtful
// frontier falls through to the original exact path; it never refuses a write.
// The caller holds the same host writer gate as cowGenerationRollover. Final
// canonical preparation still performs its original exact admission.
func (b *Batch) cowPredictionCanSkipEstimate(groups [][]memtable.COWMutation) bool {
	c := b.db.cow
	if b.db.mutableBytes.Load() > b.db.mutableFlushThreshold() {
		return false
	}
	limits := c.budget.Limits()
	max := limits.MaxGenerationBytes
	if c.generationBase > max {
		return false
	}
	trigger := max - max/4
	var total uint64
	active := false
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		active = true
		bound, valid := c.writers[i].OrderedPredictionUpperBound(group)
		if !valid || bound.Total() > ^uint64(0)-total || bound.History() > max-c.generationBase {
			return false
		}
		total += bound.Total()
		used := c.cut.shards[i].history
		if c.cut.shards[i].Len() > 0 && (used > trigger || bound.History() > trigger-used) {
			return false
		}
	}
	if !active {
		return false
	}
	stats := c.budget.Stats()
	remaining := limits.MaxRetiredBytes
	for _, n := range [...]uint64{stats.HistoryBytes, stats.ReservedBytes, stats.DeferredBytes, total} {
		if n > remaining {
			return false
		}
		remaining -= n
	}
	return stats.TotalBytes <= limits.MaxTotalBytes && total <= limits.MaxTotalBytes-stats.TotalBytes &&
		stats.ReservedBytes <= limits.MaxInFlightBytes && total <= limits.MaxInFlightBytes-stats.ReservedBytes
}
