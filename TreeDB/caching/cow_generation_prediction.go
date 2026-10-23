package caching

import (
	"bytes"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// orderQualifiedMutationGroups makes each logical history contiguous using only
// the batch's already admitted scratch. The ordered estimator can then bound
// each history by its actual group instead of charging every unordered key for
// a possible group-wide history. Canonical preparation receives the same order.
//
// Explicit MVCC puts commute across physical keys; stable ties retain their
// original last-writer value, pointer and revision. Raw/mixed groups and physical
// removals can have order-dependent routing and must keep their original order.
// Original batch entries, producer placement and command-WAL order never change.
func (c *cowCache) orderQualifiedMutationGroups(groups [][]memtable.COWMutation) {
	if !c.ordered {
		return
	}
	for _, group := range groups {
		if len(group) < 2 {
			continue
		}
		qualified, ordered := true, true
		for i := range group {
			if !group[i].MVCC || group[i].Remove {
				qualified = false
				break
			}
			if i > 0 && bytes.Compare(group[i-1].Key, group[i].Key) > 0 {
				ordered = false
			}
		}
		if qualified && !ordered {
			slices.SortStableFunc(group, cowCompareMutationKeys)
		}
	}
}

func cowCompareMutationKeys(a, b memtable.COWMutation) int {
	return bytes.Compare(a.Key, b.Key)
}

// cowPredictionCanSkipEstimate only proves the existing predictor would neither
// request pressure nor rotate. A loose bound, unsupported input, or any doubtful
// frontier falls through to the original exact path; it never refuses a write.
// The caller holds the same host writer gate as cowGenerationRollover. Final
// canonical preparation still performs its original exact admission.
func (b *Batch) cowPredictionCanSkipEstimate(groups [][]memtable.COWMutation) bool {
	control, err := b.cowKnownControlCharge(groups)
	return err == nil && b.cowPredictionCanSkipEstimateWithControl(groups, control)
}

func (b *Batch) cowPredictionCanSkipEstimateWithControl(groups [][]memtable.COWMutation, control uint64) bool {
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
	total := control
	var successors int
	active := false
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		// Canonical preparation promises one raw source and constructs one
		// successor generation for each first-dirty shard, including logical
		// groups. Already parked successors own both provisions.
		if c.successors[i].writer == nil {
			successors++
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
	if stats.Sources > limits.MaxSources || successors > limits.MaxSources-stats.Sources ||
		stats.Generations > limits.MaxGenerations || successors > limits.MaxGenerations-stats.Generations {
		return false
	}
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
