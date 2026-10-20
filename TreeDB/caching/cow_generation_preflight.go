package caching

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// cowGenerationRollover predicts path-copy pressure before producer placement
// or command append. The scalar is only a conservative rotation trigger: C1's
// shared budget remains the final admission authority. Pointer placement is
// predicted by the same existing eligibility policy; revisions change no sizes.
// The exact final resource charge remains admitted in canonical preparation.
// pressure requests one unlocked checkpoint join before any producer runs;
// it is not a reservation and cannot authorize later canonical preparation.
func (b *Batch) cowGenerationRollover() (_ *cowRollover, pressure bool, _ error) {
	c := b.db.cow
	clear(b.shardCnts)
	for _, entry := range b.entries {
		b.shardCnts[b.db.shardIndex(entry.Key)]++
	}
	groups := b.cowState.predictionGroups
	storage := b.cowState.predictionStorage
	start := 0
	for i, count := range b.shardCnts {
		groups[i] = storage[start : start : start+count]
		start += count
	}
	allowPointers := b.db.valueLogEnabled() && b.db.allowValueLogPointers() &&
		(!b.db.disableJournal || b.db.memtableValueLogPointers)
	for _, entry := range b.entries {
		m := memtable.COWMutation{Key: entry.Key, Value: entry.Value, Flags: node.FlagInline}
		if entry.Type == batch.OpDelete {
			m.Flags, m.Value = node.FlagTombstone, nil
		} else if allowPointers && b.db.shouldWriteViaValueLogForKeyValue(entry.Key, entry.Value) {
			m.Flags, m.Value = node.FlagPointer, nil
		}
		i := b.db.shardIndex(entry.Key)
		groups[i] = append(groups[i], m)
	}
	max := c.budget.Limits().MaxGenerationBytes
	rotate := false
	var total uint64
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		charge, err := c.writers[i].Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			return nil, false, err
		}
		// Canonical preparation holds every shard's reservation until the
		// complete cut is ready. Testing one shard's generation misses shared
		// parent retirement/total pressure from the retained frozen prefix.
		if charge.Total() > ^uint64(0)-total {
			return nil, false, memtable.ErrCOWCapacity
		}
		total += charge.Total()
		history := charge.History()
		if c.generationBase > max || history > max-c.generationBase {
			return nil, false, memtable.ErrCOWCapacity
		}
		// Leave a quarter of the finite generation for exact producer resource
		// metadata. Oversized single commands still receive full C1 admission;
		// they are never split or retried after acceptance.
		trigger := max - max/4
		used := c.cut.shards[i].history
		if c.cut.shards[i].Len() > 0 && (used > trigger || history > trigger-used) {
			rotate = true
		}
	}
	limits, stats := c.budget.Limits(), c.budget.Stats()
	// Match retirementFitsLocked's History+Reserved+Deferred arithmetic without
	// overflowing. Producer resource identities are not known yet; their exact
	// admission remains with the original canonical prepare, never this trigger.
	remaining := limits.MaxRetiredBytes
	for _, n := range [...]uint64{stats.HistoryBytes, stats.ReservedBytes, stats.DeferredBytes, total} {
		if n > remaining {
			return nil, true, nil
		}
		remaining -= n
	}
	if stats.TotalBytes > limits.MaxTotalBytes || total > limits.MaxTotalBytes-stats.TotalBytes {
		return nil, true, nil
	}
	if !rotate && b.db.mutableBytes.Load() <= b.db.mutableFlushThreshold() {
		return nil, false, nil
	}
	rollover, err := c.prepareRollover()
	return rollover, false, err
}
