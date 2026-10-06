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
func (b *Batch) cowGenerationRollover() (*cowRollover, error) {
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
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}
		charge, err := c.writers[i].Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			return nil, err
		}
		history := charge.History()
		if c.generationBase > max || history > max-c.generationBase {
			return nil, memtable.ErrCOWCapacity
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
	if !rotate && b.db.mutableBytes.Load() <= b.db.mutableFlushThreshold() {
		return nil, nil
	}
	return c.prepareRollover()
}
