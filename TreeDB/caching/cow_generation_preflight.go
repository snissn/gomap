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
// pressureRollover identifies one optional pre-producer rollover attempt. Its
// caller must preserve that finite attempt across every unlocked loop re-entry.
func (b *Batch) cowGenerationRollover(allowPressureRollover bool) (_ *cowRollover, pressure bool, pressureRollover bool, _ error) {
	c := b.db.cow
	groups := b.cowState.predictionGroups
	storage := b.cowState.predictionStorage
	start := 0
	for i, count := range b.shardCnts {
		groups[i] = storage[start : start : start+count]
		start += count
	}
	allowPointers := b.db.valueLogEnabled() && b.db.allowValueLogPointers() &&
		(!b.db.disableJournal || b.db.memtableValueLogPointers)
	for index, entry := range b.entries {
		m := memtable.COWMutation{Key: entry.Key, Value: entry.Value, Flags: node.FlagInline}
		if entry.Type == batch.OpDelete {
			m.Flags, m.Value = node.FlagTombstone, nil
		} else {
			if entry.Type == batch.OpPut {
				m.Timestamp, m.MVCC = entry.MVCCInput.LogicalTimestamp(entry.Key)
			}
			if allowPointers && b.db.shouldWriteViaValueLogForKeyValue(entry.Key, entry.Value) {
				m.Flags, m.Value = node.FlagPointer, nil
			}
		}
		i := b.shardIdxs[index]
		groups[i] = append(groups[i], m)
	}
	c.orderQualifiedMutationGroups(groups)
	control, err := b.cowKnownControlCharge(groups)
	if err != nil {
		return nil, false, false, err
	}
	if b.cowPredictionCanSkipEstimateWithControl(groups, control) {
		return nil, false, false, nil
	}
	max := c.budget.Limits().MaxGenerationBytes
	rotate := false
	usefulPressureRollover := false
	total := control
	var successors int
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
		charge, err := c.writers[i].Estimate(group, memtable.COWPrepareOptions{})
		if err != nil {
			return nil, false, false, err
		}
		// Sum the conservative shard estimates against the shared parent.
		// Canonical preparation reconciles unused node reservation before the
		// next shard, so this bound need not equal the actual reservation peak.
		if charge.Total() > ^uint64(0)-total {
			return nil, false, false, memtable.ErrCOWCapacity
		}
		total += charge.Total()
		history := charge.History()
		if c.generationBase > max || history > max-c.generationBase {
			return nil, false, false, memtable.ErrCOWCapacity
		}
		// Leave a quarter of the finite generation for exact producer resource
		// metadata. Oversized single commands still receive full C1 admission;
		// they are never split or retried after acceptance.
		trigger := max - max/4
		used := c.cut.shards[i].history
		currentLen := c.cut.shards[i].Len()
		if currentLen > 0 && c.successors[i].writer != nil {
			usefulPressureRollover = true
		}
		if currentLen > 0 && (used > trigger || history > trigger-used) {
			rotate = true
		}
	}
	limits, stats := c.budget.Limits(), c.budget.Stats()
	if stats.Sources > limits.MaxSources || successors > limits.MaxSources-stats.Sources ||
		stats.Generations > limits.MaxGenerations || successors > limits.MaxGenerations-stats.Generations {
		return nil, true, false, nil
	}
	// This aggregate is conservative; canonical preparation remains the final
	// admission authority. A real promised rollover can change its writer basis,
	// but does not refund source/generation slots or caller-held reservations.
	inFlightPressure := stats.ReservedBytes > limits.MaxInFlightBytes || total > limits.MaxInFlightBytes-stats.ReservedBytes
	// Match retirementFitsLocked's History+Reserved+Deferred arithmetic without
	// overflowing. Producer resource identities are not known yet; their exact
	// admission remains with the original canonical prepare, never this trigger.
	remaining := limits.MaxRetiredBytes
	for _, n := range [...]uint64{stats.HistoryBytes, stats.ReservedBytes, stats.DeferredBytes, total} {
		if n > remaining {
			return nil, true, false, nil
		}
		remaining -= n
	}
	if stats.TotalBytes > limits.MaxTotalBytes || total > limits.MaxTotalBytes-stats.TotalBytes {
		return nil, true, false, nil
	}
	requiredRollover := rotate || b.db.mutableBytes.Load() > b.db.mutableFlushThreshold()
	pressureRollover = !requiredRollover && inFlightPressure && allowPressureRollover && usefulPressureRollover
	if !requiredRollover && !pressureRollover {
		return nil, inFlightPressure, false, nil
	}
	rollover, err := c.prepareRollover()
	if err != nil {
		return rollover, false, false, err
	}
	if rollover == nil {
		// No basis changed: preserve the existing bounded checkpoint path.
		return nil, inFlightPressure, false, nil
	}
	// Install/drain outside this helper, then recompute ALL demand against the
	// actual next writers before any producer, revision or command append runs.
	return rollover, false, pressureRollover, nil
}
