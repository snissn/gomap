package db

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

type preparedRootApplyState uint8

const (
	preparedRootApplyStatePlanned preparedRootApplyState = iota
	preparedRootApplyStatePrepared
	preparedRootApplyStateInstalling
	preparedRootApplyStateInstalled
	preparedRootApplyStateAbandoned
)

type preparedRootIdentityKind uint8

const (
	preparedRootIdentityData preparedRootIdentityKind = iota
	preparedRootIdentitySystem
)

type preparedRootIdentity struct {
	kind    preparedRootIdentityKind
	ordinal int
}

type preparedRootDeltaPlanSummary struct {
	entries       uint64
	tombstones    uint64
	keyBytes      uint64
	valueBytes    uint64
	pointerValues uint64
	checksum      uint64
	firstKey      []byte
	lastKey       []byte
}

type preparedRootApply struct {
	identity     preparedRootIdentity
	baseRootID   uint64
	preparedRoot uint64
	prepared     bool
	storage      OrderedRootStoragePolicy
	plan         preparedRootDeltaPlanSummary
	state        preparedRootApplyState
}

type preparedRootApplyGroup struct {
	baseUserRootID   uint64
	baseSystemRootID uint64
	state            preparedRootApplyState
	applyCount       int
	inlineApplies    [4]preparedRootApply
	overflowApplies  []preparedRootApply
}

type preparedRootApplyStats struct {
	groups        uint64
	roots         uint64
	entries       uint64
	tombstones    uint64
	keyBytes      uint64
	valueBytes    uint64
	pointerValues uint64
	installed     uint64
	abandoned     uint64
}

const (
	preparedRootPlanChecksumOffset = 14695981039346656037
	preparedRootPlanChecksumPrime  = 1099511628211
)

func initPreparedRootApplyGroup(group *preparedRootApplyGroup, baseUserRootID, baseSystemRootID uint64, ordered []OrderedRootDeltaBatchPublishInput, includeChecksum bool) {
	if group == nil {
		return
	}
	*group = preparedRootApplyGroup{
		baseUserRootID:   baseUserRootID,
		baseSystemRootID: baseSystemRootID,
		state:            preparedRootApplyStatePlanned,
	}
	for i := range ordered {
		group.appendApply(preparedRootApply{
			identity: preparedRootIdentity{
				kind:    preparedRootIdentityData,
				ordinal: i,
			},
			baseRootID: ordered[i].BaseRoot,
			storage:    ordered[i].StoragePolicy,
			plan:       preparedRootDeltaPlanSummaryFromBatch(ordered[i].Delta, includeChecksum),
			state:      preparedRootApplyStatePlanned,
		})
	}
}

func (group *preparedRootApplyGroup) appendApply(apply preparedRootApply) int {
	if group == nil {
		return -1
	}
	idx := group.applyCount
	if idx < len(group.inlineApplies) {
		group.inlineApplies[idx] = apply
	} else {
		group.overflowApplies = append(group.overflowApplies, apply)
	}
	group.applyCount++
	return idx
}

func (group *preparedRootApplyGroup) applyLen() int {
	if group == nil {
		return 0
	}
	return group.applyCount
}

func (group *preparedRootApplyGroup) applyAt(idx int) *preparedRootApply {
	if group == nil || idx < 0 || idx >= group.applyCount {
		return nil
	}
	if idx < len(group.inlineApplies) {
		return &group.inlineApplies[idx]
	}
	return &group.overflowApplies[idx-len(group.inlineApplies)]
}

func (group *preparedRootApplyGroup) setSystemRoot(baseRootID uint64, delta *batch.Batch, includeChecksum bool) int {
	if group == nil {
		return -1
	}
	group.baseSystemRootID = baseRootID
	for i := group.applyCount - 1; i >= 0; i-- {
		apply := group.applyAt(i)
		if apply != nil && apply.identity.kind == preparedRootIdentitySystem {
			if apply.prepared {
				if apply.state != preparedRootApplyStateInstalled {
					apply.state = preparedRootApplyStateAbandoned
				}
				break
			}
			if apply.state == preparedRootApplyStateAbandoned {
				break
			}
			*apply = preparedRootApply{
				identity: preparedRootIdentity{
					kind:    preparedRootIdentitySystem,
					ordinal: -1,
				},
				baseRootID: baseRootID,
				storage:    OrderedRootStorageDefault,
				plan:       preparedRootDeltaPlanSummaryFromBatch(delta, includeChecksum),
				state:      preparedRootApplyStatePlanned,
			}
			return i
		}
	}
	return group.appendApply(preparedRootApply{
		identity: preparedRootIdentity{
			kind:    preparedRootIdentitySystem,
			ordinal: -1,
		},
		baseRootID: baseRootID,
		storage:    OrderedRootStorageDefault,
		plan:       preparedRootDeltaPlanSummaryFromBatch(delta, includeChecksum),
		state:      preparedRootApplyStatePlanned,
	})
}

func (group *preparedRootApplyGroup) markPrepared(idx int, rootID uint64) {
	apply := group.applyAt(idx)
	if apply == nil {
		return
	}
	apply.preparedRoot = rootID
	apply.prepared = true
	apply.state = preparedRootApplyStatePrepared
}

func (group *preparedRootApplyGroup) markInstalling() {
	if group == nil {
		return
	}
	group.state = preparedRootApplyStateInstalling
	for i := 0; i < group.applyCount; i++ {
		apply := group.applyAt(i)
		if apply != nil && apply.prepared && apply.state != preparedRootApplyStateAbandoned {
			apply.state = preparedRootApplyStateInstalling
		}
	}
}

func (group *preparedRootApplyGroup) markInstalled() {
	if group == nil {
		return
	}
	group.state = preparedRootApplyStateInstalled
	for i := 0; i < group.applyCount; i++ {
		if apply := group.applyAt(i); apply != nil && apply.prepared && apply.state != preparedRootApplyStateAbandoned {
			apply.state = preparedRootApplyStateInstalled
		}
	}
}

func (group *preparedRootApplyGroup) markAbandoned() {
	if group == nil {
		return
	}
	group.state = preparedRootApplyStateAbandoned
	for i := 0; i < group.applyCount; i++ {
		apply := group.applyAt(i)
		if apply != nil && apply.prepared && apply.state != preparedRootApplyStateInstalled {
			apply.state = preparedRootApplyStateAbandoned
		}
	}
}

func (stats *preparedRootApplyStats) observeGroup(group *preparedRootApplyGroup) {
	if stats == nil || group == nil || group.applyCount == 0 {
		return
	}
	groupStats := preparedRootApplyStats{}
	for i := 0; i < group.applyCount; i++ {
		apply := group.applyAt(i)
		if apply == nil || !apply.prepared {
			continue
		}
		groupStats.roots++
		switch apply.state {
		case preparedRootApplyStateInstalled:
			groupStats.installed++
		case preparedRootApplyStateAbandoned:
			groupStats.abandoned++
		}
		plan := apply.plan
		groupStats.entries += plan.entries
		groupStats.tombstones += plan.tombstones
		groupStats.keyBytes += plan.keyBytes
		groupStats.valueBytes += plan.valueBytes
		groupStats.pointerValues += plan.pointerValues
	}
	if groupStats.roots == 0 {
		return
	}
	groupStats.groups = 1
	stats.groups += groupStats.groups
	stats.roots += groupStats.roots
	stats.entries += groupStats.entries
	stats.tombstones += groupStats.tombstones
	stats.keyBytes += groupStats.keyBytes
	stats.valueBytes += groupStats.valueBytes
	stats.pointerValues += groupStats.pointerValues
	stats.installed += groupStats.installed
	stats.abandoned += groupStats.abandoned
}

func observePreparedRootApplyGroup(db *DB, phases *orderedRootDeltaGroupPublishPhaseStats, group *preparedRootApplyGroup, state preparedRootApplyState) {
	if phases == nil || group == nil || group.applyCount == 0 {
		return
	}
	switch state {
	case preparedRootApplyStateInstalled:
		group.markInstalled()
	case preparedRootApplyStateAbandoned:
		group.markAbandoned()
	case preparedRootApplyStateInstalling:
		group.markInstalling()
	}
	phases.preparedRootStats.observeGroup(group)
	if db != nil {
		if hook := db.testPreparedRootApplyHook; hook != nil {
			hook(clonePreparedRootApplyGroup(*group))
		}
	}
}

func clonePreparedRootApplyGroup(src preparedRootApplyGroup) preparedRootApplyGroup {
	dst := preparedRootApplyGroup{
		baseUserRootID:   src.baseUserRootID,
		baseSystemRootID: src.baseSystemRootID,
		state:            src.state,
	}
	for i := 0; i < src.applyCount; i++ {
		srcApply := src.applyAt(i)
		if srcApply == nil {
			continue
		}
		apply := *srcApply
		apply.plan.firstKey = append([]byte(nil), apply.plan.firstKey...)
		apply.plan.lastKey = append([]byte(nil), apply.plan.lastKey...)
		dst.appendApply(apply)
	}
	return dst
}

func preparedRootDeltaPlanSummaryFromBatch(delta *batch.Batch, includeChecksum bool) preparedRootDeltaPlanSummary {
	if delta == nil {
		return preparedRootDeltaPlanSummary{}
	}
	entries := delta.SortedEntries()
	if len(entries) == 0 {
		if includeChecksum {
			return preparedRootDeltaPlanSummary{checksum: preparedRootPlanChecksumOffset}
		}
		return preparedRootDeltaPlanSummary{}
	}
	summary := preparedRootDeltaPlanSummary{
		entries: uint64(len(entries)),
	}
	if includeChecksum {
		summary.checksum = preparedRootPlanChecksumOffset
		// Key spans are part of hook/debug metadata. They are intentionally not
		// captured on the normal hot path so prepared-root accounting stays
		// allocation-free unless stable test metadata is requested.
		summary.firstKey = append([]byte(nil), entries[0].Key...)
		summary.lastKey = append([]byte(nil), entries[len(entries)-1].Key...)
	}
	for i := range entries {
		entry := entries[i]
		summary.keyBytes += uint64(len(entry.Key))
		if includeChecksum {
			summary.checksum = preparedRootPlanChecksumAddByte(summary.checksum, byte(entry.Type))
			summary.checksum = preparedRootPlanChecksumAddBytes(summary.checksum, entry.Key)
		}
		if entry.Type == batch.OpDelete {
			summary.tombstones++
			continue
		}
		if entry.IsPtr {
			summary.pointerValues++
			summary.valueBytes += uint64(page.ValuePtrSize)
			if includeChecksum {
				summary.checksum = preparedRootPlanChecksumAddByte(summary.checksum, 1)
				summary.checksum = preparedRootPlanChecksumAddUint64(summary.checksum, uint64(entry.ValuePtr.FileID))
				summary.checksum = preparedRootPlanChecksumAddUint64(summary.checksum, entry.ValuePtr.Offset)
				summary.checksum = preparedRootPlanChecksumAddUint64(summary.checksum, uint64(entry.ValuePtr.Length))
			}
			continue
		}
		summary.valueBytes += uint64(len(entry.Value))
		if includeChecksum {
			summary.checksum = preparedRootPlanChecksumAddByte(summary.checksum, 0)
			summary.checksum = preparedRootPlanChecksumAddBytes(summary.checksum, entry.Value)
		}
	}
	return summary
}

func preparedRootPlanChecksumAddByte(sum uint64, b byte) uint64 {
	sum ^= uint64(b)
	sum *= preparedRootPlanChecksumPrime
	return sum
}

func preparedRootPlanChecksumAddBytes(sum uint64, b []byte) uint64 {
	sum = preparedRootPlanChecksumAddUint64(sum, uint64(len(b)))
	for _, c := range b {
		sum = preparedRootPlanChecksumAddByte(sum, c)
	}
	return sum
}

func preparedRootPlanChecksumAddUint64(sum, v uint64) uint64 {
	for i := 0; i < 8; i++ {
		sum = preparedRootPlanChecksumAddByte(sum, byte(v>>(uint(i)*8)))
	}
	return sum
}
