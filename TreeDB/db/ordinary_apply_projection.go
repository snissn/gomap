package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

// ordinaryApplyProjection belongs to one successful ordinary user-root Apply
// chain. It is neither serialized nor inferred from generic delta flags. A group
// certifies completeness only when every private Apply collected removed refs.
// Fixed system roots must also be checked for primary-root aliases in the pinned
// candidate view before the logical projection can certify absence.
type ordinaryApplyProjection struct {
	idx                                   *indexGen
	baseSeq, oldRoot, newRoot, systemRoot uint64
	complete                              bool
}

func (db *DB) ordinaryLeafResourceCapture() *applyLeafResourceLog {
	if !db.indexOuterLeavesInValueLog {
		return nil
	}
	if !supportsOrdinaryStableLeafCapture(db.leafPageLog) {
		return nil
	}
	capture, err := newApplyLeafResourceLog(db.leafPageLog)
	if err != nil {
		return nil // Unsupported producers retain the existing scanner.
	}
	return capture
}

// Installed forwarding adapters advertise stable APIs even when their producer
// is legacy. Inspect their actual producers before choosing stable appends.
func supportsOrdinaryStableLeafCapture(log LeafPageLog) bool {
	switch log := log.(type) {
	case *leafPageLogWithRecordLengthHints:
		return supportsOrdinaryStableLeafCapture(log.inner)
	case *leafPageLogLaneHandle:
		if log == nil || log.group == nil {
			return false
		}
		lane, _ := log.group.laneAndLock(log.index)
		return supportsOrdinaryStableLeafCapture(lane)
	case *leafPageLogLaneGroup:
		log.mu.RLock()
		defer log.mu.RUnlock()
		hasProducer := false
		for _, lane := range log.lanes {
			// Lazy worker allocation can leave unused lane slots.
			if lane == nil {
				continue
			}
			hasProducer = true
			if !supportsOrdinaryStableLeafCapture(lane) {
				return false
			}
		}
		return hasProducer
	default:
		_, ok := log.(LeafPageStableLog)
		return ok
	}
}

func certifyOrdinaryApplyProjection(delta *valueLogRefDelta, idx *indexGen, baseSeq, oldRoot, newRoot, systemRoot uint64, complete bool, capture *applyLeafResourceLog) {
	if delta == nil {
		return
	}
	delta.ordinaryProducerCapture = capture != nil
	// A successful Apply without complete removed-reference evidence cannot
	// become eligible through the additive predecessor-reuse planner either.
	if !complete && delta.outerLeafDependencyReuse {
		delta.requiresCandidateProjection = true
	}
	if capture == nil || !delta.requiresCandidateProjection {
		return
	}
	delta.ordinaryProjection = ordinaryApplyProjection{idx: idx, baseSeq: baseSeq, oldRoot: oldRoot, newRoot: newRoot, systemRoot: systemRoot, complete: complete}
}

func (p ordinaryApplyProjection) matches(idx *indexGen, next page.MetaPageBody) bool {
	return p.complete && p.idx == idx && p.baseSeq != ^uint64(0) && next.CommitSeq == p.baseSeq+1 && next.UserRootPageID == p.newRoot && next.SystemRootPageID == p.systemRoot
}

func (p ordinaryApplyProjection) matchesBase(snapshot *Snapshot) bool {
	return snapshot != nil && snapshot.idx == p.idx && snapshot.state != nil && snapshot.state.CommitSeq == p.baseSeq && snapshot.state.RootPageID == p.oldRoot && snapshot.state.SystemRootPageID == p.systemRoot
}

func (p ordinaryApplyProjection) rejectAliases(roots []maintenanceRoot) error {
	for _, root := range roots {
		if root.rootID != 0 && (root.rootID == p.oldRoot || root.rootID == p.newRoot) {
			return fmt.Errorf("ordinary Apply projection aliases unchanged maintenance root %d", root.rootID)
		}
	}
	return nil
}
