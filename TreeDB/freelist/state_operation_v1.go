package freelist

import "sync/atomic"

type stateBirthPlanV1 struct{ nodes, chunks uint64 }

func (plan *stateBirthPlanV1) add(other stateBirthPlanV1) {
	plan.nodes += other.nodes
	plan.chunks += other.chunks
}
func dirtyStateBirthPlanV1(n *stateNode, depth int) stateBirthPlanV1 {
	if n == nil || n.pageID != 0 {
		return stateBirthPlanV1{}
	}
	plan := stateBirthPlanV1{nodes: 1}
	if depth == chunkTrieDepth {
		if n.chunk != nil && n.chunk.pageID == 0 {
			plan.chunks++
		}
		return plan
	}
	for _, child := range n.child {
		plan.add(dirtyStateBirthPlanV1(child, depth+1))
	}
	return plan
}

// forcedShared models the edge retained by a prospective shallow copy;
// isolated models a zero-ID subtree which that copy detaches first. Page ID
// alone never authorizes mutation of a live object.
func mutationStateBirthPlanV1(n *stateNode, chunkNo uint64, depth int, private, forcedShared, isolated bool) stateBirthPlanV1 {
	copyNode := !private || n == nil || n.pageID != 0 || (!isolated && (forcedShared || atomic.LoadUint64(&n.ownedRefs) != 1))
	plan := stateBirthPlanV1{}
	if copyNode {
		plan.nodes++
	}
	detaches := copyNode && private && n != nil && n.pageID != 0
	if detaches {
		if depth == chunkTrieDepth {
			if n.chunk != nil && n.chunk.pageID == 0 {
				plan.chunks++
			}
		} else {
			for _, child := range n.child {
				plan.add(dirtyStateBirthPlanV1(child, depth+1))
			}
		}
	}
	if depth == chunkTrieDepth {
		var chunk *stateChunk
		if n != nil {
			chunk = n.chunk
		}
		chunkIsolated := chunk != nil && chunk.pageID == 0 && (isolated || detaches)
		if !private || chunk == nil || chunk.pageID != 0 || (!chunkIsolated && (copyNode || atomic.LoadUint64(&chunk.ownedRefs) != 1)) {
			plan.chunks++
		}
		return plan
	}
	var child *stateNode
	if n != nil {
		child = n.child[chunkNibble(chunkNo, depth)]
	}
	childIsolated := child != nil && child.pageID == 0 && (isolated || detaches)
	plan.add(mutationStateBirthPlanV1(child, chunkNo, depth+1, private, copyNode, childIsolated))
	return plan
}
func detachStateOwnedOperationV1(n *stateNode, depth int, stats *FreelistTxnStats, operation *allocationOperationV1) (*stateNode, error) {
	if n == nil {
		return nil, nil
	}
	if stats != nil {
		stats.StateIsolationVisits++
	}
	if n.pageID != 0 {
		return retainStateNodeV1(n), nil
	}
	out, err := cloneStateNodeOwnedV1(n, true, operation.creator, operation)
	if err != nil {
		return nil, err
	}
	if stats != nil {
		stats.StateNodeCopies++
		stats.StateCopyBytes += stateNodeCopyCapacityV1
	}
	if err = detachChildrenOwnedOperationV1(out, depth, stats, operation); err != nil {
		releaseStateNodeV1(out)
		return nil, err
	}
	return out, nil
}
func detachChildrenOwnedOperationV1(out *stateNode, depth int, stats *FreelistTxnStats, operation *allocationOperationV1) error {
	if depth == chunkTrieDepth {
		if out.chunk != nil && out.chunk.pageID == 0 {
			chunk, err := cloneStateChunkOwnedV1(out.chunk, true, operation.creator, operation)
			if err != nil {
				return err
			}
			replaceStateChunkV1(&out.chunk, chunk)
			if stats != nil {
				stats.StateChunkCopies++
				stats.StateCopyBytes += stateChunkCopyCapacityV1
			}
		}
		return nil
	}
	for i, child := range out.child {
		owned, err := detachStateOwnedOperationV1(child, depth+1, stats, operation)
		if err != nil {
			return err
		}
		replaceStateNodeV1(&out.child[i], owned)
	}
	return nil
}
func mutateStateOwnedOperationV1(n *stateNode, chunkNo uint64, depth int, f func(*stateChunk), private bool, stats *FreelistTxnStats, operation *allocationOperationV1) (*stateNode, error) {
	out := n
	if !private || n == nil || n.pageID != 0 || atomic.LoadUint64(&n.ownedRefs) != 1 {
		var err error
		out, err = cloneStateNodeOwnedV1(n, false, operation.creator, operation)
		if err != nil {
			return nil, err
		}
		if stats != nil {
			stats.StateNodeCopies++
			stats.StateCopyBytes += stateNodeCopyCapacityV1
		}
		if private && n != nil && n.pageID != 0 {
			if err = detachChildrenOwnedOperationV1(out, depth, stats, operation); err != nil {
				releaseStateNodeV1(out)
				return nil, err
			}
		}
	} else {
		retainStateNodeV1(out)
	}
	if depth == chunkTrieDepth {
		chunk := out.chunk
		if !private || chunk == nil || chunk.pageID != 0 || atomic.LoadUint64(&chunk.ownedRefs) != 1 {
			var err error
			chunk, err = cloneStateChunkOwnedV1(chunk, false, operation.creator, operation)
			if err != nil {
				releaseStateNodeV1(out)
				return nil, err
			}
			if stats != nil {
				stats.StateChunkCopies++
				stats.StateCopyBytes += stateChunkCopyCapacityV1
			}
		} else {
			retainStateChunkV1(chunk)
		}
		chunk.chunkNo = chunkNo
		f(chunk)
		if chunk.freeCount() == 0 {
			retired, _ := chunk.retiredSummary()
			if retired == 0 {
				replaceStateChunkV1(&out.chunk, nil)
				releaseStateChunkV1(chunk)
				recomputeStateNode(out, depth)
				return out, nil
			}
		}
		replaceStateChunkV1(&out.chunk, chunk)
		recomputeStateNode(out, depth)
		return out, nil
	}
	i := chunkNibble(chunkNo, depth)
	child, err := mutateStateOwnedOperationV1(out.child[i], chunkNo, depth+1, f, private, stats, operation)
	if err != nil {
		releaseStateNodeV1(out)
		return nil, err
	}
	replaceStateNodeV1(&out.child[i], child)
	recomputeStateNode(out, depth)
	return out, nil
}
