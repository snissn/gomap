package freelist

type stateBirthPlanV1 struct{ nodes, chunks uint64 }

func (p *stateBirthPlanV1) add(q stateBirthPlanV1) { p.nodes += q.nodes; p.chunks += q.chunks }
func dirtyStateBirthPlanV1(r stateRefV1, _ int) stateBirthPlanV1 {
	if r.zero() || r.pageID() != 0 {
		return stateBirthPlanV1{}
	}
	if r.chunk != nil {
		return stateBirthPlanV1{chunks: 1}
	}
	p := stateBirthPlanV1{nodes: 1}
	for _, child := range r.branch.child {
		p.add(dirtyStateBirthPlanV1(child, 0))
	}
	return p
}
func mutationStateBirthPlanV1(r stateRefV1, key uint64, _ int, private, forcedShared, isolated bool) stateBirthPlanV1 {
	if r.zero() || r.freeCount()+r.retiredCount() == 0 {
		return stateBirthPlanV1{chunks: 1}
	}
	if !r.containsChunk(key) {
		return stateBirthPlanV1{nodes: 1, chunks: 1}
	}
	copyRef := !private || r.pageID() != 0 || (!isolated && (forcedShared || r.ownedRefs() != 1))
	if r.chunk != nil {
		if copyRef {
			return stateBirthPlanV1{chunks: 1}
		}
		return stateBirthPlanV1{}
	}
	p := stateBirthPlanV1{}
	if copyRef {
		p.nodes++
	}
	detaches := copyRef && private && r.pageID() != 0
	if detaches {
		for _, child := range r.branch.child {
			p.add(dirtyStateBirthPlanV1(child, 0))
		}
	}
	child := r.branch.child[chunkNibble(key, int(r.branch.depth))]
	childIsolated := !child.zero() && child.pageID() == 0 && (isolated || detaches)
	p.add(mutationStateBirthPlanV1(child, key, 0, private, copyRef, childIsolated))
	return p
}
func noteStateBirthV1(r stateRefV1, stats *FreelistTxnStats) {
	if stats == nil {
		return
	}
	if r.chunk != nil {
		stats.StateChunkCopies++
		stats.StateCopyBytes += stateChunkCopyCapacityV1
	} else {
		stats.StateNodeCopies++
		stats.StateCopyBytes += stateNodeCopyCapacityV1
	}
}
func detachStateOwnedOperationV1(request AllocationRequestCreditV1, r stateRefV1, _ int, stats *FreelistTxnStats, op *allocationOperationV1) (stateRefV1, error) {
	if r.zero() {
		return stateRefV1{}, nil
	}
	if stats != nil {
		stats.StateIsolationVisits++
	}
	if r.pageID() != 0 {
		return retainStateNodeV1(r), nil
	}
	out, err := cloneStateRefOwnedV1(request, r, true, op.creator, op)
	if err != nil {
		return stateRefV1{}, err
	}
	noteStateBirthV1(out, stats)
	if err = detachChildrenOwnedOperationV1(request, out, 0, stats, op); err != nil {
		releaseStateNodeV1(out)
		return stateRefV1{}, err
	}
	return out, nil
}
func detachChildrenOwnedOperationV1(request AllocationRequestCreditV1, r stateRefV1, _ int, stats *FreelistTxnStats, op *allocationOperationV1) error {
	if r.branch == nil {
		return nil
	}
	for i, child := range r.branch.child {
		owned, err := detachStateOwnedOperationV1(request, child, 0, stats, op)
		if err != nil {
			return err
		}
		replaceStateNodeV1(&r.branch.child[i], owned)
	}
	return nil
}
func mutateStateOwnedOperationV1(request AllocationRequestCreditV1, r stateRefV1, key uint64, _ int, f func(*stateChunk), private bool, stats *FreelistTxnStats, op *allocationOperationV1) (stateRefV1, error) {
	if r.zero() || r.freeCount()+r.retiredCount() == 0 || !r.containsChunk(key) {
		chunk, err := cloneStateChunkOwnedV1(request, nil, false, op.creator, op)
		if err != nil {
			return stateRefV1{}, err
		}
		owned := stateRefV1{chunk: chunk}
		noteStateBirthV1(owned, stats)
		chunk.chunkNo = key
		f(chunk)
		refreshChunkSummaryV1(chunk)
		if chunk.freeCount()+chunk.retiredPages == 0 {
			releaseStateNodeV1(owned)
			return retainStateNodeV1(r), nil
		}
		if r.zero() || r.freeCount()+r.retiredCount() == 0 {
			return owned, nil
		}
		branch, err := cloneStateBranchOwnedV1(request, nil, false, op.creator, op)
		if err != nil {
			releaseStateNodeV1(owned)
			return stateRefV1{}, err
		}
		out := stateRefV1{branch: branch}
		noteStateBirthV1(out, stats)
		depth := firstDifferingDepthV1(key, r.minChunk())
		branch.depth, branch.prefix = uint8(depth), chunkPrefixV1(key, depth)
		branch.child[chunkNibble(key, depth)] = owned
		branch.child[chunkNibble(r.minChunk(), depth)] = retainStateNodeV1(r)
		recomputeStateNode(branch, depth)
		return out, nil
	}
	out := r
	if !private || r.pageID() != 0 || r.ownedRefs() != 1 {
		var err error
		out, err = cloneStateRefOwnedV1(request, r, false, op.creator, op)
		if err != nil {
			return stateRefV1{}, err
		}
		noteStateBirthV1(out, stats)
		if private && r.pageID() != 0 {
			if err = detachChildrenOwnedOperationV1(request, out, 0, stats, op); err != nil {
				releaseStateNodeV1(out)
				return stateRefV1{}, err
			}
		}
	} else {
		retainStateNodeV1(out)
	}
	if out.chunk != nil {
		f(out.chunk)
		refreshChunkSummaryV1(out.chunk)
		if out.freeCount()+out.retiredCount() == 0 {
			releaseStateNodeV1(out)
			return stateRefV1{}, nil
		}
		return out, nil
	}
	i := chunkNibble(key, int(out.branch.depth))
	child, err := mutateStateOwnedOperationV1(request, out.branch.child[i], key, 0, f, private, stats, op)
	if err != nil {
		releaseStateNodeV1(out)
		return stateRefV1{}, err
	}
	replaceStateNodeV1(&out.branch.child[i], child)
	recomputeStateNode(out.branch, int(out.branch.depth))
	count := 0
	var survivor stateRefV1
	for _, child := range out.branch.child {
		if !child.zero() {
			count++
			survivor = child
		}
	}
	if count < 2 {
		owned := retainStateNodeV1(survivor)
		releaseStateNodeV1(out)
		return owned, nil
	}
	return out, nil
}
