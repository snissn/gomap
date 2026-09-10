package collections

// Fold retains scalar capture costs, not another copy of captured row payloads.
// The installed same-base publication state owns every later immutable row.
type typedGraphFoldServing struct {
	capturedCost       typedGraphPublicationCost
	capturedAssetBytes int64
	metadata           *typedGraphServingBaseMetadata
}

func typedGraphStateCost(s *typedGraphPublicationState) typedGraphPublicationCost {
	return typedGraphPublicationCost{rows: s.physicalRows, tombstones: s.tombstones, slots: s.valueSlots, bytes: s.admittedPayloadBytes}
}

func (f *typedGraphFoldServing) prepareNext(before *typedGraphPublicationState, generation uint64, records []columnManifestRecord, meta CollectionMeta, cold typedGraphColdLimits) (*typedGraphPublicationState, error) {
	next := *before
	// Same-schema install validation preserves the precomputed control and
	// manifest/delete encoded bounds: those already use maximal generation,
	// LSN and mutation-part widths. Do not reset attempted-output counters.
	cost := typedGraphStateCost(before)
	cost.subtract(f.capturedCost)
	if cost.rows < 0 || cost.tombstones < 0 || cost.slots < 0 || cost.bytes < 0 || before.installedAssetBytes < f.capturedAssetBytes {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	count := 0
	for _, row := range before.rows {
		if row.Generation > generation {
			count++
		}
	}
	if count > cost.rows {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	next.rows = make([]columnPhysicalVisibleRow, 0, count)
	next.invNorms = make([]float32, 0, count)
	for i, row := range before.rows {
		if row.Generation > generation {
			next.rows = append(next.rows, row)
			next.invNorms = append(next.invNorms, before.invNorms[i])
		}
	}
	next.physicalRows, next.tombstones, next.valueSlots, next.admittedPayloadBytes = cost.rows, cost.tombstones, cost.slots, cost.bytes
	next.installedAssetBytes -= f.capturedAssetBytes
	next.servingBase = f.metadata
	next.servingAdmitted = true
	if err := next.prepareServingRefs(records, meta, cold); err != nil {
		return nil, err
	}
	return &next, nil
}
