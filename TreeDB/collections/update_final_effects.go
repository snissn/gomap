package collections

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/node"
)

// prepareBatchUpdateIndexEffects is the maintained planner's pure final-state
// boundary. Both physical and native callers supply their own exact old state;
// no snapshot, callback, write domain or publication authority enters here.
func prepareBatchUpdateIndexEffects(item *preparedBatchUpdate, runtimes []indexRuntime, opts collectionOptions, arena *indexEncodeArena, stats *CollectionUpdateStats, detailedStats bool) error {
	if len(runtimes) == 0 {
		return nil
	}
	var err error
	if item.knownAffectedIndexes && item.affectedIndexRuntimeMask == 0 {
		recordKnownUnaffectedIndexStats(runtimes, stats)
		return nil
	}
	phaseStart := updateBatchStatsNow(detailedStats)
	if p := opts.typedProjection; p != nil {
		item.newState, err = typedIndexState(p.typedRows[string(item.documentID)], p.columns, runtimes, arena)
	} else if item.knownAffectedIndexes {
		item.newState, err = orderedIndexStateForKnownValidDocumentRuntimeMask(item.document, runtimes, item.affectedIndexRuntimeMask, opts, arena)
	} else {
		item.newState, err = orderedIndexStateForDocumentWithArena(item.document, runtimes, opts, arena)
	}
	phaseDuration := updateBatchStatsSince(detailedStats, phaseStart)
	stats.IndexStateExtraction += phaseDuration
	stats.NewIndexStateExtract += phaseDuration
	if err != nil {
		return err
	}
	markBatchUpdateIndexEffects(item, runtimes, stats)
	return nil
}

// The same final-batch handoff rule applies to physical and captured native scans.
func batchUpdateUniqueOwnerConflicts(index string, encoded, owner, documentID []byte, replacements batchUniqueReplacementSet) bool {
	return !bytes.Equal(owner, documentID) && !replacements.allows(index, encoded, owner)
}

func markBatchUpdateIndexEffects(item *preparedBatchUpdate, runtimes []indexRuntime, stats *CollectionUpdateStats) {
	var changedIndexes uint64
	indexStateChanged := false
	for runtimeIdx, runtime := range runtimes {
		runtimeAffected := true
		if item.knownAffectedIndexes {
			runtimeAffected = item.affectedIndexRuntimeMask&(uint64(1)<<uint(runtimeIdx)) != 0
		}
		runtimeChanged := false
		if runtimeAffected {
			runtimeChanged = orderedDocumentIndexRuntimeChanged(item.oldState, item.newState, runtimeIdx)
		}
		maskBit, maskBitOK := updateIndexChangedMaskBit(runtimeIdx)
		if !maskBitOK {
			stats.MaskFallbacks++
		}
		if runtimeChanged {
			indexStateChanged = true
			if maskBitOK {
				changedIndexes |= maskBit
			}
			stats.IndexValueChanges++
			if runtime.def.unique {
				stats.UniqueIndexChecks++
			}
			if runtimeIdx < stats.IndexStatsCount {
				stats.IndexStats[runtimeIdx].Changed++
				if runtime.def.unique {
					stats.IndexStats[runtimeIdx].UniqueChecks++
				}
			}
			continue
		}
		stats.IndexValueUnchanged++
		if runtime.def.unique {
			stats.UniqueIndexCheckSkips++
		}
		if runtimeIdx < stats.IndexStatsCount {
			stats.IndexStats[runtimeIdx].Unchanged++
			if runtime.def.unique {
				stats.IndexStats[runtimeIdx].UniqueCheckSkips++
			}
		}
	}
	item.changedIndexes = changedIndexes
	item.indexStateChanged = indexStateChanged
}

// batchUpdateIndexStateEntry is shared by the maintained physical plan and its
// native adapter; the wire document-index state remains identical.
func batchUpdateIndexStateEntry(item preparedBatchUpdate, runtimes []indexRuntime) (directBufferedRootEntry, error) {
	entry := directBufferedRootEntry{key: item.documentID, flags: node.FlagTombstone}
	if !orderedDocumentIndexStateEmpty(item.newState) {
		value, err := encodeRuntimeOrderedDocumentIndexState(item.newState, runtimes)
		if err != nil {
			return directBufferedRootEntry{}, err
		}
		entry.value, entry.flags = value, node.FlagInline
	}
	return entry, nil
}
