package db

import "fmt"

func (db *DB) appendConditionalTxnStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}

	db.conditionalOracle.mu.Lock()
	activeRecords := len(db.conditionalOracle.active)
	retainedPoints := len(db.conditionalOracle.recent)
	retainedRanges := len(db.conditionalOracle.ranges)
	rootConflictActive := db.conditionalOracle.rootSeq != 0
	db.conditionalOracle.mu.Unlock()

	stats["treedb.conditional_txn.active"] = fmt.Sprintf("%d", db.conditionalActiveTxnCount.Load())
	stats["treedb.conditional_txn.started_total"] = fmt.Sprintf("%d", db.conditionalTxnStarted.Load())
	stats["treedb.conditional_txn.closed_total"] = fmt.Sprintf("%d", db.conditionalTxnClosed.Load())
	stats["treedb.conditional_txn.commit_attempts_total"] = fmt.Sprintf("%d", db.conditionalTxnCommitAttempts.Load())
	stats["treedb.conditional_txn.commits_total"] = fmt.Sprintf("%d", db.conditionalTxnCommits.Load())
	stats["treedb.conditional_txn.conflicts_total"] = fmt.Sprintf("%d", db.conditionalTxnConflicts.Load())
	stats["treedb.conditional_txn.read_set.samples_total"] = fmt.Sprintf("%d", db.conditionalTxnReadSetSamples.Load())
	stats["treedb.conditional_txn.read_set.entries_total"] = fmt.Sprintf("%d", db.conditionalTxnReadSetEntries.Load())
	stats["treedb.conditional_txn.read_set.max"] = fmt.Sprintf("%d", db.conditionalTxnReadSetMax.Load())
	stats["treedb.conditional_txn.command_wal_payloads_total"] = fmt.Sprintf("%d", db.conditionalTxnCommandWALPayloads.Load())
	stats["treedb.conditional_oracle.active_records"] = fmt.Sprintf("%d", activeRecords)
	stats["treedb.conditional_oracle.retained_points"] = fmt.Sprintf("%d", retainedPoints)
	stats["treedb.conditional_oracle.retained_ranges"] = fmt.Sprintf("%d", retainedRanges)
	stats["treedb.conditional_oracle.root_conflict_active"] = fmt.Sprintf("%t", rootConflictActive)
	stats["treedb.conditional_oracle.recorded_points_total"] = fmt.Sprintf("%d", db.conditionalOracleRecordedPoints.Load())
	stats["treedb.conditional_oracle.recorded_ranges_total"] = fmt.Sprintf("%d", db.conditionalOracleRecordedRanges.Load())
	stats["treedb.conditional_oracle.root_markers_total"] = fmt.Sprintf("%d", db.conditionalOracleRootMarkers.Load())
	stats["treedb.conditional_oracle.prunes_total"] = fmt.Sprintf("%d", db.conditionalOraclePrunes.Load())
	stats["treedb.conditional_oracle.pruned_points_total"] = fmt.Sprintf("%d", db.conditionalOraclePrunedPoints.Load())
	stats["treedb.conditional_oracle.pruned_ranges_total"] = fmt.Sprintf("%d", db.conditionalOraclePrunedRanges.Load())
}
