package db

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

// The original DB/startup caller owns the last ghost-manager alias. Both
// physical shutdown and its locks return before this terminal containing edge.
func (db *DB) closeGhostManagerCompletionV6(stop bool) {
	ghost := db.ghostManager
	if ghost == nil {
		return
	}
	var metadata *retainedalloc.Owner
	var charge uint64
	if stop {
		metadata, charge = ghost.stopWithCompletionV6()
	} else {
		metadata, charge = ghost.closeAllWithCompletionV6()
	}
	db.ghostManager = nil
	ghost = nil
	if metadata != nil && charge != 0 {
		metadata.RemovePending(charge)
	}
}
