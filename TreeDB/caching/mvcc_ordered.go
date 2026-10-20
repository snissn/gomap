package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// prepareOrderedMVCCRead handles the first actual canonical successor demand
// on an adaptive non-COW cache. Call before the seek attempt has acquired its
// gate, view or backend/vlog leases: this is not a shared-to-exclusive upgrade.
func (db *DB) prepareOrderedMVCCRead() error {
	// Match original writer/rotation lock ordering. The caller already holds
	// foreground read admission; no user callback or synchronous drain is run.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if db.closing.Load() {
		return backenddb.ErrClosed
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.memtableMVCCOrdered.Load() {
		return nil
	}
	// Publishing this single monotone hint while holding writer exclusion is
	// sufficient: concurrent fast readers still wait on their ordinary attempt
	// gate until rotation and the new view publication have finished.
	db.memtableMVCCOrdered.Store(true)
	db.updateAdaptiveObservationLocked()
	// Preserve populated/queued sources, pointer path capture, root promotion
	// and held views through the ordinary rotation/retirement machinery. This
	// requests background flush, without rotating WAL or forcing a checkpoint.
	return db.rotateMutableShardsLocked(-1, true)
}
