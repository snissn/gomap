package db

// ValueLogSetStats summarizes the backend's current in-process value-log set.
// It is intended for runtime diagnostics, not persistence.
type ValueLogSetStats struct {
	CurrentSetSegments int
	CurrentSetBytes    int64
	RefreshScans       uint64
}

func (db *DB) ValueLogSetStats() ValueLogSetStats {
	var stats ValueLogSetStats
	if db == nil || db.valueLogManager == nil {
		return stats
	}
	stats.RefreshScans = db.valueLogManager.RefreshScanCount()
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		return stats
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	stats.CurrentSetSegments = len(set.Files)
	for _, f := range set.Files {
		stats.CurrentSetBytes += fileSize(f)
	}
	return stats
}
