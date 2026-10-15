package db

// acquireStateValueLogSetLocked borrows the exact Set selected under db.mu and
// rootReuseMu. It must run before either capture fence is released. The caller
// keeps this borrow through the final map/tree/zipper use and joins its release.
func (db *DB) acquireStateValueLogSetLocked(state *DBState) {
	if state != nil && state.ValueLogSet != nil && db.valueLogManager != nil {
		db.valueLogManager.Acquire(state.ValueLogSet)
	}
}

func (db *DB) releaseStateValueLogSet(state *DBState) error {
	if state != nil && state.ValueLogSet != nil && db.valueLogManager != nil {
		return db.valueLogManager.Release(state.ValueLogSet)
	}
	return nil
}

func (db *DB) valueLogStateCaptured(stage string, state *DBState) {
	if hook := db.testValueLogStateCapturedHook; hook != nil {
		hook(stage, state)
	}
}
