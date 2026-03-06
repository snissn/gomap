package db

func (db *DB) SystemRootVersion() uint64 {
	if db == nil {
		return 0
	}
	state := db.State()
	if state == nil {
		return 0
	}
	return state.SystemRootPageID
}
