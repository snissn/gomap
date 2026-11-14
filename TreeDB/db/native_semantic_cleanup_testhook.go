package db

// JoinNativeSemanticCleanupForTest waits for the existing cache worker's fixed
// cleanup frontier. Tests call it after a publication has returned and released
// its gates; it performs no checkpoint and releases no retained read cut.
func (db *DB) JoinNativeSemanticCleanupForTest() error {
	if db == nil {
		return ErrClosed
	}
	bridge, ok := db.nativeSemanticBridge.(interface{ JoinNativeSemanticCleanupForTest() error })
	if !ok {
		return ErrNativeSemanticUnsupported
	}
	return bridge.JoinNativeSemanticCleanupForTest()
}
