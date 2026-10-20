package treedb

import "github.com/snissn/gomap/TreeDB/caching"

// MVCCReadSnapshot is the existing Snapshot owner plus its owned successor.
// Application timestamps filter records; this handle pins one physical cut.
// Close releases it, and every returned key/value remains caller-owned.
type MVCCReadSnapshot interface {
	Snapshot
	SeekGEVersionRange(start, end []byte) (key, value []byte, found bool, err error)
}

// SupportsMVCCReadCut qualifies the resolved engine's whole-call publication
// and coherent cut capture, including ordinary and explicit sync ACK paths.
// SeekGEVersionRange method presence alone does not qualify legacy engines.
func (db *DB) SupportsMVCCReadCut() bool {
	cached, _, err := db.captureReadOwners()
	return err == nil && cached != nil && cached.COWMode()
}

// AcquireMVCCReadCut returns one pinned physical cut or its admission error.
// Callers must first qualify SupportsMVCCReadCut and close the returned owner.
func (db *DB) AcquireMVCCReadCut() (MVCCReadSnapshot, error) {
	cached, _, err := db.captureReadOwners()
	if err != nil {
		return nil, err
	}
	if cached == nil || !cached.COWMode() {
		return nil, caching.ErrCOWUnsupported
	}
	cut, err := cached.AcquireMVCCReadCut()
	if err != nil {
		return nil, err
	}
	return cut, nil
}

// PreflightMVCCPrune checks public lifecycle admission before any floor or
// delete effects. COW pruning uses its original retained cut and ordinary
// admitted batches; legacy producers retain their established Store fence.
func (db *DB) PreflightMVCCPrune() error {
	_, _, err := db.captureReadOwners()
	return err
}
