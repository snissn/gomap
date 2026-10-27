package treedb

import (
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

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

// PrepareMVCCPrune establishes the durable basis for a qualified floor prune
// on the optional logical-history data plane. Other producers retain their
// existing pruning path and return no completion work. Checkpoint enters its
// own public admission; no lifecycle read lock is held across either call.
//
// After any physical delete-write attempt, the caller must run complete after
// closing its prune iterator and snapshot, including when the write returned
// an error. Completion transfers any accepted delete prefix to the successor
// basis; it does not certify that a failed write was accepted or erase its error.
// Both boundaries incur the existing checkpoint cost and durability behavior.
func (db *DB) PrepareMVCCPrune() (complete func() error, err error) {
	cached, _, err := db.captureReadOwners()
	if err != nil {
		return nil, err
	}
	if cached == nil || !cached.SupportsLogicalMVCC() {
		return nil, nil
	}
	if err := db.Checkpoint(); err != nil {
		return nil, err
	}
	return db.Checkpoint, nil
}

// MVCCVersionRangeResult is one owned physical range successor outcome.
type MVCCVersionRangeResult = mvcckey.VersionRangeResult

// MVCCReadBatchSnapshot is optional; callers must first qualify and capture an
// MVCCReadSnapshot. It never captures a newer cut, and accepts at most32 probes.
// Results has the same length as starts/ends and receives ordered owned values.
// Individual failures belong to Results[i].Err; admission errors affect the call.
type MVCCReadBatchSnapshot interface {
	SeekGEVersionRanges(starts, ends [][]byte, results []MVCCVersionRangeResult) error
}
