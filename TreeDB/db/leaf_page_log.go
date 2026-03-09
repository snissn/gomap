package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

// LeafPageLog appends and flushes B+Tree leaf pages stored in the value log.
//
// This is used when Options.IndexOuterLeavesInValueLog is enabled.
// Implementations are expected to reuse the existing value-log record encoding
// and compression semantics (i.e. they should append normal value-log records
// and return ValuePtr references).
type LeafPageLog interface {
	AppendLeafPage(leafPage []byte) (page.ValuePtr, error)
	Flush() error
	Sync() error
}

// SetLeafPageLog installs the value-log appender used for value-log-backed leaf
// pages. It is typically wired by the cached layer after opening the backend.
func (db *DB) SetLeafPageLog(log LeafPageLog) {
	if db == nil {
		return
	}
	db.writeMu.Lock()
	db.leafPageLog = log
	if log != nil {
		db.leafRefState.Store(leafRefStateUnknown)
	}
	if idx := db.idx.Load(); idx != nil && idx.zipper != nil {
		idx.zipper.SetLeafPageLog(log)
	}
	db.writeMu.Unlock()
}

// RegisterValueLogSegment registers a newly created value-log segment with the
// backend read manager without scanning the filesystem. Cached mode uses this
// when it rotates the shared value log so outer-leaf commits can publish a
// current ValueLogSet via CurrentSetNoRefresh.
func (db *DB) RegisterValueLogSegment(path string, fileID uint32) error {
	if db == nil {
		return nil
	}
	if path == "" || fileID == 0 {
		return fmt.Errorf("invalid value-log segment registration: path=%q file_id=%d", path, fileID)
	}
	if db.valueLogManager == nil {
		return fmt.Errorf("value-log segment registration unavailable: manager not initialized")
	}
	return db.valueLogManager.RegisterSegment(path, fileID)
}
