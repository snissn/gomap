package db

import "github.com/snissn/gomap/TreeDB/page"

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
	if idx := db.idx.Load(); idx != nil && idx.zipper != nil {
		idx.zipper.SetLeafPageLog(log)
	}
	db.writeMu.Unlock()
}
