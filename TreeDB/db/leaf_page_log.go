package db

import (
	"errors"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/page"
)

// LeafPageLog appends and flushes B+Tree leaf pages stored in the value log.
//
// This is used when Options.IndexOuterLeavesInValueLog is enabled.
// Implementations are expected to reuse the existing value-log record encoding
// and compression semantics (i.e. they should append normal value-log records
// and return LeafLogPtr references).
type LeafPageLog interface {
	AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error)
	Flush() error
	Sync() error
}

// Optional interface implemented by leaf-page logs that can report their
// current value-log segment identity.
type leafPageLogCurrentSegmentProvider interface {
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
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
	if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
		return err
	}
	return db.valueLogManager.PromoteCurrentWritable(fileID)
}

// ensureLeafPageLogSegmentRegistered tries to keep the leaf-page log's current
// writable segment visible in the value-log manager without a full directory
// scan. Returns (true, nil) when registration is confirmed on the no-refresh
// path; callers should fall back to manager.Refresh() when it returns false.
func (db *DB) ensureLeafPageLogSegmentRegistered(commitSeq uint64) (bool, error) {
	if db == nil || db.valueLogManager == nil || db.leafPageLog == nil {
		return false, nil
	}
	provider, ok := db.leafPageLog.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return false, nil
	}
	path, fileID, ok := provider.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		return false, nil
	}
	if db.valueLogManager.HasSegment(fileID) {
		if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
			return false, err
		}
		if err := db.noteLeafGenerationWritableFileID(fileID, commitSeq); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
		// Segment may have rotated/deleted between report and registration;
		// caller can fall back to a full refresh in this case.
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
		return false, err
	}
	if err := db.noteLeafGenerationWritableFileID(fileID, commitSeq); err != nil {
		return false, err
	}
	return true, nil
}

// SetCurrentValueLogReadBarrier installs a callback that will be invoked before
// backend-internal reads of segments still marked currentWritable.
func (db *DB) SetCurrentValueLogReadBarrier(fn func(fileID uint32) error) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetCurrentWritableReadBarrier(fn)
}
