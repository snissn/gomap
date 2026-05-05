package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

type LeafPageBatchLog interface {
	// AppendLeafPages appends every leaf page and returns one pointer per input
	// page in the same order. Callers use that positional relationship for
	// cache population and per-page record-length metadata.
	AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error)
}

// Optional interface implemented by leaf-page logs that can report their
// current value-log segment identity.
type leafPageLogCurrentSegmentProvider interface {
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
}

type leafPageLogRecordLengthProvider interface {
	LastLeafPageRecordLength() uint32
}

type leafPageLogWithRecordLengthHints struct {
	db    *DB
	inner LeafPageLog
}

type preparedOutputLeafPageAppender interface {
	AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error)
}

type preparedOutputLeafPageLog struct {
	inner   preparedOutputLeafPageAppender
	tracker preparedLeafLogOutputRecorder
}

func (l preparedOutputLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	ptr, err := l.inner.AppendLeafPage(leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if l.tracker != nil {
		l.tracker.notePreparedLeafLogPtr(ptr)
	}
	return ptr, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	ptr, err := l.inner.AppendLeafPage(leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if l.db != nil {
		l.db.storeLeafPageReadCache(ptr, leafPage)
		if provider, ok := l.inner.(leafPageLogRecordLengthProvider); ok {
			l.db.noteLeafGenerationRecordLengthRaw(ptr.FileID, ptr.Offset, provider.LastLeafPageRecordLength())
		}
	}
	return ptr, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	var ptrs []page.LeafLogPtr
	if batcher, ok := l.inner.(LeafPageBatchLog); ok {
		var err error
		ptrs, err = batcher.AppendLeafPages(leafPages)
		if err != nil {
			return nil, err
		}
	} else {
		ptrs = make([]page.LeafLogPtr, len(leafPages))
		for i, leafPage := range leafPages {
			ptr, err := l.inner.AppendLeafPage(leafPage)
			if err != nil {
				return nil, err
			}
			ptrs[i] = ptr
		}
	}
	if len(ptrs) != len(leafPages) {
		return nil, fmt.Errorf("leaf page batch log returned %d ptrs for %d leaf pages", len(ptrs), len(leafPages))
	}
	if l.db != nil {
		lastRecordLen := uint32(0)
		if provider, ok := l.inner.(leafPageLogRecordLengthProvider); ok {
			lastRecordLen = provider.LastLeafPageRecordLength()
		}
		for i, ptr := range ptrs {
			// LeafPageBatchLog guarantees returned pointers are positional:
			// ptrs[i] references leafPages[i].
			l.db.storeLeafPageReadCache(ptr, leafPages[i])
			recordLen := ptr.RecordLengthHint
			if recordLen == 0 {
				recordLen = lastRecordLen
			}
			l.db.noteLeafGenerationRecordLengthRaw(ptr.FileID, ptr.Offset, recordLen)
		}
	}
	return ptrs, nil
}

func (l *leafPageLogWithRecordLengthHints) Flush() error {
	if l == nil || l.inner == nil {
		return nil
	}
	return l.inner.Flush()
}

func (l *leafPageLogWithRecordLengthHints) Sync() error {
	if l == nil || l.inner == nil {
		return nil
	}
	return l.inner.Sync()
}

func (l *leafPageLogWithRecordLengthHints) CurrentValueLogSegment() (path string, fileID uint32, ok bool) {
	if l == nil || l.inner == nil {
		return "", 0, false
	}
	provider, ok := l.inner.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return "", 0, false
	}
	return provider.CurrentValueLogSegment()
}

func (db *DB) currentLeafPageLogSegment() (path string, fileID uint32, ok bool) {
	if db == nil || db.leafPageLog == nil {
		return "", 0, false
	}
	provider, ok := db.leafPageLog.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return "", 0, false
	}
	return provider.CurrentValueLogSegment()
}

func wrapLeafPageLogWithRecordLengthHints(db *DB, log LeafPageLog) LeafPageLog {
	if db == nil || log == nil || !db.indexOuterLeavesInValueLog {
		return log
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		wrapped.db = db
		return wrapped
	}
	return &leafPageLogWithRecordLengthHints{db: db, inner: log}
}

// SetLeafPageLog installs the value-log appender used for value-log-backed leaf
// pages. It is typically wired by the cached layer after opening the backend.
func (db *DB) SetLeafPageLog(log LeafPageLog) {
	if db == nil {
		return
	}
	wrapped := wrapLeafPageLogWithRecordLengthHints(db, log)
	db.writeMu.Lock()
	db.leafPageLog = wrapped
	if idx := db.idx.Load(); idx != nil && idx.zipper != nil {
		idx.zipper.SetLeafPageLog(wrapped)
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
	if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
		return err
	}
	if db.isLeafGenerationSegmentPath(path) {
		db.queueLeafGenerationWritableFileID(fileID)
	}
	return nil
}

// ensureLeafPageLogSegmentRegistered tries to keep the leaf-page log's current
// writable segment visible in the value-log manager without a full directory
// scan. Returns (true, nil) when registration is confirmed on the no-refresh
// path; callers should fall back to manager.Refresh() when it returns false.
func (db *DB) ensureLeafPageLogSegmentRegistered(commitSeq uint64) (bool, error) {
	path, fileID, ok := db.currentLeafPageLogSegment()
	if !ok || path == "" || fileID == 0 {
		return false, nil
	}
	return db.ensureLeafPageLogSegmentRegisteredAt(path, fileID, commitSeq)
}

func (db *DB) ensureLeafPageLogSegmentRegisteredAt(path string, fileID uint32, commitSeq uint64) (bool, error) {
	if db == nil || db.valueLogManager == nil || path == "" || fileID == 0 {
		return false, nil
	}
	if db.valueLogManager.HasSegment(fileID) {
		if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
			return false, err
		}
		if commitSeq > 0 {
			db.queueLeafGenerationWritableFileIDAtCommit(fileID, commitSeq)
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
	if commitSeq > 0 {
		db.queueLeafGenerationWritableFileIDAtCommit(fileID, commitSeq)
	}
	return true, nil
}

func (db *DB) isLeafGenerationSegmentPath(path string) bool {
	if db == nil || path == "" || db.leafGenerationManifest == nil {
		return false
	}
	leafDir := LeafLogDirPath(db.dir)
	if leafDir == "" {
		return false
	}
	return filepath.Clean(filepath.Dir(path)) == filepath.Clean(leafDir)
}

// SetCurrentValueLogReadBarrier installs a callback that will be invoked before
// backend-internal reads of segments still marked currentWritable.
func (db *DB) SetCurrentValueLogReadBarrier(fn func(fileID uint32) error) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetCurrentWritableReadBarrier(fn)
}

// SetCurrentValueLogReadBarrierWithSize is like SetCurrentValueLogReadBarrier,
// but the callback can return the flushed file size. Current-writable mmap
// reads use that size hint to avoid a per-read file Stat on freshly flushed
// segments.
func (db *DB) SetCurrentValueLogReadBarrierWithSize(fn func(fileID uint32) (int64, error)) {
	if db == nil || db.valueLogManager == nil {
		return
	}
	db.valueLogManager.SetCurrentWritableReadBarrierWithSize(fn)
}
