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

type LeafPagePreparedLog interface {
	// AppendPreparedLeafPage appends one caller-prepared leaf-log payload. The
	// original leafPage is used for read-cache population and integrity checks;
	// preparedPayload contains the already-compacted value-log record payload.
	AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error)
}

type LeafPagePreparedAppendLog interface {
	PreparedLeafPageAppends() bool
}

type LeafPagePreparedBatchAppendLog interface {
	PreparedLeafPageBatchAppends() bool
}

type LeafPageConcurrentAppendLog interface {
	ConcurrentLeafPageAppends() bool
}

type LeafPagePreparedBatchLog interface {
	// AppendPreparedLeafPages appends caller-prepared leaf-log payloads while
	// preserving the positional relationship to the original leaf pages. The
	// original leafPages are used for read-cache population and integrity checks;
	// preparedPayloads contain the already-compacted value-log record payloads.
	AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error)
}

type LeafPageLogSegment struct {
	Path   string
	FileID uint32
}

type LeafPageLogCreatedSegmentProvider interface {
	CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error)
}

// LeafPageLogCurrentSegmentProvider optionally reports every currently tracked
// leaf-log segment. Implementations may return multiple current segments while
// keeping the singular CurrentValueLogSegment compatibility path available.
type LeafPageLogCurrentSegmentProvider interface {
	CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error)
}

type LeafPageLogSegmentRegistrationObserver interface {
	MarkLeafPageLogSegmentsRegistered([]LeafPageLogSegment)
}

// Optional interface implemented by leaf-page logs that can report their
// current value-log segment identity.
type leafPageLogCurrentSegmentProvider interface {
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
}

// Optional interface implemented by leaf-page logs that can report every
// currently writable value-log segment identity.

type leafPageLogProtectedRootProvider interface {
	ProtectedLeafGenerationRootIDs() []uint64
}

type leafPageLogProtectedSystemRootProvider interface {
	ProtectedLeafGenerationSystemRootIDs() []uint64
}

type leafPageLogProtectedRootPairProvider interface {
	ProtectedLeafGenerationRootIDPair() (rootIDs []uint64, systemRootIDs []uint64)
}

type leafPageLogRecordLengthProvider interface {
	LastLeafPageRecordLength() uint32
}

type leafPageLogWithRecordLengthHints struct {
	db    *DB
	inner LeafPageLog
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

func (l *leafPageLogWithRecordLengthHints) ConcurrentLeafPageAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	concurrent, ok := l.inner.(LeafPageConcurrentAppendLog)
	return ok && concurrent.ConcurrentLeafPageAppends()
}

func (l *leafPageLogWithRecordLengthHints) PreparedLeafPageAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	_, ok := l.inner.(LeafPagePreparedLog)
	return ok
}

func (l *leafPageLogWithRecordLengthHints) PreparedLeafPageBatchAppends() bool {
	if l == nil || l.inner == nil {
		return false
	}
	_, ok := l.inner.(LeafPagePreparedBatchLog)
	return ok
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
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	prepared, ok := l.inner.(LeafPagePreparedLog)
	if !ok {
		return l.AppendLeafPage(leafPage)
	}
	ptr, err := prepared.AppendPreparedLeafPage(leafPage, preparedPayload)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	l.noteLeafPagePointer(leafPage, ptr)
	return ptr, nil
}

func (l *leafPageLogWithRecordLengthHints) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, fmt.Errorf("leaf page prepared batch has %d payloads for %d leaf pages", len(preparedPayloads), len(leafPages))
	}
	prepared, ok := l.inner.(LeafPagePreparedBatchLog)
	if !ok {
		return l.AppendLeafPages(leafPages)
	}
	ptrs, err := prepared.AppendPreparedLeafPages(leafPages, preparedPayloads)
	if err != nil {
		return nil, err
	}
	if err := l.noteLeafPageBatchPointers(leafPages, ptrs); err != nil {
		return nil, err
	}
	return ptrs, nil
}

func (l *leafPageLogWithRecordLengthHints) noteLeafPageBatchPointers(leafPages [][]byte, ptrs []page.LeafLogPtr) error {
	if len(ptrs) != len(leafPages) {
		return fmt.Errorf("leaf page batch log returned %d ptrs for %d leaf pages", len(ptrs), len(leafPages))
	}
	for i, ptr := range ptrs {
		// Batch logs guarantee returned pointers are positional: ptrs[i]
		// references leafPages[i].
		l.noteLeafPagePointer(leafPages[i], ptr)
	}
	return nil
}

func (l *leafPageLogWithRecordLengthHints) noteLeafPagePointer(leafPage []byte, ptr page.LeafLogPtr) {
	if l.db == nil {
		return
	}
	l.db.storeLeafPageReadCache(ptr, leafPage)
	recordLen := ptr.RecordLength()
	if recordLen == 0 {
		if provider, ok := l.inner.(leafPageLogRecordLengthProvider); ok {
			recordLen = provider.LastLeafPageRecordLength()
		}
	}
	l.db.noteLeafGenerationRecordLengthRaw(ptr.FileID, ptr.Offset, recordLen)
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

func (l *leafPageLogWithRecordLengthHints) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	return leafPageLogCurrentSegments(l.inner)
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationRootIDs() []uint64 {
	if l == nil || l.inner == nil {
		return nil
	}
	provider, ok := l.inner.(leafPageLogProtectedRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationRootIDs()
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationSystemRootIDs() []uint64 {
	if l == nil || l.inner == nil {
		return nil
	}
	provider, ok := l.inner.(leafPageLogProtectedSystemRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationSystemRootIDs()
}

func (l *leafPageLogWithRecordLengthHints) ProtectedLeafGenerationRootIDPair() ([]uint64, []uint64) {
	if l == nil || l.inner == nil {
		return nil, nil
	}
	provider, ok := l.inner.(leafPageLogProtectedRootPairProvider)
	if !ok {
		return nil, nil
	}
	return provider.ProtectedLeafGenerationRootIDPair()
}

func (db *DB) currentLeafPageLogSegment() (path string, fileID uint32, ok bool) {
	if db == nil || db.leafPageLog == nil {
		return "", 0, false
	}
	segments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil || len(segments) == 0 {
		return "", 0, false
	}
	return segments[0].Path, segments[0].FileID, true
}

func (db *DB) protectedLeafGenerationRootIDsFromLeafPageLog() []uint64 {
	if db == nil || db.leafPageLog == nil {
		return nil
	}
	provider, ok := db.leafPageLog.(leafPageLogProtectedRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationRootIDs()
}

func (db *DB) protectedLeafGenerationSystemRootIDsFromLeafPageLog() []uint64 {
	if db == nil || db.leafPageLog == nil {
		return nil
	}
	provider, ok := db.leafPageLog.(leafPageLogProtectedSystemRootProvider)
	if !ok {
		return nil
	}
	return provider.ProtectedLeafGenerationSystemRootIDs()
}

func (db *DB) protectedLeafGenerationRootIDPairFromLeafPageLog() ([]uint64, []uint64) {
	if db == nil || db.leafPageLog == nil {
		return nil, nil
	}
	pairProvider, ok := db.leafPageLog.(leafPageLogProtectedRootPairProvider)
	if ok {
		return pairProvider.ProtectedLeafGenerationRootIDPair()
	}
	return db.protectedLeafGenerationRootIDsFromLeafPageLog(), db.protectedLeafGenerationSystemRootIDsFromLeafPageLog()
}

func leafPageLogCreatedSegments(log LeafPageLog) ([]LeafPageLogSegment, error) {
	if log == nil {
		return nil, nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return leafPageLogCreatedSegments(wrapped.inner)
	}
	if provider, ok := log.(LeafPageLogCreatedSegmentProvider); ok {
		created, err := provider.CreatedLeafPageLogSegmentsSnapshot()
		if err != nil || len(created) == 0 {
			return nil, err
		}
		return sanitizeLeafPageLogCreatedSegments(created), nil
	}
	provider, ok := log.(interface {
		createdSegmentsSnapshot() ([]rewriteCreatedSegment, error)
	})
	if !ok {
		return nil, nil
	}
	created, err := provider.createdSegmentsSnapshot()
	if err != nil || len(created) == 0 {
		return nil, err
	}
	out := make([]LeafPageLogSegment, 0, len(created))
	for _, seg := range created {
		if seg.path == "" || seg.fileID == 0 {
			continue
		}
		out = append(out, LeafPageLogSegment{Path: seg.path, FileID: seg.fileID})
	}
	return sanitizeLeafPageLogCreatedSegments(out), nil
}

func leafPageLogCurrentSegments(log LeafPageLog) ([]LeafPageLogSegment, error) {
	if log == nil {
		return nil, nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return leafPageLogCurrentSegments(wrapped.inner)
	}
	if provider, ok := log.(LeafPageLogCurrentSegmentProvider); ok {
		current, err := provider.CurrentLeafPageLogSegmentsSnapshot()
		if err != nil || len(current) == 0 {
			return nil, err
		}
		return sanitizeLeafPageLogCreatedSegments(current), nil
	}
	provider, ok := log.(leafPageLogCurrentSegmentProvider)
	if !ok {
		return nil, nil
	}
	path, fileID, ok := provider.CurrentValueLogSegment()
	if !ok || path == "" || fileID == 0 {
		return nil, nil
	}
	return []LeafPageLogSegment{{Path: path, FileID: fileID}}, nil
}

func sanitizeLeafPageLogCreatedSegments(created []LeafPageLogSegment) []LeafPageLogSegment {
	return dedupeLeafPageLogSegments(created)
}

func dedupeLeafPageLogSegments(segments []LeafPageLogSegment) []LeafPageLogSegment {
	if len(segments) == 0 {
		return nil
	}
	out := make([]LeafPageLogSegment, 0, len(segments))
	seen := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.Path == "" || seg.FileID == 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; ok {
			continue
		}
		seen[seg.FileID] = struct{}{}
		out = append(out, seg)
	}
	return out
}

func markLeafPageLogSegmentsRegistered(log LeafPageLog, segments []LeafPageLogSegment) {
	if log == nil || len(segments) == 0 {
		return
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		markLeafPageLogSegmentsRegistered(wrapped.inner, segments)
		return
	}
	observer, ok := log.(LeafPageLogSegmentRegistrationObserver)
	if !ok {
		return
	}
	observer.MarkLeafPageLogSegmentsRegistered(segments)
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

func (db *DB) registerLeafPageLogSegmentsForPublish() (bool, error) {
	if db == nil || db.valueLogManager == nil || db.leafPageLog == nil {
		return true, nil
	}
	createdSegments, err := leafPageLogCreatedSegments(db.leafPageLog)
	if err != nil {
		return false, err
	}
	currentSegments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		return false, err
	}
	segments := dedupeLeafPageLogSegments(append(createdSegments, currentSegments...))
	registeredSegments := make([]LeafPageLogSegment, 0, len(segments))
	for _, seg := range segments {
		if err := db.valueLogManager.RegisterSegment(seg.Path, seg.FileID); err != nil {
			return false, err
		}
		registeredSegments = append(registeredSegments, seg)
		if db.isLeafGenerationSegmentPath(seg.Path) {
			db.queueLeafGenerationWritableFileID(seg.FileID)
		}
	}
	if len(createdSegments) > 0 {
		markLeafPageLogSegmentsRegistered(db.leafPageLog, createdSegments)
	}
	return len(registeredSegments) > 0, nil
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

func (db *DB) ensureValueLogSegmentRegisteredAt(path string, fileID uint32) (bool, error) {
	if db == nil || db.valueLogManager == nil || path == "" || fileID == 0 {
		return false, nil
	}
	if db.valueLogManager.HasSegment(fileID) {
		if err := db.valueLogManager.PromoteCurrentWritable(fileID); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := db.RegisterValueLogSegment(path, fileID); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
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
