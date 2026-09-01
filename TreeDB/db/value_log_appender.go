package db

import (
	"context"
	"errors"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrValueLogAppenderUnavailable = errors.New("value-log appender unavailable")
var ErrValueLogReaderUnavailable = errors.New("value-log reader unavailable")

// ValueLogAppender appends user values to the persistent value log and returns
// stable ValuePtr references that may be stored by native-root callers.
//
// AppendValues must finish reading each values element before returning; callers
// may reuse or release the backing buffers once the call completes.
type ValueLogAppender interface {
	AppendValues(values [][]byte) ([]page.ValuePtr, error)
	Flush() error
	Sync() error
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
}

// ValueLogRecordReader is an optional appender extension used by native-root
// callers that must read an appended pointer before its root is published.
type ValueLogRecordReader interface {
	ReadValueLogRecordAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

// ValueLogExternalRefFlusher is an optional extension for appenders that can
// flush the value-log lanes containing specific file IDs. An empty file-ID
// slice requests a barrier for all pending appender writes. When sync is true,
// the implementation owns durability for the referenced file IDs: active
// segments should follow the appender's sync policy, and older referenced
// segments should be synced directly if needed. Command-WAL SetRID logging uses
// it to make freshly written pointer records visible before RID lookup without
// flushing unrelated lanes.
type ValueLogExternalRefFlusher interface {
	FlushValueLogExternalRefs(fileIDs []uint32, sync bool) error
}

type valueLogAppenderHolder struct {
	appender ValueLogAppender
}

type valueLogRIDReserver interface {
	ReserveRIDs(count int) (start uint64, err error)
}

// SetValueLogAppender installs the appender used by native-root APIs that need
// to create persistent value-log pointers. Cached mode wires this to its normal
// value-log writer.
func (db *DB) SetValueLogAppender(appender ValueLogAppender) {
	if db == nil {
		return
	}
	if appender == nil {
		db.valueLogAppender.Store(nil)
		return
	}
	db.valueLogAppender.Store(&valueLogAppenderHolder{appender: appender})
}

func (db *DB) currentValueLogAppender() ValueLogAppender {
	if db == nil {
		return nil
	}
	holder := db.valueLogAppender.Load()
	if holder == nil {
		return nil
	}
	return holder.appender
}

func (db *DB) currentValueLogRIDReserver() valueLogRIDReserver {
	appender := db.currentValueLogAppender()
	if appender == nil {
		return nil
	}
	reserver, ok := appender.(valueLogRIDReserver)
	if !ok {
		return nil
	}
	return reserver
}

// HasValueLogAppender reports whether native-root callers can append user
// values to the persistent value log.
func (db *DB) HasValueLogAppender() bool {
	return db.currentValueLogAppender() != nil
}

// HasValueLogRecordReader reports whether pending native-root pointers can be
// resolved before publication.
func (db *DB) HasValueLogRecordReader() bool {
	if db == nil {
		return false
	}
	_, ok := db.currentValueLogAppender().(ValueLogRecordReader)
	return ok
}

// ReadValueLogRecordAppend resolves a pending native-root pointer into dst.
func (db *DB) ReadValueLogRecordAppend(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	reader, ok := db.currentValueLogAppender().(ValueLogRecordReader)
	if !ok {
		return nil, ErrValueLogReaderUnavailable
	}
	return reader.ReadValueLogRecordAppend(ptr, dst)
}

// AppendValueLogValues appends values through the configured persistent value
// log appender.
func (db *DB) AppendValueLogValues(values [][]byte) ([]page.ValuePtr, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if db == nil {
		return nil, ErrClosed
	}
	appender := db.currentValueLogAppender()
	if appender == nil {
		return nil, ErrValueLogAppenderUnavailable
	}
	db.publishPrepareMu.RLock()
	defer db.publishPrepareMu.RUnlock()
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeDependencyAppend, durabilitycut.ResourceValueLog, db.dir); err != nil {
		return nil, err
	}
	ptrs, err := appender.AppendValues(values)
	if err != nil {
		return nil, err
	}
	if err := db.emitValueLogDependencyAppend(ptrs); err != nil {
		return nil, err
	}
	db.protectPendingValueLogAppendPtrs(ptrs)
	return ptrs, nil
}

func (db *DB) emitValueLogDependencyAppend(ptrs []page.ValuePtr) error {
	if !durabilitycut.Enabled() {
		return nil
	}
	if db == nil || db.valueLogManager == nil {
		return durabilitycut.EmitBasic(durabilitycut.AfterDependencyAppend, durabilitycut.ResourceValueLog, "")
	}
	paths := make([]string, 0, len(ptrs))
	seen := make(map[string]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		path := db.valueLogManager.SegmentPath(ptr.FileID)
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return durabilitycut.Emit(durabilitycut.Event{Point: durabilitycut.AfterDependencyAppend, Resource: durabilitycut.ResourceValueLog, Root: db.dir, Paths: paths})
}

// protectPendingValueLogAppendPtrs pins value-log files returned to native-root
// callers until a later root publish makes those pointers reachable. A shared
// appender flush can make another writer's not-yet-published segment visible on
// disk; ValueLogGC consults this set so it cannot reclaim that segment against
// the older root snapshot.
func (db *DB) protectPendingValueLogAppendPtrs(ptrs []page.ValuePtr) {
	if db == nil || len(ptrs) == 0 {
		return
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	for _, ptr := range ptrs {
		if ptr.FileID == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		if db.pendingValueLogAppendFileIDRefs == nil {
			db.pendingValueLogAppendFileIDRefs = make(map[uint32]int)
		}
		if db.pendingValueLogAppendPtrRefs == nil {
			db.pendingValueLogAppendPtrRefs = make(map[page.ValuePtr]int)
		}
		db.pendingValueLogAppendFileIDRefs[ptr.FileID]++
		db.pendingValueLogAppendPtrRefs[ptr]++
	}
}

// ReleaseValueLogValues releases pending GC pins for previously appended
// value-log pointers. Native-root callers should call this when pointers are
// abandoned before publication or after a detached root containing those
// pointers has been made reachable by a later catalog/system-root publish.
func (db *DB) ReleaseValueLogValues(ptrs []page.ValuePtr) {
	if db == nil || len(ptrs) == 0 {
		return
	}
	release := make(map[page.ValuePtr]int64, len(ptrs))
	for _, ptr := range ptrs {
		if ptr.FileID == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		release[ptr]++
	}
	db.releasePendingValueLogAppendPtrCounts(release)
}

func (db *DB) releasePendingValueLogAppendFileIDsFromEntries(entries []batchpkg.Entry) {
	if db == nil || len(entries) == 0 {
		return
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	for _, entry := range entries {
		if entry.Type != batchpkg.OpPut || !entry.IsPtr || entry.ValuePtr.FileID == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			continue
		}
		refs := db.pendingValueLogAppendPtrRefs[entry.ValuePtr]
		if refs <= 0 {
			continue
		}
		if refs == 1 {
			delete(db.pendingValueLogAppendPtrRefs, entry.ValuePtr)
		} else {
			db.pendingValueLogAppendPtrRefs[entry.ValuePtr] = refs - 1
		}
		fileRefs := db.pendingValueLogAppendFileIDRefs[entry.ValuePtr.FileID]
		if fileRefs <= 1 {
			delete(db.pendingValueLogAppendFileIDRefs, entry.ValuePtr.FileID)
		} else {
			db.pendingValueLogAppendFileIDRefs[entry.ValuePtr.FileID] = fileRefs - 1
		}
	}
	if len(db.pendingValueLogAppendPtrRefs) == 0 {
		db.pendingValueLogAppendPtrRefs = nil
	}
	if len(db.pendingValueLogAppendFileIDRefs) == 0 {
		db.pendingValueLogAppendFileIDRefs = nil
	}
}

func (db *DB) releasePendingValueLogAppendFileIDsFromBatch(delta *batchpkg.Batch) {
	if db == nil || delta == nil || delta.IsEmpty() {
		return
	}
	db.releasePendingValueLogAppendFileIDsFromEntries(delta.OrderedEntries())
}

func collectPendingValueLogAppendPtrCount(counts map[page.ValuePtr]int64, ptr page.ValuePtr, flags byte) map[page.ValuePtr]int64 {
	if flags&node.FlagPointer == 0 || ptr.FileID == 0 || !page.IsValueLogFileID(ptr.FileID) {
		return counts
	}
	if counts == nil {
		counts = make(map[page.ValuePtr]int64)
	}
	counts[ptr]++
	return counts
}

type pendingValueLogAppendPtrCollectingIterator struct {
	iterator.UnsafeIterator
	ptrCounts map[page.ValuePtr]int64
}

func newPendingValueLogAppendPtrCollectingIterator(iter iterator.UnsafeIterator) (*pendingValueLogAppendPtrCollectingIterator, iterator.UnsafeIterator) {
	if iter == nil {
		return nil, nil
	}
	collector := &pendingValueLogAppendPtrCollectingIterator{UnsafeIterator: iter}
	return collector, collector
}

func (iter *pendingValueLogAppendPtrCollectingIterator) UnsafeEntry() (val []byte, ptr page.ValuePtr, flags byte) {
	val, ptr, flags = iter.UnsafeIterator.UnsafeEntry()
	iter.ptrCounts = collectPendingValueLogAppendPtrCount(iter.ptrCounts, ptr, flags)
	return val, ptr, flags
}

func (iter *pendingValueLogAppendPtrCollectingIterator) UnsafeEntryWithRevision() (val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	val, ptr, flags, revision = iterator.UnsafeEntryWithRevision(iter.UnsafeIterator)
	iter.ptrCounts = collectPendingValueLogAppendPtrCount(iter.ptrCounts, ptr, flags)
	return val, ptr, flags, revision
}

func (db *DB) releasePendingValueLogAppendPtrCollector(collector *pendingValueLogAppendPtrCollectingIterator) {
	if db == nil || collector == nil || len(collector.ptrCounts) == 0 {
		return
	}
	db.releasePendingValueLogAppendPtrCounts(collector.ptrCounts)
}

func (db *DB) releasePendingValueLogAppendFileIDsFromDelta(delta *valueLogRefDelta) {
	if db == nil || delta == nil {
		return
	}
	release := make(map[uint32]int64)
	_ = delta.forEachPositive(func(fileID uint32, count int64) error {
		release[fileID] += count
		return nil
	})
	db.releasePendingValueLogAppendFileIDCounts(release)
}

func (db *DB) releasePendingValueLogAppendPtrCounts(release map[page.ValuePtr]int64) {
	if db == nil || len(release) == 0 {
		return
	}
	fileIDs := make(map[uint32]int64, len(release))
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	for ptr, n := range release {
		refs := db.pendingValueLogAppendPtrRefs[ptr]
		if refs <= 0 {
			continue
		}
		actual := int(n)
		if refs < actual {
			actual = refs
		}
		if refs == actual {
			delete(db.pendingValueLogAppendPtrRefs, ptr)
		} else {
			db.pendingValueLogAppendPtrRefs[ptr] = refs - actual
		}
		fileIDs[ptr.FileID] += int64(actual)
	}
	for fileID, n := range fileIDs {
		refs := db.pendingValueLogAppendFileIDRefs[fileID]
		if refs <= int(n) {
			delete(db.pendingValueLogAppendFileIDRefs, fileID)
			continue
		}
		db.pendingValueLogAppendFileIDRefs[fileID] = refs - int(n)
	}
	if len(db.pendingValueLogAppendPtrRefs) == 0 {
		db.pendingValueLogAppendPtrRefs = nil
	}
	if len(db.pendingValueLogAppendFileIDRefs) == 0 {
		db.pendingValueLogAppendFileIDRefs = nil
	}
}

func (db *DB) releasePendingValueLogAppendFileIDCounts(release map[uint32]int64) {
	if db == nil || len(release) == 0 {
		return
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	for fileID, n := range release {
		refs := db.pendingValueLogAppendFileIDRefs[fileID]
		if refs <= int(n) {
			delete(db.pendingValueLogAppendFileIDRefs, fileID)
			continue
		}
		db.pendingValueLogAppendFileIDRefs[fileID] = refs - int(n)
	}
	if len(db.pendingValueLogAppendFileIDRefs) == 0 {
		db.pendingValueLogAppendFileIDRefs = nil
	}
}

func (db *DB) releasePendingValueLogAppendFileIDsReferenced(ctx context.Context) error {
	pending := db.pendingValueLogAppendPtrs()
	if len(pending) == 0 {
		return nil
	}
	referenced, err := db.referencedPendingValueLogAppendPtrs(ctx, pending)
	if err != nil {
		return err
	}
	db.releasePendingValueLogAppendPtrCounts(referenced)
	return nil
}

func (db *DB) referencedPendingValueLogAppendPtrs(ctx context.Context, pending map[page.ValuePtr]struct{}) (map[page.ValuePtr]int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return nil, nil
	}
	roots, err := maintenanceRootsForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	roots = dedupeMaintenanceRootsByRootID(roots)
	referenced := make(map[page.ValuePtr]int64)
	for _, root := range roots {
		iter := scanValueLogRefCountRootIterator(snap, root)
		for iter.Valid() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				_ = snap.Close()
				return nil, err
			}
			_, ptr, flags := iter.UnsafeEntry()
			if flags&node.FlagPointer != 0 && page.IsValueLogFileID(ptr.FileID) {
				if _, ok := pending[ptr]; ok {
					referenced[ptr]++
				}
			}
			iter.Next()
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			_ = snap.Close()
			return nil, err
		}
		_ = iter.Close()
	}
	if err := snap.Close(); err != nil {
		return nil, err
	}
	return referenced, nil
}

func (db *DB) pendingValueLogAppendPtrs() map[page.ValuePtr]struct{} {
	if db == nil {
		return nil
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	if len(db.pendingValueLogAppendPtrRefs) == 0 {
		return nil
	}
	out := make(map[page.ValuePtr]struct{}, len(db.pendingValueLogAppendPtrRefs))
	for ptr := range db.pendingValueLogAppendPtrRefs {
		out[ptr] = struct{}{}
	}
	return out
}

func (db *DB) pendingValueLogAppendFileIDs() map[uint32]struct{} {
	if db == nil {
		return nil
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	if len(db.pendingValueLogAppendFileIDRefs) == 0 {
		return nil
	}
	out := make(map[uint32]struct{}, len(db.pendingValueLogAppendFileIDRefs))
	for fileID := range db.pendingValueLogAppendFileIDRefs {
		out[fileID] = struct{}{}
	}
	return out
}

func (db *DB) installCommandWALValueLogAppender() error {
	if db == nil {
		return ErrClosed
	}
	segments, err := listRecoverySegments(db.dir)
	if err != nil {
		return err
	}
	nextRID, err := nextReplayAppenderRIDStart(segments)
	if err != nil {
		return err
	}
	appender, err := newReplayInlineAppenderWithNextRID(db, segments, nextRID)
	if err != nil {
		return err
	}
	db.SetValueLogAppender(appender)
	db.SetLeafPageLog(replayInlineLeafPageLog{appender: appender})
	// Ordered-root finalization deliberately releases writeMu before its durable
	// candidate and post-work complete. Defer appender teardown until Close owns
	// teardownMu exclusively so those publications cannot race leaf-log removal.
	db.registerCaptureTeardownHook(func() error {
		db.SetLeafPageLog(nil)
		db.SetValueLogAppender(nil)
		return appender.close()
	})
	return nil
}
