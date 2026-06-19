package db

import (
	"context"
	"errors"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrValueLogAppenderUnavailable = errors.New("value-log appender unavailable")

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
	ptrs, err := appender.AppendValues(values)
	if err != nil {
		return nil, err
	}
	db.protectPendingValueLogAppendPtrs(ptrs)
	return ptrs, nil
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
		db.pendingValueLogAppendFileIDRefs[ptr.FileID]++
	}
}

func (db *DB) releasePendingValueLogAppendFileIDsFromEntries(entries []batchpkg.Entry) {
	if db == nil || len(entries) == 0 {
		return
	}
	release := make(map[uint32]int64)
	for _, entry := range entries {
		if entry.Type != batchpkg.OpPut || !entry.IsPtr || entry.ValuePtr.FileID == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
			continue
		}
		release[entry.ValuePtr.FileID]++
	}
	db.releasePendingValueLogAppendFileIDCounts(release)
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
	pending := db.pendingValueLogAppendFileIDs()
	if len(pending) == 0 {
		return nil
	}
	referenced, err := db.referencedValueLogSegments(ctx)
	if err != nil {
		return err
	}
	db.pendingValueLogAppendMu.Lock()
	defer db.pendingValueLogAppendMu.Unlock()
	for fileID := range pending {
		if _, ok := referenced[fileID]; ok {
			delete(db.pendingValueLogAppendFileIDRefs, fileID)
		}
	}
	if len(db.pendingValueLogAppendFileIDRefs) == 0 {
		db.pendingValueLogAppendFileIDRefs = nil
	}
	return nil
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
	db.RegisterCloseHook(func() error {
		db.SetLeafPageLog(nil)
		db.SetValueLogAppender(nil)
		return appender.close()
	})
	return nil
}
