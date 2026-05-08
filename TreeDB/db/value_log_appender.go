package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

// ValueLogAppender appends user values to the persistent value log and returns
// stable ValuePtr references that may be stored by native-root callers.
type ValueLogAppender interface {
	AppendValues(values [][]byte) ([]page.ValuePtr, error)
	Flush() error
	Sync() error
	CurrentValueLogSegment() (path string, fileID uint32, ok bool)
}

// SetValueLogAppender installs the appender used by native-root APIs that need
// to create persistent value-log pointers. Cached mode wires this to its normal
// value-log writer.
func (db *DB) SetValueLogAppender(appender ValueLogAppender) {
	if db == nil {
		return
	}
	db.writeMu.Lock()
	db.valueLogAppender = appender
	db.writeMu.Unlock()
}

// HasValueLogAppender reports whether native-root callers can append user
// values to the persistent value log.
func (db *DB) HasValueLogAppender() bool {
	if db == nil {
		return false
	}
	db.writeMu.RLock()
	ok := db.valueLogAppender != nil
	db.writeMu.RUnlock()
	return ok
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
	db.writeMu.RLock()
	appender := db.valueLogAppender
	db.writeMu.RUnlock()
	if appender == nil {
		return nil, errors.New("value-log appender unavailable")
	}
	return appender.AppendValues(values)
}
