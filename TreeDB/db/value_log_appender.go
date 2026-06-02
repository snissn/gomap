package db

import (
	"errors"

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
	return appender.AppendValues(values)
}

func (db *DB) installCommandWALValueLogAppender() error {
	if db == nil {
		return ErrClosed
	}
	segments, err := listRecoverySegments(db.dir)
	if err != nil {
		return err
	}
	ridMap, err := scanValueLogSegments(segments, db.valueLogDictLookup)
	if err != nil {
		return err
	}
	appender, err := newReplayInlineAppender(db, segments, ridMap)
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
