package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

type runtimeLeafPageLog struct {
	db *DB
}

func (l *runtimeLeafPageLog) AppendLeafPage(leafPage []byte) (page.ValuePtr, error) {
	if l == nil || l.db == nil {
		return page.ValuePtr{}, fmt.Errorf("missing db")
	}
	l.db.inlineAppendMu.Lock()
	defer l.db.inlineAppendMu.Unlock()

	app, err := l.db.inlineAppenderLocked()
	if err != nil {
		return page.ValuePtr{}, err
	}
	return app.AppendLeafPage(leafPage)
}

func (l *runtimeLeafPageLog) Flush() error {
	if l == nil || l.db == nil {
		return fmt.Errorf("missing db")
	}
	l.db.inlineAppendMu.Lock()
	defer l.db.inlineAppendMu.Unlock()

	if l.db.inlineAppender == nil {
		return nil
	}
	return l.db.inlineAppender.Flush()
}

func (l *runtimeLeafPageLog) Sync() error {
	if l == nil || l.db == nil {
		return fmt.Errorf("missing db")
	}
	l.db.inlineAppendMu.Lock()
	defer l.db.inlineAppendMu.Unlock()

	if l.db.inlineAppender == nil {
		return nil
	}
	return l.db.inlineAppender.Sync()
}
