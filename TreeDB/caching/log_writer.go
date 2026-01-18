package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	logOpSetRID    = commitlog.OpSetRID
	logOpSetInline = commitlog.OpSetInline
	logOpDelete    = commitlog.OpDelete
)

type logRecord = commitlog.Record

type commitWriter interface {
	Append(record commitlog.Record) error
	AppendBatch(records []commitlog.Record) error
	RotateTo(path string) error
	Size() int64
	Flush() error
	Sync() error
	Close() error
}

type valueWriter interface {
	Append(rid uint64, value []byte) (page.ValuePtr, error)
	AppendBatch(records []valuelog.Record) ([]page.ValuePtr, error)
	RotateTo(path string, fileID uint32) error
	Size() int64
	Flush() error
	Sync() error
	Close() error
}
