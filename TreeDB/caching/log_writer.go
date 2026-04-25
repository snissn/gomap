package caching

import (
	"github.com/snissn/compress/zstd"
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

type commitBatchFuncWriter interface {
	AppendBatchFunc(count int, recordAt func(int) commitlog.Record) error
}

type valueWriter interface {
	Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error)
	AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error)
	SetDictFrameEncoderOptions(level zstd.EncoderLevel, enableEntropy bool)
	RotateTo(path string, fileID uint32) error
	Size() int64
	Flush() error
	Sync() error
	Close() error
}
