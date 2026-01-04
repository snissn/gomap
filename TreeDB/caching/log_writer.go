package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/vlog"
	"github.com/snissn/gomap/TreeDB/internal/wal"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	logOpSet        = byte(0)
	logOpDelete     = byte(1)
	logOpSetPointer = byte(2)
)

type logRecord struct {
	Op    byte
	Key   []byte
	Value []byte
}

type logWriter interface {
	Append(op byte, key, value []byte) (page.ValuePtr, error)
	AppendBatch(records []logRecord) ([]page.ValuePtr, error)
	RotateTo(path string, fileID uint32) error
	Size() int64
	Flush() error
	Sync() error
	Close() error
}

type walWriterAdapter struct {
	w       *wal.Writer
	scratch []wal.Record
}

func (a *walWriterAdapter) Append(op byte, key, value []byte) (page.ValuePtr, error) {
	if err := a.w.Append(op, key, value); err != nil {
		return page.ValuePtr{}, err
	}
	return page.ValuePtr{}, nil
}

func (a *walWriterAdapter) AppendBatch(records []logRecord) ([]page.ValuePtr, error) {
	if len(records) == 0 {
		return nil, nil
	}

	if cap(a.scratch) < len(records) {
		a.scratch = make([]wal.Record, len(records))
	}
	walRecords := a.scratch[:len(records)]
	for i := range records {
		r := &records[i]
		walRecords[i] = wal.Record{Op: r.Op, Key: r.Key, Value: r.Value}
	}
	if err := a.w.AppendBatch(walRecords); err != nil {
		return nil, err
	}
	// WAL mode does not return meaningful pointers.
	return nil, nil
}

func (a *walWriterAdapter) RotateTo(path string, _ uint32) error {
	return a.w.RotateTo(path)
}

func (a *walWriterAdapter) Size() int64 {
	return a.w.Size()
}

func (a *walWriterAdapter) Flush() error {
	return a.w.Flush()
}

func (a *walWriterAdapter) Sync() error {
	return a.w.Sync()
}

func (a *walWriterAdapter) Close() error {
	return a.w.Close()
}

type vlogWriterAdapter struct {
	w       *vlog.Writer
	scratch []vlog.Record
}

func (a *vlogWriterAdapter) Append(op byte, key, value []byte) (page.ValuePtr, error) {
	return a.w.Append(op, key, value)
}

func (a *vlogWriterAdapter) AppendBatch(records []logRecord) ([]page.ValuePtr, error) {
	if len(records) == 0 {
		return nil, nil
	}

	if cap(a.scratch) < len(records) {
		a.scratch = make([]vlog.Record, len(records))
	}
	vlogRecords := a.scratch[:len(records)]
	for i := range records {
		r := &records[i]
		vlogRecords[i] = vlog.Record{Op: r.Op, Key: r.Key, Value: r.Value}
	}
	return a.w.AppendBatch(vlogRecords)
}

func (a *vlogWriterAdapter) RotateTo(path string, fileID uint32) error {
	return a.w.RotateTo(path, fileID)
}

func (a *vlogWriterAdapter) Size() int64 {
	return a.w.Size()
}

func (a *vlogWriterAdapter) Flush() error {
	return a.w.Flush()
}

func (a *vlogWriterAdapter) Sync() error {
	return a.w.Sync()
}

func (a *vlogWriterAdapter) Close() error {
	return a.w.Close()
}
