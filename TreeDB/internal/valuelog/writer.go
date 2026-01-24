package valuelog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

const (
	defaultBufferSize = 4 << 20
	headerWithoutCRC  = HeaderSize - 4
)

var syncDirFn = syncDir

func recordSizeExceedsMax(valueLen uint32) bool {
	if slab.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(HeaderSize) + int64(valueLen)
	return recordLen > slab.MaxRecordSize
}

type Writer struct {
	f       *os.File
	bw      *bufio.Writer
	size    int64
	fileID  uint32
	scratch []byte
	syncFn  func(*os.File) error
}

func NewWriter(path string, fileID uint32) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if err := syncDirFn(path); err != nil {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Writer{
		f:       f,
		bw:      bufio.NewWriterSize(f, defaultBufferSize),
		size:    info.Size(),
		fileID:  fileID,
		scratch: make([]byte, 0, defaultBufferSize),
		syncFn:  func(file *os.File) error { return file.Sync() },
	}, nil
}

func (w *Writer) FileID() uint32 {
	if w == nil {
		return 0
	}
	return w.fileID
}

func (w *Writer) Size() int64 {
	if w == nil {
		return 0
	}
	return w.size
}

func (w *Writer) Flush() error {
	if w == nil || w.f == nil {
		return nil
	}
	return w.bw.Flush()
}

func (w *Writer) RotateTo(path string, fileID uint32) error {
	if w == nil {
		return errors.New("valuelog: nil writer")
	}

	if w.f == nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		if err := syncDirFn(path); err != nil {
			_ = f.Close()
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		w.f = f
		w.bw.Reset(f)
		w.size = info.Size()
		w.fileID = fileID
		w.scratch = w.scratch[:0]
		return nil
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}
	if w.syncFn != nil {
		if err := w.syncFn(w.f); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if err := syncDirFn(path); err != nil {
		_ = f.Close()
		return err
	}

	old := w.f
	w.f = f
	w.bw.Reset(f)
	w.size = info.Size()
	w.fileID = fileID
	w.scratch = w.scratch[:0]
	if err := old.Close(); err != nil {
		return err
	}
	return nil
}

func syncDir(path string) (err error) {
	if runtime.GOOS == "windows" || path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Append(rid uint64, value []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("valuelog: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, errors.New("valuelog: missing rid")
	}
	if len(value) > int(^uint32(0)) {
		return page.ValuePtr{}, ErrRecordTooLarge
	}
	valueLen := uint32(len(value))
	if recordSizeExceedsMax(valueLen) {
		return page.ValuePtr{}, ErrRecordTooLarge
	}

	recordLen := HeaderSize + len(value)
	start := w.size
	if cap(w.scratch) < recordLen {
		w.scratch = make([]byte, recordLen)
	}
	buf := w.scratch[:recordLen]

	buf[4] = Version
	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	binary.LittleEndian.PutUint64(buf[8:16], rid)
	binary.LittleEndian.PutUint32(buf[16:20], valueLen)
	copy(buf[HeaderSize:], value)

	sum := crc.ChecksumParts(buf[4:HeaderSize], value)
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	if _, err := w.bw.Write(buf); err != nil {
		return page.ValuePtr{}, err
	}
	w.size += int64(recordLen)

	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: uint32(headerWithoutCRC + len(value)),
		FileID: w.fileID,
	}, nil
}

func (w *Writer) AppendBatch(records []Record) ([]page.ValuePtr, error) {
	if len(records) == 0 {
		return nil, nil
	}

	ptrs := make([]page.ValuePtr, len(records))
	total := 0
	start := w.size
	for i, r := range records {
		if r.RID == 0 {
			return nil, errors.New("valuelog: missing rid")
		}
		if len(r.Value) > int(^uint32(0)) {
			return nil, ErrRecordTooLarge
		}
		valueLen := uint32(len(r.Value))
		if recordSizeExceedsMax(valueLen) {
			return nil, ErrRecordTooLarge
		}
		recordLen := HeaderSize + len(r.Value)
		ptrs[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: uint32(headerWithoutCRC + len(r.Value)),
			FileID: w.fileID,
		}
		start += int64(recordLen)
		total += recordLen
	}

	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	}
	buf := w.scratch[:total]

	off := 0
	for _, r := range records {
		valueLen := uint32(len(r.Value))
		recordLen := HeaderSize + len(r.Value)
		buf[off+4] = Version
		buf[off+5] = 0
		buf[off+6] = 0
		buf[off+7] = 0
		binary.LittleEndian.PutUint64(buf[off+8:off+16], r.RID)
		binary.LittleEndian.PutUint32(buf[off+16:off+20], valueLen)
		copy(buf[off+HeaderSize:off+recordLen], r.Value)
		sum := crc.ChecksumParts(buf[off+4:off+HeaderSize], r.Value)
		binary.LittleEndian.PutUint32(buf[off:off+4], sum)
		off += recordLen
	}

	if _, err := w.bw.Write(buf); err != nil {
		return nil, err
	}
	w.size += int64(total)
	return ptrs, nil
}

func (w *Writer) Sync() error {
	if w == nil || w.f == nil {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
