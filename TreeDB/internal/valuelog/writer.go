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

func (w *Writer) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("valuelog: nil writer")
	}
	if rid == 0 {
		return page.ValuePtr{}, errors.New("valuelog: missing rid")
	}
	ptrs, err := w.AppendFrame(dictID, dict, []Record{{RID: rid, Value: value}})
	if err != nil {
		return page.ValuePtr{}, err
	}
	return ptrs[0], nil
}

func (w *Writer) AppendFrame(dictID uint64, dict []byte, records []Record) ([]page.ValuePtr, error) {
	if w == nil {
		return nil, errors.New("valuelog: nil writer")
	}
	if len(records) == 0 {
		return nil, nil
	}

	body, _, err := EncodeFrame(dictID, dict, records)
	if err != nil {
		return nil, err
	}
	if len(body) > int(^uint32(0)) {
		return nil, ErrRecordTooLarge
	}
	bodyLen := uint32(len(body))
	if recordSizeExceedsMax(bodyLen) {
		return nil, ErrRecordTooLarge
	}

	recordLen := HeaderSize + len(body)
	start := w.size
	if cap(w.scratch) < recordLen {
		w.scratch = make([]byte, recordLen)
	}
	buf := w.scratch[:recordLen]

	buf[4] = Version
	buf[5] = recordFlagGrouped
	buf[6] = 0
	buf[7] = 0
	binary.LittleEndian.PutUint64(buf[8:16], 0)
	binary.LittleEndian.PutUint32(buf[16:20], bodyLen)
	copy(buf[HeaderSize:], body)

	sum := crc.ChecksumParts(buf[4:HeaderSize], body)
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	if _, err := w.bw.Write(buf); err != nil {
		return nil, err
	}
	w.size += int64(recordLen)

	ptrs := make([]page.ValuePtr, len(records))
	recordLenNoCRC := uint32(headerWithoutCRC) + bodyLen
	for i := range records {
		ptrs[i] = page.ValuePtr{
			Offset: uint64(start + 4),
			Length: page.ValuePtrMarkGrouped(recordLenNoCRC, uint8(i)),
			FileID: w.fileID,
		}
	}
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
