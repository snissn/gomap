package vlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

const (
	OpSet    = byte(0)
	OpDelete = byte(1)

	// HeaderSize: CRC(4) + KeyLen(2) + ValueLen(4) + Op(1)
	HeaderSize = 11
)

const defaultBufferSize = 4 << 20

// MaxDeadMappings caps the number of old mmaps retained to avoid exhausting
// vm.max_map_count. Set <= 0 to disable the cap.
var MaxDeadMappings = 64

var (
	ErrCorrupt        = errors.New("vlog: corrupt record")
	ErrRecordTooLarge = errors.New("vlog: record too large")
	ErrKeyTooLarge    = errors.New("vlog: key too large")
	ErrValueTooLarge  = errors.New("vlog: value too large")
)

var syncDirFn = syncDir

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	Op    byte
	Key   []byte
	Value []byte
}

func recordSizeExceedsMax(keyLen uint16, valLen uint32) bool {
	if slab.MaxRecordSize <= 0 {
		return false
	}
	recordLen := int64(HeaderSize) + int64(keyLen) + int64(valLen)
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
		return errors.New("vlog: nil writer")
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

func (w *Writer) Append(op byte, key, value []byte) (page.ValuePtr, error) {
	if w == nil {
		return page.ValuePtr{}, errors.New("vlog: nil writer")
	}
	if len(key) > int(^uint16(0)) {
		return page.ValuePtr{}, ErrKeyTooLarge
	}
	if len(value) > int(^uint32(0)) {
		return page.ValuePtr{}, ErrValueTooLarge
	}
	keyLen := uint16(len(key))
	valLen := uint32(len(value))
	if recordSizeExceedsMax(keyLen, valLen) {
		return page.ValuePtr{}, ErrRecordTooLarge
	}

	recordLen := HeaderSize + len(key) + len(value)
	start := w.size

	if cap(w.scratch) < recordLen {
		w.scratch = make([]byte, recordLen)
	}
	buf := w.scratch[:recordLen]

	binary.LittleEndian.PutUint16(buf[4:6], keyLen)
	binary.LittleEndian.PutUint32(buf[6:10], valLen)
	buf[10] = op
	copy(buf[11:], key)
	copy(buf[11+len(key):], value)
	sum := crc32.Update(0, crc32cTable, buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	if _, err := w.bw.Write(buf); err != nil {
		return page.ValuePtr{}, err
	}
	w.size += int64(recordLen)

	if op != OpSet {
		return page.ValuePtr{}, nil
	}

	return page.ValuePtr{
		Offset: uint64(start + 4),
		Length: uint32(2 + 4 + 1 + len(key) + len(value)),
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
		if len(r.Key) > int(^uint16(0)) {
			return nil, ErrKeyTooLarge
		}
		if len(r.Value) > int(^uint32(0)) {
			return nil, ErrValueTooLarge
		}
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		if recordSizeExceedsMax(keyLen, valLen) {
			return nil, ErrRecordTooLarge
		}
		recordLen := HeaderSize + len(r.Key) + len(r.Value)
		if r.Op == OpSet {
			ptrs[i] = page.ValuePtr{
				Offset: uint64(start + 4),
				Length: uint32(2 + 4 + 1 + len(r.Key) + len(r.Value)),
				FileID: w.fileID,
			}
		}
		start += int64(recordLen)
		total += recordLen
	}

	if cap(w.scratch) < total {
		w.scratch = make([]byte, total)
	}
	buf := w.scratch[:total]

	off := 0
	for i, r := range records {
		keyLen := uint16(len(r.Key))
		valLen := uint32(len(r.Value))
		recordLen := HeaderSize + len(r.Key) + len(r.Value)

		binary.LittleEndian.PutUint16(buf[off+4:off+6], keyLen)
		binary.LittleEndian.PutUint32(buf[off+6:off+10], valLen)
		buf[off+10] = r.Op
		copy(buf[off+11:], r.Key)
		copy(buf[off+11+len(r.Key):], r.Value)
		sum := crc32.Update(0, crc32cTable, buf[off+4:off+recordLen])
		binary.LittleEndian.PutUint32(buf[off:off+4], sum)
		off += recordLen
		if r.Op != OpSet {
			ptrs[i] = page.ValuePtr{}
		}
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

type Reader struct {
	f        *os.File
	r        *bufio.Reader
	pos      int64
	fileID   uint32
	verifies bool
}

func NewReader(path string, fileID uint32) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:        f,
		r:        bufio.NewReaderSize(f, defaultBufferSize),
		fileID:   fileID,
		verifies: true,
	}, nil
}

func (r *Reader) DisableChecksum() {
	r.verifies = false
}

func (r *Reader) ReadNext() (byte, []byte, []byte, page.ValuePtr, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return 0, nil, nil, page.ValuePtr{}, err
	}

	crc := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint16(header[4:6])
	valLen := binary.LittleEndian.Uint32(header[6:10])
	op := header[10]

	if recordSizeExceedsMax(keyLen, valLen) {
		return 0, nil, nil, page.ValuePtr{}, ErrRecordTooLarge
	}

	payloadLen := int(keyLen) + int(valLen)
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r.r, payload); err != nil {
		return 0, nil, nil, page.ValuePtr{}, err
	}

	if r.verifies {
		sum := crc32.Update(0, crc32cTable, header[4:])
		sum = crc32.Update(sum, crc32cTable, payload)
		if sum != crc {
			return 0, nil, nil, page.ValuePtr{}, ErrCorrupt
		}
	}

	start := r.pos
	r.pos += int64(HeaderSize + payloadLen)

	ptr := page.ValuePtr{
		Offset: uint64(start + 4),
		Length: uint32(2 + 4 + 1 + payloadLen),
		FileID: r.fileID,
	}

	key := payload[:keyLen]
	value := payload[keyLen:]
	return op, key, value, ptr, nil
}

func (r *Reader) Close() error {
	if r == nil || r.f == nil {
		return nil
	}
	return r.f.Close()
}

func ReadAt(f *os.File, ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	if f == nil {
		return nil, errors.New("vlog: nil file")
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt
	}
	start := int64(ptr.Offset - 4)
	if ptr.Length != 0 {
		totalLen := int(ptr.Length) + 4
		if totalLen < HeaderSize {
			return nil, ErrCorrupt
		}
		buf := make([]byte, totalLen)
		if _, err := f.ReadAt(buf, start); err != nil {
			return nil, err
		}
		header := buf[:HeaderSize]
		payload := buf[HeaderSize:]

		crc := binary.LittleEndian.Uint32(header[0:4])
		keyLen := binary.LittleEndian.Uint16(header[4:6])
		valLen := binary.LittleEndian.Uint32(header[6:10])
		op := header[10]
		if op != OpSet && op != OpDelete {
			return nil, ErrCorrupt
		}
		if recordSizeExceedsMax(keyLen, valLen) {
			return nil, ErrRecordTooLarge
		}
		payloadLen := int(keyLen) + int(valLen)
		if payloadLen != len(payload) {
			return nil, ErrCorrupt
		}
		expectedLen := uint32(2 + 4 + 1 + payloadLen)
		if ptr.Length != expectedLen {
			return nil, fmt.Errorf("vlog: pointer length mismatch (ptr=%d record=%d)", ptr.Length, expectedLen)
		}
		if verifyCRC {
			sum := crc32.Update(0, crc32cTable, buf[4:])
			if sum != crc {
				return nil, ErrCorrupt
			}
		}
		return payload[keyLen:], nil
	}

	var header [HeaderSize]byte
	if _, err := f.ReadAt(header[:], start); err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint16(header[4:6])
	valLen := binary.LittleEndian.Uint32(header[6:10])
	op := header[10]
	if op != OpSet && op != OpDelete {
		return nil, ErrCorrupt
	}
	if recordSizeExceedsMax(keyLen, valLen) {
		return nil, ErrRecordTooLarge
	}

	payloadLen := int(keyLen) + int(valLen)
	payload := make([]byte, payloadLen)
	if _, err := f.ReadAt(payload, start+HeaderSize); err != nil {
		return nil, err
	}

	if verifyCRC {
		sum := crc32.Update(0, crc32cTable, header[4:])
		sum = crc32.Update(sum, crc32cTable, payload)
		if sum != crc {
			return nil, ErrCorrupt
		}
	}

	return payload[keyLen:], nil
}
