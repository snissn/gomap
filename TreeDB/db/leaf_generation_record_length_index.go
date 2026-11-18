package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationRecordLengthIndexChunkBytes = 256 << 10
	leafGenerationRecordLengthIndexMagic      = "TLGI"
	leafGenerationRecordLengthIndexVersion    = 1
	leafGenerationRecordLengthIndexSuffix     = ".lenidx"
)

type leafGenerationRecordLengthIndex struct {
	offsets []uint32
	lengths []uint32
}

var leafGenerationRecordLengthIndexScanHook struct {
	mu sync.Mutex
	fn func(rawFileID uint32)
}

func registerLeafGenerationRecordLengthIndexScanHook(hook func(rawFileID uint32)) func() {
	leafGenerationRecordLengthIndexScanHook.mu.Lock()
	prev := leafGenerationRecordLengthIndexScanHook.fn
	leafGenerationRecordLengthIndexScanHook.fn = hook
	leafGenerationRecordLengthIndexScanHook.mu.Unlock()
	return func() {
		leafGenerationRecordLengthIndexScanHook.mu.Lock()
		leafGenerationRecordLengthIndexScanHook.fn = prev
		leafGenerationRecordLengthIndexScanHook.mu.Unlock()
	}
}

func runLeafGenerationRecordLengthIndexScanHook(rawFileID uint32) {
	leafGenerationRecordLengthIndexScanHook.mu.Lock()
	hook := leafGenerationRecordLengthIndexScanHook.fn
	leafGenerationRecordLengthIndexScanHook.mu.Unlock()
	if hook != nil {
		hook(rawFileID)
	}
}

func (idx *leafGenerationRecordLengthIndex) len() int {
	if idx == nil {
		return 0
	}
	return len(idx.offsets)
}

func (idx *leafGenerationRecordLengthIndex) clone() *leafGenerationRecordLengthIndex {
	if idx == nil {
		return nil
	}
	return &leafGenerationRecordLengthIndex{
		offsets: append([]uint32(nil), idx.offsets...),
		lengths: append([]uint32(nil), idx.lengths...),
	}
}

func (idx *leafGenerationRecordLengthIndex) lookup(offset uint64) (uint32, bool) {
	length, _, ok := idx.lookupWithHint(offset, 0)
	return length, ok
}

func (idx *leafGenerationRecordLengthIndex) lookupWithHint(offset uint64, hint int) (uint32, int, bool) {
	if idx == nil || len(idx.offsets) == 0 || offset > uint64(^uint32(0)) {
		return 0, 0, false
	}
	off32 := uint32(offset)
	n := len(idx.offsets)
	if hint < 0 {
		hint = 0
	} else if hint >= n {
		hint = n - 1
	}
	if idx.offsets[hint] == off32 {
		return idx.lengths[hint], hint, true
	}
	if idx.offsets[hint] < off32 {
		for i := hint + 1; i < n && i <= hint+4; i++ {
			off := idx.offsets[i]
			if off == off32 {
				return idx.lengths[i], i, true
			}
			if off > off32 {
				return 0, i, false
			}
		}
		i := hint + sort.Search(n-hint, func(i int) bool {
			return idx.offsets[hint+i] >= off32
		})
		if i >= n || idx.offsets[i] != off32 {
			return 0, i, false
		}
		return idx.lengths[i], i, true
	}
	for i := hint - 1; i >= 0 && i >= hint-2; i-- {
		off := idx.offsets[i]
		if off == off32 {
			return idx.lengths[i], i, true
		}
		if off < off32 {
			return 0, i + 1, false
		}
	}
	i := sort.Search(hint+1, func(i int) bool {
		return idx.offsets[i] >= off32
	})
	if i > hint || idx.offsets[i] != off32 {
		return 0, i, false
	}
	return idx.lengths[i], i, true
}

func (idx *leafGenerationRecordLengthIndex) add(offset uint64, length uint32) bool {
	if idx == nil || length == 0 || offset > uint64(^uint32(0)) {
		return false
	}
	off32 := uint32(offset)
	n := len(idx.offsets)
	if n == 0 || idx.offsets[n-1] < off32 {
		idx.offsets = append(idx.offsets, off32)
		idx.lengths = append(idx.lengths, length)
		return true
	}
	i := sort.Search(n, func(i int) bool {
		return idx.offsets[i] >= off32
	})
	if i < n && idx.offsets[i] == off32 {
		if idx.lengths[i] == length {
			return false
		}
		idx.lengths[i] = length
		return true
	}
	idx.offsets = append(idx.offsets, 0)
	copy(idx.offsets[i+1:], idx.offsets[i:])
	idx.offsets[i] = off32
	idx.lengths = append(idx.lengths, 0)
	copy(idx.lengths[i+1:], idx.lengths[i:])
	idx.lengths[i] = length
	return true
}

func leafGenerationRecordLengthIndexPath(rootDir string, rawFileID uint32) string {
	if rootDir == "" || rawFileID == 0 {
		return ""
	}
	return leafGenerationFallbackPath(rootDir, rawFileID) + leafGenerationRecordLengthIndexSuffix
}

func saveLeafGenerationRecordLengthIndexFile(path string, rawFileID uint32, idx *leafGenerationRecordLengthIndex) error {
	if path == "" || rawFileID == 0 {
		return nil
	}
	count := 0
	if idx != nil {
		count = len(idx.offsets)
		if len(idx.lengths) != count {
			return fmt.Errorf("leaf generation record-length index: encode %q: offset/length mismatch", path)
		}
	}
	const headerSize = 16
	buf := bytes.NewBuffer(make([]byte, 0, headerSize+count*8))
	buf.WriteString(leafGenerationRecordLengthIndexMagic)
	var hdr [12]byte
	binary.LittleEndian.PutUint32(hdr[0:4], leafGenerationRecordLengthIndexVersion)
	binary.LittleEndian.PutUint32(hdr[4:8], rawFileID)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(count))
	buf.Write(hdr[:])
	if idx != nil {
		for i := 0; i < count; i++ {
			if idx.lengths[i] == 0 {
				return fmt.Errorf("leaf generation record-length index: encode %q: zero length at entry %d", path, i)
			}
			var entry [8]byte
			binary.LittleEndian.PutUint32(entry[0:4], idx.offsets[i])
			binary.LittleEndian.PutUint32(entry[4:8], idx.lengths[i])
			buf.Write(entry[:])
		}
	}
	return writeFileAtomic(path, buf.Bytes(), 0o600)
}

func (db *DB) loadLeafGenerationRecordLengthIndex(rawFileID uint32) (*leafGenerationRecordLengthIndex, bool) {
	if db == nil || rawFileID == 0 {
		return nil, false
	}
	db.leafGenerationRecordLengthMu.RLock()
	idx := db.leafGenerationRecordLengthByFile[rawFileID]
	if idx != nil {
		idx = idx.clone()
	}
	db.leafGenerationRecordLengthMu.RUnlock()
	if idx == nil {
		return nil, false
	}
	return idx, true
}

func (db *DB) storeLeafGenerationRecordLengthIndex(rawFileID uint32, idx *leafGenerationRecordLengthIndex) {
	if db == nil || rawFileID == 0 || idx == nil {
		return
	}
	db.leafGenerationRecordLengthMu.Lock()
	if db.leafGenerationRecordLengthByFile == nil {
		db.leafGenerationRecordLengthByFile = make(map[uint32]*leafGenerationRecordLengthIndex)
	}
	db.leafGenerationRecordLengthByFile[rawFileID] = idx.clone()
	db.leafGenerationRecordLengthMu.Unlock()
}

func (db *DB) noteLeafGenerationRecordLengthRaw(rawFileID uint32, offset uint64, recordLen uint32) {
	if db == nil || rawFileID == 0 || recordLen == 0 {
		return
	}
	db.leafGenerationRecordLengthMu.Lock()
	if db.leafGenerationRecordLengthByFile == nil {
		db.leafGenerationRecordLengthByFile = make(map[uint32]*leafGenerationRecordLengthIndex)
	}
	idx := db.leafGenerationRecordLengthByFile[rawFileID]
	if idx == nil {
		idx = &leafGenerationRecordLengthIndex{}
		db.leafGenerationRecordLengthByFile[rawFileID] = idx
	}
	idx.add(offset, recordLen)
	db.leafGenerationRecordLengthMu.Unlock()
}

func (db *DB) NoteLeafGenerationRecordLength(ptr page.ValuePtr) {
	if db == nil || ptr.FileID == 0 {
		return
	}
	recordLen := page.ValuePtrRecordLength(ptr)
	if recordLen == 0 {
		return
	}
	rawFileID, ok := rawLeafGenerationFileID(ptr.FileID)
	if !ok {
		return
	}
	db.noteLeafGenerationRecordLengthRaw(rawFileID, ptr.Offset, recordLen)
}

func (db *DB) persistLeafGenerationRecordLengthIndex(rawFileID uint32) error {
	if db == nil || db.readOnly || rawFileID == 0 {
		return nil
	}
	idx, ok := db.loadLeafGenerationRecordLengthIndex(rawFileID)
	if !ok {
		var err error
		idx, err = db.scanLeafGenerationRecordLengthIndexFromDisk(rawFileID)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		db.storeLeafGenerationRecordLengthIndex(rawFileID, idx)
	}
	return saveLeafGenerationRecordLengthIndexFile(leafGenerationRecordLengthIndexPath(db.dir, rawFileID), rawFileID, idx)
}

func (db *DB) scanLeafGenerationRecordLengthIndexFromDisk(rawFileID uint32) (*leafGenerationRecordLengthIndex, error) {
	if db == nil || rawFileID == 0 {
		return &leafGenerationRecordLengthIndex{}, nil
	}
	path := leafGenerationFallbackPath(db.dir, rawFileID)
	return scanLeafGenerationRecordLengthIndexPath(path, page.ValueLogFileID(rawFileID))
}

// The index populated by this process's writers is optional. A retained file
// is the source of truth for pointers whose length is absent from both places.
func (db *DB) leafGenerationRecordLengthFromSource(file *leafGenerationScanFileState, ptr page.LeafLogPtr) (uint32, error) {
	if file.idx == nil {
		idx, ok := db.loadLeafGenerationRecordLengthIndex(file.fileID)
		if !ok {
			idx = &leafGenerationRecordLengthIndex{}
		}
		file.idx = idx
	}
	length, hint, ok := file.idx.lookupWithHint(ptr.Offset, file.lookupHint)
	file.lookupHint = hint
	if ok {
		return length, nil
	}
	if err := file.source.EnsureCurrentWritableReadableFor(ptr.ValuePtr()); err != nil {
		return 0, err
	}
	length, err := readValueLogRecordLengthFromHeader(file.source.File, int64(ptr.Offset)-4)
	if err != nil {
		return 0, err
	}
	if err := checkLeafGenerationRecordExtent(file.source, ptr.Offset, length); err != nil {
		return 0, err
	}
	return length, nil
}

func checkLeafGenerationRecordExtent(source *valuelog.File, offset uint64, length uint32) error {
	if length < valuelog.HeaderSize-4 {
		return valuelog.ErrCorrupt
	}
	info, err := source.File.Stat()
	if err != nil {
		return err
	}
	if offset+uint64(length) > uint64(info.Size()) {
		return fmt.Errorf("leaf generation record exceeds retained file %d at offset %d: %w", source.ID, offset, io.ErrUnexpectedEOF)
	}
	return nil
}

func scanLeafGenerationRecordLengthIndexPath(path string, fileID uint32) (*leafGenerationRecordLengthIndex, error) {
	if path == "" {
		return &leafGenerationRecordLengthIndex{}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return scanLeafGenerationRecordLengthIndex(&valuelog.File{ID: fileID, Path: path, File: f})
}

func scanLeafGenerationRecordLengthIndex(seg *valuelog.File) (*leafGenerationRecordLengthIndex, error) {
	if seg == nil || seg.File == nil {
		return &leafGenerationRecordLengthIndex{}, nil
	}
	if seg.ID != 0 {
		runLeafGenerationRecordLengthIndexScanHook(page.ValueLogSegmentID(seg.ID))
	}
	size := fileSize(seg)
	if seg.File != nil {
		if info, err := seg.File.Stat(); err == nil && info.Size() > 0 {
			size = info.Size()
		}
	}
	if size <= 0 || size < int64(valuelog.HeaderSize) {
		return &leafGenerationRecordLengthIndex{}, nil
	}
	buf := make([]byte, leafGenerationRecordLengthIndexChunkBytes)
	estimated := int(size / 2048)
	if estimated < 16 {
		estimated = 16
	}
	idx := &leafGenerationRecordLengthIndex{
		offsets: make([]uint32, 0, estimated),
		lengths: make([]uint32, 0, estimated),
	}
	for off := int64(0); off+int64(valuelog.HeaderSize) <= size; {
		toRead := int64(len(buf))
		if remaining := size - off; remaining < toRead {
			toRead = remaining
		}
		n, err := seg.File.ReadAt(buf[:toRead], off)
		if err != nil && n == 0 {
			return nil, err
		}
		if n < valuelog.HeaderSize {
			break
		}
		chunkAdvanced := int64(0)
		for pos := 0; pos+valuelog.HeaderSize <= n; {
			header := buf[pos : pos+valuelog.HeaderSize]
			if header[4] != valuelog.Version {
				return nil, fmt.Errorf("leaf generation plan: invalid value-log version at %s offset=%d", seg.Path, off+int64(pos))
			}
			bodyLen := int64(binary.LittleEndian.Uint32(header[16:20]))
			if bodyLen < int64(valuelog.FrameHeaderSize) {
				return nil, fmt.Errorf("leaf generation plan: invalid grouped leaf record body at %s offset=%d", seg.Path, off+int64(pos))
			}
			recordSize := int64(valuelog.HeaderSize) + bodyLen
			recordOff := off + int64(pos)
			if recordOff+recordSize > size {
				return nil, fmt.Errorf("leaf generation plan: record exceeds file size at %s offset=%d", seg.Path, recordOff)
			}
			idx.offsets = append(idx.offsets, uint32(recordOff+4))
			idx.lengths = append(idx.lengths, uint32(valuelog.HeaderSize-4)+uint32(bodyLen))
			nextPos := pos + int(recordSize)
			if nextPos > n {
				off = recordOff + recordSize
				chunkAdvanced = -1
				break
			}
			pos = nextPos
			chunkAdvanced = int64(pos)
		}
		if chunkAdvanced >= 0 {
			if chunkAdvanced == 0 {
				break
			}
			off += chunkAdvanced
		}
	}
	return idx, nil
}
