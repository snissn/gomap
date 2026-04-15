package db

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const leafGenerationRecordLengthIndexChunkBytes = 256 << 10

type leafGenerationRecordLengthIndex struct {
	offsets []uint32
	lengths []uint32
}

func (idx *leafGenerationRecordLengthIndex) lookup(offset uint64) (uint32, bool) {
	if idx == nil || len(idx.offsets) == 0 || offset > uint64(^uint32(0)) {
		return 0, false
	}
	off32 := uint32(offset)
	i := sort.Search(len(idx.offsets), func(i int) bool {
		return idx.offsets[i] >= off32
	})
	if i >= len(idx.offsets) || idx.offsets[i] != off32 {
		return 0, false
	}
	return idx.lengths[i], true
}

func (db *DB) loadLeafGenerationRecordLengthIndex(rawFileID uint32) (*leafGenerationRecordLengthIndex, bool) {
	if db == nil || rawFileID == 0 {
		return nil, false
	}
	db.leafGenerationRecordLengthMu.RLock()
	idx := db.leafGenerationRecordLengthByFile[rawFileID]
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
	db.leafGenerationRecordLengthByFile[rawFileID] = idx
	db.leafGenerationRecordLengthMu.Unlock()
}

func (db *DB) leafGenerationRecordLengthForPlan(ptr page.LeafLogPtr, set *valuelog.Set, view *leafGenerationView) (uint32, bool, error) {
	if db == nil || set == nil || view == nil || ptr.FileID == 0 || ptr.Offset == 0 {
		return 0, false, nil
	}
	genID, ok := view.FileToGeneration[ptr.FileID]
	if !ok {
		return 0, false, nil
	}
	gen, ok := view.Generations[genID]
	if !ok || gen.State != leafGenerationStateSealed {
		return 0, false, nil
	}
	if idx, ok := db.loadLeafGenerationRecordLengthIndex(ptr.FileID); ok {
		if length, ok := idx.lookup(ptr.Offset); ok {
			return length, true, nil
		}
	}
	seg := set.Files[page.ValueLogFileID(ptr.FileID)]
	if seg == nil || seg.File == nil {
		return 0, false, nil
	}
	idx, err := scanLeafGenerationRecordLengthIndex(seg)
	if err != nil {
		return 0, false, err
	}
	db.storeLeafGenerationRecordLengthIndex(ptr.FileID, idx)
	length, ok := idx.lookup(ptr.Offset)
	if !ok {
		return 0, false, fmt.Errorf("leaf generation plan: missing record length for file=%d offset=%d", ptr.FileID, ptr.Offset)
	}
	return length, true, nil
}

func scanLeafGenerationRecordLengthIndex(seg *valuelog.File) (*leafGenerationRecordLengthIndex, error) {
	if seg == nil || seg.File == nil {
		return &leafGenerationRecordLengthIndex{}, nil
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
