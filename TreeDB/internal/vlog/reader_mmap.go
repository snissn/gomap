package vlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/snissn/gomap/TreeDB/page"
)

func (f *File) Read(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return f.read(ptr, verifyCRC, false)
}

func (f *File) ReadUnsafe(ptr page.ValuePtr, verifyCRC bool) ([]byte, error) {
	return f.read(ptr, verifyCRC, true)
}

func (f *File) read(ptr page.ValuePtr, verifyCRC bool, unsafe bool) ([]byte, error) {
	if f == nil || f.File == nil {
		return nil, errors.New("vlog: nil file")
	}
	if ptr.Offset < 4 {
		return nil, ErrCorrupt
	}

	realStart := int64(ptr.Offset - 4)
	if val, err, ok := f.readViaMmap(realStart, verifyCRC); ok {
		return val, err
	}
	if unsafe {
		f.remapToFileSize()
		if val, err, ok := f.readViaMmap(realStart, verifyCRC); ok {
			return val, err
		}
	}
	return ReadAt(f.File, ptr, verifyCRC)
}

func (f *File) readViaMmap(realStart int64, verifyCRC bool) ([]byte, error, bool) {
	data, _ := f.mmapData.Load().([]byte)
	if data == nil || realStart < 0 || realStart+HeaderSize > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	header := data[realStart : realStart+HeaderSize]
	keyLen := binary.LittleEndian.Uint16(header[4:6])
	valLen := binary.LittleEndian.Uint32(header[6:10])
	op := header[10]
	if op != OpSet && op != OpDelete {
		return nil, ErrCorrupt, true
	}
	if recordSizeExceedsMax(keyLen, valLen) {
		return nil, ErrRecordTooLarge, true
	}

	totalLen64 := int64(keyLen) + int64(valLen)
	if totalLen64 < 0 || totalLen64 > int64(int(totalLen64)) {
		return nil, ErrRecordTooLarge, true
	}
	dataEnd := realStart + HeaderSize + totalLen64
	if dataEnd > int64(len(data)) {
		f.maybeScheduleRemap()
		return nil, nil, false
	}

	record := data[realStart+HeaderSize : dataEnd]
	if verifyCRC {
		crc := binary.LittleEndian.Uint32(header[0:4])
		sum := crc32.Update(0, crc32cTable, header[4:])
		sum = crc32.Update(sum, crc32cTable, record)
		if sum != crc {
			return nil, ErrCorrupt, true
		}
	}
	return record[int64(keyLen):], nil, true
}

func (f *File) maybeScheduleRemap() {
	if f == nil || f.closed.Load() {
		return
	}
	if !f.remapRequested.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer f.remapRequested.Store(false)
		f.remapToFileSize()
	}()
}

func (f *File) remapToFileSize() {
	if f == nil || f.closed.Load() {
		return
	}

	f.remapMu.Lock()
	defer f.remapMu.Unlock()
	if f.closed.Load() {
		return
	}

	info, err := f.File.Stat()
	if err != nil {
		return
	}
	currentSize := info.Size()
	if currentSize <= 0 || currentSize > int64(int(currentSize)) {
		return
	}

	data, _ := f.mmapData.Load().([]byte)
	if data != nil && int64(len(data)) >= currentSize {
		return
	}
	if data != nil && MaxDeadMappings > 0 && len(f.deadMappings) >= MaxDeadMappings {
		return
	}
	if data != nil {
		f.deadMappings = append(f.deadMappings, data)
	}

	b, err := mmapReadOnly(f.File, int(currentSize))
	if err != nil {
		return
	}
	f.mmapData.Store(b)
}
