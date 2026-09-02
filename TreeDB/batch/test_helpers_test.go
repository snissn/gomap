package batch

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

type mapValueReader struct {
	values     map[page.ValuePtr][]byte
	nextOffset uint64
	fileID     uint32
}

func newMapValueReader() *mapValueReader {
	return &mapValueReader{
		values: make(map[page.ValuePtr][]byte),
		fileID: page.ValueLogFileID(1),
	}
}

func (m *mapValueReader) Add(value []byte) page.ValuePtr {
	ptr := page.ValuePtr{
		FileID: m.fileID,
		Offset: m.nextOffset,
		Length: uint32(len(value)),
	}
	m.values[ptr] = append([]byte(nil), value...)
	m.nextOffset += uint64(len(value))
	return ptr
}

func (m *mapValueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	val, ok := m.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found")
	}
	return append([]byte(nil), val...), nil
}

func (m *mapValueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	val, ok := m.values[ptr]
	if !ok {
		return nil, fmt.Errorf("value pointer not found")
	}
	return val, nil
}
