package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	slabs tree.SlabReader
	vlogs tree.SlabReader
}

// ValueReaderForState returns a reader that can resolve slab and value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}
}

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if page.IsValueLogFileID(ptr.FileID) {
		if r.vlogs == nil {
			return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
		}
		return r.vlogs.Read(ptr)
	}
	if r.slabs == nil {
		return nil, fmt.Errorf("slab reader unavailable for file %d", ptr.FileID)
	}
	return r.slabs.Read(ptr)
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if page.IsValueLogFileID(ptr.FileID) {
		if r.vlogs == nil {
			return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
		}
		return r.vlogs.ReadUnsafe(ptr)
	}
	if r.slabs == nil {
		return nil, fmt.Errorf("slab reader unavailable for file %d", ptr.FileID)
	}
	return r.slabs.ReadUnsafe(ptr)
}
