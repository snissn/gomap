package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type valueReader struct {
	vlogs tree.SlabReader
}

// ValueReaderForState returns a reader that resolves value-log pointers.
func ValueReaderForState(state *DBState) tree.SlabReader {
	if state == nil {
		return nil
	}
	return valueReader{vlogs: state.ValueLogSet}
}

func (r valueReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.Read(ptr)
}

func (r valueReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if !page.IsValueLogFileID(ptr.FileID) {
		return nil, fmt.Errorf("expected value log pointer, got file %d", ptr.FileID)
	}
	if r.vlogs == nil {
		return nil, fmt.Errorf("value log reader unavailable for file %d", ptr.FileID)
	}
	return r.vlogs.ReadUnsafe(ptr)
}
