package collections

import (
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func (c *Collection) searchNativeText(opts TextSearchOptions, mode textSearchResultMode, read *NativeJSONRead, response TextSearchResponse) (result TextSearchResponse, resultErr error) {
	if read == nil {
		var err error
		read, err = c.nativeJSON.Capture(c.name)
		if err != nil {
			return response, err
		}
		defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return response, err
	}
	defer lease.Close()
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	// Query tokens, terms and candidate/result headers are source/request bounded.
	// Posting/position block decoding is admitted by each captured domain read.
	unit := uint64(unsafe.Sizeof(TextToken{})) + uint64(unsafe.Sizeof(textV2SearchCandidate{})) + uint64(unsafe.Sizeof(TextSearchResult{}))
	size := uint64(len(opts.Query)) + uint64(opts.TopK) + 1
	if opts.Phrase != nil {
		size += uint64(len(opts.Phrase.Query))
	}
	if unit > ^uint64(0)/size/2 {
		return response, memtable.ErrCOWCapacity
	}
	if err = admit(memtable.COWAllocationCharge(size * unit * 2)); err != nil {
		return response, fmt.Errorf("collections: native text query workspace: %w", err)
	}
	for _, def := range read.meta.TextIndexes {
		if def.Name != opts.IndexName {
			continue
		}
		charge, err := nativeTextDefinitionCharge(def, 2)
		if err != nil {
			return response, err
		}
		if err = admit(charge); err != nil {
			return response, err
		}
	}
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		return response, err
	}
	return c.searchTextAtSnapshot(opts, mode, read.capture.Snapshot(), catalog, response)
}

func nativeTextViewRead(view *CollectionReadView) (*NativeJSONRead, error) {
	if view == nil || view.closed || view.nativeRead == nil {
		return nil, backenddb.ErrClosed
	}
	return view.nativeRead, nil
}

// Storage inspection borrows the same complete cut for all V1/V2 roots and
// retains decoded-record/workspace admission until the maintained validator ends.
func (c *Collection) nativeTextStorageStats(indexName string) (stats TextIndexStorageStats, resultErr error) {
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return stats, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	def, ok := findTextIndex(read.meta.TextIndexes, indexName)
	if !ok {
		return stats, ErrIndexNotFound
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		return stats, err
	}
	defer lease.Close()
	var charged uint64
	admit := func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}
	catalog, err := newNativeTextCatalog(read, admit)
	if err != nil {
		return stats, err
	}
	return inspectTextIndexStorage(read.capture.Snapshot(), catalog, def)
}
