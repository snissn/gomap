package caching

import (
	"context"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const leafRefCancellationCheckInterval = 256 // power of two: checked via bitmask in shouldCheckLeafRefCancellation

func shouldCheckLeafRefCancellation(i uint16) bool {
	return i > 0 && i&(leafRefCancellationCheckInterval-1) == 0
}

type pageGetter interface {
	Get(pageID uint64) ([]byte, error)
}

type fallbackSlabReader struct {
	primary  tree.SlabReader
	fallback tree.SlabReader
}

func newCachedLiveScanReader(primary, fallback tree.SlabReader) tree.SlabReader {
	if primary == nil {
		return fallback
	}
	if fallback == nil || primary == fallback {
		return primary
	}
	return fallbackSlabReader{primary: primary, fallback: fallback}
}

func (r fallbackSlabReader) Read(ptr page.ValuePtr) ([]byte, error) {
	if r.primary != nil {
		val, err := r.primary.Read(ptr)
		if err == nil {
			return val, nil
		}
		if !isValueLogFileNotFound(err) {
			return nil, err
		}
	}
	if r.fallback == nil {
		return nil, fmt.Errorf("missing value-log reader for file %d", ptr.FileID)
	}
	return r.fallback.Read(ptr)
}

func (r fallbackSlabReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if r.primary != nil {
		val, err := r.primary.ReadUnsafe(ptr)
		if err == nil {
			return val, nil
		}
		if !isValueLogFileNotFound(err) {
			return nil, err
		}
	}
	if r.fallback == nil {
		return nil, fmt.Errorf("missing value-log reader for file %d", ptr.FileID)
	}
	return r.fallback.ReadUnsafe(ptr)
}

func isValueLogFileNotFound(err error) bool {
	return errors.Is(err, valuelog.ErrFileNotFound)
}

func collectLeafRefValueLogLiveIDs(ctx context.Context, p pageGetter, rootID uint64, reader tree.SlabReader, live map[uint32]struct{}) error {
	return collectLeafRefNestedValueLogLiveIDsWithMode(ctx, p, rootID, reader, live, true)
}

func collectLeafRefNestedValueLogLiveIDs(ctx context.Context, p pageGetter, rootID uint64, reader tree.SlabReader, live map[uint32]struct{}) error {
	return collectLeafRefNestedValueLogLiveIDsWithMode(ctx, p, rootID, reader, live, false)
}

func collectLeafRefNestedValueLogLiveIDsWithMode(ctx context.Context, p pageGetter, rootID uint64, reader tree.SlabReader, live map[uint32]struct{}, includeLeafLogIDs bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return leafrefscan.Walk(ctx, rootID, p.Get, func(pageID uint64, n node.Node) error {
		if !n.VerifyChecksum() {
			return fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		return nil
	}, func(ptr page.LeafLogPtr) error {
		if includeLeafLogIDs {
			live[ptr.ValueLogFileID()] = struct{}{}
		}
		return collectNestedLeafPageValueLogLiveIDs(ctx, ptr.ValuePtr(), reader, live)
	})
}

func collectNestedLeafPageValueLogLiveIDs(ctx context.Context, ptr page.ValuePtr, reader tree.SlabReader, live map[uint32]struct{}) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader == nil || live == nil || !page.IsValueLogFileID(ptr.FileID) {
		return nil
	}
	leafPage, err := reader.ReadUnsafe(ptr)
	if err != nil {
		return err
	}
	if len(leafPage) != page.PageSize {
		return fmt.Errorf("invalid leaf page size in value log file=%d offset=%d got=%d want=%d", ptr.FileID, ptr.Offset, len(leafPage), page.PageSize)
	}
	leaf := node.NewNodeView(leafPage)
	if leaf.Type() != page.PageTypeLeaf {
		return fmt.Errorf("expected leaf page in value log file=%d offset=%d, got type=%d", ptr.FileID, ptr.Offset, leaf.Type())
	}
	if !leaf.VerifyChecksum() {
		return fmt.Errorf("checksum mismatch for value-log leaf page file=%d offset=%d", ptr.FileID, ptr.Offset)
	}
	// Leaf pages stored in the value log are treated as terminal containers for
	// payload pointers here. Their entries may point at value-log payloads, but we
	// do not recursively interpret those payload pointers as more leaf pages.
	for i := uint16(0); i < leaf.Count(); i++ {
		if shouldCheckLeafRefCancellation(i) {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		_, _, valPtr, flags, err := leaf.GetLeafEntryView(i)
		if err != nil {
			return err
		}
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(valPtr.FileID) {
			continue
		}
		live[valPtr.FileID] = struct{}{}
	}
	return nil
}

func valueReaderForBackendState(state *backenddb.DBState) tree.SlabReader {
	return backenddb.ValueReaderForState(state)
}
