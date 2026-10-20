package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// The existing API's native role owns no legacy coordinator, write domain or
// raw barrier. Both public frontends use the same NativeJSONTxn preparation.
func (m *CollectionManager) openNativeCollection(name string) (collection *Collection, resultErr error) {
	read, err := m.nativeJSON.Capture(name)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, errors.Join(ErrCollectionNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	meta := read.meta.copy()
	return &Collection{manager: m, nativeJSON: m.nativeJSON, name: name, meta: *meta}, nil
}

func (c *Collection) nativeInsertBatch(ids, documents [][]byte) ([][]byte, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if len(ids) != len(documents) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Returned IDs are caller-owned, like the legacy planner result, and cannot
	// borrow transaction scratch whose original receipt ends at Commit/Close.
	result, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, err
	}
	if err = c.nativeJSON.InsertBatch(c.name, ids, documents); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Collection) nativeGetInto(id, dst []byte) ([]byte, bool, error) {
	if c.manager.isClosing() {
		return dst[:0], false, backenddb.ErrClosed
	}
	if len(id) == 0 {
		return dst[:0], false, errors.New("collections: document id cannot be empty")
	}
	value, err := c.nativeJSON.Get(c.name, id)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	if dst == nil {
		return value, true, nil
	}
	return append(dst[:0], value...), true, nil
}

func (c *Collection) openNativeReadView() (*CollectionReadView, error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return nil, err
	}
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(CollectionReadView{}))))
	if err != nil {
		return nil, errors.Join(err, read.Close())
	}
	return &CollectionReadView{collection: c, nativeRead: read, nativeLease: lease}, nil
}

func (v *CollectionReadView) fetchNativeDocuments(ids [][]byte, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	if v.closed || v.nativeRead == nil {
		return DocumentFetchResponse{}, backenddb.ErrClosed
	}
	// Reuse the public stored-format contract before returning owned raw bytes.
	// Projection remains outside the native document capability.
	if _, err := normalizeDocumentFetchProjection(v.nativeRead.meta, DocumentFetchOptions{Format: opts.Format}); err != nil {
		return DocumentFetchResponse{}, err
	}
	if len(opts.IncludePaths)+len(opts.ExcludePaths) != 0 {
		if normalizedDocumentFormat(v.nativeRead.meta.Options.DocumentFormat) == DocumentFormatBSON {
			_, err := normalizeDocumentFetchProjection(v.nativeRead.meta, opts)
			return DocumentFetchResponse{}, err
		}
		return DocumentFetchResponse{}, backenddb.ErrNativeSemanticUnsupported
	}
	out := DocumentFetchResponse{Results: make([]DocumentFetchResult, len(ids))}
	out.Stats.DocumentsRequested = uint64(len(ids))
	for i, id := range ids {
		if opts.Context != nil {
			if err := opts.Context.Err(); err != nil {
				return DocumentFetchResponse{}, err
			}
		}
		value, err := v.nativeRead.Get(id)
		out.Results[i].ID = append([]byte(nil), id...)
		if errors.Is(err, tree.ErrKeyNotFound) {
			out.Stats.DocumentsMissing++
			continue
		}
		if err != nil {
			return DocumentFetchResponse{}, err
		}
		out.Results[i].Found = true
		out.Results[i].Document = value
		out.Stats.DocumentsFetched++
		out.Stats.DocumentBytes += uint64(len(value))
		out.Stats.OutputBytes += uint64(len(value))
	}
	return out, nil
}
