package collections

import (
	"bytes"
	"context"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Native scans retain one complete cut for traversal and document resolution.
// The original native owner has no mutable buffered writes to flush.
func (c *Collection) nativeScanDocuments(afterID []byte, maxDocuments int, documents bool, fn func(DocumentRecord) (bool, error)) (truncated bool, resultErr error) {
	if maxDocuments <= 0 {
		return false, errors.New("collections: max documents must be positive")
	}
	if fn == nil {
		return false, errors.New("collections: scan callback is nil")
	}
	if c.manager.isClosing() {
		return false, backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	return read.scanDocuments(afterID, maxDocuments, documents, fn)
}

func (r *NativeJSONRead) scanDocuments(afterID []byte, maxDocuments int, documents bool, fn func(DocumentRecord) (bool, error)) (truncated bool, resultErr error) {
	r.mu.Lock()
	if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
		r.mu.Unlock()
		return false, backenddb.ErrClosed
	}
	r.visits++
	defer func() {
		r.visits--
		closeRequested := r.closing && r.visits == 0
		r.mu.Unlock()
		if closeRequested {
			resultErr = errors.Join(resultErr, r.Close())
		}
	}()
	if r.domain.domain == nil {
		return false, nil
	}
	if len(afterID) > page.PageSize {
		return false, memtable.ErrCOWCapacity
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(page.PageSize + 1))
	if err != nil {
		return false, err
	}
	operation := func() (bool, error) {
		cursor := make([]byte, page.PageSize+1)
		var start []byte
		if len(afterID) != 0 {
			n := copy(cursor, afterID)
			cursor[n] = 0
			start = cursor[:n+1]
		}
		scanned := 0
		for {
			if r.closing || r.capture == nil || r.failure != nil {
				return false, backenddb.ErrClosed
			}
			entry, found, err := nativeJSONSeekLive(&r.domain, r, start, nil, cursor)
			if err != nil || !found {
				return false, err
			}
			if scanned == maxDocuments {
				return true, nil
			}
			if len(entry.Key) >= len(cursor) {
				return false, memtable.ErrCOWCapacity
			}
			id := bytes.Clone(entry.Key)
			n := copy(cursor, entry.Key)
			cursor[n] = 0
			start = cursor[:n+1]
			entry = node.LeafEntry{}
			scanned++
			// IDs and documents belong to the caller. No page/entry aliases or
			// read mutex cross arbitrary callbacks; the visit retains this cut.
			next, err := func() (bool, error) {
				r.mu.Unlock()
				defer r.mu.Lock()
				record := DocumentRecord{ID: id}
				if documents {
					var err error
					record.Document, err = r.Get(id)
					if err != nil {
						return false, err
					}
				}
				return fn(record)
			}()
			id = nil
			if err != nil || !next {
				return false, err
			}
		}
	}
	truncated, resultErr = operation()
	operation = nil // detach cursor/start aliases before the original refund
	lease.Close()
	return truncated, resultErr
}

// These receipt families require column publication, whose metadata survives
// index removal. Only an exact captured non-column schema proves absence; native
// typed roles remain unsupported here rather than silently losing receipts.
func (c *Collection) nativeNoColumnLogicalState(ctx context.Context) (resultErr error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if c.manager.isClosing() {
		return backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	if columnStoreWriteEnabled(read.meta) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return nil
}
