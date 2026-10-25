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

func (c *Collection) nativeScanDocumentIDsPhysical(maxEntries int, fn func([]byte) (bool, error)) (inspected int, truncated bool, resultErr error) {
	if maxEntries < 0 {
		return 0, false, errors.New("collections: max physical entries must not be negative")
	}
	if fn == nil {
		return 0, false, errors.New("collections: scan callback is nil")
	}
	if c.manager.isClosing() {
		return 0, false, backenddb.ErrClosed
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		return 0, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, read.Close()) }()
	return read.scanDocumentIDsPhysical(maxEntries, fn)
}

func (r *NativeJSONRead) scanDocumentIDsPhysical(maxEntries int, fn func([]byte) (bool, error)) (inspected int, truncated bool, resultErr error) {
	r.mu.Lock()
	if r.capture == nil || r.failure != nil || r.closing || r.visits == ^uint32(0) {
		r.mu.Unlock()
		return 0, false, backenddb.ErrClosed
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
		return 0, false, nil // exact captured metadata proves no primary sources
	}
	if maxEntries == 0 {
		return 0, true, nil
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return 0, false, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(page.PageSize + 1))
	if err != nil {
		return 0, false, err
	}
	cursor := make([]byte, page.PageSize+1)
	var start []byte
	defer func() {
		clear(cursor)
		cursor, start = nil, nil
		lease.Close()
	}()
	inspect := func() error {
		if inspected == maxEntries {
			return errCollectionIndexScanWorkCap
		}
		inspected++
		return nil
	}
	for {
		if r.closing || r.failure != nil {
			return inspected, false, backenddb.ErrClosed
		}
		// Charge before every source probe, including tombstones, shadowed
		// candidates and exhausted sources. A partially probed merge never
		// reaches fn, and no further source work runs after the cap.
		entry, found, err := r.domain.seek(start, nil, scratch, r.readLeaf, false, inspect)
		if errors.Is(err, errCollectionIndexScanWorkCap) {
			return inspected, true, nil
		}
		if err != nil || !found {
			return inspected, false, err
		}
		if len(entry.Key) >= len(cursor) {
			return inspected, false, memtable.ErrCOWCapacity
		}
		n := copy(cursor, entry.Key)
		cursor[n] = 0
		start = cursor[:n+1]
		if entry.Flags&node.FlagTombstone != 0 {
			continue
		}
		id := bytes.Clone(entry.Key)
		entry = node.LeafEntry{}
		next, err := func() (bool, error) {
			r.mu.Unlock()
			defer r.mu.Lock()
			return fn(id)
		}()
		id = nil
		if err != nil || !next {
			return inspected, false, err
		}
	}
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
