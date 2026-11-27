package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const nativeBSONSetCoalescerMaxBatch = 64

type nativeBSONSetCoalescerItem struct {
	col      *Collection
	id       []byte
	fields   []BSONSetField
	matched  bool
	modified bool
	err      error
	done     chan struct{}
}

var nativeBSONSetCoalescerItemPool = sync.Pool{
	New: func() any {
		return &nativeBSONSetCoalescerItem{
			done: make(chan struct{}, 1),
		}
	},
}

type nativeBSONSetRetry struct {
	id                 []byte
	incarnation, start uint64
	rowConflict        bool
}

func (c *Collection) nativeUpdateBSONSet(id []byte, fields []BSONSetField) (bool, bool, error) {
	if c == nil || c.nativeJSON == nil || c.nativeJSON.db == nil || durabilitycut.Enabled() || c.nativeJSON.db.NativeCommandWALGroupCommitActive() {
		matched, modified, _, err := c.nativeUpdateBSONSetWithJournal(id, fields, false)
		return matched, modified, err
	}
	coord := collectionSchemaCoordinatorForDBCollection(c.nativeJSON.db, c.name)
	if coord == nil {
		matched, modified, _, err := c.nativeUpdateBSONSetWithJournal(id, fields, false)
		return matched, modified, err
	}
	coord.nativeBSONSetMu.Lock()
	if !coord.nativeBSONSetActive {
		coord.nativeBSONSetActive = true
		coord.nativeBSONSetMu.Unlock()
		matched, modified, _, err := c.nativeUpdateBSONSetWithJournal(id, fields, false)
		coord.drainNativeBSONSetPending()
		return matched, modified, err
	}
	item := nativeBSONSetCoalescerItemPool.Get().(*nativeBSONSetCoalescerItem)
	item.col = c
	item.id = id
	item.fields = fields
	item.matched = false
	item.modified = false
	item.err = nil
	coord.nativeBSONSetPending = append(coord.nativeBSONSetPending, item)
	coord.nativeBSONSetMu.Unlock()

	<-item.done
	matched, modified, err := item.matched, item.modified, item.err
	item.col = nil
	item.id = nil
	item.fields = nil
	item.err = nil
	nativeBSONSetCoalescerItemPool.Put(item)
	return matched, modified, err
}

func (coord *collectionSchemaCoordinator) drainNativeBSONSetPending() {
	var batchBuf [nativeBSONSetCoalescerMaxBatch]*nativeBSONSetCoalescerItem
	var itemsBuf [nativeBSONSetCoalescerMaxBatch]BSONSetUpdateBatchItem
	var resultsBuf [nativeBSONSetCoalescerMaxBatch]UpdateBatchResult
	for {
		coord.nativeBSONSetMu.Lock()
		if len(coord.nativeBSONSetPending) == 0 {
			coord.nativeBSONSetActive = false
			coord.nativeBSONSetMu.Unlock()
			return
		}
		// Take up to nativeBSONSetCoalescerMaxBatch compatible distinct-key
		// items for the first pending collection; keep remaining items queued
		// for the next wave so duplicate keys execute in FIFO order.
		firstCol := coord.nativeBSONSetPending[0].col
		n := 0
		keep := 0
		pending := coord.nativeBSONSetPending
		for _, item := range pending {
			canBatch := n < nativeBSONSetCoalescerMaxBatch && item.col == firstCol
			if canBatch && n > 0 {
				for j := 0; j < n; j++ {
					if bytes.Equal(batchBuf[j].id, item.id) {
						canBatch = false
						break
					}
				}
			}
			if canBatch {
				batchBuf[n] = item
				n++
			} else {
				pending[keep] = item
				keep++
			}
		}
		clear(pending[keep:])
		coord.nativeBSONSetPending = pending[:keep]
		coord.nativeBSONSetMu.Unlock()

		batch := batchBuf[:n]
		if n == 1 {
			it := batch[0]
			it.matched, it.modified, _, it.err = it.col.nativeUpdateBSONSetWithJournal(it.id, it.fields, false)
			it.done <- struct{}{}
			batch[0] = nil
			continue
		}
		bsonItems := itemsBuf[:n]
		for i, it := range batch {
			bsonItems[i] = BSONSetUpdateBatchItem{DocumentID: it.id, Fields: it.fields}
		}
		results, batched, err := firstCol.nativeUpdateBatchModeWithJournalDst(nil, updateBatchModeNoSecondaryUniqueIndexChanges, bsonItems, nil, nil, resultsBuf[:n])
		if err == nil && batched && len(results) == n {
			for i, it := range batch {
				it.matched = results[i].Matched
				it.modified = results[i].Modified
				it.done <- struct{}{}
			}
		} else if errors.Is(err, ErrCommitAmbiguous) {
			for _, it := range batch {
				it.err = err
				it.done <- struct{}{}
			}
		} else {
			// Atomic batch did not commit; isolate errors and retries per caller.
			for _, it := range batch {
				it.matched, it.modified, _, it.err = it.col.nativeUpdateBSONSetWithJournal(it.id, it.fields, false)
				it.done <- struct{}{}
			}
		}
		clear(bsonItems)
		clear(batch)
	}
}

func (c *Collection) nativeUpdateBSONSetWithJournal(id []byte, fields []BSONSetField, requestJournal bool) (bool, bool, backenddb.NativeJournalCompletion, error) {
	matched, modified, completion, _, err := c.nativeUpdateBSONSetWithJournalObserved(id, fields, requestJournal)
	return matched, modified, completion, err
}

func (c *Collection) nativeUpdateBSONSetWithJournalObserved(id []byte, fields []BSONSetField, requestJournal bool) (bool, bool, backenddb.NativeJournalCompletion, bool, error) {
	items := [...]BSONSetUpdateBatchItem{{DocumentID: id, Fields: fields}}
	var resBuf [1]UpdateBatchResult
	retry := nativeBSONSetRetry{id: id}
	var lastErr error
	contended := false
	for attempt := range maxCollectionMutationRetries {
		var operation nativeOperationJournal
		var journal *nativeOperationJournal
		mode := updateBatchModeAny
		if requestJournal {
			operation.requested = true
			journal = &operation
			mode = updateBatchModeNoSecondaryUniqueIndexChanges
		}
		results, batched, err := c.nativeUpdateBatchModeWithJournalDst(nil, mode, items[:], &retry, journal, resBuf[:])
		if requestJournal && !batched && err == nil {
			// The conditional path declined before publication. Give the
			// original unrestricted singleton its own current catalog basis.
			requestJournal = false
			retry = nativeBSONSetRetry{id: id}
			results, _, err = c.nativeUpdateBatchModeWithJournalDst(nil, updateBatchModeAny, items[:], &retry, nil, resBuf[:])
		}
		completion := operation.completion
		if retry.rowConflict && completion.AssignedLSN() == 0 && !completion.Attempted() {
			contended = true
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		if err != nil && !completion.Installed() {
			var item *UpdateBatchItemError
			if errors.As(err, &item) && item.Index == 0 {
				err = item.Err
			}
			return false, false, completion, contended, err
		}
		if len(results) != 1 {
			countErr := fmt.Errorf("collections: BSON $set result count %d for single update", len(results))
			return false, false, completion.WithoutSuccess(), contended, errors.Join(err, countErr)
		}
		return results[0].Matched, results[0].Modified, completion, contended, err
	}
	return false, false, backenddb.NativeJournalCompletion{}, contended, collectionMutationRetryExhausted(lastErr)
}

func nativeBSONSetMeta(meta *CollectionMeta) bool {
	return meta.Options.ColumnStore == nil && normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatBSON
}

// Only synchronous staging borrows the caller's fields. The maintained BSON
// transformer runs before Commit and no callback/spec enters an immutable cut
// or canonical frame. Its output backing is admitted before allocation.
func (t *NativeJSONTxn) prepareNativeBSONSetItems(name string, input []BSONSetUpdateBatchItem) ([]UpdateBatchItem, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	g := t.binding(name)
	if t.core == nil {
		return nil, backenddb.ErrClosed
	}
	if g == nil || !nativeBSONSetMeta(&g.probe.meta) {
		return nil, errBSONSetRequiresBSONFormat
	}
	if err := t.admitRowControls(g, len(input)); err != nil {
		return nil, err
	}
	// newBSONSetUpdate's large-field map uses the same conservative map-entry
	// backing reserve as maintained semantic construction. Include both used-bit
	// arrays (same-shape probe and fallback), specs and callback environments.
	controls, err := nativeTypedCheckedMul(uint64(len(input)), 2*preparedSemanticStreamMapEntryReserveBytes)
	if err != nil {
		return nil, err
	}
	for _, item := range input {
		n, err := nativeTypedCheckedMul(uint64(len(item.Fields)), uint64(unsafe.Sizeof(BSONSetField{}))+preparedSemanticStreamMapEntryReserveBytes+2)
		if err != nil {
			return nil, err
		}
		if err = nativeTypedCheckedAdd(&controls, n); err != nil {
			return nil, err
		}
	}
	if err := t.admitStage(g, memtable.COWAllocationCharge(controls)); err != nil {
		return nil, err
	}
	specs, err := prepareBSONSetUpdateBatchItems(input)
	if err != nil {
		return nil, err
	}
	items := make([]UpdateBatchItem, len(input))
	for i := range specs {
		spec := specs[i].bsonSet
		items[i] = UpdateBatchItem{DocumentID: specs[i].DocumentID, Update: func(current []byte) ([]byte, bool, error) {
			// An upper bound for the unchanged original image plus every assigned
			// element permits one exact destination allocation: appendReplacement can
			// use the same-shape or growing-image algorithm without growing backing.
			n := uint64(len(current)) + uint64(bsonSetReplacementSlackBytes)
			for _, field := range spec.fields {
				for _, size := range [...]uint64{2, uint64(len(field.Key)), uint64(len(field.Value.Value))} {
					if err := nativeTypedCheckedAdd(&n, size); err != nil {
						return nil, false, err
					}
				}
			}
			if n > uint64(int(^uint(0)>>1)) {
				return nil, false, memtable.ErrCOWCapacity
			}
			if err := t.admitStage(g, memtable.COWAllocationCharge(n)); err != nil {
				return nil, false, err
			}
			_, replacement, changed, err := callBSONSetUpdateAppendReplacement(spec, make([]byte, 0, int(n)), current)
			return replacement, changed, err
		}}
	}
	return items, nil
}

func (t *NativeJSONTxn) stageNativeCommandWALBSONSet(name string, items []BSONSetUpdateBatchItem) (result NativeCommandWALApplyResult, docs []commitlog.CollectionDocument, err error) {
	updates, err := t.prepareNativeBSONSetItems(name, items)
	if err != nil {
		return result, nil, err
	}
	results, err := t.UpdateBatchInto(name, updates)
	if err != nil {
		return result, nil, err
	}
	g := t.binding(name)
	// prepareNativeBSONSetItems admitted the same per-row control inventory.
	docs = make([]commitlog.CollectionDocument, 0, len(items))
	for i, item := range results {
		if item.Matched {
			result.Matched++
		}
		if item.Modified {
			result.Affected++
			row := g.rows.find(items[i].DocumentID)
			docs = append(docs, commitlog.CollectionDocument{ID: row.id, Document: row.current})
		}
	}
	return result, docs, nil
}

// These deterministic producer seams share the original advisory staging and
// unassigned native commit. They never accept a preassigned legacy WAL intent.
func (m *CollectionManager) PreflightNativeCommandWALBSONSet(name string, items []BSONSetUpdateBatchItem) (resultErr error) {
	tx, err := m.beginNativeCommandWALMutation(name, DocumentFormatBSON, commitlog.CommandKindCollectionUpdateBatchByID, nil, nil)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	if _, _, err = tx.stageNativeCommandWALBSONSet(name, items); err != nil {
		return err
	}
	return tx.validateNativeCommandWALMutation()
}

func (m *CollectionManager) ApplyNativeCommandWALBSONSet(name string, items []BSONSetUpdateBatchItem, sync bool, observe func(uint64) error) (result NativeCommandWALApplyResult, resultErr error) {
	tx, err := m.beginNativeCommandWALMutation(name, DocumentFormatBSON, commitlog.CommandKindCollectionUpdateBatchByID, nil, nil)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	var docs []commitlog.CollectionDocument
	result, docs, err = tx.stageNativeCommandWALBSONSet(name, items)
	if err != nil {
		return result, err
	}
	return tx.commitNativeCommandWALMutation(name, commitlog.CommandKindCollectionUpdateBatchByID, nil, docs, result, sync, observe)
}
