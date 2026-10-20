package collections

import (
	"bytes"
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// Callback updates consume complete JSON documents, then enter the same declared
// parser, retained transform and immutable delta installer as ordinary insert.
// Only final owned replacement bytes enter a canonical member.
func (t *NativeJSONTxn) stageNativeTypedUpdate(g *nativeJSONTxnBinding, items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	if !nativeTypedMeta(g.probe.meta) || columnRetainedPayloadEffectiveEncoding(g.probe.meta.Options.ColumnStore) != ColumnRetainedPayloadEncodingJSON {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if err := nativeTypedProfile(t.db, g.probe.meta); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	if err := t.admitRowControls(g, len(items)); err != nil {
		return nil, err
	}
	for _, item := range items {
		if err := t.admitRowDocument(g, item.DocumentID, nil); err != nil {
			return nil, err
		}
	}
	owned, err := prepareUpdateBatchItems(items)
	if err != nil {
		return nil, err
	}
	results := make([]UpdateBatchResult, len(items))
	ids := make([][]byte, 0, len(items))
	documents := make([][]byte, 0, len(items))
	for i, item := range owned {
		// Missing rows also participate in the original point conflict check.
		if err = t.trackRead(g, item.DocumentID); err != nil {
			return nil, err
		}
		current, err := t.nativeTypedUpdateDocument(g, item.DocumentID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		results[i].Matched = true
		document, modified, err := callCollectionUpdateCallback(item.Update, current)
		current = nil
		if err != nil {
			return nil, updateBatchItemError(i, err)
		}
		if !modified {
			continue
		}
		if len(document) == 0 {
			return nil, updateBatchItemError(i, errors.New("changed replacement document cannot be empty"))
		}
		if err = t.admitRowDocument(g, item.DocumentID, document); err != nil {
			return nil, err
		}
		// A later callback may reuse or overwrite the same output buffer.
		ownedDocument := bytes.Clone(document)
		if err = t.validateNativeTypedUpdateReplacement(g, item.DocumentID, ownedDocument); err != nil {
			return nil, updateBatchItemError(i, err)
		}
		ids = append(ids, item.DocumentID)
		documents = append(documents, ownedDocument)
		results[i].Modified = true
	}
	if len(ids) == 0 {
		return results, nil
	}
	if err = t.stageNativeTypedDocuments(g, ids, documents, commitlog.CommandKindCollectionUpdateBatchByID); err != nil {
		return nil, err
	}
	return results, nil
}

// Validate before another callback can run. Parsed backing is operation-local;
// only the admitted complete replacement survives into the common batch stage.
// Index uniqueness still belongs to the original final-batch plan, allowing
// two replacements to exchange unique values atomically.
func (t *NativeJSONTxn) validateNativeTypedUpdateReplacement(g *nativeJSONTxnBinding, id, document []byte) error {
	retained, scratch, err := nativeTypedStageCapacity(g.probe.meta, [][]byte{id}, [][]byte{document})
	if err != nil {
		return err
	}
	lease, err := t.core.NativeAllocator().AdmitNativeBytes(retained)
	if err != nil {
		return err
	}
	work, err := t.core.NativeAllocator().AdmitNativeBytes(scratch)
	if err != nil {
		lease.Close()
		return err
	}
	var rows []columnDeclaredRow
	var residual [][]byte
	defer func() {
		rows, residual = nil, nil
		work.Close()
		lease.Close()
	}()
	rows, residual, err = nativeTypedDocumentRows(g.probe.meta, []columnWriteDocument{{ID: id, Document: document}})
	if err != nil {
		return err
	}
	capacity, err := nativeTypedDeltaBuildCapacity(g.probe.meta, rows, residual)
	if err != nil {
		return err
	}
	validation, err := nativeTypedCollectionBuildCapacity(g.probe.meta, capacity)
	if err != nil {
		return err
	}
	if err = work.Resize(max(scratch, validation)); err != nil {
		return err
	}
	if err = nativeTypedValidateResidual(g.probe.meta, rows, residual); err != nil {
		return err
	}
	return nativeTypedValidateIndexRows(g.probe.meta, rows, func(n uint64) error { return t.admitStage(g, n) })
}

// Read only the private chain followed by the captured opening cut. The original
// reconstructor returns an independent caller image; admit its output in the
// transaction stage BEFORE reconstruction so that receipt remains live through
// the callback, after the reconstructor's operation-local scratch is released.
// The transaction mutex excludes concurrent use of this private probe.
func (t *NativeJSONTxn) nativeTypedUpdateDocument(g *nativeJSONTxnBinding, id []byte) ([]byte, error) {
	if row, residual, ok := g.typed.find(id); ok {
		return t.nativeTypedUpdatePendingDocument(g, row, residual)
	}
	r := &g.probe
	if r.domain.domain == nil {
		return nil, tree.ErrKeyNotFound
	}
	if row, residual, ok := r.domain.domain.typed.find(id); ok {
		return t.nativeTypedUpdatePendingDocument(g, row, residual)
	}
	// Match touchNativeRow's admitted internal borrow. The shared physical
	// lookupEntry(copyValue=false) path returns metadata only, while true would
	// allocate a public caller copy before this stage can admit its backing.
	entry, resolved, err := r.domain.memoryEntry(id)
	if !resolved {
		scratch, scratchErr := r.physicalScratch()
		if scratchErr != nil {
			return nil, scratchErr
		}
		entry, err = r.domain.baseEntry(id, scratch, r.readLeaf)
	}
	if err != nil {
		return nil, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	value := entry.Value
	if entry.Flags&node.FlagPointer != 0 {
		value, err = r.readValue(entry.ValuePtr, func(n uint64) error { return t.admitStage(g, n) })
		if err != nil {
			return nil, err
		}
	}
	if r.meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadFull {
		if err = t.admitStage(g, memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
			return nil, err
		}
		// Full's maintained reader returns complete retained bytes directly;
		// it must not inspect or reconstruct declared assets.
		return bytes.Clone(value), nil
	}
	if r.collection == nil || r.database == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	manifest, ok := r.collection.domains.Get("column/manifest")
	if !ok {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	_, typedJSON, err := nativeTypedFamilyReadCapacity(r.meta, r.capture.Snapshot(), manifest.baseRoot, r.database.ColumnAssetRootDir(), nil, func(n uint64) error { return t.admitStage(g, n) })
	if err != nil {
		return nil, err
	}
	charge, err := nativeTypedReconstructionCharge(r.meta, uint64(len(value)), typedJSON)
	if err != nil {
		return nil, err
	}
	if err = t.admitStage(g, charge); err != nil {
		return nil, err
	}
	return r.reconstructTyped(id, value)
}

func (t *NativeJSONTxn) nativeTypedUpdatePendingDocument(g *nativeJSONTxnBinding, row *columnDeclaredRow, residual []byte) ([]byte, error) {
	if row == nil {
		return nil, tree.ErrKeyNotFound
	}
	meta := g.probe.meta
	var charge uint64
	if meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadFull {
		charge = memtable.COWAllocationCharge(uint64(len(residual)))
	} else {
		if len(row.Values) != len(meta.Options.ColumnStore.Columns) {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		var typedJSON uint64
		for i, col := range meta.Options.ColumnStore.Columns {
			_, n, err := nativeTypedValueCapacity(col, row.Values[i])
			if err != nil {
				return nil, err
			}
			if err = nativeTypedCheckedAdd(&typedJSON, n); err != nil {
				return nil, err
			}
		}
		var err error
		charge, err = nativeTypedReconstructionCharge(meta, uint64(len(residual)), typedJSON)
		if err != nil {
			return nil, err
		}
	}
	if err := t.admitStage(g, charge); err != nil {
		return nil, err
	}
	if meta.Options.ColumnStore.RetainedPayload == ColumnRetainedPayloadFull {
		return bytes.Clone(residual), nil
	}
	return g.probe.reconstructPendingTyped(row, residual)
}
