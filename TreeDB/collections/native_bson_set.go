package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

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
	if g == nil || g.probe.meta.Options.ColumnStore != nil || normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) != DocumentFormatBSON {
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
