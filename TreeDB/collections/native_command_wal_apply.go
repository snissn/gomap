package collections

import (
	"bytes"
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commandwalapply"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// NativeCommandWALApplyResult carries the original native publisher's assigned
// identity and physical receipt. It never represents a separately appended intent.
type NativeCommandWALApplyResult struct {
	Publication       backenddb.NativePublicationResult
	AlreadyApplied    bool
	Affected, Matched int
}

func (m *CollectionManager) UsesNativeCommandWALApply() bool { return m != nil && m.nativeJSON != nil }

// ApplyNativeCommandWALCreate reuses ordinary identity-free catalog preparation.
// observe is request-local, synchronous, nonblocking and must neither reenter the
// database nor acquire locks: it executes under the original publication gate.
func (m *CollectionManager) ApplyNativeCommandWALCreate(meta CollectionMeta, sync bool, observe func(uint64) error) (result NativeCommandWALApplyResult, err error) {
	if !m.UsesNativeCommandWALApply() || m.isClosing() {
		return result, backenddb.ErrClosed
	}
	if meta.Options.ColumnStore != nil || meta.nativeDocumentCoverageVersion != 0 {
		return result, backenddb.ErrNativeSemanticUnsupported
	}
	apply := nativeCommandWALApplyCommit{sync: sync, observe: observe}
	_, err = m.nativeJSON.createCollection(meta, &apply)
	result.Publication, result.AlreadyApplied = apply.result, apply.alreadyApplied
	if !result.AlreadyApplied {
		result.Affected = 1
	}
	return result, err
}

// ApplyNativeCommandWALMutation lowers only the existing ordinary document by-ID
// commands. The original conditional transaction owns all reads, scalar index
// effects and OCC dependencies; its publisher assigns the still-unassigned frame.
// templateRecords is an optional single encoded SectionTemplateRecords byte-vector,
// borrowed synchronously until the original stage admits and owns its lowering.
func (m *CollectionManager) ApplyNativeCommandWALMutation(name string, format DocumentFormat, kind commitlog.CommandKind, ids, documents [][]byte, sync bool, observe func(uint64) error, templateRecords ...[]byte) (result NativeCommandWALApplyResult, resultErr error) {
	tx, err := m.beginNativeCommandWALMutation(name, format, kind, ids, documents)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	var docs []commitlog.CollectionDocument
	result, docs, err = tx.stageNativeCommandWALMutation(name, kind, ids, documents, templateRecords...)
	if err != nil {
		return result, err
	}
	return tx.commitNativeCommandWALMutation(name, kind, ids, docs, result, sync, observe)
}

func (tx *NativeJSONTxn) commitNativeCommandWALMutation(name string, kind commitlog.CommandKind, ids [][]byte, docs []commitlog.CollectionDocument, result NativeCommandWALApplyResult, sync bool, observe func(uint64) error) (NativeCommandWALApplyResult, error) {
	var err error
	g := tx.binding(name)
	var payload []byte
	var payloadFormat commitlog.PayloadFormat
	var class commandwalapply.LoweredFrameClass
	if kind == commitlog.CommandKindCollectionDeleteBatchByID {
		// Retain ALL requested IDs in the canonical frame, including absent rows.
		if err = tx.admitMemberPayload(g, nil, ids); err != nil {
			return result, err
		}
		payload, err = commitlog.EncodeCollectionDeleteBatchByIDPayload(name, ids)
		payloadFormat, class = commitlog.PayloadFormatCollectionDeleteBatchByIDV1, commandwalapply.LoweredFrameClassCollectionDeleteBatchByID
	} else {
		if err = tx.admitMemberPayload(g, docs, nil); err != nil {
			return result, err
		}
		if kind == commitlog.CommandKindCollectionInsertBatchByID {
			payload, err = commitlog.EncodeCollectionInsertBatchByIDPayload(name, docs)
			payloadFormat, class = commitlog.PayloadFormatCollectionInsertBatchByIDV1, commandwalapply.LoweredFrameClassCollectionInsertBatchByID
		} else {
			payload, err = commitlog.EncodeCollectionUpdateBatchByIDPayload(name, docs)
			payloadFormat, class = commitlog.PayloadFormatCollectionUpdateBatchByIDV1, commandwalapply.LoweredFrameClassCollectionUpdateBatchByID
		}
	}
	if err != nil {
		return result, err
	}
	if len(tx.members) == 0 {
		// A deterministic no-effect command still consumes its original identity and
		// receives a fresh immutable engine/build header, not the old single-use build.
		if err = tx.admitStage(g, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(commitlog.CompositeMutationMember{})))); err != nil {
			return result, err
		}
		tx.members = make([]commitlog.CompositeMutationMember, 1)
	}
	tx.members[0] = commitlog.CompositeMutationMember{Kind: kind, PayloadFormat: payloadFormat, Payload: payload}
	apply := nativeCommandWALApplyCommit{frame: commandwalapply.LoweredFrame{Class: class, Kind: kind, Scope: commitlog.CommandScopeCollection, PayloadFormat: payloadFormat, Payload: payload}, sync: sync, observe: observe}
	err = tx.commitNativeApply(true, &apply)
	result.Publication = apply.result
	return result, err
}

// PreflightNativeCommandWALCreate uses the same normalized metadata eligibility
// as native create, without preparing a successor or claiming an identity.
func (m *CollectionManager) PreflightNativeCommandWALCreate(meta CollectionMeta) error {
	if !m.UsesNativeCommandWALApply() || m.isClosing() {
		return backenddb.ErrClosed
	}
	// Typed columns remain a different producer. Template requests use canonical
	// hash-keyed envelopes, never a caller-selected collection-local compact ID.
	if meta.Options.ColumnStore != nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	_, err := nativeJSONMeta(meta)
	return err
}

// PreflightNativeCommandWALMutation is advisory: it stages and validates the
// same captured rows and final unique owners as apply, then closes every partial
// plan. It neither reserves that cut nor appends, installs, seals, or calls the
// post-append observer. Apply must capture and validate again under its owner.
func (m *CollectionManager) PreflightNativeCommandWALMutation(name string, format DocumentFormat, kind commitlog.CommandKind, ids, documents [][]byte, templateRecords ...[]byte) (resultErr error) {
	tx, err := m.beginNativeCommandWALMutation(name, format, kind, ids, documents)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	if _, _, err = tx.stageNativeCommandWALMutation(name, kind, ids, documents, templateRecords...); err != nil {
		return err
	}
	return tx.validateNativeCommandWALMutation()
}

func (tx *NativeJSONTxn) validateNativeCommandWALMutation() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.core == nil {
		return backenddb.ErrClosed
	}
	if tx.failure != nil {
		return tx.failure
	}
	if len(tx.members) == 0 {
		// Empty/unchanged rows have no new unique owners to validate. Only apply
		// creates their canonical no-effect member and immutable successor.
		return nil
	}
	return tx.finalizeNativeRows(false)
}

func (m *CollectionManager) beginNativeCommandWALMutation(name string, format DocumentFormat, kind commitlog.CommandKind, ids, documents [][]byte) (*NativeJSONTxn, error) {
	if !m.UsesNativeCommandWALApply() || m.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if kind != commitlog.CommandKindCollectionInsertBatchByID && kind != commitlog.CommandKindCollectionUpdateBatchByID && kind != commitlog.CommandKindCollectionDeleteBatchByID {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if kind != commitlog.CommandKindCollectionDeleteBatchByID && len(ids) != len(documents) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	tx, err := m.nativeJSON.Begin(name)
	if err != nil {
		return nil, err
	}
	g := tx.binding(name)
	if g == nil || g.probe.meta.Options.ColumnStore != nil {
		return nil, errors.Join(backenddb.ErrNativeSemanticUnsupported, tx.Close())
	}
	if kind != commitlog.CommandKindCollectionDeleteBatchByID {
		requested, err := normalizeDocumentFormat(format)
		if err != nil {
			return nil, errors.Join(err, tx.Close())
		}
		if requested != normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) {
			return nil, errors.Join(backenddb.ErrNativeSemanticUnsupported, tx.Close())
		}
	}
	return tx, nil
}

func (tx *NativeJSONTxn) stageNativeCommandWALMutation(name string, kind commitlog.CommandKind, ids, documents [][]byte, templateRecords ...[]byte) (result NativeCommandWALApplyResult, docs []commitlog.CollectionDocument, err error) {
	g := tx.binding(name)
	if g == nil || g.probe.meta.Options.ColumnStore != nil {
		return result, docs, backenddb.ErrNativeSemanticUnsupported
	}
	// Admission precedes the adapter's header arrays and synchronous callbacks.
	// The ordinary row staging separately admits its own retained copies.
	if err = tx.admitRowControls(g, len(ids)); err != nil {
		return result, docs, err
	}
	if normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) == DocumentFormatTemplateV1 && kind != commitlog.CommandKindCollectionDeleteBatchByID {
		documents, err = tx.lowerNativeTemplateDocuments(g, kind, ids, documents, templateRecords)
		if err != nil {
			return result, docs, err
		}
	} else if len(templateRecords) > 1 || (len(templateRecords) == 1 && len(templateRecords[0]) != 0) {
		return result, docs, backenddb.ErrNativeSemanticUnsupported
	}
	docs = make([]commitlog.CollectionDocument, 0, len(documents))
	switch kind {
	case commitlog.CommandKindCollectionInsertBatchByID:
		if len(ids) != 0 {
			if err = tx.InsertBatch(ids, documents); err != nil {
				return result, docs, err
			}
		}
		for i := range documents {
			docs = append(docs, commitlog.CollectionDocument{ID: ids[i], Document: documents[i]})
		}
		result.Affected = len(documents)
	case commitlog.CommandKindCollectionUpdateBatchByID:
		items := make([]UpdateBatchItem, len(ids))
		for i := range ids {
			replacement := documents[i]
			items[i] = UpdateBatchItem{DocumentID: ids[i], Update: func(current []byte) ([]byte, bool, error) {
				return replacement, !bytes.Equal(current, replacement), nil
			}}
		}
		changed, updateErr := tx.UpdateBatch(items)
		if updateErr != nil {
			return result, docs, updateErr
		}
		for i, item := range changed {
			if item.Matched {
				result.Matched++
			}
			if item.Modified {
				result.Affected++
				docs = append(docs, commitlog.CollectionDocument{ID: ids[i], Document: documents[i]})
			}
		}
	case commitlog.CommandKindCollectionDeleteBatchByID:
		result.Affected, err = tx.DeleteBatch(ids)
		if err != nil {
			return result, docs, err
		}
	}
	return result, docs, nil
}

// A no-effect accepted command needs its own physical build ledger while sharing
// the same immutable directory/domain edges. No logical data or schema changes.
func prepareNativeCommandWALNoop(base *nativeEngineVersion) (*nativeEngineVersion, error) {
	next, err := newNativeEnginePreparation(base, 0, 0, base.allocator)
	if err != nil {
		return nil, err
	}
	retainNativeEngineDirectories(next, base)
	if base.catalog != nil {
		(nativeDomainLifetime{}).Retain(base.catalog)
		next.catalog = base.catalog
	}
	return next, nil
}
