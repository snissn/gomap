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
	if meta.Options.ColumnStore != nil {
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

// ApplyNativeCommandWALMutation lowers only the existing ordinary JSON by-ID
// commands. The original conditional transaction owns all reads, scalar index
// effects and OCC dependencies; its publisher assigns the still-unassigned frame.
func (m *CollectionManager) ApplyNativeCommandWALMutation(name string, format DocumentFormat, kind commitlog.CommandKind, ids, documents [][]byte, sync bool, observe func(uint64) error) (result NativeCommandWALApplyResult, resultErr error) {
	if !m.UsesNativeCommandWALApply() || m.isClosing() {
		return result, backenddb.ErrClosed
	}
	if kind != commitlog.CommandKindCollectionInsertBatchByID && kind != commitlog.CommandKindCollectionUpdateBatchByID && kind != commitlog.CommandKindCollectionDeleteBatchByID {
		return result, backenddb.ErrNativeSemanticUnsupported
	}
	if kind != commitlog.CommandKindCollectionDeleteBatchByID && (len(ids) != len(documents) || normalizedDocumentFormat(format) != DocumentFormatJSON) {
		return result, backenddb.ErrNativeSemanticUnsupported
	}
	tx, err := m.nativeJSON.Begin(name)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	g := tx.binding(name)
	if g == nil || g.probe.meta.Options.ColumnStore != nil || normalizedDocumentFormat(g.probe.meta.Options.DocumentFormat) != DocumentFormatJSON {
		return result, backenddb.ErrNativeSemanticUnsupported
	}
	// Admission precedes the adapter's header arrays and synchronous callbacks.
	// The ordinary row staging separately admits its own retained copies.
	if err = tx.admitRowControls(g, len(ids)); err != nil {
		return result, err
	}
	docs := make([]commitlog.CollectionDocument, 0, len(documents))
	switch kind {
	case commitlog.CommandKindCollectionInsertBatchByID:
		if len(ids) != 0 {
			if err = tx.InsertBatch(ids, documents); err != nil {
				return result, err
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
			return result, updateErr
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
			return result, err
		}
	}
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
