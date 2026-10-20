package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// The private member chain precedes the captured opening cut. A tombstone is
// authoritative, and an overlapping source insertion wins within its member.
func (g *nativeJSONTxnBinding) typedRowEqual(id, retained []byte, wanted []columnDeclaredValue) (bool, bool, error) {
	if row, old, ok := g.typed.find(id); ok {
		if row == nil {
			return false, false, nil
		}
		if wanted == nil || !bytes.Equal(bytes.TrimSpace(old), retained) {
			return true, false, nil
		}
		equal, err := typedDeclaredValuesEqual(row.Values, wanted)
		return true, equal, err
	}
	return g.probe.typedRowEqual(id, retained, wanted)
}

// stageNativeTypedInsert uses the maintained declared-value parser and residual
// transform. The original independent receipt transfers with the immutable
// delta before any transaction stage receipt can be refunded.
func (t *NativeJSONTxn) stageNativeTypedInsert(g *nativeJSONTxnBinding, ids, documents [][]byte) error {
	return t.stageNativeTypedDocuments(g, ids, documents, commitlog.CommandKindCollectionInsertBatchByID)
}

// Both document producers use the same original parser, retained policy and
// independent delta receipt. Update callers have already read/matched each ID.
func (t *NativeJSONTxn) stageNativeTypedDocuments(g *nativeJSONTxnBinding, ids, documents [][]byte, kind commitlog.CommandKind) (resultErr error) {
	if len(g.probe.meta.VectorIndexes) != 0 {
		return backenddb.ErrNativeSemanticUnsupported
	}
	operation := ColumnPublishOperationInsert
	if kind == commitlog.CommandKindCollectionUpdateBatchByID {
		operation = ColumnPublishOperationUpdate
	} else if kind != commitlog.CommandKindCollectionInsertBatchByID {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if len(ids) == 0 || len(ids) != len(documents) {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if err := nativeTypedProfile(t.db, g.probe.meta); err != nil {
		return err
	}
	retained, scratch, err := nativeTypedStageCapacity(g.probe.meta, ids, documents)
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
	var docs []columnWriteDocument
	defer func() {
		docs = nil
		if resultErr != nil {
			rows = nil
			residual = nil
			if lease != nil {
				lease.Close()
			}
		}
		work.Close()
	}()
	docs = make([]columnWriteDocument, len(ids))
	for i, id := range ids {
		if len(id) == 0 {
			return backenddb.ErrNativeSemanticUnsupported
		}
		if kind == commitlog.CommandKindCollectionInsertBatchByID {
			if err = t.trackRead(g, id); err != nil {
				return err
			}
			if found, _, e := g.typedRowEqual(id, nil, nil); e != nil {
				return e
			} else if found {
				return ErrDocumentExists
			}
		}
		docs[i] = columnWriteDocument{ID: id, Document: documents[i]}
	}
	rows, residual, err = nativeTypedDocumentRows(g.probe.meta, docs)
	if err != nil {
		return err
	}
	sort.Sort(nativeTypedRowsOrder{rows: rows, retained: residual})
	for i := 1; i < len(rows); i++ {
		if bytes.Equal(rows[i-1].ID, rows[i].ID) {
			return ErrDocumentExists
		}
	}
	// Parsing is complete. Drop its remaining document-header aliases before
	// admitting constructor scratch from actual declared backing and residuals.
	// Rows and residuals remain owned by their separate retained receipt.
	docs = nil
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
	if err = nativeTypedValidateIndexRows(g.probe.meta, rows, func(n uint64) error { return t.admitStage(g, n) }); err != nil {
		return err
	}
	if err = t.adoptNativeTyped(g, rows, residual, nil, operation, capacity, lease); err != nil {
		return err
	}
	lease = nil
	docs, rows, residual = nil, nil, nil
	if err = t.admitStage(g, memtable.COWAllocationCharge(uint64(len(ids))*uint64(unsafe.Sizeof(commitlog.CollectionDocument{})))); err != nil {
		return err
	}
	canonical := make([]commitlog.CollectionDocument, len(ids))
	for i := range ids {
		canonical[i] = commitlog.CollectionDocument{ID: ids[i], Document: documents[i]}
	}
	if err = t.admitMemberPayload(g, canonical, nil); err != nil {
		return err
	}
	format := commitlog.PayloadFormatCollectionInsertBatchByIDV1
	var payload []byte
	if kind == commitlog.CommandKindCollectionUpdateBatchByID {
		format = commitlog.PayloadFormatCollectionUpdateBatchByIDV1
		payload, err = commitlog.EncodeCollectionUpdateBatchByIDPayload(g.name, canonical)
	} else {
		payload, err = commitlog.EncodeCollectionInsertBatchByIDPayload(g.name, canonical)
	}
	if err != nil {
		return err
	}
	if err = t.appendNativeMember(g, kind, format, payload); err != nil {
		return err
	}
	g.typed.memberOrdinal = uint32(len(t.members) - 1)
	return nil
}

// The ordinary document producers and callback replacement validation share
// the maintained declared parser and retained-policy transform. Callers admit
// all parser scratch and returned backing before entering this allocation scope.
func nativeTypedDocumentRows(meta CollectionMeta, docs []columnWriteDocument) ([]columnDeclaredRow, [][]byte, error) {
	rows, err := extractColumnDeclaredRowsFromJSONDocuments(*meta.Options.ColumnStore, docs)
	if err != nil {
		return nil, nil, err
	}
	residual := make([][]byte, len(docs))
	for i, doc := range docs {
		residual[i], err = columnRetainedPayloadJSONFromJSONDocument(*meta.Options.ColumnStore, doc.Document)
		if err != nil {
			return nil, nil, err
		}
	}
	return rows, residual, nil
}

func (t *NativeJSONTxn) adoptNativeTyped(g *nativeJSONTxnBinding, rows []columnDeclaredRow, residual, deleted [][]byte, operation ColumnPublishOperation, capacity uint64, lease *memtable.COWExternalLease) error {
	header, err := t.core.NativeAllocator().AdmitNativeBytes(nativeTypedDeltaHeaderCharge())
	if err != nil {
		return err
	} // caller still owns the input receipt on refusal
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	data := &nativeTypedRows{rows: rows, retained: residual, deleted: deleted, refund: refund}
	data.refs.Store(1)
	nodeRefund := new(cowbtree.OwnedMapRefund)
	nodeRefund.Init(header)
	delta := &nativeTypedDelta{previous: g.typed, data: data, rows: rows, retained: residual, deleted: deleted, operation: operation, buildCapacity: capacity, refund: nodeRefund}
	delta.refs.Store(1)
	g.typed = delta
	return nil
}

// Reduce the same private member chain for final publication and conditional
// unique-index eligibility. Only original keys are borrowed into this header run.
func (t *NativeJSONTxn) nativeTypedFinalMutations(g *nativeJSONTxnBinding) ([]memtable.COWMutation, error) {
	d := g.typed
	var count uint64
	for member := d; member != nil; member = member.previous {
		if err := nativeTypedCheckedAdd(&count, uint64(len(member.rows))+uint64(len(member.deleted))); err != nil {
			return nil, err
		}
	}
	charge, err := nativeTypedCheckedMul(count, uint64(unsafe.Sizeof(memtable.COWMutation{})))
	if err != nil {
		return nil, err
	}
	if err = t.admitStage(g, memtable.COWAllocationCharge(charge)); err != nil {
		return nil, err
	}
	mutations := make([]memtable.COWMutation, 0, count)
	for member := d; member != nil; member = member.previous {
		for _, id := range member.deleted {
			mutations = append(mutations, memtable.COWMutation{Key: id})
		}
		for _, row := range member.rows {
			mutations = append(mutations, memtable.COWMutation{Key: row.ID})
		}
	}
	sort.Slice(mutations, func(i, j int) bool { return bytes.Compare(mutations[i].Key, mutations[j].Key) < 0 })
	kept := 0
	for _, mutation := range mutations {
		if kept != 0 && bytes.Equal(mutations[kept-1].Key, mutation.Key) {
			continue
		}
		row, _, _ := d.find(mutation.Key)
		mutation.Flags = node.FlagInline
		if row == nil {
			mutation.Flags = node.FlagTombstone
		}
		mutations[kept] = mutation
		kept++
	}
	clear(mutations[kept:])
	return mutations[:kept], nil
}

func (t *NativeJSONTxn) finalizeNativeTyped(g *nativeJSONTxnBinding) error {
	d := g.typed
	mutations, err := t.nativeTypedFinalMutations(g)
	if err != nil {
		return err
	}
	if err = admitNativeInsertArray(len(mutations), uint64(unsafe.Sizeof(txnhistory.Effect{})), func(n uint64) error { return t.admitStage(g, n) }); err != nil {
		return err
	}
	for _, charge := range []uint64{uint64(unsafe.Sizeof(nativeJSONLogicalInsert{})), 3 * uint64(unsafe.Sizeof(nativeJSONDomainChange{}))} {
		if err := t.admitStage(g, memtable.COWAllocationCharge(charge)); err != nil {
			return err
		}
	}
	effects := make([]txnhistory.Effect, 0, len(mutations))
	for _, mutation := range mutations {
		effects = append(effects, txnhistory.Effect{Namespace: g.namespace(), Kind: txnhistory.PointEffect, Key: mutation.Key})
	}
	var indexChanges []nativeJSONDomainChange
	if len(g.probe.meta.Indexes) != 0 {
		var err error
		indexChanges, effects, err = t.finalizeNativeTypedIndexes(g, mutations)
		if err != nil {
			return err
		}
	}
	if len(indexChanges) != 0 {
		if err := admitNativeInsertArray(3+len(indexChanges), uint64(unsafe.Sizeof(nativeJSONDomainChange{})), func(n uint64) error { return t.admitStage(g, n) }); err != nil {
			return err
		}
	}
	additional := make([]nativeJSONDomainChange, 0, 3+len(indexChanges))
	additional = append(additional, indexChanges...)
	for _, name := range []string{"column/manifest", "column/row-locator", "retained/semantic-stream-v1"} {
		if name == "retained/semantic-stream-v1" && !columnStoreRetainedPayloadUsesSemanticStreamV1(g.probe.meta.Options.ColumnStore) {
			continue
		}
		policy, err := collectionRootStoragePolicy(g.probe.meta, g.name+"/"+name)
		if err != nil {
			return err
		}
		additional = append(additional, nativeJSONDomainChange{name: name, policy: policy, entries: mutations})
	}
	g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, mutations: mutations, additional: additional, typed: d, custody: t.custody}
	if g.allowSourceImport || (d != nil && d.data != nil && d.data.graph != nil) {
		if err := t.admitStage(g, memtable.COWAllocationCharge(uint64(len(g.name)))+memtable.COWAllocationCharge(uint64(unsafe.Sizeof(txnhistory.Effect{})))); err != nil {
			return err
		}
		effects = append(effects, txnhistory.Effect{Namespace: txnhistory.Namespace{Kind: txnhistory.CatalogNamespace}, Kind: txnhistory.PointEffect, Key: []byte(g.name)})
	}
	g.effects = effects
	return nil
}

// This assigned operation only binds canonical identity in private descriptors.
// It cannot allocate assets, invoke callbacks, recapture or publish anything.
func (p *nativeJSONLogicalGroup) FinalizeNativePublication(ctx backenddb.CommandWALPublishContext) (backenddb.NativeFinalizedPublication, error) {
	if p.prepared == nil || ctx.AppliedCommandLSN == 0 {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	// Only the prepared successor's cloned private headers receive identity.
	// Its inherited tail already has canonical identity and remains immutable.
	for i := 0; i < p.count; i++ {
		g := &p.bindings[i]
		if g.typed == nil {
			continue
		}
		collection, ok := p.prepared.collections.Get(g.incarnation)
		if !ok {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
		}
		primary, ok := collection.domains.Get("primary")
		if !ok {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
		}
		d := primary.typed
		for staged := g.typed; staged != nil; staged = staged.previous {
			if d == nil || d.appliedLSN != 0 || d.memberOrdinal != staged.memberOrdinal {
				return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
			}
			if p.participants != nil && (int(staged.memberOrdinal) >= len(p.participants) || p.participants[staged.memberOrdinal].AssignedLSN() == 0 || p.participants[staged.memberOrdinal].AssignedLSN() > ctx.AppliedCommandLSN) {
				return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
			}
			d = d.previous
		}
	}
	for i := 0; i < p.count; i++ {
		g := &p.bindings[i]
		if g.typed == nil {
			continue
		}
		collection, _ := p.prepared.collections.Get(g.incarnation)
		primary, _ := collection.domains.Get("primary")
		d := primary.typed
		for staged := g.typed; staged != nil; staged = staged.previous {
			d.appliedLSN = ctx.AppliedCommandLSN
			if p.participants != nil {
				d.appliedLSN = p.participants[staged.memberOrdinal].AssignedLSN()
				d.memberOrdinal = 0 // each participant has its own singleton frame
			}
			if p.raw {
				d.memberOrdinal++
			}
			d = d.previous
		}
	}
	p.prepared.Retain()
	return backenddb.NativeFinalizedPublication{State: p.prepared}, nil
}

type nativeTypedRowsOrder struct {
	rows     []columnDeclaredRow
	retained [][]byte
}

func (s nativeTypedRowsOrder) Len() int { return len(s.rows) }
func (s nativeTypedRowsOrder) Less(i, j int) bool {
	return bytes.Compare(s.rows[i].ID, s.rows[j].ID) < 0
}
func (s nativeTypedRowsOrder) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
	s.retained[i], s.retained[j] = s.retained[j], s.retained[i]
}

// Public typed APIs share the original native transaction and its captured cut.
func (c *Collection) nativeInsertTypedBatch(ids, retained [][]byte, columns []TypedColumnBatch) (out [][]byte, stats CollectionInsertStats, resultErr error) {
	if c.manager.isClosing() {
		return nil, stats, backenddb.ErrClosed
	}
	stats.Documents = len(ids)
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, stats, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	_, _, err = tx.stageNativeTypedBatch(&tx.bindings[0], ids, retained, columns, nil, commitlog.CommandKindCollectionInsertBatchByID, false)
	if err != nil {
		return nil, stats, err
	}
	if len(tx.members) != 0 {
		if err = tx.Commit(); err != nil {
			return nil, stats, err
		}
	}
	out, err = cloneBatchDocumentIDs(ids)
	return out, stats, err
}
func (c *Collection) nativeReplaceTypedBatch(ids, retained [][]byte, columns []TypedColumnBatch) (results []UpdateBatchResult, resultErr error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	results, _, err = tx.stageNativeTypedBatch(&tx.bindings[0], ids, retained, columns, nil, commitlog.CommandKindCollectionUpdateBatchByID, false)
	if err != nil {
		return nil, err
	}
	if len(tx.members) != 0 {
		err = tx.Commit()
	}
	return results, err
}
func (c *Collection) nativeReplaceTypedSource(deleted, ids, retained [][]byte, columns []TypedColumnBatch, upsert bool, stats *CollectionInsertStats) (matched int, resultErr error) {
	if c.manager.isClosing() {
		return 0, backenddb.ErrClosed
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return 0, err
	}
	defer func() { resultErr = errors.Join(resultErr, tx.Close()) }()
	_, matched, err = tx.stageNativeTypedBatch(&tx.bindings[0], ids, retained, columns, deleted, commitlog.CommandKindCollectionReplaceSourceByID, upsert)
	if err != nil {
		return 0, err
	}
	if stats != nil {
		stats.Documents = len(ids)
	}
	if len(tx.members) != 0 {
		err = tx.Commit()
	}
	return matched, err
}

// Bound the maintained projection/parser before it allocates. Owned declared
// backing and exact residuals use the same independent delta receipt; projection
// maps, conversion controls and the temporary column input use original work.
func nativeTypedBatchStageCapacity(meta CollectionMeta, ids, retained [][]byte, columns []TypedColumnBatch, deleted [][]byte) (owned, work uint64, err error) {
	if !nativeTypedMeta(meta) || meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadNonColumn || len(ids) != len(retained) {
		return 0, 0, backenddb.ErrNativeSemanticUnsupported
	}
	owned, work, err = nativeTypedStageCapacity(meta, ids, retained)
	if err != nil {
		return
	}
	for _, input := range columns {
		for _, value := range input.Strings {
			if err = nativeTypedCheckedAdd(&owned, memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
				return
			}
		}
		for _, value := range input.Float32Vectors {
			n, e := nativeTypedCheckedMul(uint64(len(value)), 4)
			if e != nil {
				return 0, 0, e
			}
			if err = nativeTypedCheckedAdd(&owned, memtable.COWAllocationCharge(n)); err != nil {
				return
			}
		}
	}
	for _, id := range deleted {
		if err = nativeTypedCheckedAdd(&owned, memtable.COWAllocationCharge(uint64(len(id)))); err != nil {
			return
		}
	}
	n, err := nativeTypedCheckedMul(uint64(len(deleted)), uint64(unsafe.Sizeof([]byte{})))
	if err != nil {
		return 0, 0, err
	}
	if err = nativeTypedCheckedAdd(&owned, memtable.COWAllocationCharge(n)); err != nil {
		return
	}
	// One map group/entry quota per projection row plus copied map keys; the
	// existing parser bound already covers retained JSON validation scratch.
	controls, err := nativeTypedCheckedMul(uint64(len(ids)), preparedSemanticStreamMapEntryReserveBytes)
	if err != nil {
		return 0, 0, err
	}
	for _, id := range ids {
		if err = nativeTypedCheckedAdd(&controls, uint64(len(id))); err != nil {
			return
		}
	}
	columnControls, e := nativeTypedCheckedMul(uint64(len(meta.Options.ColumnStore.Columns)), 8*preparedSemanticStreamMapEntryReserveBytes)
	if e != nil {
		return 0, 0, e
	}
	for _, col := range meta.Options.ColumnStore.Columns {
		if err = nativeTypedCheckedAdd(&columnControls, uint64(len(col.Name))+uint64(len(col.Path))); err != nil {
			return
		}
	}
	if err = nativeTypedCheckedAdd(&controls, columnControls); err != nil {
		return
	}
	if err = nativeTypedCheckedAdd(&work, memtable.COWAllocationCharge(controls)); err != nil {
		return
	}
	return
}

func (t *NativeJSONTxn) stageNativeTypedBatch(g *nativeJSONTxnBinding, ids, retained [][]byte, columns []TypedColumnBatch, deleteIDs [][]byte, kind commitlog.CommandKind, upsert bool) (results []UpdateBatchResult, matched int, resultErr error) {
	if len(g.probe.meta.VectorIndexes) != 0 && !g.allowSourceImport {
		return nil, 0, backenddb.ErrNativeSemanticUnsupported
	}
	if !nativeTypedMeta(g.probe.meta) || g.probe.meta.Options.ColumnStore.RetainedPayload != ColumnRetainedPayloadNonColumn {
		return nil, 0, backenddb.ErrNativeSemanticUnsupported
	}
	if err := nativeTypedProfile(t.db, g.probe.meta); err != nil {
		return nil, 0, err
	}
	deleting := kind == commitlog.CommandKindCollectionDeleteBatchByID
	if !deleting && columnRetainedPayloadEffectiveEncoding(g.probe.meta.Options.ColumnStore) != ColumnRetainedPayloadEncodingJSON {
		return nil, 0, backenddb.ErrNativeSemanticUnsupported
	}
	owned, scratch, err := nativeTypedBatchStageCapacity(g.probe.meta, ids, retained, columns, deleteIDs)
	if err != nil {
		return nil, 0, err
	}
	lease, err := t.core.NativeAllocator().AdmitNativeBytes(owned)
	if err != nil {
		return nil, 0, fmt.Errorf("collections: native typed batch owned bytes=%d: %w", owned, err)
	}
	work, err := t.core.NativeAllocator().AdmitNativeBytes(scratch)
	if err != nil {
		lease.Close()
		return nil, 0, fmt.Errorf("collections: native typed batch scratch bytes=%d: %w", scratch, err)
	}
	var p *trustedFloat32Projection
	var documents []columnWriteDocument
	var rows []columnDeclaredRow
	var residual, deleted [][]byte
	defer func() {
		p = nil
		documents = nil
		rows = nil
		residual = nil
		deleted = nil
		if lease != nil {
			lease.Close()
		}
		work.Close()
	}()
	for i, id := range deleteIDs {
		if len(id) == 0 {
			return nil, 0, backenddb.ErrNativeSemanticUnsupported
		}
		for _, prior := range deleteIDs[:i] {
			if bytes.Equal(prior, id) {
				return nil, 0, ErrDuplicateDocumentID
			}
		}
	}
	if !deleting {
		if len(ids) == 0 && len(columns) == 0 {
			columns = make([]TypedColumnBatch, len(g.probe.meta.Options.ColumnStore.Columns))
			for i, col := range g.probe.meta.Options.ColumnStore.Columns {
				columns[i].Name = col.Name
			}
		}
		p, err = newTrustedTypedProjection(g.probe.meta, ids, retained, columns)
		if err != nil {
			return nil, 0, err
		}
		documents = make([]columnWriteDocument, len(ids))
		for i, id := range ids {
			documents[i] = columnWriteDocument{ID: id, Document: retained[i]}
		}
		if err = applyTypedProjection(ids, documents, p); err != nil {
			return nil, 0, err
		}
		input, e := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{meta: g.probe.meta, operation: ColumnPublishOperationUpdate, documents: documents, rows: len(documents)})
		if e != nil {
			return nil, 0, e
		}
		rows = input.declaredRows
		residual = make([][]byte, len(ids))
		results = make([]UpdateBatchResult, len(ids))
		kept := 0
		for i, id := range ids {
			if err = t.trackRead(g, id); err != nil {
				return nil, 0, err
			}
			trimmed := bytes.TrimSpace(retained[i])
			found, equal, e := g.typedRowEqual(id, trimmed, rows[i].Values)
			if e != nil {
				return nil, 0, e
			}
			if kind == commitlog.CommandKindCollectionInsertBatchByID && found {
				return nil, 0, ErrDocumentExists
			}
			if kind == commitlog.CommandKindCollectionReplaceSourceByID && !upsert && found {
				replacing := false
				for _, oldID := range deleteIDs {
					if bytes.Equal(oldID, id) {
						replacing = true
						break
					}
				}
				if !replacing {
					return nil, 0, ErrDocumentExists
				}
			}
			results[i] = UpdateBatchResult{Matched: found, Modified: found && !equal}
			if kind == commitlog.CommandKindCollectionUpdateBatchByID && (!found || equal) {
				continue
			}
			if upsert && equal {
				continue
			}
			rows[kept] = columnDeclaredRow{ID: bytes.Clone(id), Values: rows[i].Values}
			residual[kept] = bytes.Clone(trimmed)
			kept++
		}
		rows, residual = rows[:kept], residual[:kept]
	}
	deleted = make([][]byte, 0, len(deleteIDs))
	for _, id := range deleteIDs {
		if err = t.trackRead(g, id); err != nil {
			return nil, 0, err
		}
		found, _, e := g.typedRowEqual(id, nil, nil)
		if e != nil {
			return nil, 0, e
		}
		if found {
			matched++
		}
		if deleting && !found {
			continue
		}
		if upsert {
			changed := false
			for _, row := range rows {
				if bytes.Equal(row.ID, id) {
					changed = true
					break
				}
			}
			if !changed {
				continue
			}
		}
		deleted = append(deleted, bytes.Clone(id))
	}
	sort.Sort(nativeTypedRowsOrder{rows: rows, retained: residual})
	sort.Slice(deleted, func(i, j int) bool { return bytes.Compare(deleted[i], deleted[j]) < 0 })
	if len(rows) == 0 && len(deleted) == 0 {
		if kind == commitlog.CommandKindCollectionUpdateBatchByID {
			if err = t.admitMemberPayload(g, nil, nil); err != nil {
				return nil, 0, err
			}
			payload, e := commitlog.EncodeCollectionUpdateBatchByIDPayload(g.name, nil)
			if e != nil {
				return nil, 0, e
			}
			if err = t.admitStage(g, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeJSONLogicalInsert{})))); err != nil {
				return nil, 0, err
			}
			g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, custody: t.custody}
			err = t.appendNativeMember(g, kind, commitlog.PayloadFormatCollectionUpdateBatchByIDV1, payload)
		}
		return results, matched, err
	}
	p = nil
	documents = nil
	capacity, err := nativeTypedMutationBuildCapacity(g.probe.meta, rows, residual, deleted)
	if err != nil {
		return nil, 0, err
	}
	validation, err := nativeTypedCollectionBuildCapacity(g.probe.meta, capacity)
	if err != nil {
		return nil, 0, err
	}
	if err = work.Resize(max(scratch, validation)); err != nil {
		return nil, 0, err
	}
	if len(rows) != 0 {
		if err = nativeTypedValidateResidual(g.probe.meta, rows, residual); err != nil {
			return nil, 0, err
		}
	}
	if err = nativeTypedValidateIndexRows(g.probe.meta, rows, func(n uint64) error { return t.admitStage(g, n) }); err != nil {
		return nil, 0, err
	}
	// Canonical typed/source buffers coexist with their typed descriptor arrays
	// and source intermediate encodings; reserve their checked source envelope
	// before constructing any of them, on the original transaction stage owner.
	payloadBytes := uint64(16 * preparedSemanticStreamMapEntryReserveBytes)
	nameBytes, e := nativeTypedCheckedMul(uint64(len(g.name)), 16)
	if e != nil {
		return nil, 0, e
	}
	if e = nativeTypedCheckedAdd(&payloadBytes, nameBytes); e != nil {
		return nil, 0, e
	}
	for _, col := range g.probe.meta.Options.ColumnStore.Columns {
		n := uint64(len(col.Name))
		if e = nativeTypedCheckedAdd(&n, preparedSemanticStreamMapEntryReserveBytes); e != nil {
			return nil, 0, e
		}
		n, e = nativeTypedCheckedMul(n, 16)
		if e != nil {
			return nil, 0, e
		}
		if e = nativeTypedCheckedAdd(&payloadBytes, n); e != nil {
			return nil, 0, e
		}
	}
	for i, row := range rows {
		var n uint64
		if len(row.Values) != len(g.probe.meta.Options.ColumnStore.Columns) {
			return nil, 0, backenddb.ErrNativeSemanticUnsupported
		}
		for j, col := range g.probe.meta.Options.ColumnStore.Columns {
			backing, _, e := nativeTypedValueCapacity(col, row.Values[j])
			if e != nil {
				return nil, 0, e
			}
			if e = nativeTypedCheckedAdd(&n, backing); e != nil {
				return nil, 0, e
			}
		}
		valueControls, e := nativeTypedCheckedMul(uint64(len(row.Values)), preparedSemanticStreamMapEntryReserveBytes)
		if e != nil {
			return nil, 0, e
		}
		for _, v := range []uint64{uint64(len(row.ID)), uint64(len(residual[i])), valueControls} {
			if e = nativeTypedCheckedAdd(&n, v); e != nil {
				return nil, 0, e
			}
		}
		n, e = nativeTypedCheckedMul(n, 16)
		if e != nil {
			return nil, 0, e
		}
		if e = nativeTypedCheckedAdd(&payloadBytes, n); e != nil {
			return nil, 0, e
		}
	}
	for _, id := range deleted {
		n := uint64(len(id))
		e := nativeTypedCheckedAdd(&n, preparedSemanticStreamMapEntryReserveBytes)
		if e != nil {
			return nil, 0, e
		}
		n, e = nativeTypedCheckedMul(n, 16)
		if e != nil {
			return nil, 0, e
		}
		if e = nativeTypedCheckedAdd(&payloadBytes, n); e != nil {
			return nil, 0, e
		}
	}
	if err = t.admitStage(g, memtable.COWAllocationCharge(payloadBytes)); err != nil {
		return nil, 0, err
	}
	canonical := make([]commitlog.CollectionDocument, len(rows))
	projection := &trustedFloat32Projection{columns: g.probe.meta.Options.ColumnStore.Columns, schemaHash: g.probe.meta.Options.ColumnStore.SchemaHash, typedRows: make(map[string][]columnDeclaredValue, len(rows)), retainedJSON: residual}
	for i, row := range rows {
		canonical[i] = commitlog.CollectionDocument{ID: row.ID, Document: residual[i]}
		projection.typedRows[string(row.ID)] = row.Values
	}
	var payload []byte
	format := commitlog.PayloadFormatCollectionTypedBatchByIDV1
	operation := ColumnPublishOperationInsert
	if kind == commitlog.CommandKindCollectionDeleteBatchByID {
		payload, err = commitlog.EncodeCollectionDeleteBatchByIDPayload(g.name, deleted)
		format = commitlog.PayloadFormatCollectionDeleteBatchByIDV1
		operation = ColumnPublishOperationDelete
	} else if kind == commitlog.CommandKindCollectionReplaceSourceByID && len(rows) == 0 {
		payload, err = commitlog.EncodeCollectionReplaceSourceByIDPayload(g.name, deleted, nil)
		format = commitlog.PayloadFormatCollectionReplaceSourceByIDV1
		operation = ColumnPublishOperationDelete
	} else {
		typed, e := typedCommandPayload(g.probe.meta, canonical, projection)
		if e != nil {
			return nil, 0, e
		}
		if kind == commitlog.CommandKindCollectionReplaceSourceByID {
			payload, err = commitlog.EncodeCollectionTypedSourcePayload(deleted, typed)
			format = commitlog.PayloadFormatCollectionTypedSourceByIDV1
			operation = ColumnPublishOperationUpdate
		} else {
			payload, err = commitlog.EncodeCollectionTypedBatchPayload(typed)
			if kind == commitlog.CommandKindCollectionUpdateBatchByID {
				operation = ColumnPublishOperationUpdate
			}
		}
	}
	projection = nil
	canonical = nil
	if err != nil {
		return nil, 0, err
	}
	if err = t.appendNativeMember(g, kind, format, payload); err != nil {
		return nil, 0, err
	}
	if err = t.adoptNativeTyped(g, rows, residual, deleted, operation, capacity, lease); err != nil {
		return nil, 0, err
	}
	lease = nil
	g.typed.memberOrdinal = uint32(len(t.members) - 1)
	return results, matched, nil
}
