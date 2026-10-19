package collections

import (
	"bytes"
	"errors"
	"sort"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// stageNativeTypedInsert uses the maintained declared-value parser and residual
// transform. The original independent receipt transfers with the immutable
// delta before any transaction stage receipt can be refunded.
func (t *NativeJSONTxn) stageNativeTypedInsert(g *nativeJSONTxnBinding, ids, documents [][]byte) (resultErr error) {
	if len(ids) == 0 || len(ids) != len(documents) || g.typed != nil {
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
		if _, e := g.probe.Get(id); e == nil {
			return ErrDocumentExists
		} else if !errors.Is(e, tree.ErrKeyNotFound) {
			return e
		}
		docs[i] = columnWriteDocument{ID: id, Document: documents[i]}
	}
	rows, err = extractColumnDeclaredRowsFromJSONDocuments(*g.probe.meta.Options.ColumnStore, docs)
	if err != nil {
		return err
	}
	residual = make([][]byte, len(documents))
	for i, doc := range documents {
		residual[i], err = columnRetainedPayloadJSONFromJSONDocument(*g.probe.meta.Options.ColumnStore, doc)
		if err != nil {
			return err
		}
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
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	data := &nativeTypedRows{rows: rows, retained: residual, refund: refund}
	data.refs.Store(1)
	// Header construction has its own receipt, independent from vector backing.
	header, e := t.core.NativeAllocator().AdmitNativeBytes(nativeTypedDeltaHeaderCharge())
	if e != nil {
		rows, residual = nil, nil
		lease = nil
		data.release().Complete()
		return e
	}
	nodeRefund := new(cowbtree.OwnedMapRefund)
	nodeRefund.Init(header)
	delta := &nativeTypedDelta{data: data, rows: rows, retained: residual, buildCapacity: capacity, refund: nodeRefund}
	delta.refs.Store(1)
	lease = nil
	g.typed = delta // adopted before fallible canonical member preparation
	docs = nil
	rows = nil
	residual = nil
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
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload(g.name, canonical)
	if err != nil {
		return err
	}
	if err = t.appendNativeMember(g, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload); err != nil {
		return err
	}
	delta.memberOrdinal = uint32(len(t.members) - 1)
	return nil
}

func (t *NativeJSONTxn) finalizeNativeTyped(g *nativeJSONTxnBinding) error {
	d := g.typed
	for _, charge := range []uint64{uint64(unsafe.Sizeof(nativeJSONLogicalInsert{})), uint64(len(d.rows)) * uint64(unsafe.Sizeof(memtable.COWMutation{})), uint64(len(d.rows)) * uint64(unsafe.Sizeof(txnhistory.Effect{})), 3 * uint64(unsafe.Sizeof(nativeJSONDomainChange{}))} {
		if err := t.admitStage(g, memtable.COWAllocationCharge(charge)); err != nil {
			return err
		}
	}
	mutations := make([]memtable.COWMutation, len(d.rows))
	effects := make([]txnhistory.Effect, len(d.rows))
	for i, row := range d.rows {
		mutations[i] = memtable.COWMutation{Key: row.ID, Flags: node.FlagInline}
		effects[i] = txnhistory.Effect{Namespace: g.namespace(), Kind: txnhistory.PointEffect, Key: row.ID}
	}
	additional := make([]nativeJSONDomainChange, 0, 3)
	for _, name := range []string{"column/manifest", "column/row-locator", "retained/semantic-stream-v1"} {
		policy, err := collectionRootStoragePolicy(g.probe.meta, g.name+"/"+name)
		if err != nil {
			return err
		}
		additional = append(additional, nativeJSONDomainChange{name: name, policy: policy, entries: mutations})
	}
	g.plan = &nativeJSONLogicalInsert{name: g.name, incarnation: g.incarnation, mutations: mutations, additional: additional, typed: d, custody: t.custody}
	g.effects = effects
	return nil
}

// This assigned operation only binds canonical identity in private descriptors.
// It cannot allocate assets, invoke callbacks, recapture or publish anything.
func (p *nativeJSONLogicalGroup) FinalizeNativePublication(ctx backenddb.CommandWALPublishContext) (backenddb.NativeFinalizedPublication, error) {
	if p.prepared == nil || ctx.AppliedCommandLSN == 0 {
		return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
	}
	for i := 0; i < p.count; i++ {
		d := p.bindings[i].typed
		if d == nil {
			continue
		}
		if d.appliedLSN != 0 {
			return backenddb.NativeFinalizedPublication{}, backenddb.ErrNativeSemanticUnsupported
		}
	}
	for i := 0; i < p.count; i++ {
		d := p.bindings[i].typed
		if d == nil {
			continue
		}
		d.appliedLSN = ctx.AppliedCommandLSN
		if p.raw {
			d.memberOrdinal++
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
