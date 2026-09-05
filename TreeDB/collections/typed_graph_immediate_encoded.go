package collections

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Only immediate receipts need this borrowed proof. The producing plan owns all
// headers until synchronous publication returns; no document payload is copied.
type typedGraphImmediateReceiptInput struct {
	operation          ColumnPublishOperation
	documents, deleted []columnWriteDocument
	rows               []columnDeclaredRow
}

// prepareImmediateTypedGraphEncoded reserves one conservative attempt before
// any pointerization. Tables are owning prepared native output, not one-shot
// publisher iterators. Typed inputs reuse their owning projection. Legacy JSON
// source replacement performs its existing one-time extraction here, not again
// at publication; that compatibility route is not a zero-JSON producer.
func (c *Collection) prepareImmediateTypedGraphEncoded(input columnWritePublishInput, tables []memtable.Table) (columnWritePublishInput, func(), error) {
	noop := func() {}
	if !c.typedGraphEncodedAdmissionEnabled() {
		return input, noop, nil
	}
	prepared, err := prepareColumnWritePublishInputBeforeCommandWAL(input)
	if err != nil {
		return input, noop, err
	}
	bound, err := typedGraphColumnEncodedBound(prepared)
	if err != nil {
		return input, noop, err
	}
	for _, table := range tables {
		n, err := typedGraphTableEncodedBound(table)
		if err != nil {
			return input, noop, err
		}
		if err := addTypedGraphEncodedBytes(&bound, n); err != nil {
			return input, noop, err
		}
	}
	state := c.collectionSchemaCoordinator().typedPublication.Load()
	if state == nil {
		return input, noop, ErrVectorIndexSnapshotMismatch
	}
	cost, err := typedGraphPublicationInputCost(prepared, state.limits)
	if err != nil {
		return input, noop, err
	}
	r, err := c.reserveTypedGraphPublication(cost, typedGraphEncodedCost{primary: bound})
	if err != nil {
		return input, noop, err
	}
	r.immediate = &typedGraphImmediateReceiptInput{operation: prepared.operation, documents: prepared.documents, deleted: prepared.sourceDeleteDocuments, rows: prepared.declaredRows}
	prepared.typedReceipts = []*typedGraphPublicationReceipt{r}
	cleanup := func() {
		// An assigned command may have been accepted ambiguously. Logical debt
		// remains fenced in that case; a successful install already consumed it.
		if prepared.commandWALIntent == nil || prepared.commandWALIntent.AssignedLSN() == 0 {
			r.rejectBeforeAppend()
		}
	}
	if err := beginTypedGraphEncodedAttempt(prepared.typedReceipts, true); err != nil {
		r.rejectBeforeAppend()
		return input, noop, err
	}
	return prepared, cleanup, nil
}

func validateTypedGraphImmediateReceiptInput(coord *collectionSchemaCoordinator, input columnWritePublishInput, cost typedGraphPublicationCost) error {
	coord.typedPublicationDebtMu.Lock()
	defer coord.typedPublicationDebtMu.Unlock()
	r := input.typedReceipts[0]
	p := r.immediate
	if r.coord != coord || r.consumed || r.cost != cost || p.operation != input.operation || len(p.documents) != len(input.documents) || len(p.deleted) != len(input.sourceDeleteDocuments) || len(p.rows) != len(input.declaredRows) || !input.declaredRowsReady {
		return ErrVectorIndexSnapshotMismatch
	}
	for i, doc := range p.documents {
		if !bytes.Equal(doc.ID, input.documents[i].ID) {
			return ErrVectorIndexSnapshotMismatch
		}
	}
	for i, doc := range p.deleted {
		if !bytes.Equal(doc.ID, input.sourceDeleteDocuments[i].ID) {
			return ErrVectorIndexSnapshotMismatch
		}
	}
	for i, row := range p.rows {
		actual := input.declaredRows[i]
		if !bytes.Equal(row.ID, actual.ID) || len(row.Values) != len(actual.Values) || (len(row.Values) > 0 && &row.Values[0] != &actual.Values[0]) {
			return ErrVectorIndexSnapshotMismatch
		}
	}
	return nil
}
