package collections

import (
	"encoding/binary"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const columnPrimaryRowLocatorValueSize = 4 + 8 + 8 + 8 + 8

var columnPrimaryRowLocatorMagic = [4]byte{'C', 'R', 'L', '1'}

// Metadata rows preserve one ordinary full row, never another metadata row.
// Keep these coordinates out of DocumentRowRef: ordinary query refs stay small.
type columnRowCoordinates struct {
	Generation        uint64
	PartID            uint64
	RowIndex          int
	AppliedCommandLSN uint64
}

func columnCoordinates(ref DocumentRowRef) columnRowCoordinates {
	return columnRowCoordinates{ref.Generation, ref.PartID, ref.RowIndex, ref.AppliedCommandLSN}
}

func (p columnRowCoordinates) ref(id []byte) DocumentRowRef {
	return DocumentRowRef{DocumentID: id, Generation: p.Generation, PartID: p.PartID, RowIndex: p.RowIndex, AppliedCommandLSN: p.AppliedCommandLSN}
}

func encodeColumnMetadataRowLocator(latest DocumentRowRef, preserved columnRowCoordinates) []byte {
	b := make([]byte, columnPrimaryRowLocatorValueSize+32)
	copy(b, encodeColumnPrimaryRowLocator(latest))
	b[3] = '2'
	binary.BigEndian.PutUint64(b[36:], preserved.Generation)
	binary.BigEndian.PutUint64(b[44:], preserved.PartID)
	binary.BigEndian.PutUint64(b[52:], uint64(preserved.RowIndex))
	binary.BigEndian.PutUint64(b[60:], preserved.AppliedCommandLSN)
	return b
}

// decodeColumnScoringRowLocatorBorrowedID resolves scoring identity without
// opening the metadata or vector assets. CRL1 rows are their own scoring rows.
func decodeColumnScoringRowLocatorBorrowedID(id, value []byte) (DocumentRowRef, error) {
	latest, err := decodeColumnPrimaryRowLocatorBorrowedID(id, value)
	if err != nil || len(value) == columnPrimaryRowLocatorValueSize {
		return latest, err
	}
	return decodeColumnRowCoordinates(id, value[36:])
}

func decodeColumnRowCoordinates(id, value []byte) (DocumentRowRef, error) {
	row := binary.BigEndian.Uint64(value[16:])
	if row > uint64(^uint(0)>>1) {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator for id %q row index overflows int", id)
	}
	ref := DocumentRowRef{DocumentID: id, Generation: binary.BigEndian.Uint64(value), PartID: binary.BigEndian.Uint64(value[8:]), RowIndex: int(row), AppliedCommandLSN: binary.BigEndian.Uint64(value[24:])}
	if err := validateDocumentRowRefForPointFetch(0, ref); err != nil {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator: %w", err)
	}
	return ref, nil
}

// encodeColumnPrimaryRowLocator stores only physical coordinates; the primary
// key supplies the document ID. The locator root is co-published with the
// primary and manifest roots, so a snapshot cannot observe a new primary value
// without its matching locator.
func encodeColumnPrimaryRowLocator(ref DocumentRowRef) []byte {
	b := make([]byte, columnPrimaryRowLocatorValueSize)
	copy(b, columnPrimaryRowLocatorMagic[:])
	binary.BigEndian.PutUint64(b[4:], ref.Generation)
	binary.BigEndian.PutUint64(b[12:], ref.PartID)
	binary.BigEndian.PutUint64(b[20:], uint64(ref.RowIndex))
	binary.BigEndian.PutUint64(b[28:], ref.AppliedCommandLSN)
	return b
}

func decodeColumnPrimaryRowLocator(id, value []byte) (DocumentRowRef, error) {
	ref, err := decodeColumnPrimaryRowLocatorBorrowedID(id, value)
	if err != nil {
		return DocumentRowRef{}, err
	}
	ref.DocumentID = append([]byte(nil), id...)
	return ref, nil
}

// The caller must consume the row ref before reusing id. Coordinates are owned
// scalar values; only DocumentID borrows, never the encoded locator bytes.
func decodeColumnPrimaryRowLocatorBorrowedID(id, value []byte) (DocumentRowRef, error) {
	legacy := len(value) == columnPrimaryRowLocatorValueSize && string(value[:4]) == "CRL1"
	metadata := len(value) == columnPrimaryRowLocatorValueSize+32 && string(value[:4]) == "CRL2"
	if !legacy && !metadata {
		return DocumentRowRef{}, fmt.Errorf("collections: invalid primary row locator for id %q value=%x", string(id), value)
	}
	ref, err := decodeColumnRowCoordinates(id, value[4:36])
	if err != nil {
		return DocumentRowRef{}, err
	}
	if metadata {
		preserved, err := decodeColumnRowCoordinates(id, value[36:])
		if err != nil {
			return DocumentRowRef{}, err
		}
		if preserved.Generation >= ref.Generation || preserved.AppliedCommandLSN >= ref.AppliedCommandLSN {
			return DocumentRowRef{}, fmt.Errorf("collections: metadata row locator must preserve an earlier full row")
		}
	}
	return ref, nil
}

func buildColumnPrimaryRowLocatorDelta(plan ColumnPublishPlan, documents []columnWriteDocument, baseRoot uint64, policy backenddb.OrderedRootStoragePolicy) (backenddb.OrderedRootDeltaPublishInput, error) {
	table, err := buildColumnPrimaryRowLocatorTable(plan, documents)
	if err != nil {
		return backenddb.OrderedRootDeltaPublishInput{}, err
	}
	return backenddb.OrderedRootDeltaPublishInput{BaseRoot: baseRoot, Iter: table.NewIterator(nil, nil), StoragePolicy: policy}, nil
}

func buildColumnPrimaryRowLocatorDeltaBatch(plan ColumnPublishPlan, documents []columnWriteDocument, baseRoot uint64, policy backenddb.OrderedRootStoragePolicy) (backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	table, err := buildColumnPrimaryRowLocatorTable(plan, documents)
	if err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	it := table.NewIterator(nil, nil)
	delta, err := backenddb.OrderedRootDeltaBatchFromIterator(it)
	if err != nil {
		_ = it.Close()
		return backenddb.OrderedRootDeltaBatchPublishInput{}, func() {}, err
	}
	cleanup := func() {
		_ = delta.Close()
		_ = it.Close()
	}
	return backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: baseRoot, Delta: delta, StoragePolicy: policy}, cleanup, nil
}

func buildColumnPrimaryRowLocatorTable(plan ColumnPublishPlan, documents []columnWriteDocument) (memtable.Table, error) {
	if len(documents) != plan.Rows {
		return nil, fmt.Errorf("collections: row locator documents=%d rows=%d", len(documents), plan.Rows)
	}
	table := newCollectionRunTable(len(documents))
	for row, document := range documents {
		if len(document.ID) == 0 {
			return nil, fmt.Errorf("collections: row locator row %d missing document id", row)
		}
		if plan.Operation == ColumnPublishOperationDelete {
			table.DeleteSteal(append([]byte(nil), document.ID...))
			continue
		}
		ref := DocumentRowRef{Generation: plan.UpdatedActiveManifest.Generation, PartID: columnPhysicalRowAssetPartID, RowIndex: row, AppliedCommandLSN: plan.AppliedCommandLSN}
		encoded := encodeColumnPrimaryRowLocator(ref)
		if document.preserved != nil {
			encoded = encodeColumnMetadataRowLocator(ref, *document.preserved)
		}
		setCollectionRunValue(table, append([]byte(nil), document.ID...), encoded)
	}
	table.Freeze()
	return table, nil
}

func closeColumnPrimaryRowLocatorDelta(delta backenddb.OrderedRootDeltaPublishInput) {
	if delta.Iter != nil {
		_ = delta.Iter.Close()
	}
}
