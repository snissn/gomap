package collections

import (
	"encoding/binary"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const columnPrimaryRowLocatorValueSize = 4 + 8 + 8 + 8 + 8

var columnPrimaryRowLocatorMagic = [4]byte{'C', 'R', 'L', '1'}

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
	if len(value) != columnPrimaryRowLocatorValueSize || string(value[:4]) != string(columnPrimaryRowLocatorMagic[:]) {
		return DocumentRowRef{}, fmt.Errorf("collections: invalid primary row locator for id %q value=%x", string(id), value)
	}
	row := binary.BigEndian.Uint64(value[20:])
	if row > uint64(^uint(0)>>1) {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator for id %q row index overflows int", string(id))
	}
	ref := DocumentRowRef{DocumentID: append([]byte(nil), id...), Generation: binary.BigEndian.Uint64(value[4:]), PartID: binary.BigEndian.Uint64(value[12:]), RowIndex: int(row), AppliedCommandLSN: binary.BigEndian.Uint64(value[28:])}
	if err := validateDocumentRowRefForPointFetch(0, ref); err != nil {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator: %w", err)
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
		setCollectionRunValue(table, append([]byte(nil), document.ID...), encodeColumnPrimaryRowLocator(DocumentRowRef{Generation: plan.UpdatedActiveManifest.Generation, PartID: columnPhysicalRowAssetPartID, RowIndex: row, AppliedCommandLSN: plan.AppliedCommandLSN}))
	}
	table.Freeze()
	return table, nil
}

func closeColumnPrimaryRowLocatorDelta(delta backenddb.OrderedRootDeltaPublishInput) {
	if delta.Iter != nil {
		_ = delta.Iter.Close()
	}
}
