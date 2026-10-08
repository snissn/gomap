package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"

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
	writeColumnPrimaryRowLocator(b, latest)
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
	if string(value[:4]) == "CRL3" {
		return DocumentRowRef{}, fmt.Errorf("collections: sparse string row has no single scoring coordinate")
	}
	return decodeColumnRowCoordinates(id, value[36:])
}

func decodeColumnRowCoordinates(id, value []byte) (DocumentRowRef, error) {
	if len(value) != 32 {
		return DocumentRowRef{}, fmt.Errorf("collections: invalid row coordinate width")
	}
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
	writeColumnPrimaryRowLocator(b, ref)
	return b
}

func writeColumnPrimaryRowLocator(b []byte, ref DocumentRowRef) {
	copy(b, columnPrimaryRowLocatorMagic[:])
	writeColumnRowCoordinates(b[4:], columnCoordinates(ref))
}
func writeColumnRowCoordinates(b []byte, ref columnRowCoordinates) {
	binary.BigEndian.PutUint64(b, ref.Generation)
	binary.BigEndian.PutUint64(b[8:], ref.PartID)
	binary.BigEndian.PutUint64(b[16:], uint64(ref.RowIndex))
	binary.BigEndian.PutUint64(b[24:], ref.AppliedCommandLSN)
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
	sparse := len(value) >= 44 && string(value[:4]) == "CRL3"
	if sparse {
		if _, err := validateColumnFieldLocator(id, value, -1); err != nil {
			return DocumentRowRef{}, err
		}
	}
	if !legacy && !metadata && !sparse {
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
	return buildColumnPrimaryRowLocatorTableForOperation(columnRowLocatorOperation{plan.Rows, plan.Operation, plan.UpdatedActiveManifest.Generation, plan.AppliedCommandLSN}, documents)
}

// Coordinates are operation facts. They confer no installed resource/manifest
// authority and never require a fabricated public ColumnPublishPlan.
type columnRowLocatorOperation struct {
	rows       int
	operation  ColumnPublishOperation
	generation uint64
	commandLSN uint64
}

func buildColumnPrimaryRowLocatorTableForOperation(plan columnRowLocatorOperation, documents []columnWriteDocument) (memtable.Table, error) {
	if len(documents) != plan.rows {
		return nil, fmt.Errorf("collections: row locator documents=%d rows=%d", len(documents), plan.rows)
	}
	table := newCollectionRunTable(len(documents))
	for row, document := range documents {
		if len(document.ID) == 0 {
			return nil, fmt.Errorf("collections: row locator row %d missing document id", row)
		}
		if plan.operation == ColumnPublishOperationDelete {
			table.DeleteSteal(append([]byte(nil), document.ID...))
			continue
		}
		ref := DocumentRowRef{DocumentID: document.ID, Generation: plan.generation, PartID: columnPhysicalRowAssetPartID, RowIndex: row, AppliedCommandLSN: plan.commandLSN}
		encoded := encodeColumnPrimaryRowLocator(ref)
		if document.fieldSources != nil {
			var err error
			encoded, err = encodeColumnFieldRowLocator(ref, document.fieldSources)
			if err != nil {
				return nil, err
			}
		} else if document.preserved != nil {
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

// CRL3 stores a flattened, schema-ordinal field map. A zero source is replaced
// by this publication's latest row; all other coordinates remain exact.
func compareColumnCoordinates(a, b columnRowCoordinates) int {
	if a.Generation != b.Generation {
		if a.Generation < b.Generation {
			return -1
		}
		return 1
	}
	if a.PartID != b.PartID {
		if a.PartID < b.PartID {
			return -1
		}
		return 1
	}
	if a.RowIndex != b.RowIndex {
		if a.RowIndex < b.RowIndex {
			return -1
		}
		return 1
	}
	if a.AppliedCommandLSN != b.AppliedCommandLSN {
		if a.AppliedCommandLSN < b.AppliedCommandLSN {
			return -1
		}
		return 1
	}
	return 0
}

func encodeColumnFieldRowLocator(latest DocumentRowRef, fields []columnRowCoordinates) ([]byte, error) {
	if len(fields) == 0 || uint64(len(fields)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("collections: invalid sparse field count")
	}
	current := columnCoordinates(latest)
	groups := make([]columnRowCoordinates, 0, len(fields))
	for _, c := range fields {
		if c == (columnRowCoordinates{}) {
			c = current
		}
		var raw [32]byte
		writeColumnRowCoordinates(raw[:], c)
		if _, err := decodeColumnRowCoordinates(latest.DocumentID, raw[:]); err != nil {
			return nil, err
		}
		if c != current && (c.Generation >= current.Generation || c.AppliedCommandLSN >= current.AppliedCommandLSN) {
			return nil, fmt.Errorf("collections: sparse field source must precede latest row")
		}
		groups = append(groups, c)
	}
	slices.SortFunc(groups, compareColumnCoordinates)
	groups = slices.Compact(groups)
	b := make([]byte, 44+32*len(groups)+4*len(fields))
	writeColumnPrimaryRowLocator(b, latest)
	b[3] = '3'
	binary.BigEndian.PutUint32(b[36:], uint32(len(fields)))
	binary.BigEndian.PutUint32(b[40:], uint32(len(groups)))
	for i, c := range groups {
		writeColumnRowCoordinates(b[44+i*32:], c)
	}
	for i, c := range fields {
		if c == (columnRowCoordinates{}) {
			c = current
		}
		j, _ := slices.BinarySearchFunc(groups, c, compareColumnCoordinates)
		binary.BigEndian.PutUint32(b[44+32*len(groups)+i*4:], uint32(j))
	}
	return b, nil
}

// expectedSchema is -1 only while validating an untyped locator envelope.
// Counts and exact encoded length are proved before allocating owned coordinates.
// validateColumnFieldLocator validates borrowed canonical bytes without
// allocating schema-sized backing before the captured reader admits its credit.
// The all-groups-used check scans the bounded ordinal section per source group;
// this work is charged to the public read (O(schema*groups), groups<=schema).
func validateColumnFieldLocator(id, raw []byte, expectedSchema int) (DocumentRowRef, error) {
	if len(raw) < 44 || string(raw[:4]) != "CRL3" {
		return DocumentRowRef{}, fmt.Errorf("collections: invalid sparse row locator")
	}
	s, g := uint64(binary.BigEndian.Uint32(raw[36:])), uint64(binary.BigEndian.Uint32(raw[40:]))
	if s == 0 || g == 0 || g > s || s > uint64(len(raw))/4 || 44+32*g+4*s != uint64(len(raw)) || expectedSchema >= 0 && s != uint64(expectedSchema) {
		return DocumentRowRef{}, fmt.Errorf("collections: invalid sparse locator schema/source counts")
	}
	latest, err := decodeColumnRowCoordinates(id, raw[4:36])
	if err != nil {
		return DocumentRowRef{}, err
	}
	current := columnCoordinates(latest)
	var previous columnRowCoordinates
	for i := 0; i < int(g); i++ {
		ref, err := decodeColumnRowCoordinates(id, raw[44+i*32:44+(i+1)*32])
		if err != nil {
			return DocumentRowRef{}, err
		}
		c := columnCoordinates(ref)
		if i > 0 && compareColumnCoordinates(previous, c) >= 0 {
			return DocumentRowRef{}, fmt.Errorf("collections: noncanonical sparse source order")
		}
		if c != current && (c.Generation >= current.Generation || c.AppliedCommandLSN >= current.AppliedCommandLSN) {
			return DocumentRowRef{}, fmt.Errorf("collections: sparse source does not precede latest")
		}
		previous = c
	}
	start := 44 + 32*int(g)
	for i := 0; i < int(s); i++ {
		if uint64(binary.BigEndian.Uint32(raw[start+i*4:])) >= g {
			return DocumentRowRef{}, fmt.Errorf("collections: sparse source ordinal outside groups")
		}
	}
	for j := 0; j < int(g); j++ {
		used := false
		for i := 0; i < int(s); i++ {
			if binary.BigEndian.Uint32(raw[start+i*4:]) == uint32(j) {
				used = true
				break
			}
		}
		if !used {
			return DocumentRowRef{}, fmt.Errorf("collections: unused sparse source group")
		}
	}
	return latest, nil
}

func decodeColumnFieldSources(id, raw []byte, expectedSchema int) ([]columnRowCoordinates, error) {
	_, fields, err := decodeColumnFieldLocator(id, raw, expectedSchema)
	return fields, err
}

// decodeColumnFieldLocator allocates only the final field-coordinate backing;
// group coordinates remain borrowed raw bytes, never a second decoded arena.
func decodeColumnFieldLocator(id, raw []byte, expectedSchema int) (DocumentRowRef, []columnRowCoordinates, error) {
	latest, err := validateColumnFieldLocator(id, raw, expectedSchema)
	if err != nil {
		return DocumentRowRef{}, nil, err
	}
	s, g := int(binary.BigEndian.Uint32(raw[36:])), int(binary.BigEndian.Uint32(raw[40:]))
	if uint64(s) > uint64(^uint(0)>>1)/32 {
		return DocumentRowRef{}, nil, fmt.Errorf("collections: sparse field backing overflows int")
	}
	fields := make([]columnRowCoordinates, s)
	start := 44 + 32*g
	for i := range fields {
		j := int(binary.BigEndian.Uint32(raw[start+i*4:]))
		ref, err := decodeColumnRowCoordinates(id, raw[44+j*32:44+(j+1)*32])
		if err != nil {
			return DocumentRowRef{}, nil, err
		}
		fields[i] = columnCoordinates(ref)
	}
	return latest, fields, nil
}

// columnLocatorOperationShape uses only existing scalar coordinates. Its
// quadratic duplicate count is bounded by the admitted schema; it does not
// materialize a speculative locator, source table or temporary encoded row.
func columnLocatorOperationShape(op columnRowLocatorOperation, documents []columnWriteDocument, row int) (uint64, error) {
	d := documents[row]
	if len(d.ID) == 0 {
		return 0, ErrTypedStringPatchInvalid
	}
	if op.operation == ColumnPublishOperationDelete {
		return 0, nil
	}
	current := columnRowCoordinates{op.generation, columnPhysicalRowAssetPartID, row, op.commandLSN}
	var raw [32]byte
	writeColumnRowCoordinates(raw[:], current)
	if _, err := decodeColumnRowCoordinates(d.ID, raw[:]); err != nil {
		return 0, err
	}
	if d.fieldSources == nil {
		if d.preserved != nil {
			return columnPrimaryRowLocatorValueSize + 32, nil
		}
		return columnPrimaryRowLocatorValueSize, nil
	}
	if len(d.fieldSources) == 0 || uint64(len(d.fieldSources)) > math.MaxUint32 {
		return 0, ErrTypedStringPatchInvalid
	}
	groups := uint64(0)
	for i, c := range d.fieldSources {
		if c == (columnRowCoordinates{}) {
			c = current
		}
		writeColumnRowCoordinates(raw[:], c)
		if _, err := decodeColumnRowCoordinates(d.ID, raw[:]); err != nil {
			return 0, err
		}
		if c != current && (c.Generation >= current.Generation || c.AppliedCommandLSN >= current.AppliedCommandLSN) {
			return 0, ErrTypedStringPatchInvalid
		}
		unique := true
		for _, previous := range d.fieldSources[:i] {
			if previous == (columnRowCoordinates{}) {
				previous = current
			}
			if c == previous {
				unique = false
				break
			}
		}
		if unique {
			groups++
		}
	}
	return 44 + 32*groups + 4*uint64(len(d.fieldSources)), nil
}

// buildColumnPrimaryRowLocatorOwnedBatch is the same CRL1/2/3 encoder over a
// complete sorted operation. All actual birth classes (including temporary
// group coordinates and both entry arrays) are debited before the first copy.
// The returned batch borrows only this call's owned key/value backing, never an
// arena/table pool. Its caller keeps it through synchronous publication terminal.
func buildColumnPrimaryRowLocatorOwnedBatch(op columnRowLocatorOperation, documents []columnWriteDocument, baseRoot uint64, policy backenddb.OrderedRootStoragePolicy, account rootpublication.StableMetadataAccount) (backenddb.OrderedRootDeltaBatchPublishInput, error) {
	if len(documents) != op.rows || op.rows < 0 {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrTypedStringPatchInvalid
	}
	if err := validateColumnPublishOperation(op.operation); err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, err
	}
	var total uint64
	add := func(raw uint64, scan bool) error {
		class, err := rootpublication.StableBackingClassBytes(raw, scan)
		if err != nil || class > math.MaxUint64-total {
			return ErrPreparedInsertResourceLimit
		}
		total += class
		return nil
	}
	if uint64(len(documents)) > math.MaxUint64/uint64(unsafe.Sizeof(batch.Entry{})) {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
	}
	// Scratch entries and the exact Batch entries are separate actual arrays.
	for range 2 {
		if err := add(uint64(len(documents))*uint64(unsafe.Sizeof(batch.Entry{})), true); err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
	}
	if err := add(uint64(unsafe.Sizeof(batch.Batch{})), true); err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, err
	}
	for row, d := range documents {
		if row > 0 && bytes.Compare(documents[row-1].ID, d.ID) >= 0 {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrTypedStringPatchInvalid
		}
		width, err := columnLocatorOperationShape(op, documents, row)
		if err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
		if err = add(uint64(len(d.ID)), false); err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
		if err = add(width, false); err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
		if d.fieldSources != nil && op.operation != ColumnPublishOperationDelete {
			if uint64(len(d.fieldSources)) > math.MaxUint64/uint64(unsafe.Sizeof(columnRowCoordinates{})) {
				return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
			}
			if err = add(uint64(len(d.fieldSources))*uint64(unsafe.Sizeof(columnRowCoordinates{})), false); err != nil {
				return backenddb.OrderedRootDeltaBatchPublishInput{}, err
			}
		}
	}
	if account != nil {
		if err := account.ReserveStableMetadata(total); err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
	}
	entries := make([]batch.Entry, len(documents))
	for row, d := range documents {
		entry := &entries[row]
		entry.Key = bytes.Clone(d.ID)
		if op.operation == ColumnPublishOperationDelete {
			entry.Type = batch.OpDelete
			continue
		}
		ref := DocumentRowRef{DocumentID: d.ID, Generation: op.generation, PartID: columnPhysicalRowAssetPartID, RowIndex: row, AppliedCommandLSN: op.commandLSN}
		var err error
		if d.fieldSources != nil {
			entry.Value, err = encodeColumnFieldRowLocator(ref, d.fieldSources)
		} else if d.preserved != nil {
			entry.Value = encodeColumnMetadataRowLocator(ref, *d.preserved)
		} else {
			entry.Value = encodeColumnPrimaryRowLocator(ref)
		}
		if err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
	}
	delta, err := batch.NewOwnedPointBatch(entries, nil) // both birth classes prepaid above
	if err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, err
	}
	return backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: baseRoot, Delta: delta, StoragePolicy: policy}, nil
}

// validateColumnPrimaryRowLocatorOwnedBatch checks the same CRL1/2/3 grammar
// against actual operation facts without copying, sorting or making group arrays.
// This does not grant a resource pin or replace the installed durability closure.
func validateColumnPrimaryRowLocatorOwnedBatch(op columnRowLocatorOperation, documents []columnWriteDocument, owned *batch.Batch) error {
	if owned == nil || op.rows != len(documents) || op.rows < 0 {
		return errColumnNativeContextIncomplete
	}
	entries := owned.SortedEntries()
	if len(entries) != len(documents) {
		return errColumnNativeContextIncomplete
	}
	for row, d := range documents {
		e := entries[row]
		width, err := columnLocatorOperationShape(op, documents, row)
		if err != nil {
			return err
		}
		if e.IsPtr || !bytes.Equal(e.Key, d.ID) || uint64(len(e.Value)) != width {
			return errColumnNativeContextIncomplete
		}
		if op.operation == ColumnPublishOperationDelete {
			if e.Type != batch.OpDelete {
				return errColumnNativeContextIncomplete
			}
			continue
		}
		if e.Type != batch.OpPut {
			return errColumnNativeContextIncomplete
		}
		var latest [columnPrimaryRowLocatorValueSize]byte
		writeColumnPrimaryRowLocator(latest[:], DocumentRowRef{DocumentID: d.ID, Generation: op.generation, PartID: columnPhysicalRowAssetPartID, RowIndex: row, AppliedCommandLSN: op.commandLSN})
		if d.fieldSources != nil {
			latest[3] = '3'
		} else if d.preserved != nil {
			latest[3] = '2'
		}
		if !bytes.Equal(latest[:], e.Value[:columnPrimaryRowLocatorValueSize]) {
			return errColumnNativeContextIncomplete
		}
		if d.fieldSources != nil {
			if _, err := validateColumnFieldLocator(d.ID, e.Value, len(d.fieldSources)); err != nil {
				return err
			}
			groups := int(binary.BigEndian.Uint32(e.Value[40:]))
			current := columnRowCoordinates{op.generation, columnPhysicalRowAssetPartID, row, op.commandLSN}
			for ordinal, source := range d.fieldSources {
				if source == (columnRowCoordinates{}) {
					source = current
				}
				index := int(binary.BigEndian.Uint32(e.Value[44+32*groups+4*ordinal:]))
				var encoded [32]byte
				writeColumnRowCoordinates(encoded[:], source)
				if !bytes.Equal(encoded[:], e.Value[44+32*index:44+32*(index+1)]) {
					return errColumnNativeContextIncomplete
				}
			}
		} else if d.preserved != nil {
			var encoded [32]byte
			writeColumnRowCoordinates(encoded[:], *d.preserved)
			if !bytes.Equal(encoded[:], e.Value[36:]) {
				return errColumnNativeContextIncomplete
			}
		}
	}
	return nil
}
