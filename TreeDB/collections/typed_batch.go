package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// TypedColumnBatch supplies one required, non-null declared column. Exactly one
// carrier is used: Strings for string columns, Float32Vectors for vector columns.
type TypedColumnBatch struct {
	Name           string
	Strings        []string
	Float32Vectors [][]float32
}

func newTrustedTypedProjection(meta CollectionMeta, ids, retained [][]byte, columns []TypedColumnBatch) (*trustedFloat32Projection, error) {
	cfg := meta.Options.ColumnStore
	if cfg == nil || len(ids) != len(retained) || len(columns) != len(cfg.Columns) {
		return nil, errors.New("collections: typed batch counts do not match schema")
	}
	if len(columns) == 0 || len(ids) > int(^uint(0)>>1)/len(columns) {
		return nil, errors.New("collections: typed batch value count overflow")
	}
	p := &trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash, typedRows: make(map[string][]columnDeclaredValue, len(ids)), retainedJSON: retained}
	if err := validateTypedProjectionMeta(meta, p); err != nil {
		return nil, err
	}
	values := make([]columnDeclaredValue, len(ids)*len(columns))
	for row, id := range ids {
		if len(id) == 0 {
			return nil, errors.New("collections: typed batch ID is empty")
		}
		key := string(id)
		if _, exists := p.typedRows[key]; exists {
			return nil, ErrDuplicateDocumentID
		}
		p.typedRows[key] = values[row*len(columns) : (row+1)*len(columns)]
	}
	seen := make([]bool, len(columns))
	for _, input := range columns {
		colIndex := -1
		for i, col := range cfg.Columns {
			if input.Name == col.Name {
				colIndex = i
				break
			}
		}
		if colIndex < 0 || seen[colIndex] {
			return nil, errors.New("collections: unknown or duplicate typed column")
		}
		seen[colIndex] = true
		col := cfg.Columns[colIndex]
		if col.ValueType == ColumnStoreValueString {
			if input.Float32Vectors != nil || len(input.Strings) != len(ids) {
				return nil, errors.New("collections: typed string carrier mismatch")
			}
			for row, s := range input.Strings {
				if !utf8.ValidString(s) {
					return nil, errors.New("collections: typed string is not UTF-8")
				}
				values[row*len(columns)+colIndex] = columnDeclaredValue{Type: col.ValueType, Present: true, String: strings.Clone(s)}
			}
		} else {
			if input.Strings != nil || len(input.Float32Vectors) != len(ids) {
				return nil, errors.New("collections: typed vector carrier mismatch")
			}
			if col.VectorDims <= 0 || len(ids) > int(^uint(0)>>1)/col.VectorDims {
				return nil, errors.New("collections: typed batch vector size overflow")
			}
			for _, vector := range input.Float32Vectors {
				if len(vector) != col.VectorDims {
					return nil, errors.New("collections: typed vector dimensions mismatch")
				}
			}
			owned := make([]float32, len(ids)*col.VectorDims)
			for row, vector := range input.Float32Vectors {
				if len(vector) != col.VectorDims {
					return nil, errors.New("collections: typed vector dimensions mismatch")
				}
				if err := validateFloat32Vector(vector); err != nil {
					return nil, err
				}
				for _, index := range meta.VectorIndexes {
					if index.Field == col.Path && index.Metric == VectorMetricCosine && vectorNormSquared(vector) == 0 {
						return nil, errors.New("collections: typed cosine vector has zero magnitude")
					}
				}
				v := owned[row*col.VectorDims : (row+1)*col.VectorDims : (row+1)*col.VectorDims]
				copy(v, vector)
				values[row*len(columns)+colIndex] = columnDeclaredValue{Type: col.ValueType, Present: true, Float32Vector: v}
			}
		}
	}
	if _, err := validateTrustedFloat32ProjectionRetainedJSONWithOwnership(ids, p, false); err != nil {
		return nil, err
	}
	return p, nil
}

// InsertTypedBatchWithStats inserts authoritative declared strings and FP32
// vectors, with retained JSON kept separately for document output. It requires
// command WAL and non-column retained JSON; acknowledgements follow the selected
// durability profile. Every declared column
// must be supplied, non-null, with one value per ID. Retained JSON must contain
// the matching id and no declared paths. Inputs may be reused after return.
// Scalar/text indexes are maintained immediately; column_graph serving remains
// subject to its existing explicit rebuild and insert-only base restrictions.
// Registered ad-hoc vector runtimes are not supported by this typed admission.
func (c *Collection) InsertTypedBatchWithStats(ids, retained [][]byte, columns []TypedColumnBatch) ([][]byte, CollectionInsertStats, error) {
	if c == nil || c.db == nil {
		return nil, CollectionInsertStats{}, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, CollectionInsertStats{}, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return nil, CollectionInsertStats{}, err
	}
	p, err := newTrustedTypedProjection(c.Meta(), ids, retained, columns)
	if err != nil {
		return nil, CollectionInsertStats{}, err
	}
	var stats CollectionInsertStats
	out, err := c.insertBatchWithCommandWALIntentSchemaLocked(ids, retained, false, nil, nil, insertBatchExecutionOptions{returnResultIDs: true, insertStats: &stats, trustedFloat32Projection: p})
	if err == nil {
		err = commitAmbiguousError("typed batch vector maintenance", c.notifyVectorIndexesUpsert(out))
	}
	return out, stats, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

func typedReplacementItems(ids, retained [][]byte, projection *trustedFloat32Projection) []updateBatchItem {
	items := make([]updateBatchItem, len(ids))
	for i := range ids {
		items[i] = updateBatchItem{UpdateBatchItem: UpdateBatchItem{DocumentID: bytes.Clone(ids[i])}, typedProjection: projection, typedRetained: bytes.Clone(retained[i])}
	}
	return items
}

func (v *CollectionReadView) typedReplacementValuesEqual(id []byte, wanted []columnDeclaredValue) (bool, error) {
	cfg := *v.catalog.meta.Options.ColumnStore
	refs, err := v.lookupDocumentRowRefsByID([][]byte{id}, DocumentFetchOptions{})
	if err != nil {
		return false, err
	}
	if !refs.Results[0].Found {
		return false, errors.New("collections: typed replacement old row missing")
	}
	if err := v.ensureAssetReadCaches(cfg, ColumnAssetReadIntegrityVerify); err != nil {
		return false, err
	}
	physical, err := v.materializerColumnSnapshotView(cfg)
	if err != nil {
		return false, err
	}
	projection, err := v.pointRowScanProjection(physical, nil)
	if err != nil {
		return false, err
	}
	var scratch columnPhysicalRowReaderScratch
	row, err := v.fetchDocumentPointRow(physical, refs.Results[0].RowRef, projection, &scratch, nil)
	if err != nil {
		return false, err
	}
	cache := v.typedColumnReconstructionCacheForConfig(cfg)
	typed, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(v.snapshot, v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name)), cfg, row, cache, nil)
	if err != nil {
		return false, err
	}
	values, err := mergeColumnReconstructionValues(cfg, row.Values, typed.Values)
	if err != nil {
		return false, err
	}
	if len(values) != len(wanted) {
		return false, errors.New("collections: typed replacement old column count mismatch")
	}
	for i, value := range values {
		if value.Type != wanted[i].Type || value.Present != wanted[i].Present || value.Null != wanted[i].Null {
			return false, nil
		}
		switch value.Type {
		case ColumnStoreValueString:
			if value.StringBytes != nil {
				if !bytes.Equal(value.StringBytes, []byte(wanted[i].String)) {
					return false, nil
				}
			} else if value.String != wanted[i].String {
				return false, nil
			}
		case ColumnStoreValueFloat32Vector:
			if !slices.EqualFunc(value.Float32Vector, wanted[i].Float32Vector, func(a, b float32) bool { return math.Float32bits(a) == math.Float32bits(b) }) {
				return false, nil
			}
		default:
			return false, errors.New("collections: unsupported typed replacement old value")
		}
	}
	return true, nil
}

// ReplaceTypedBatch replaces existing IDs using the same typed contract as
// InsertTypedBatchWithStats. Missing IDs are unmatched; equal retained bytes and
// typed values (FP32 vectors compared bitwise) are unmodified. Unique checks and
// all replacements are atomic.
// A commit-ambiguous error can mean the durable mutation was accepted.
func (c *Collection) ReplaceTypedBatch(ids, retained [][]byte, columns []TypedColumnBatch) ([]UpdateBatchResult, error) {
	if c == nil || c.db == nil {
		return nil, errCollectionNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	if err := c.requireTypedBatchVectorAdmission(); err != nil {
		return nil, err
	}
	projection, err := newTrustedTypedProjection(c.Meta(), ids, retained, columns)
	if err != nil {
		return nil, err
	}
	results, _, err := c.updateBatchOwnedItemsWithCommandWALIntent(typedReplacementItems(ids, retained, projection), updateBatchModeAny, nil)
	return results, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// Called with schema and ad-hoc admission read locks held, so registration
// cannot introduce a document-extracting runtime during typed admission.
func (c *Collection) requireTypedBatchVectorAdmission() error {
	if coord := c.collectionSchemaCoordinator(); coord != nil && coord.adHocVectorIndexes.Load() != 0 {
		return errors.New("collections: typed batches do not support registered ad-hoc vector runtimes")
	}
	if len(c.registeredVectorIndexes()) != 0 {
		return errors.New("collections: typed batches require column_graph vector indexes")
	}
	return nil
}

func columnStoreTypedScalarIndexesSupported(meta CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload != ColumnRetainedPayloadNonColumn {
		return false
	}
	return validateTypedProjectionMeta(meta, &trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash}) == nil
}

func validateTypedProjectionMeta(meta CollectionMeta, p *trustedFloat32Projection) error {
	cfg := meta.Options.ColumnStore
	if !columnStoreWriteEnabled(meta) || cfg == nil || cfg.SchemaHash != p.schemaHash || len(cfg.Columns) != len(p.columns) {
		return errors.New("collections: typed batch schema mismatch")
	}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON || cfg.RetainedPayload != ColumnRetainedPayloadNonColumn || columnRetainedPayloadEffectiveEncoding(cfg) != ColumnRetainedPayloadEncodingJSON {
		return errors.New("collections: typed batch requires non-column JSON retained payload")
	}
	for i, c := range cfg.Columns {
		if c != p.columns[i] || c.Nullable || !utf8.ValidString(c.Name) || !utf8.ValidString(c.Path) {
			return errors.New("collections: typed batch requires matching non-null columns")
		}
		owner, err := columnStoreColumnOwner(c)
		if err != nil {
			return err
		}
		if !((c.ValueType == ColumnStoreValueString && owner == TypedStorageOwnerRowAsset) || (c.ValueType == ColumnStoreValueFloat32Vector && owner == TypedStorageOwnerColumnPart)) {
			return fmt.Errorf("collections: typed batch unsupported column %q", c.Name)
		}
	}
	for _, index := range meta.Indexes {
		if index.ValueType != IndexValueString || index.MultiKey || len(index.Components) != 0 || typedStringColumnIndex(cfg.Columns, index.Field) < 0 {
			return fmt.Errorf("collections: typed batch unsupported scalar index %q", index.Name)
		}
	}
	for _, index := range meta.TextIndexes {
		for _, field := range index.Fields {
			if typedStringColumnIndex(cfg.Columns, field.Field) < 0 {
				return fmt.Errorf("collections: typed batch unsupported text field %q", field.Field)
			}
		}
	}
	for _, index := range meta.VectorIndexes {
		if index.Strategy != VectorIndexStrategyColumnGraph {
			return errors.New("collections: typed batch requires column_graph vector indexes")
		}
		found := false
		for _, c := range cfg.Columns {
			if c.Path == index.Field && c.ValueType == ColumnStoreValueFloat32Vector && c.VectorDims == index.Dimensions {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("collections: typed batch unmatched vector index %q", index.Name)
		}
	}
	return nil
}

func typedStringColumnIndex(columns []ColumnStoreColumn, path string) int {
	for i, c := range columns {
		if c.Path == path && c.ValueType == ColumnStoreValueString {
			return i
		}
	}
	return -1
}

func typedProjectionFromPayload(meta CollectionMeta, payload commitlog.CollectionTypedBatchPayload) (*trustedFloat32Projection, [][]byte, [][]byte, error) {
	cfg := meta.Options.ColumnStore
	if cfg == nil || payload.Collection != meta.Name || payload.SchemaHash != cfg.SchemaHash || len(payload.Columns) != len(cfg.Columns) {
		return nil, nil, nil, errors.New("collections: typed command schema mismatch")
	}
	p := &trustedFloat32Projection{columns: cfg.Columns, schemaHash: cfg.SchemaHash, typedRows: make(map[string][]columnDeclaredValue, len(payload.Documents))}
	if payload.LegacyProjection {
		if len(cfg.Columns) != 1 || cfg.Columns[0].ValueType != ColumnStoreValueFloat32Vector {
			return nil, nil, nil, errors.New("collections: invalid legacy projection columns")
		}
		p.legacyTyped, p.column = true, cfg.Columns[0].Name
		if len(meta.VectorIndexes) == 1 {
			p.metric = meta.VectorIndexes[0].Metric
		}
	}
	if err := validateTrustedFloat32ProjectionMeta(meta, p); err != nil {
		return nil, nil, nil, err
	}
	positions := make([]int, len(cfg.Columns))
	for i, col := range cfg.Columns {
		j := sort.Search(len(payload.Columns), func(j int) bool { return payload.Columns[j].Name >= col.Name })
		if j == len(payload.Columns) || payload.Columns[j].Name != col.Name {
			return nil, nil, nil, errors.New("collections: typed command column mismatch")
		}
		c := payload.Columns[j]
		if (col.ValueType == ColumnStoreValueString && c.Type != commitlog.CollectionTypedString) || (col.ValueType == ColumnStoreValueFloat32Vector && (c.Type != commitlog.CollectionTypedFloat32Vector || int(c.Dimensions) != col.VectorDims)) {
			return nil, nil, nil, errors.New("collections: typed command value type mismatch")
		}
		positions[i] = j
	}
	ids, docs := make([][]byte, len(payload.Documents)), make([][]byte, len(payload.Documents))
	values := make([]columnDeclaredValue, len(payload.Documents)*len(cfg.Columns))
	for row, d := range payload.Documents {
		ids[row], docs[row] = d.ID, d.Retained
		v := values[row*len(cfg.Columns) : (row+1)*len(cfg.Columns)]
		for i, col := range cfg.Columns {
			value := d.Values[positions[i]]
			if col.ValueType == ColumnStoreValueString && !utf8.ValidString(value.String) {
				return nil, nil, nil, errors.New("collections: typed string is not UTF-8")
			}
			v[i] = columnDeclaredValue{Type: col.ValueType, Present: true, String: value.String, Float32Vector: value.Vector}
			if col.ValueType == ColumnStoreValueFloat32Vector {
				for _, index := range meta.VectorIndexes {
					if index.Field == col.Path && index.Metric == VectorMetricCosine && vectorNormSquared(value.Vector) == 0 {
						return nil, nil, nil, errors.New("collections: typed cosine vector has zero magnitude")
					}
				}
			}
		}
		p.typedRows[string(d.ID)] = v
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn {
		p.retainedJSON = docs
	}
	if !p.legacyTyped {
		if _, err := validateTrustedFloat32ProjectionRetainedJSONWithOwnership(ids, p, false); err != nil {
			return nil, nil, nil, err
		}
	}
	return p, ids, docs, nil
}

func typedCommandPayload(meta CollectionMeta, docs []commitlog.CollectionDocument, p *trustedFloat32Projection) (commitlog.CollectionTypedBatchPayload, error) {
	cfg := meta.Options.ColumnStore
	if cfg == nil {
		return commitlog.CollectionTypedBatchPayload{}, errors.New("collections: typed command missing columns")
	}
	// Legacy vector-only callers already have ordered owned vectors. Only build
	// an ID permutation when the planner changed input order.
	var legacyPositions map[string]int
	if p.typedRows == nil {
		ordered := len(docs) == len(p.ids)
		for i := 0; ordered && i < len(docs); i++ {
			ordered = bytes.Equal(docs[i].ID, p.ids[i])
		}
		if !ordered {
			var err error
			legacyPositions, err = p.legacyVectorRows(p.ids)
			if err != nil {
				return commitlog.CollectionTypedBatchPayload{}, err
			}
		}
	}
	columns := cfg.Columns
	if len(columns) > 1 {
		columns = append([]ColumnStoreColumn(nil), columns...)
		sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
	}
	if p.preparedRetainedJSON != nil && (len(p.preparedRetainedJSON) != len(docs) || len(p.preparedRetainedDocuments) != len(docs)) {
		return commitlog.CollectionTypedBatchPayload{}, errors.New("collections: typed command prepared retained row count mismatch")
	}
	payload := commitlog.CollectionTypedBatchPayload{LegacyProjection: p.columns == nil || p.legacyTyped, Collection: meta.Name, SchemaHash: cfg.SchemaHash, Columns: make([]commitlog.CollectionTypedColumn, len(columns)), Documents: make([]commitlog.CollectionTypedDocument, len(docs))}
	positions := make([]int, len(columns))
	for i, col := range columns {
		c := commitlog.CollectionTypedColumn{Name: col.Name}
		if col.ValueType == ColumnStoreValueString {
			c.Type = commitlog.CollectionTypedString
		} else if col.ValueType == ColumnStoreValueFloat32Vector {
			c.Type = commitlog.CollectionTypedFloat32Vector
			c.Dimensions = uint32(col.VectorDims)
		} else {
			return payload, errors.New("collections: unsupported typed command column")
		}
		payload.Columns[i] = c
		for j, original := range cfg.Columns {
			if original.Name == col.Name {
				positions[i] = j
				break
			}
		}
	}
	values := make([]commitlog.CollectionTypedValue, len(docs)*len(columns))
	for row, d := range docs {
		v := values[row*len(columns) : (row+1)*len(columns)]
		legacyRow := row
		if p.typedRows == nil {
			if legacyPositions != nil {
				var ok bool
				legacyRow, ok = legacyPositions[string(d.ID)]
				if !ok {
					return payload, errors.New("collections: typed command missing row")
				}
			}
			v[0].Vector = p.vectors[legacyRow]
		} else {
			declared, ok := p.typedRows[string(d.ID)]
			if !ok {
				return payload, errors.New("collections: typed command missing row")
			}
			for i := range columns {
				src := declared[positions[i]]
				v[i] = commitlog.CollectionTypedValue{String: src.String, Vector: src.Float32Vector}
			}
		}
		retained := d.Document
		if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && p.columns == nil {
			if p.retainedJSON != nil {
				retained = p.retainedJSON[legacyRow]
			} else if p.preparedRetainedJSON != nil {
				if !bytes.Equal(p.preparedRetainedDocuments[row].ID, d.ID) {
					return payload, errors.New("collections: typed command prepared retained row id mismatch")
				}
				retained = p.preparedRetainedJSON[row]
			} else {
				var err error
				retained, err = columnRetainedPayloadJSONFromJSONDocument(*cfg, d.Document)
				if err != nil {
					return payload, err
				}
			}
		}
		payload.Documents[row] = commitlog.CollectionTypedDocument{ID: d.ID, Retained: retained, Values: v}
	}
	return payload, nil
}

func validateTypedRetainedColumn(document []byte, columnPath string, path []string) error {
	seen := false
	return jsonparser.ObjectEach(document, func(key, value []byte, typ jsonparser.ValueType, _ int) error {
		if string(key) != path[0] {
			return nil
		}
		if seen || len(path) == 1 || typ != jsonparser.Object {
			return fmt.Errorf("collections: retained JSON overlaps or duplicates declared path %q", columnPath)
		}
		seen = true
		return validateTypedRetainedColumn(value, columnPath, path[1:])
	})
}

func applyTypedProjection(ids [][]byte, documents []columnWriteDocument, p *trustedFloat32Projection) error {
	if len(ids) != len(documents) || len(p.typedRows) != len(documents) {
		return errors.New("collections: typed batch row count mismatch")
	}
	return applyTypedProjectionSubset(ids, documents, p)
}

func applyTypedProjectionSubset(ids [][]byte, documents []columnWriteDocument, p *trustedFloat32Projection) error {
	if len(ids) != len(documents) {
		return errors.New("collections: typed batch row count mismatch")
	}
	for row := range documents {
		v, ok := p.typedRows[string(documents[row].ID)]
		if !ok {
			return errors.New("collections: typed batch ID mismatch")
		}
		documents[row].declaredValues, documents[row].declaredValuesReady = v, true
		documents[row].reconstructFromRetained = p.retainedJSON != nil
	}
	return nil
}

func typedIndexState(values []columnDeclaredValue, columns []ColumnStoreColumn, runtimes []indexRuntime, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	state := encoder.appendState(len(runtimes))
	for i, runtime := range runtimes {
		j := typedStringColumnIndex(columns, runtime.def.field)
		if j < 0 || j >= len(values) || !values[j].Present || values[j].Null {
			return nil, errors.New("collections: missing typed scalar index value")
		}
		start := len(encoder.buf)
		encoder.buf = appendIndexStringComponent(encoder.buf, []byte(values[j].String))
		state[i] = encoder.appendSingleValueRef(encoder.buf[start:])
	}
	return state, nil
}

func typedTextAnalysis(def TextIndexDefinition, values []columnDeclaredValue, columns []ColumnStoreColumn) (textAnalyzedDocument, error) {
	out := textAnalyzedDocument{Fields: make([]textAnalyzedField, 0, len(def.Fields))}
	for _, f := range def.Fields {
		i := typedStringColumnIndex(columns, f.Field)
		if i < 0 || i >= len(values) || !values[i].Present || values[i].Null {
			return out, errors.New("collections: missing typed text value")
		}
		text := values[i].String
		if values[i].StringBytes != nil {
			text = string(values[i].StringBytes)
		}
		field, ok, err := analyzeTextIndexField(def, f.Field, text)
		if err != nil {
			return out, err
		}
		if ok {
			out.Fields = append(out.Fields, field)
		}
	}
	return out, nil
}

// Reuse the published row locator and the existing bounded asset read view.
// Text v2 needs old terms for statistics/positions, but must not derive them
// from a retained document that deliberately excludes the declared strings.
func loadTypedTextOldStates(snap *backenddb.Snapshot, catalog *collectionCatalog, opts collectionOptions, def TextIndexDefinition, mutations []textDocumentMutation) ([]textDocumentStateValue, error) {
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload != ColumnRetainedPayloadNonColumn {
		return nil, nil
	}
	columns := columnStoreRowAssetColumns(*cfg)
	for _, field := range def.Fields {
		if typedStringColumnIndex(columns, field.Field) < 0 {
			return nil, nil
		}
	}
	var ids [][]byte
	for _, mutation := range mutations {
		if mutation.deleteOld {
			ids = append(ids, mutation.documentID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	view := newCollectionReadViewAtSnapshot(&Collection{db: opts.db}, snap, catalog, false, "")
	defer view.Close()
	refs, err := view.lookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
	if err != nil {
		return nil, err
	}
	if err := view.ensureAssetReadCaches(*cfg, ColumnAssetReadIntegrityVerify); err != nil {
		return nil, err
	}
	physical, err := view.materializerColumnSnapshotView(*cfg)
	if err != nil {
		return nil, err
	}
	projection, err := view.pointRowScanProjection(physical, nil)
	if err != nil {
		return nil, err
	}
	states := make([]textDocumentStateValue, len(mutations))
	var scratch columnPhysicalRowReaderScratch
	refIndex := 0
	for i, mutation := range mutations {
		if !mutation.deleteOld {
			continue
		}
		ref := refs.Results[refIndex]
		refIndex++
		if !ref.Found {
			return nil, fmt.Errorf("collections: typed text old row missing for %q", mutation.documentID)
		}
		row, err := view.fetchDocumentPointRow(physical, ref.RowRef, projection, &scratch, nil)
		if err != nil {
			return nil, err
		}
		analysis, err := typedTextAnalysis(def, row.Values, columns)
		if err != nil {
			return nil, err
		}
		states[i] = textDocumentStateValueFromAnalysis(analysis)
	}
	return states, nil
}

func (p insertBatchPlanner) itemIndexState(id, document []byte, runtimes []indexRuntime, encoder *indexEncodeArena) (orderedDocumentIndexState, error) {
	if typed := p.options.typedProjection; typed != nil && typed.typedRows != nil && !typed.legacyTyped {
		values, ok := typed.typedRows[string(id)]
		if !ok {
			return nil, errors.New("collections: typed scalar row missing")
		}
		return typedIndexState(values, typed.columns, runtimes, encoder)
	}
	return orderedIndexStateForDocumentWithArena(document, runtimes, p.options, encoder)
}
