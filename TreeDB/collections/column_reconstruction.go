package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func columnStoreNeedsRetainedPayloadTransform(meta CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	return cfg != nil && cfg.Enabled && cfg.RetainedPayload != ColumnRetainedPayloadFull
}

func columnStoreCanReconstructDocument(meta CollectionMeta) bool {
	cfg := meta.Options.ColumnStore
	return cfg != nil &&
		cfg.Enabled &&
		cfg.RetainedPayload != ColumnRetainedPayloadFull &&
		cfg.Reconstruction == ColumnReconstructionRetainedPayloadAndColumns &&
		cfg.ActiveManifest != nil &&
		cfg.RecoveryAuthoritativeManifest != nil &&
		normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatJSON
}

type columnDocumentReconstructionDiagnostics struct {
	VisibilityRows       int
	ReconstructionRows   int
	PhysicalBytesScanned int64
	VisibilityNanos      int64
	ReconstructionNanos  int64
}

func columnRetainedPayloadJSONFromJSONDocument(cfg ColumnStoreConfig, document []byte) ([]byte, error) {
	switch cfg.RetainedPayload {
	case ColumnRetainedPayloadFull:
		return bytes.Clone(document), nil
	case ColumnRetainedPayloadNone:
		return []byte("{}"), nil
	case ColumnRetainedPayloadNonColumn:
		if retained, ok, err := columnRetainedPayloadTopLevelJSON(cfg, document); ok || err != nil {
			return retained, err
		}
		obj, err := columnRetainedPayloadJSONObjectFromJSONDocument(cfg, document)
		if err != nil {
			return nil, err
		}
		out, err := json.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("collections: encode retained column payload: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("collections: unsupported retained payload policy %q", cfg.RetainedPayload)
	}
}

// columnRetainedPayloadTopLevelJSON avoids decoding large declared arrays into
// interface values when every declared column is a top-level JSON member. The
// RawMessage values retain the standard JSON decoder's validation and duplicate
// key semantics while the nested-path case keeps using the established walker.
func columnRetainedPayloadTopLevelJSON(cfg ColumnStoreConfig, document []byte) ([]byte, bool, error) {
	for _, col := range cfg.Columns {
		if col.Path == "" || strings.Contains(col.Path, ".") {
			return nil, false, nil
		}
	}
	obj := make(map[string]json.RawMessage)
	if err := json.Unmarshal(document, &obj); err != nil {
		return nil, true, fmt.Errorf("collections: invalid JSON document for column retained payload: %w", err)
	}
	if obj == nil {
		return nil, true, errors.New("collections: column retained payload root must be a JSON object")
	}
	for _, col := range cfg.Columns {
		delete(obj, col.Path)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, true, fmt.Errorf("collections: encode retained column payload: %w", err)
	}
	return out, true, nil
}

func columnRetainedPayloadJSONObjectFromJSONDocument(cfg ColumnStoreConfig, document []byte) (map[string]any, error) {
	switch cfg.RetainedPayload {
	case ColumnRetainedPayloadNone:
		return map[string]any{}, nil
	case ColumnRetainedPayloadNonColumn:
		obj, err := decodeColumnJSONObject(document)
		if err != nil {
			return nil, err
		}
		for _, col := range cfg.Columns {
			deleteColumnJSONPath(obj, col.Path)
		}
		return obj, nil
	default:
		return nil, fmt.Errorf("collections: retained payload policy %q cannot produce object payload", cfg.RetainedPayload)
	}
}

func columnRetainedPayloadFromJSONDocument(cfg ColumnStoreConfig, document []byte) ([]byte, error) {
	retained, err := columnRetainedPayloadJSONFromJSONDocument(cfg, document)
	if err != nil {
		return nil, err
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingTemplateV1 {
		encoded, err := EncodeTemplateV1DocumentJSON(retained)
		if err != nil {
			return nil, fmt.Errorf("collections: encode template-v1 retained payload: %w", err)
		}
		return encoded, nil
	}
	return retained, nil
}

type columnRetainedPayloadStorageDocuments struct {
	documents                    [][]byte
	templateRecords              []templateV1Record
	semanticStreamBlocks         memtable.Table
	semanticStreamPrepareMetrics columnRetainedSemanticStreamV1PrepareMetrics
	declaredRows                 []columnDeclaredRow
	declaredRowsReady            bool
	declaredStringBackingBytes   int64
}

func prepareColumnRetainedPayloadStorageDocuments(cfg ColumnStoreConfig, documents [][]byte, fallback templateV1Resolver) (columnRetainedPayloadStorageDocuments, error) {
	out := columnRetainedPayloadStorageDocuments{documents: make([][]byte, len(documents))}
	if len(documents) == 0 {
		return out, nil
	}
	retainedJSON := make([][]byte, len(documents))
	for i, document := range documents {
		retained, err := columnRetainedPayloadJSONFromJSONDocument(cfg, document)
		if err != nil {
			return columnRetainedPayloadStorageDocuments{}, err
		}
		retainedJSON[i] = retained
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingTemplateV1 {
		encoded := make([][]byte, len(retainedJSON))
		for i, retained := range retainedJSON {
			next, err := EncodeTemplateV1DocumentJSON(retained)
			if err != nil {
				return columnRetainedPayloadStorageDocuments{}, fmt.Errorf("collections: encode template-v1 retained payload: %w", err)
			}
			encoded[i] = next
		}
		prepared, records, _, _, err := prepareTemplateV1InsertDocuments(encoded, fallback, false, false)
		if err != nil {
			return columnRetainedPayloadStorageDocuments{}, fmt.Errorf("collections: prepare template-v1 retained payload: %w", err)
		}
		out.documents = prepared
		out.templateRecords = records
		return out, nil
	}
	out.documents = retainedJSON
	return out, nil
}

func columnRetainedPayloadTemplateResolver(snap *backenddb.Snapshot, catalog *collectionCatalog) templateV1Resolver {
	if snap == nil || catalog == nil || !columnStoreRetainedPayloadUsesTemplateV1(catalog.meta.Options.ColumnStore) {
		return nil
	}
	return &templateV1SnapshotResolver{
		snap:   snap,
		rootID: catalog.rootID(collectionTemplateRootName(catalog.meta.Name)),
		byID:   make(map[uint64]*templateV1Template),
		byHash: make(map[[32]byte]*templateV1Template),
	}
}

// ColumnRetainedPayloadFromJSONDocument applies the production retained-payload
// transform used when column-store declared fields are stripped from primary
// row payloads.
func ColumnRetainedPayloadFromJSONDocument(cfg ColumnStoreConfig, document []byte) ([]byte, error) {
	return columnRetainedPayloadFromJSONDocument(cfg, document)
}

func (c *Collection) reconstructColumnDocumentAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, documentID []byte, retained []byte) ([]byte, error) {
	out, _, err := c.reconstructColumnDocumentAtSnapshotWithDiagnostics(snap, catalog, documentID, retained)
	return out, err
}

func (c *Collection) reconstructColumnDocumentAtSnapshotWithDiagnostics(snap *backenddb.Snapshot, catalog *collectionCatalog, documentID []byte, retained []byte) ([]byte, columnDocumentReconstructionDiagnostics, error) {
	var diag columnDocumentReconstructionDiagnostics
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || !catalog.meta.Options.ColumnStore.Enabled {
		return bytes.Clone(retained), diag, nil
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	if cfg.RetainedPayload == ColumnRetainedPayloadFull {
		return bytes.Clone(retained), diag, nil
	}
	resolvedRetained, err := resolveColumnRetainedPayloadAtSnapshot(snap, catalog, cfg, retained)
	if err != nil {
		return nil, diag, err
	}
	retained = resolvedRetained
	if cfg.Reconstruction != ColumnReconstructionRetainedPayloadAndColumns {
		return nil, diag, fmt.Errorf("collections: unsupported column reconstruction policy %q", cfg.Reconstruction)
	}
	layout, err := ResolveTypedStorageLayout(catalog.meta)
	if err != nil {
		return nil, diag, err
	}
	if err := layout.EnsureReadSupported(); err != nil {
		return nil, diag, err
	}
	visibilityStart := time.Now()
	row, scanDiag, found, err := c.latestColumnPhysicalVisibleRowAtSnapshot(snap, catalog, documentID, nil)
	diag.VisibilityNanos = time.Since(visibilityStart).Nanoseconds()
	diag.PhysicalBytesScanned = scanDiag.PhysicalBytesScanned
	if err != nil {
		return nil, diag, err
	}
	if !found {
		return nil, diag, fmt.Errorf("collections: column reconstruction missing visible physical row for id %q", string(documentID))
	}
	diag.VisibilityRows = 1
	if row.Deleted {
		return nil, diag, fmt.Errorf("collections: column reconstruction latest physical row is deleted for id %q", string(documentID))
	}
	reconstructionStart := time.Now()
	manifestRootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	typedValues, err := c.typedColumnPartValuesForVisibleRowAtSnapshot(snap, manifestRootID, cfg, row)
	if err != nil {
		return nil, diag, err
	}
	fullValues, err := mergeColumnReconstructionValues(cfg, row.Values, typedValues.Values)
	if err != nil {
		return nil, diag, err
	}
	_, out, err := reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(nil, cfg, retained, row, fullValues, nil, nil, columnRetainedPayloadTemplateResolver(snap, catalog))
	if err != nil {
		return nil, diag, err
	}
	diag.ReconstructionNanos = time.Since(reconstructionStart).Nanoseconds()
	diag.ReconstructionRows = 1
	return out, diag, nil
}

func reconstructColumnDocumentFromVisibleRow(cfg ColumnStoreConfig, retained []byte, row columnPhysicalVisibleRow) ([]byte, error) {
	if row.Deleted {
		return nil, errors.New("collections: column reconstruction latest physical row is deleted")
	}
	values, err := mergeColumnReconstructionValues(cfg, row.Values, nil)
	if err != nil {
		return nil, err
	}
	return reconstructColumnJSONDocument(cfg, retained, values)
}

func reconstructColumnDocumentFromVisibleRowValues(cfg ColumnStoreConfig, retained []byte, row columnPhysicalVisibleRow, values []columnDeclaredValue) ([]byte, error) {
	return reconstructColumnDocumentFromVisibleRowValuesProjected(cfg, retained, row, values, nil, nil)
}

func reconstructColumnDocumentFromVisibleRowValuesProjected(cfg ColumnStoreConfig, retained []byte, row columnPhysicalVisibleRow, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	_, doc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(nil, cfg, retained, row, values, projection, stats)
	return doc, err
}

func reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena []byte, cfg ColumnStoreConfig, retained []byte, row columnPhysicalVisibleRow, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, []byte, error) {
	return reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(arena, cfg, retained, row, values, projection, stats, nil)
}

func reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(arena []byte, cfg ColumnStoreConfig, retained []byte, row columnPhysicalVisibleRow, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats, resolver templateV1Resolver) ([]byte, []byte, error) {
	start := len(arena)
	if row.Deleted {
		return arena[:start], nil, errors.New("collections: column reconstruction latest physical row is deleted")
	}
	arena, doc, err := reconstructColumnJSONDocumentProjectedIntoWithResolver(arena, cfg, retained, values, projection, stats, resolver)
	if err != nil {
		return arena[:start], nil, err
	}
	return arena, doc, nil
}

func mergeColumnReconstructionValues(cfg ColumnStoreConfig, rowValues, typedColumnValues []columnDeclaredValue) ([]columnDeclaredValue, error) {
	return mergeColumnReconstructionValuesInto(cfg, rowValues, typedColumnValues, nil)
}

func mergeColumnReconstructionValuesInto(cfg ColumnStoreConfig, rowValues, typedColumnValues, dst []columnDeclaredValue) ([]columnDeclaredValue, error) {
	return mergeColumnReconstructionValuesProjectedInto(cfg, rowValues, typedColumnValues, nil, dst)
}

func mergeColumnReconstructionValuesProjectedInto(cfg ColumnStoreConfig, rowValues, typedColumnValues []columnDeclaredValue, selected []bool, dst []columnDeclaredValue) ([]columnDeclaredValue, error) {
	if selected != nil && len(selected) != len(cfg.Columns) {
		return nil, fmt.Errorf("collections: column reconstruction projection columns=%d want %d", len(selected), len(cfg.Columns))
	}
	var values []columnDeclaredValue
	if cap(dst) < len(cfg.Columns) {
		values = make([]columnDeclaredValue, len(cfg.Columns))
	} else {
		values = dst[:len(cfg.Columns)]
		if selected != nil {
			clear(values)
		}
	}
	rowIdx := 0
	typedIdx := 0
	for i, col := range cfg.Columns {
		switch columnStoreColumnOwnerOrRowAsset(col) {
		case TypedStorageOwnerRowAsset:
			if selected != nil && !selected[i] {
				continue
			}
			if rowIdx >= len(rowValues) {
				return nil, fmt.Errorf("collections: column reconstruction missing typed_row_asset value for column %q", col.Name)
			}
			values[i] = rowValues[rowIdx]
			rowIdx++
		case TypedStorageOwnerColumnPart:
			if typedIdx >= len(typedColumnValues) {
				return nil, fmt.Errorf("collections: column reconstruction missing typed_column_part value for column %q", col.Name)
			}
			values[i] = typedColumnValues[typedIdx]
			typedIdx++
		default:
			return nil, fmt.Errorf("collections: column reconstruction unsupported owner %q for column %q", col.Owner, col.Name)
		}
	}
	if rowIdx != len(rowValues) {
		return nil, fmt.Errorf("collections: column reconstruction unused typed_row_asset values=%d", len(rowValues)-rowIdx)
	}
	if typedIdx != len(typedColumnValues) {
		return nil, fmt.Errorf("collections: column reconstruction unused typed_column_part values=%d", len(typedColumnValues)-typedIdx)
	}
	return values, nil
}

func decodeColumnRetainedPayloadObject(cfg ColumnStoreConfig, retained []byte, resolver templateV1Resolver) (map[string]any, error) {
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingSemanticStreamV1 {
		if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(retained); err != nil {
			return nil, err
		} else if ok {
			return nil, errors.New("collections: unresolved semantic-stream-v1 retained payload locator")
		}
		trimmed := bytes.TrimSpace(retained)
		if len(trimmed) == 0 {
			return make(map[string]any), nil
		}
		if hasTemplateV1Magic(trimmed, templateV1StoredMagic) || hasTemplateV1Magic(trimmed, templateV1InputMagic) || hasTemplateV1Magic(trimmed, templateV1InsertDocumentMagic) {
			obj, err := decodeTemplateV1RetainedPayloadObject(trimmed, resolver)
			if err != nil {
				return nil, fmt.Errorf("collections: decode template-v1 retained payload: %w", err)
			}
			return obj, nil
		}
		return decodeColumnJSONObject(trimmed)
	}
	trimmed := bytes.TrimSpace(retained)
	if len(trimmed) == 0 {
		return make(map[string]any), nil
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNonColumn && columnRetainedPayloadEffectiveEncoding(&cfg) == ColumnRetainedPayloadEncodingTemplateV1 {
		obj, err := decodeTemplateV1RetainedPayloadObject(trimmed, resolver)
		if err != nil {
			return nil, fmt.Errorf("collections: decode template-v1 retained payload: %w", err)
		}
		return obj, nil
	}
	return decodeColumnJSONObject(trimmed)
}

func decodeTemplateV1RetainedPayloadObject(retained []byte, resolver templateV1Resolver) (map[string]any, error) {
	switch {
	case hasTemplateV1Magic(retained, templateV1StoredMagic):
		if resolver == nil {
			return nil, errTemplateV1MissingResolver
		}
		return templateV1StoredDocumentObject(retained, resolver)
	case hasTemplateV1Magic(retained, templateV1InputMagic), hasTemplateV1Magic(retained, templateV1InsertDocumentMagic):
		prepared, _, _, preparedResolver, err := prepareTemplateV1InsertDocuments([][]byte{retained}, resolver, false, false)
		if err != nil {
			return nil, err
		}
		if len(prepared) != 1 {
			return nil, errors.New("collections: template-v1 retained payload prepared unexpected document count")
		}
		return templateV1StoredDocumentObject(prepared[0], preparedResolver)
	default:
		return nil, errors.New("collections: retained payload is not template-v1 encoded")
	}
}

func reconstructColumnJSONDocument(cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue) ([]byte, error) {
	return reconstructColumnJSONDocumentProjected(cfg, retained, values, nil, nil)
}

func reconstructColumnJSONDocumentProjected(cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	_, doc, err := reconstructColumnJSONDocumentProjectedInto(nil, cfg, retained, values, projection, stats)
	return doc, err
}

func reconstructColumnJSONDocumentProjectedInto(arena []byte, cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, []byte, error) {
	return reconstructColumnJSONDocumentProjectedIntoWithResolver(arena, cfg, retained, values, projection, stats, nil)
}

func reconstructColumnJSONDocumentProjectedIntoWithResolver(arena []byte, cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats, resolver templateV1Resolver) ([]byte, []byte, error) {
	return reconstructColumnJSONDocumentProjectedIntoWithScratch(arena, cfg, retained, values, projection, stats, resolver, nil)
}

func reconstructColumnJSONDocumentProjectedIntoWithScratch(arena []byte, cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats, resolver templateV1Resolver, scratch *columnDocumentReconstructionScratch) ([]byte, []byte, error) {
	start := len(arena)
	projectionActive := projection.active()
	obj, err := decodeColumnRetainedPayloadObjectForEmission(cfg, retained, resolver, scratch)
	if err != nil {
		return arena[:start], nil, err
	}
	if len(values) != len(cfg.Columns) {
		return arena[:start], nil, fmt.Errorf("collections: column reconstruction values=%d columns=%d", len(values), len(cfg.Columns))
	}
	var declared []columnReconstructedDeclaredValue
	if scratch != nil && cap(scratch.declared) >= len(cfg.Columns) {
		declared = scratch.declared[:len(cfg.Columns)]
		clear(declared)
	} else {
		declared = make([]columnReconstructedDeclaredValue, len(cfg.Columns))
		if scratch != nil {
			scratch.declared = declared
		}
	}
	if projectionActive {
		for i, col := range cfg.Columns {
			if !projection.wantsPath(col.Path) {
				if stats != nil {
					stats.FieldsSkipped++
				}
				continue
			}
			if !strings.Contains(col.Path, ".") && columnDeclaredJSONScalar(values[i].Type) {
				declared[i] = columnReconstructedDeclaredValue{Scalar: &values[i], Present: values[i].Present}
				continue
			}
			raw, err := columnDeclaredValueToJSON(values[i])
			if err != nil {
				return arena[:start], nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
			}
			declared[i] = columnReconstructedDeclaredValue{
				Value:   raw,
				Present: values[i].Present,
			}
			if !values[i].Present {
				continue
			}
			recordEmbeddingVectorMaterialization(stats, col, values[i])
			if strings.Contains(col.Path, ".") {
				if err := setColumnJSONPath(obj, col.Path, raw); err != nil {
					return arena[:start], nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
				}
			}
		}
	} else {
		for i, col := range cfg.Columns {
			if !strings.Contains(col.Path, ".") && columnDeclaredJSONScalar(values[i].Type) {
				declared[i] = columnReconstructedDeclaredValue{Scalar: &values[i], Present: values[i].Present}
				continue
			}
			raw, err := columnDeclaredValueToJSON(values[i])
			if err != nil {
				return arena[:start], nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
			}
			declared[i] = columnReconstructedDeclaredValue{
				Value:   raw,
				Present: values[i].Present,
			}
			recordEmbeddingVectorMaterialization(stats, col, values[i])
			if !values[i].Present {
				continue
			}
			if strings.Contains(col.Path, ".") {
				if err := setColumnJSONPath(obj, col.Path, raw); err != nil {
					return arena[:start], nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
				}
			}
		}
	}
	arena, err = marshalColumnReconstructedJSONObjectProjectedInto(arena, cfg, obj, declared, projection, stats)
	if err != nil {
		return arena[:start], nil, fmt.Errorf("collections: encode reconstructed column payload: %w", err)
	}
	doc := arena[start:len(arena):len(arena)]
	return arena, doc, nil
}

type columnReconstructedDeclaredValue struct {
	Value   any
	Scalar  *columnDeclaredValue
	Present bool
}

func recordEmbeddingVectorMaterialization(stats *DocumentMaterializationStats, col ColumnStoreColumn, value columnDeclaredValue) {
	if stats == nil || col.Path != "embedding" || !value.Present || value.Null || value.Type != ColumnStoreValueFloat32Vector {
		return
	}
	stats.EmbeddingVectorReads++
	stats.EmbeddingVectorBytes += uint64(len(value.Float32Vector)) * 4
}

func marshalColumnReconstructedJSONObject(cfg ColumnStoreConfig, retained map[string]any, declared []columnReconstructedDeclaredValue) ([]byte, error) {
	return marshalColumnReconstructedJSONObjectProjected(cfg, retained, declared, nil, nil)
}

func marshalColumnReconstructedJSONObjectProjected(cfg ColumnStoreConfig, retained map[string]any, declared []columnReconstructedDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	out, err := marshalColumnReconstructedJSONObjectProjectedInto(nil, cfg, retained, declared, projection, stats)
	if err != nil {
		return nil, err
	}
	return out[:len(out):len(out)], nil
}

func marshalColumnReconstructedJSONObjectProjectedInto(arena []byte, cfg ColumnStoreConfig, retained map[string]any, declared []columnReconstructedDeclaredValue, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	projectionActive := projection.active()
	arena = append(arena, '{')
	written := make(map[string]struct{}, len(cfg.Columns))
	first := true
	reconstructed := uint64(0)
	writeField := func(key string, value any, scalar *columnDeclaredValue) error {
		if !first {
			arena = append(arena, ',')
		}
		first = false
		arena = appendColumnJSONString(arena, key)
		arena = append(arena, ':')
		var err error
		if scalar != nil {
			arena, err = appendColumnDeclaredJSONScalar(arena, *scalar)
		} else {
			arena, err = appendColumnJSONValue(arena, value)
		}
		if err != nil {
			return err
		}
		reconstructed++
		return nil
	}

	if projectionActive {
		for i, col := range cfg.Columns {
			if strings.Contains(col.Path, ".") {
				continue
			}
			if !projection.wantsPath(col.Path) {
				written[col.Path] = struct{}{}
				continue
			}
			if !declared[i].Present {
				continue
			}
			before := len(arena)
			if err := writeField(col.Path, declared[i].Value, declared[i].Scalar); err != nil {
				return arena, err
			}
			if stats != nil && col.Path == "embedding" {
				stats.EmbeddingOutputBytes += uint64(len(arena) - before)
			}
			written[col.Path] = struct{}{}
		}
	} else {
		for i, col := range cfg.Columns {
			if strings.Contains(col.Path, ".") {
				continue
			}
			if !declared[i].Present {
				continue
			}
			before := len(arena)
			if err := writeField(col.Path, declared[i].Value, declared[i].Scalar); err != nil {
				return arena, err
			}
			if stats != nil && col.Path == "embedding" {
				stats.EmbeddingOutputBytes += uint64(len(arena) - before)
			}
			written[col.Path] = struct{}{}
		}
	}
	keys := make([]string, 0, len(retained))
	for key := range retained {
		if _, ok := written[key]; ok {
			continue
		}
		if projectionActive && !projection.wantsPath(key) {
			if stats != nil {
				stats.FieldsSkipped++
			}
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writeField(key, retained[key], nil); err != nil {
			return arena, err
		}
	}
	arena = append(arena, '}')
	if stats != nil {
		stats.FieldsReconstructed += reconstructed
	}
	return arena, nil
}

// appendColumnJSONString matches encoding/json with EscapeHTML enabled,
// including invalid UTF-8 replacement and the JavaScript line separators.
func appendColumnJSONString[S ~string | ~[]byte](dst []byte, src S) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(src); {
		b := src[i]
		if b < utf8.RuneSelf {
			if b >= 0x20 && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			dst = append(dst, src[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hex[b>>4], hex[b&15])
			}
			i++
			start = i
			continue
		}
		r, n := utf8.DecodeRuneInString(string(src[i:min(i+utf8.UTFMax, len(src))]))
		if r == utf8.RuneError && n == 1 {
			dst = append(dst, src[start:i]...)
			dst = append(dst, `\ufffd`...)
			i++
			start = i
			continue
		}
		if r == '\u2028' || r == '\u2029' {
			dst = append(dst, src[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hex[r&15])
			i += n
			start = i
			continue
		}
		i += n
	}
	dst = append(dst, src[start:]...)
	return append(dst, '"')
}

func appendColumnJSONFloat(dst []byte, f float64, bits int) ([]byte, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		// Preserve the established encoding/json error on this exceptional path.
		var value any = f
		if bits == 32 {
			value = float32(f)
		}
		_, err := json.Marshal(value)
		return dst, err
	}
	format := byte('f')
	abs := math.Abs(f)
	if abs != 0 && ((bits == 64 && (abs < 1e-6 || abs >= 1e21)) || (bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21))) {
		format = 'e'
	}
	dst = strconv.AppendFloat(dst, f, format, -1, bits)
	n := len(dst)
	if format == 'e' && n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
		dst[n-2] = dst[n-1]
		dst = dst[:n-1]
	}
	return dst, nil
}

func appendColumnJSONValue(dst []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return append(dst, "null"...), nil
	case string:
		return appendColumnJSONString(dst, v), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case uint64:
		return strconv.AppendUint(dst, v, 10), nil
	case float32:
		return appendColumnJSONFloat(dst, float64(v), 32)
	case float64:
		return appendColumnJSONFloat(dst, v, 64)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return dst, err
		}
		return append(dst, raw...), nil
	}
}

func columnDeclaredJSONScalar(t ColumnStoreValueType) bool {
	switch t {
	case ColumnStoreValueBool, ColumnStoreValueString, ColumnStoreValueInt64, ColumnStoreValueDouble, ColumnStoreValueFloat32,
		ColumnStoreValueInt8, ColumnStoreValueUint8, ColumnStoreValueInt16, ColumnStoreValueUint16, ColumnStoreValueInt32, ColumnStoreValueUint32, ColumnStoreValueUint64, ColumnStoreValueFloat16, ColumnStoreValueBFloat16:
		return true
	default:
		return false
	}
}

func appendColumnDeclaredJSONScalar(dst []byte, v columnDeclaredValue) ([]byte, error) {
	if v.Null {
		return append(dst, "null"...), nil
	}
	switch v.Type {
	case ColumnStoreValueString:
		if v.StringBytes != nil {
			return appendColumnJSONString(dst, v.StringBytes), nil
		}
		return appendColumnJSONString(dst, v.String), nil
	case ColumnStoreValueBool:
		return strconv.AppendBool(dst, v.Bool), nil
	case ColumnStoreValueInt64:
		return strconv.AppendInt(dst, v.Int64, 10), nil
	case ColumnStoreValueInt8:
		return strconv.AppendInt(dst, int64(v.Int8), 10), nil
	case ColumnStoreValueInt16:
		return strconv.AppendInt(dst, int64(v.Int16), 10), nil
	case ColumnStoreValueInt32:
		return strconv.AppendInt(dst, int64(v.Int32), 10), nil
	case ColumnStoreValueUint8:
		return strconv.AppendUint(dst, uint64(v.Uint8), 10), nil
	case ColumnStoreValueUint16:
		return strconv.AppendUint(dst, uint64(v.Uint16), 10), nil
	case ColumnStoreValueUint32:
		return strconv.AppendUint(dst, uint64(v.Uint32), 10), nil
	case ColumnStoreValueUint64:
		return strconv.AppendUint(dst, v.Uint64, 10), nil
	case ColumnStoreValueFloat16:
		return strconv.AppendUint(dst, uint64(v.Float16), 10), nil
	case ColumnStoreValueBFloat16:
		return strconv.AppendUint(dst, uint64(v.BFloat16), 10), nil
	case ColumnStoreValueFloat32:
		return appendColumnJSONFloat(dst, float64(v.Float32), 32)
	case ColumnStoreValueDouble:
		return appendColumnJSONFloat(dst, v.Double, 64)
	default:
		return dst, fmt.Errorf("unsupported declared scalar %q", v.Type)
	}
}

func projectJSONDocument(raw []byte, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	if !json.Valid(raw) {
		return nil, errors.New("collections: invalid JSON document for column retained payload")
	}
	obj := make(map[string]json.RawMessage)
	if err := jsonparser.ObjectEach(raw, func(key, value []byte, valueType jsonparser.ValueType, valueEndOffset int) error {
		value, err := columnRetainedSemanticStreamV1JSONParserRawValue(raw, value, valueType, valueEndOffset)
		if err != nil {
			return err
		}
		keyString := string(key)
		if !utf8.ValidString(keyString) {
			keyString = string([]rune(keyString))
		}
		obj[keyString] = value
		return nil
	}); err != nil {
		obj = make(map[string]json.RawMessage)
		if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
			return nil, errors.New("collections: column retained payload root must be a JSON object")
		}
	}
	keys := make([]string, 0, len(obj))
	outCap := 2
	for key := range obj {
		if !projection.wantsPath(key) {
			delete(obj, key)
			if stats != nil {
				stats.FieldsSkipped++
			}
			continue
		}
		if len(keys) > 0 {
			outCap++
		}
		outCap += len(key) + len(obj[key]) + 3
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]byte, 0, outCap)
	out = append(out, '{')
	for i, key := range keys {
		if i != 0 {
			out = append(out, ',')
		}
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		out = append(out, keyJSON...)
		out = append(out, ':')
		value := obj[key]
		if !utf8.Valid(value) || bytes.Contains(value, []byte(`\u`)) {
			var decoded any
			decoder := json.NewDecoder(bytes.NewReader(value))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return nil, err
			}
			value, err = json.Marshal(decoded)
			if err != nil {
				return nil, err
			}
		} else {
			value, err = json.Marshal(value)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, value...)
	}
	out = append(out, '}')
	if cap(out) > len(out)+64 {
		out = bytes.Clone(out)
	}
	if stats != nil {
		stats.FieldsReconstructed += uint64(len(keys))
	}
	return out, nil
}

func marshalProjectedJSONObject(obj map[string]any, projection *documentProjection, stats *DocumentMaterializationStats) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	keys := make([]string, 0, len(obj))
	for key := range obj {
		if !projection.wantsPath(key) {
			if stats != nil {
				stats.FieldsSkipped++
			}
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		if i != 0 {
			b.WriteByte(',')
		}
		keyBytes, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		valueBytes, err := json.Marshal(obj[key])
		if err != nil {
			return nil, err
		}
		b.Write(keyBytes)
		b.WriteByte(':')
		b.Write(valueBytes)
	}
	b.WriteByte('}')
	if stats != nil {
		stats.FieldsReconstructed += uint64(len(keys))
	}
	return b.Bytes(), nil
}

// This scratch belongs to one non-concurrent captured reader. The cursor is
// the existing retained-payload parser; its fixed descriptor arena admits
// small flat scalar objects. Larger, nested, escaped or unusual JSON keeps
// the established UseNumber decoder and arbitrary-value writer.
type columnDocumentReconstructionScratch struct {
	cursor   columnRetainedSemanticStreamV1JSONCursor
	object   map[string]any
	declared []columnReconstructedDeclaredValue
}

func (s *columnDocumentReconstructionScratch) clearBorrowed() {
	if s == nil {
		return
	}
	clear(s.object)
	clear(s.declared)
	s.cursor.document = nil
	s.cursor.pathInterner = nil
	s.cursor.nodes = s.cursor.nodes[:0]
	s.cursor.members = s.cursor.members[:0]
	s.cursor.unescapeScratch = nil
}

func decodeColumnRetainedPayloadObjectForEmission(cfg ColumnStoreConfig, raw []byte, resolver templateV1Resolver, scratch *columnDocumentReconstructionScratch) (map[string]any, error) {
	if scratch == nil || columnRetainedPayloadEffectiveEncoding(&cfg) != ColumnRetainedPayloadEncodingJSON {
		return decodeColumnRetainedPayloadObject(cfg, raw, resolver)
	}
	// Bound the optional parse before it can grow either fixed descriptor arena.
	scratch.cursor.maxDescriptors = 64
	scratch.cursor.maxDepth = 64
	root, err := scratch.cursor.parseDocument(raw, nil)
	if err != nil {
		return decodeColumnRetainedPayloadObject(cfg, raw, resolver)
	}
	for member := scratch.cursor.nodes[root].firstMember; member >= 0; member = scratch.cursor.members[member].next {
		m := scratch.cursor.members[member]
		n := scratch.cursor.nodes[m.node]
		key := raw[m.keyStart:m.keyEnd]
		if bytes.IndexByte(key, '\\') >= 0 || !utf8.Valid(key) || (n.valueType != jsonparser.String && n.valueType != jsonparser.Number && n.valueType != jsonparser.Boolean && n.valueType != jsonparser.Null) {
			return decodeColumnRetainedPayloadObject(cfg, raw, resolver)
		}
		if n.valueType == jsonparser.String {
			value := raw[n.valueStart:n.valueEnd]
			if bytes.IndexByte(value, '\\') >= 0 || !utf8.Valid(value) {
				return decodeColumnRetainedPayloadObject(cfg, raw, resolver)
			}
		}
	}
	if scratch.object == nil {
		scratch.object = make(map[string]any)
	} else {
		clear(scratch.object)
	}
	for member := scratch.cursor.nodes[root].firstMember; member >= 0; member = scratch.cursor.members[member].next {
		m := scratch.cursor.members[member]
		n := scratch.cursor.nodes[m.node]
		var value any
		switch n.valueType {
		case jsonparser.String:
			value = string(raw[n.valueStart:n.valueEnd])
		case jsonparser.Number:
			value = json.Number(string(raw[n.rawStart:n.rawEnd]))
		case jsonparser.Boolean:
			value = raw[n.rawStart] == 't'
		case jsonparser.Null:
			value = nil
		}
		scratch.object[string(raw[m.keyStart:m.keyEnd])] = value
	}
	return scratch.object, nil
}

func decodeColumnJSONObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("collections: invalid JSON document for column retained payload: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return nil, fmt.Errorf("collections: invalid JSON document for column retained payload: %w", err)
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return nil, errors.New("collections: column retained payload root must be a JSON object")
	}
	return obj, nil
}

func deleteColumnJSONPath(obj map[string]any, path string) {
	if obj == nil || path == "" {
		return
	}
	parts := strings.Split(path, ".")
	current := obj
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
	delete(current, parts[len(parts)-1])
}

func setColumnJSONPath(obj map[string]any, path string, value any) error {
	if obj == nil || path == "" {
		return nil
	}
	parts := strings.Split(path, ".")
	current := obj
	for _, part := range parts[:len(parts)-1] {
		existing, ok := current[part]
		if !ok {
			next := make(map[string]any)
			current[part] = next
			current = next
			continue
		}
		next, ok := existing.(map[string]any)
		if !ok {
			return fmt.Errorf("path %q has non-object ancestor %q", path, part)
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
	return nil
}

func columnDeclaredValueToJSON(value columnDeclaredValue) (any, error) {
	if value.Null {
		return nil, nil
	}
	switch value.Type {
	case ColumnStoreValueBool:
		return value.Bool, nil
	case ColumnStoreValueInt64:
		return value.Int64, nil
	case ColumnStoreValueFloat32:
		return value.Float32, nil
	case ColumnStoreValueDouble:
		return value.Double, nil
	case ColumnStoreValueString:
		if value.StringBytes != nil {
			return string(value.StringBytes), nil
		}
		return value.String, nil
	case ColumnStoreValueInt8:
		return value.Int8, nil
	case ColumnStoreValueUint8:
		return value.Uint8, nil
	case ColumnStoreValueInt16:
		return value.Int16, nil
	case ColumnStoreValueUint16:
		return value.Uint16, nil
	case ColumnStoreValueInt32:
		return value.Int32, nil
	case ColumnStoreValueUint32:
		return value.Uint32, nil
	case ColumnStoreValueUint64:
		return value.Uint64, nil
	case ColumnStoreValueFloat16:
		return value.Float16, nil
	case ColumnStoreValueBFloat16:
		return value.BFloat16, nil
	case ColumnStoreValueFloat32Vector:
		return value.Float32Vector, nil
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		return columnDeclaredDenseNumericVectorToJSONArray(value)
	case ColumnStoreValueByteVector:
		return columnDeclaredBytesToJSONArray(value.Bytes), nil
	case ColumnStoreValuePackedBitVector, ColumnStoreValuePackedUint2Vector, ColumnStoreValuePackedUint4Vector:
		return columnDeclaredPackedUintVectorToJSONArray(value)
	case ColumnStoreValueUint32List:
		return value.Uint32List, nil
	case ColumnStoreValueBytes:
		return columnDeclaredBytesToJSONArray(value.Bytes), nil
	case ColumnStoreValueAdjacencyList:
		return value.AdjacencyList, nil
	default:
		return nil, fmt.Errorf("unsupported declared value type %q", value.Type)
	}
}

func columnDeclaredPackedUintVectorToJSONArray(value columnDeclaredValue) (any, error) {
	bitsPerElement, ok := columnStorePackedUintVectorBits(value.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported packed declared value type %q", value.Type)
	}
	if value.BitsPerElement != 0 && value.BitsPerElement != bitsPerElement {
		return nil, fmt.Errorf("%s bits_per_element=%d want %d", value.Type, value.BitsPerElement, bitsPerElement)
	}
	if value.ElementsPerRow <= 0 {
		return nil, fmt.Errorf("%s missing positive elements_per_row", value.Type)
	}
	rowBytes, err := columnDeclaredPackedUintVectorBytesPerRow(value.Type, value.ElementsPerRow)
	if err != nil {
		return nil, err
	}
	if len(value.Bytes) != rowBytes {
		return nil, fmt.Errorf("%s bytes=%d want row_bytes=%d", value.Type, len(value.Bytes), rowBytes)
	}
	out := make([]int, value.ElementsPerRow)
	mask := byte((1 << uint(bitsPerElement)) - 1)
	for element := 0; element < value.ElementsPerRow; element++ {
		bitOffset := element * bitsPerElement
		byteOffset := bitOffset / 8
		shift := uint(bitOffset % 8)
		out[element] = int((value.Bytes[byteOffset] >> shift) & mask)
	}
	for bitOffset := value.ElementsPerRow * bitsPerElement; bitOffset < rowBytes*8; bitOffset++ {
		if (value.Bytes[bitOffset/8]>>uint(bitOffset%8))&1 != 0 {
			return nil, fmt.Errorf("%s non-zero padding bit at bit offset %d", value.Type, bitOffset)
		}
	}
	return out, nil
}

func columnDeclaredBytesToJSONArray(value []byte) []int {
	out := make([]int, len(value))
	for i, b := range value {
		out[i] = int(b)
	}
	return out
}

func columnDeclaredDenseNumericVectorToJSONArray(value columnDeclaredValue) (any, error) {
	width, ok := columnStoreDenseNumericVectorWidth(value.Type)
	if !ok {
		return nil, fmt.Errorf("unsupported dense numeric vector value type %q", value.Type)
	}
	if width <= 0 || len(value.DenseNumericVector)%width != 0 {
		return nil, fmt.Errorf("%s bytes=%d not divisible by width=%d", value.Type, len(value.DenseNumericVector), width)
	}
	elements := len(value.DenseNumericVector) / width
	switch value.Type {
	case ColumnStoreValueUint8Vector:
		out := make([]int, elements)
		for i, b := range value.DenseNumericVector {
			out[i] = int(b)
		}
		return out, nil
	case ColumnStoreValueInt8Vector:
		out := make([]int8, elements)
		for i, b := range value.DenseNumericVector {
			out[i] = int8(b)
		}
		return out, nil
	case ColumnStoreValueUint16Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector:
		out := make([]uint16, elements)
		for i := range out {
			out[i] = binary.LittleEndian.Uint16(value.DenseNumericVector[i*2:])
		}
		return out, nil
	case ColumnStoreValueInt16Vector:
		out := make([]int16, elements)
		for i := range out {
			out[i] = int16(binary.LittleEndian.Uint16(value.DenseNumericVector[i*2:]))
		}
		return out, nil
	case ColumnStoreValueUint32Vector:
		out := make([]uint32, elements)
		for i := range out {
			out[i] = binary.LittleEndian.Uint32(value.DenseNumericVector[i*4:])
		}
		return out, nil
	case ColumnStoreValueInt32Vector:
		out := make([]int32, elements)
		for i := range out {
			out[i] = int32(binary.LittleEndian.Uint32(value.DenseNumericVector[i*4:]))
		}
		return out, nil
	case ColumnStoreValueUint64Vector:
		out := make([]uint64, elements)
		for i := range out {
			out[i] = binary.LittleEndian.Uint64(value.DenseNumericVector[i*8:])
		}
		return out, nil
	case ColumnStoreValueInt64Vector:
		out := make([]int64, elements)
		for i := range out {
			out[i] = int64(binary.LittleEndian.Uint64(value.DenseNumericVector[i*8:]))
		}
		return out, nil
	case ColumnStoreValueFloat64Vector:
		out := make([]float64, elements)
		for i := range out {
			out[i] = math.Float64frombits(binary.LittleEndian.Uint64(value.DenseNumericVector[i*8:]))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported dense numeric vector value type %q", value.Type)
	}
}
