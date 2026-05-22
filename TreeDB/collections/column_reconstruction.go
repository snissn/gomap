package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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

func columnRetainedPayloadFromJSONDocument(cfg ColumnStoreConfig, document []byte) ([]byte, error) {
	switch cfg.RetainedPayload {
	case ColumnRetainedPayloadFull:
		return bytes.Clone(document), nil
	case ColumnRetainedPayloadNone:
		return []byte("{}"), nil
	case ColumnRetainedPayloadNonColumn:
		obj, err := decodeColumnJSONObject(document)
		if err != nil {
			return nil, err
		}
		for _, col := range cfg.Columns {
			deleteColumnJSONPath(obj, col.Path)
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
	if cfg.Reconstruction != ColumnReconstructionRetainedPayloadAndColumns {
		return nil, diag, fmt.Errorf("collections: unsupported column reconstruction policy %q", cfg.Reconstruction)
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
	out, err := reconstructColumnDocumentFromVisibleRow(cfg, retained, row)
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
	return reconstructColumnJSONDocument(cfg, retained, row.Values)
}

func reconstructColumnJSONDocument(cfg ColumnStoreConfig, retained []byte, values []columnDeclaredValue) ([]byte, error) {
	var obj map[string]any
	var err error
	if len(bytes.TrimSpace(retained)) == 0 {
		obj = make(map[string]any)
	} else {
		obj, err = decodeColumnJSONObject(retained)
		if err != nil {
			return nil, err
		}
	}
	if len(values) != len(cfg.Columns) {
		return nil, fmt.Errorf("collections: column reconstruction values=%d columns=%d", len(values), len(cfg.Columns))
	}
	declared := make([]columnReconstructedDeclaredValue, len(cfg.Columns))
	for i, col := range cfg.Columns {
		raw, err := columnDeclaredValueToJSON(values[i])
		if err != nil {
			return nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
		}
		declared[i] = columnReconstructedDeclaredValue{
			Value:   raw,
			Present: values[i].Present,
		}
		if !values[i].Present {
			continue
		}
		if strings.Contains(col.Path, ".") {
			if err := setColumnJSONPath(obj, col.Path, raw); err != nil {
				return nil, fmt.Errorf("collections: column reconstruction column %q: %w", col.Name, err)
			}
		}
	}
	out, err := marshalColumnReconstructedJSONObject(cfg, obj, declared)
	if err != nil {
		return nil, fmt.Errorf("collections: encode reconstructed column payload: %w", err)
	}
	return out, nil
}

type columnReconstructedDeclaredValue struct {
	Value   any
	Present bool
}

func marshalColumnReconstructedJSONObject(cfg ColumnStoreConfig, retained map[string]any, declared []columnReconstructedDeclaredValue) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	written := make(map[string]struct{}, len(cfg.Columns))
	first := true
	writeField := func(key string, value any) error {
		if !first {
			b.WriteByte(',')
		}
		first = false
		keyBytes, err := json.Marshal(key)
		if err != nil {
			return err
		}
		valueBytes, err := json.Marshal(value)
		if err != nil {
			return err
		}
		b.Write(keyBytes)
		b.WriteByte(':')
		b.Write(valueBytes)
		return nil
	}
	for i, col := range cfg.Columns {
		if strings.Contains(col.Path, ".") {
			continue
		}
		if !declared[i].Present {
			continue
		}
		if err := writeField(col.Path, declared[i].Value); err != nil {
			return nil, err
		}
		written[col.Path] = struct{}{}
	}
	keys := make([]string, 0, len(retained))
	for key := range retained {
		if _, ok := written[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writeField(key, retained[key]); err != nil {
			return nil, err
		}
	}
	b.WriteByte('}')
	return b.Bytes(), nil
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
	case ColumnStoreValueFloat32Vector:
		return value.Float32Vector, nil
	case ColumnStoreValueAdjacencyList:
		return value.AdjacencyList, nil
	default:
		return nil, fmt.Errorf("unsupported declared value type %q", value.Type)
	}
}
