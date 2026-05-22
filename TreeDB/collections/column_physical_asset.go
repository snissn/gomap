package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnPhysicalAssetMagic     = uint32(0x54435041) // TCPA
	columnPhysicalAssetVersionV1 = uint16(1)
	columnPhysicalAssetVersion   = uint16(2)
)

var ErrColumnDeclaredValueUnsupported = errors.New("collections: unsupported column declared value")

type columnWriteDocument struct {
	ID       []byte
	Document []byte
}

type columnDeclaredValue struct {
	Type   ColumnStoreValueType
	Null   bool
	Bool   bool
	Int64  int64
	Double float64
	String string
	// StringBytes is used by physical scan/query hot paths as an asset-buffer
	// view; String remains the owned representation for full asset decoding.
	StringBytes []byte
}

type columnDeclaredRow struct {
	ID      []byte
	Deleted bool
	Values  []columnDeclaredValue
}

type columnPhysicalAssetEncodeInput struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	SchemaHash        uint64
	Columns           []ColumnStoreColumn
	Rows              []columnDeclaredRow
}

type columnPhysicalAssetSummary struct {
	RowCount     int
	ColumnCount  int
	PayloadBytes int64
}

type columnPhysicalAssetHeader struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	Operation         ColumnPublishOperation
	SchemaHash        uint64
	RowCount          int
	ColumnCount       int
}

type columnPhysicalAsset struct {
	Header  columnPhysicalAssetHeader
	Columns []ColumnStoreColumn
	Rows    []columnDeclaredRow
}

func extractColumnDeclaredRowsFromJSONDocuments(cfg ColumnStoreConfig, docs []columnWriteDocument) ([]columnDeclaredRow, error) {
	rows := make([]columnDeclaredRow, 0, len(docs))
	for docIdx, doc := range docs {
		decoder := json.NewDecoder(bytes.NewReader(doc.Document))
		decoder.UseNumber()
		var root any
		if err := decoder.Decode(&root); err != nil {
			return nil, fmt.Errorf("%w: document[%d] invalid JSON: %v", ErrColumnDeclaredValueUnsupported, docIdx, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				err = errors.New("trailing JSON value")
			}
			return nil, fmt.Errorf("%w: document[%d] invalid JSON: %v", ErrColumnDeclaredValueUnsupported, docIdx, err)
		}
		obj, ok := root.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: document[%d] root is not object", ErrColumnDeclaredValueUnsupported, docIdx)
		}
		values := make([]columnDeclaredValue, len(cfg.Columns))
		for colIdx, col := range cfg.Columns {
			raw, exists := lookupColumnJSONPath(obj, col.Path)
			value, err := convertColumnDeclaredValue(col, raw, exists)
			if err != nil {
				return nil, fmt.Errorf("%w: document[%d] column[%d] %q: %v", ErrColumnDeclaredValueUnsupported, docIdx, colIdx, col.Name, err)
			}
			values[colIdx] = value
		}
		rows = append(rows, columnDeclaredRow{
			ID:     bytes.Clone(doc.ID),
			Values: values,
		})
	}
	return rows, nil
}

func lookupColumnJSONPath(obj map[string]any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	// Dotted column paths mirror collection/index path semantics: they traverse
	// nested objects and do not address literal top-level keys containing dots.
	if !strings.Contains(path, ".") {
		value, ok := obj[path]
		return value, ok
	}
	parts := strings.Split(path, ".")
	var current any = obj
	for _, part := range parts {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := next[part]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func convertColumnDeclaredValue(col ColumnStoreColumn, raw any, exists bool) (columnDeclaredValue, error) {
	value := columnDeclaredValue{Type: col.ValueType}
	if !exists || raw == nil {
		if col.Nullable {
			value.Null = true
			return value, nil
		}
		return columnDeclaredValue{}, errors.New("missing non-null declared value")
	}
	switch col.ValueType {
	case ColumnStoreValueBool:
		v, ok := raw.(bool)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected bool got %T", raw)
		}
		value.Bool = v
	case ColumnStoreValueInt64:
		n, ok := raw.(json.Number)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected int64 number got %T", raw)
		}
		v, err := n.Int64()
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Int64 = v
	case ColumnStoreValueDouble:
		n, ok := raw.(json.Number)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected double number got %T", raw)
		}
		v, err := n.Float64()
		if err != nil {
			return columnDeclaredValue{}, err
		}
		value.Double = v
	case ColumnStoreValueString:
		v, ok := raw.(string)
		if !ok {
			return columnDeclaredValue{}, fmt.Errorf("expected string got %T", raw)
		}
		value.String = v
	default:
		return columnDeclaredValue{}, fmt.Errorf("unsupported declared value type %q", col.ValueType)
	}
	return value, nil
}

func encodeColumnPhysicalAsset(input columnPhysicalAssetEncodeInput) ([]byte, columnPhysicalAssetSummary, error) {
	if input.Collection == "" || input.Namespace == "" || input.Generation == 0 || input.PartID == 0 {
		return nil, columnPhysicalAssetSummary{}, errors.New("collections: column physical asset missing collection, namespace, generation, or part_id")
	}
	if !isSupportedColumnPhysicalAssetOperation(input.Operation) {
		return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical asset operation %q", input.Operation)
	}
	for rowIdx, row := range input.Rows {
		switch input.Operation {
		case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
			if row.Deleted {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset %s row[%d] is marked deleted", input.Operation, rowIdx)
			}
			if len(row.Values) != len(input.Columns) {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(input.Columns))
			}
		case ColumnPublishOperationDelete:
			if !row.Deleted {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			if len(row.Values) != 0 {
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: column physical asset delete row[%d] values=%d want 0", rowIdx, len(row.Values))
			}
		default:
			return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical asset operation %q", input.Operation)
		}
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnPhysicalAssetMagic)
	writeManifestUint16(&b, columnPhysicalAssetVersion)
	writeManifestString(&b, input.Collection)
	writeManifestString(&b, input.Namespace)
	writeManifestUint64(&b, input.Generation)
	writeManifestUint64(&b, input.PartID)
	writeManifestUint64(&b, input.AppliedCommandLSN)
	writeManifestString(&b, string(input.Operation))
	writeManifestUint64(&b, input.SchemaHash)
	writeManifestUint64(&b, uint64(len(input.Columns)))
	writeManifestUint64(&b, uint64(len(input.Rows)))
	for _, col := range input.Columns {
		writeManifestString(&b, col.Name)
		writeManifestString(&b, col.Path)
		writeManifestString(&b, string(col.ValueType))
		writeManifestBool(&b, col.Nullable)
		writeManifestBool(&b, col.Dictionary)
	}
	for _, row := range input.Rows {
		writeManifestBytes(&b, row.ID)
		writeManifestBool(&b, row.Deleted)
		if row.Deleted {
			continue
		}
		for _, value := range row.Values {
			writeManifestString(&b, string(value.Type))
			writeManifestBool(&b, value.Null)
			if value.Null {
				continue
			}
			switch value.Type {
			case ColumnStoreValueBool:
				writeManifestBool(&b, value.Bool)
			case ColumnStoreValueInt64:
				writeManifestUint64(&b, uint64(value.Int64))
			case ColumnStoreValueDouble:
				writeManifestUint64(&b, math.Float64bits(value.Double))
			case ColumnStoreValueString:
				writeManifestString(&b, value.String)
			default:
				return nil, columnPhysicalAssetSummary{}, fmt.Errorf("collections: unsupported column physical value type %q", value.Type)
			}
		}
	}
	payload := b.Bytes()
	return payload, columnPhysicalAssetSummary{
		RowCount:     len(input.Rows),
		ColumnCount:  len(input.Columns),
		PayloadBytes: int64(len(payload)),
	}, nil
}

func decodeColumnPhysicalAsset(raw []byte) (columnPhysicalAsset, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return columnPhysicalAsset{}, fmt.Errorf("collections: bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if version != columnPhysicalAssetVersionV1 && version != columnPhysicalAssetVersion {
		return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical asset version=%d", version)
	}
	header := columnPhysicalAssetHeader{
		Collection:        cur.string(),
		Namespace:         cur.string(),
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		Operation:         ColumnPublishOperation(cur.string()),
		SchemaHash:        cur.u64(),
	}
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnPhysicalAsset{}, err
	}
	if columnCount > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnPhysicalAsset{}, errors.New("collections: column physical asset dimensions overflow int")
	}
	header.ColumnCount = int(columnCount)
	header.RowCount = int(rowCount)
	asset := columnPhysicalAsset{
		Header:  header,
		Columns: make([]ColumnStoreColumn, int(columnCount)),
		Rows:    make([]columnDeclaredRow, 0, int(rowCount)),
	}
	for i := range asset.Columns {
		asset.Columns[i] = ColumnStoreColumn{
			Name:       cur.string(),
			Path:       cur.string(),
			ValueType:  ColumnStoreValueType(cur.string()),
			Nullable:   cur.bool(),
			Dictionary: cur.bool(),
		}
	}
	for rowIdx := 0; rowIdx < int(rowCount); rowIdx++ {
		row := columnDeclaredRow{
			ID: cur.bytes(),
		}
		if version >= columnPhysicalAssetVersion {
			row.Deleted = cur.bool()
		}
		if !row.Deleted {
			row.Values = make([]columnDeclaredValue, int(columnCount))
			for colIdx := 0; colIdx < int(columnCount); colIdx++ {
				value := columnDeclaredValue{
					Type: ColumnStoreValueType(cur.string()),
					Null: cur.bool(),
				}
				if !value.Null {
					switch value.Type {
					case ColumnStoreValueBool:
						value.Bool = cur.bool()
					case ColumnStoreValueInt64:
						value.Int64 = int64(cur.u64())
					case ColumnStoreValueDouble:
						value.Double = math.Float64frombits(cur.u64())
					case ColumnStoreValueString:
						value.String = cur.string()
					default:
						return columnPhysicalAsset{}, fmt.Errorf("collections: unsupported column physical value type %q", value.Type)
					}
				}
				row.Values[colIdx] = value
			}
		}
		asset.Rows = append(asset.Rows, row)
	}
	if cur.err != nil {
		return columnPhysicalAsset{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnPhysicalAsset{}, errors.New("collections: trailing bytes in column physical asset")
	}
	return asset, nil
}

func validateColumnPhysicalAssetForManifest(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig) error {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	if int64(len(raw)) != ref.Length {
		return fmt.Errorf("collections: column physical asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if checksum := page.Checksum(raw); checksum != ref.Checksum {
		return fmt.Errorf("collections: column physical asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
	}
	asset, err := decodeColumnPhysicalAsset(raw)
	if err != nil {
		return err
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: column physical asset validation requires asset manager")
	}
	if asset.Header.Namespace != cfg.AssetManager.Namespace || ref.Namespace != cfg.AssetManager.Namespace {
		return fmt.Errorf("collections: column physical asset namespace=%q ref_namespace=%q want %q", asset.Header.Namespace, ref.Namespace, cfg.AssetManager.Namespace)
	}
	if asset.Header.Generation != ref.Generation {
		return fmt.Errorf("collections: column physical asset generation=%d does not match ref generation=%d", asset.Header.Generation, ref.Generation)
	}
	if asset.Header.PartID != ref.PartID {
		return fmt.Errorf("collections: column physical asset part_id=%d does not match ref part_id=%d", asset.Header.PartID, ref.PartID)
	}
	if asset.Header.SchemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: column physical asset schema_hash=%d want %d", asset.Header.SchemaHash, cfg.SchemaHash)
	}
	if len(asset.Columns) != len(cfg.Columns) {
		return fmt.Errorf("collections: column physical asset columns=%d want %d", len(asset.Columns), len(cfg.Columns))
	}
	for i := range cfg.Columns {
		if asset.Columns[i] != cfg.Columns[i] {
			return fmt.Errorf("collections: column physical asset column[%d]=%+v want %+v", i, asset.Columns[i], cfg.Columns[i])
		}
	}
	if asset.Header.RowCount != len(asset.Rows) {
		return fmt.Errorf("collections: column physical asset row_count=%d rows=%d", asset.Header.RowCount, len(asset.Rows))
	}
	if !isSupportedColumnPhysicalAssetOperation(asset.Header.Operation) {
		return fmt.Errorf("collections: unsupported column physical asset operation %q", asset.Header.Operation)
	}
	for rowIdx, row := range asset.Rows {
		switch asset.Header.Operation {
		case ColumnPublishOperationInsert, ColumnPublishOperationUpdate:
			if row.Deleted {
				return fmt.Errorf("collections: column physical asset %s row[%d] is marked deleted", asset.Header.Operation, rowIdx)
			}
		case ColumnPublishOperationDelete:
			if !row.Deleted {
				return fmt.Errorf("collections: column physical asset delete row[%d] is not marked deleted", rowIdx)
			}
			if len(row.Values) != 0 {
				return fmt.Errorf("collections: column physical asset delete row[%d] values=%d want 0", rowIdx, len(row.Values))
			}
			continue
		default:
			return fmt.Errorf("collections: unsupported column physical asset operation %q", asset.Header.Operation)
		}
		if len(row.Values) != len(cfg.Columns) {
			return fmt.Errorf("collections: column physical asset row[%d] values=%d want %d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		for colIdx, value := range row.Values {
			if value.Type != cfg.Columns[colIdx].ValueType {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] type=%q want %q", rowIdx, colIdx, value.Type, cfg.Columns[colIdx].ValueType)
			}
			if value.Null && !cfg.Columns[colIdx].Nullable {
				return fmt.Errorf("collections: column physical asset row[%d] column[%d] is null but column is not nullable", rowIdx, colIdx)
			}
		}
	}
	return nil
}

func isSupportedColumnPhysicalAssetOperation(operation ColumnPublishOperation) bool {
	switch operation {
	case ColumnPublishOperationInsert, ColumnPublishOperationUpdate, ColumnPublishOperationDelete:
		return true
	default:
		return false
	}
}

func writeManifestBool(b *bytes.Buffer, value bool) {
	if value {
		_ = b.WriteByte(1)
		return
	}
	_ = b.WriteByte(0)
}

func writeManifestBytes(b *bytes.Buffer, value []byte) {
	writeManifestUint64(b, uint64(len(value)))
	_, _ = b.Write(value)
}

func (c *manifestCursor) bool() bool {
	if c.err != nil {
		return false
	}
	if len(c.raw)-c.pos < 1 {
		c.err = errors.New("collections: short column binary bool")
		return false
	}
	value := c.raw[c.pos] != 0
	c.pos++
	return value
}

func (c *manifestCursor) bytes() []byte {
	value := c.bytesView()
	if c.err != nil {
		return nil
	}
	return bytes.Clone(value)
}

func (c *manifestCursor) bytesView() []byte {
	if c.err != nil {
		return nil
	}
	n := c.u64()
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.raw)-c.pos) {
		c.err = errors.New("collections: short column binary bytes")
		return nil
	}
	value := c.raw[c.pos : c.pos+int(n)]
	c.pos += int(n)
	return value
}
