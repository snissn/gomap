package quantizedasset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/page"
)

// Role names a logical quantized-vector asset column. These roles are schema
// metadata above typed-column storage; scoring/query semantics are intentionally
// not implemented here.
type Role string

const (
	RoleCodes                  Role = "codes"
	RolePackedCodes            Role = "packed_codes"
	RoleNorm                   Role = "norm"
	RoleStep                   Role = "step"
	RoleLower                  Role = "lower"
	RoleCodeSum                Role = "code_sum"
	RoleNorm2                  Role = "norm2"
	RoleCodeCount              Role = "code_count"
	RoleCentroidDistance       Role = "centroid_distance"
	RoleQuantizedDotProductInv Role = "quantized_dot_product_inv"
	RoleCentroidDotProduct     Role = "centroid_dot_product"
	RoleCentroidID             Role = "centroid_id"
	RoleListID                 Role = "list_id"
	RoleScalarU8Alpha          Role = "scalar_u8_alpha"
	RoleGranuleRowCount        Role = "granule_row_count"
)

// GraphOrdinalOrder describes how typed-column rows line up with scorer input
// ordinals. The current contract is row i == graph/vector ordinal i; callers
// that need remapping must prepare that side state before using this package.
type GraphOrdinalOrder string

const GraphOrdinalOrderVectorOrdinal GraphOrdinalOrder = "vector_ordinal"

// CodecDescriptor is the fail-closed codec identity recorded by a quantized
// asset manifest. Config may be empty for codecs with no serialized config;
// ConfigHash is caller-defined manifest identity and is compared when non-zero.
type CodecDescriptor struct {
	Name       string
	Version    uint32
	ConfigHash uint64
	Config     []byte
}

// BaseGraphIdentity ties quantized state to the graph/base manifest whose row
// order it was built for.
type BaseGraphIdentity struct {
	IndexName              string
	Field                  string
	Metric                 string
	Dimensions             int
	RowCount               int
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	GraphSchemaHash        uint64
}

// AssetRefIdentity is a typed-column asset identity independent of collections'
// concrete ColumnAssetRef type. Present=false skips ref checks for in-memory
// tests/fixtures; persisted callers should set it.
type AssetRefIdentity struct {
	Present    bool
	Kind       string
	Namespace  string
	Generation uint64
	PartID     uint64
	FileID     uint32
	Offset     int64
	Length     int64
	Checksum   uint32
}

// SchemaDescriptor is the manifest-declared quantized asset schema. It maps
// logical roles to columns in one or more typed-column part images.
type SchemaDescriptor struct {
	Name             string
	Metric           string
	VectorDimensions int
	CodeDimensions   int
	CodeWidthBits    int
	RowCount         int
	GranuleCount     int
	OrdinalOrder     GraphOrdinalOrder
	Codec            CodecDescriptor
	BaseGraph        BaseGraphIdentity
	Columns          []ColumnDescriptor
}

// ExpectedSchema is the scorer/index definition expected by the caller. Prepare
// compares SchemaDescriptor against this before any row access is returned.
type ExpectedSchema struct {
	Metric           string
	VectorDimensions int
	CodeDimensions   int
	CodeWidthBits    int
	RowCount         int
	GranuleCount     int
	OrdinalOrder     GraphOrdinalOrder
	Codec            CodecDescriptor
	BaseGraph        BaseGraphIdentity
	RequiredRoles    []Role
}

// ColumnDescriptor binds one logical role to one typed-column column.
type ColumnDescriptor struct {
	Role             Role
	Column           string
	AssetID          string
	Required         bool
	LogicalType      string
	Type             typedcolumn.ColumnType
	Encoding         typedcolumn.Encoding
	RowCount         int
	ElementsPerRow   int
	BytesPerRow      int
	BitsPerElement   int
	SourceSchemaHash uint64
	AssetBytes       int64
	Ref              AssetRefIdentity
}

// PartImageSource supplies one typed-column part image referenced by descriptor
// columns. Image.Bytes must remain immutable for the lifetime of Prepared.
type PartImageSource struct {
	AssetID          string
	Image            typedcolumn.ColumnPartImage
	Ref              AssetRefIdentity
	AssetBytes       int64
	SourceSchemaHash uint64
}

// PrepareRequest validates a manifest schema against caller expectations and
// prepares allocation-free ordinal readers over typed-column section bytes.
type PrepareRequest struct {
	Schema   SchemaDescriptor
	Expected ExpectedSchema
	Parts    []PartImageSource
}

// Prepared is immutable and safe for concurrent read-only ordinal access. Caller
// scratch passed to RowWords, PackedElements, or DenseUint32Row is caller-owned
// and must not be shared concurrently without synchronization.
type Prepared struct {
	rows      int
	columns   map[Role]preparedColumn
	footprint Footprint
}

// CodeRowView is an immutable, zero-copy fixed-width row view over a prepared
// code role. RowBytes slices the validated payload directly, avoiding per-row
// role-map lookups. The returned row slices alias prepared image bytes and must
// be treated as read-only.
type CodeRowView struct {
	role           Role
	rows           int
	payload        []byte
	bytesPerRow    int
	elementsPerRow int
}

type preparedColumnKind uint8

const (
	preparedColumnUnknown preparedColumnKind = iota
	preparedColumnScalarFloat32
	preparedColumnScalarUint32
	preparedColumnScalarUint64
	preparedColumnFixedBytes
	preparedColumnPackedUint
	preparedColumnDenseBytes
)

type preparedColumn struct {
	role              Role
	column            string
	kind              preparedColumnKind
	logicalType       string
	typeName          typedcolumn.ColumnType
	encoding          typedcolumn.Encoding
	rows              int
	payload           []byte
	bytesPerRow       int
	elementsPerRow    int
	bitsPerElement    int
	elementWidthBytes int
}

// Footprint records whole-asset and per-role storage accounting captured during
// prepare. BytesPerVector values are zero when Rows is zero.
type Footprint struct {
	Rows           int
	AssetBytes     int64
	SectionBytes   int64
	PaddingBytes   int64
	Columns        []ColumnFootprint
	BytesPerVector float64
}

// ColumnFootprint records the typed-column section footprint for one role.
type ColumnFootprint struct {
	Role              Role
	Column            string
	AssetID           string
	LogicalType       string
	Type              typedcolumn.ColumnType
	Encoding          typedcolumn.Encoding
	Rows              int
	ElementsPerRow    int
	BytesPerRow       int
	BitsPerElement    int
	ElementWidthBytes int
	SectionBytes      int64
	AssetBytes        int64
	BytesPerVector    float64
}

// Prepare validates req and returns prepared ordinal readers. It fails closed on
// schema, identity, typed-column layout, row-count, role, or ref mismatches.
func Prepare(req PrepareRequest) (*Prepared, error) {
	if err := validateSchema(req.Schema, req.Expected); err != nil {
		return nil, err
	}
	parts, err := indexParts(req.Parts)
	if err != nil {
		return nil, err
	}
	prepared := &Prepared{rows: req.Schema.RowCount, columns: make(map[Role]preparedColumn, len(req.Schema.Columns))}
	footprint := Footprint{Rows: req.Schema.RowCount}
	seenAssets := make(map[string]struct{}, len(req.Parts))
	for _, src := range req.Parts {
		assetKey := partSourceKey(src)
		if _, ok := seenAssets[assetKey]; ok {
			continue
		}
		seenAssets[assetKey] = struct{}{}
		assetBytes := src.AssetBytes
		if assetBytes == 0 {
			assetBytes = int64(src.Image.TotalBytes())
		}
		footprint.AssetBytes += assetBytes
		footprint.PaddingBytes += int64(src.Image.PaddingBytes())
	}
	for _, desc := range req.Schema.Columns {
		if _, exists := prepared.columns[desc.Role]; exists {
			return nil, fmt.Errorf("quantizedasset: duplicate role %q", desc.Role)
		}
		src, err := resolvePart(parts, desc)
		if err != nil {
			return nil, err
		}
		col, cf, err := prepareColumn(req.Schema, desc, src)
		if err != nil {
			return nil, err
		}
		prepared.columns[desc.Role] = col
		footprint.Columns = append(footprint.Columns, cf)
		footprint.SectionBytes += cf.SectionBytes
	}
	if req.Schema.RowCount > 0 {
		footprint.BytesPerVector = float64(footprint.AssetBytes) / float64(req.Schema.RowCount)
	}
	prepared.footprint = footprint
	return prepared, nil
}

// Validate checks only descriptor/expected schema metadata, without opening any
// typed-column images.
func Validate(schema SchemaDescriptor, expected ExpectedSchema) error {
	return validateSchema(schema, expected)
}

func validateSchema(schema SchemaDescriptor, expected ExpectedSchema) error {
	if schema.RowCount < 0 {
		return fmt.Errorf("quantizedasset: row_count=%d", schema.RowCount)
	}
	if schema.Metric == "" {
		return errors.New("quantizedasset: missing metric")
	}
	if expected.Metric != "" && schema.Metric != expected.Metric {
		return fmt.Errorf("quantizedasset: metric=%q want %q", schema.Metric, expected.Metric)
	}
	if schema.VectorDimensions <= 0 {
		return fmt.Errorf("quantizedasset: vector_dimensions=%d must be positive", schema.VectorDimensions)
	}
	if expected.VectorDimensions != 0 && schema.VectorDimensions != expected.VectorDimensions {
		return fmt.Errorf("quantizedasset: vector_dimensions=%d want %d", schema.VectorDimensions, expected.VectorDimensions)
	}
	if schema.CodeDimensions <= 0 {
		return fmt.Errorf("quantizedasset: code_dimensions=%d must be positive", schema.CodeDimensions)
	}
	if expected.CodeDimensions != 0 && schema.CodeDimensions != expected.CodeDimensions {
		return fmt.Errorf("quantizedasset: code_dimensions=%d want %d", schema.CodeDimensions, expected.CodeDimensions)
	}
	if schema.CodeWidthBits <= 0 {
		return fmt.Errorf("quantizedasset: code_width_bits=%d must be positive", schema.CodeWidthBits)
	}
	if expected.CodeWidthBits != 0 && schema.CodeWidthBits != expected.CodeWidthBits {
		return fmt.Errorf("quantizedasset: code_width_bits=%d want %d", schema.CodeWidthBits, expected.CodeWidthBits)
	}
	if schema.RowCount != expected.RowCount {
		return fmt.Errorf("quantizedasset: row_count=%d want %d", schema.RowCount, expected.RowCount)
	}
	if schema.GranuleCount < 0 {
		return fmt.Errorf("quantizedasset: granule_count=%d", schema.GranuleCount)
	}
	if expected.GranuleCount != 0 && schema.GranuleCount != expected.GranuleCount {
		return fmt.Errorf("quantizedasset: granule_count=%d want %d", schema.GranuleCount, expected.GranuleCount)
	}
	if schema.OrdinalOrder == "" {
		return errors.New("quantizedasset: missing ordinal_order")
	}
	if schema.OrdinalOrder != GraphOrdinalOrderVectorOrdinal {
		return fmt.Errorf("quantizedasset: unsupported ordinal_order=%q want %q", schema.OrdinalOrder, GraphOrdinalOrderVectorOrdinal)
	}
	if expected.OrdinalOrder != "" && schema.OrdinalOrder != expected.OrdinalOrder {
		return fmt.Errorf("quantizedasset: ordinal_order=%q want %q", schema.OrdinalOrder, expected.OrdinalOrder)
	}
	if err := validateCodec(schema.Codec, expected.Codec); err != nil {
		return err
	}
	if err := validateBaseGraph(schema.BaseGraph, expected.BaseGraph, schema); err != nil {
		return err
	}
	seen := make(map[Role]struct{}, len(schema.Columns))
	for _, column := range schema.Columns {
		if !KnownRole(column.Role) {
			return fmt.Errorf("quantizedasset: unknown role %q", column.Role)
		}
		if column.Column == "" {
			return fmt.Errorf("quantizedasset: role %q missing column", column.Role)
		}
		if column.RowCount < 0 {
			return fmt.Errorf("quantizedasset: role %q row_count=%d", column.Role, column.RowCount)
		}
		if _, ok := seen[column.Role]; ok {
			return fmt.Errorf("quantizedasset: duplicate role %q", column.Role)
		}
		seen[column.Role] = struct{}{}
	}
	for _, role := range expected.RequiredRoles {
		if !KnownRole(role) {
			return fmt.Errorf("quantizedasset: unknown required role %q", role)
		}
		if _, ok := seen[role]; !ok {
			return fmt.Errorf("quantizedasset: missing required role %q", role)
		}
	}
	return nil
}

func validateCodec(actual, expected CodecDescriptor) error {
	if actual.Name == "" {
		return errors.New("quantizedasset: missing codec name")
	}
	if actual.Version == 0 {
		return errors.New("quantizedasset: missing codec version")
	}
	if expected.Name != "" && actual.Name != expected.Name {
		return fmt.Errorf("quantizedasset: codec name=%q want %q", actual.Name, expected.Name)
	}
	if expected.Version != 0 && actual.Version != expected.Version {
		return fmt.Errorf("quantizedasset: codec version=%d want %d", actual.Version, expected.Version)
	}
	if expected.ConfigHash != 0 && actual.ConfigHash != expected.ConfigHash {
		return fmt.Errorf("quantizedasset: codec config_hash=%d want %d", actual.ConfigHash, expected.ConfigHash)
	}
	if len(expected.Config) > 0 && !bytes.Equal(actual.Config, expected.Config) {
		return errors.New("quantizedasset: codec config bytes mismatch")
	}
	return nil
}

func validateBaseGraph(actual, expected BaseGraphIdentity, schema SchemaDescriptor) error {
	if actual.IndexName == "" || actual.Field == "" || actual.Metric == "" {
		return fmt.Errorf("quantizedasset: incomplete base graph identity index=%q field=%q metric=%q", actual.IndexName, actual.Field, actual.Metric)
	}
	if actual.Dimensions <= 0 || actual.RowCount < 0 {
		return fmt.Errorf("quantizedasset: invalid base graph dimensions/rows=(%d,%d)", actual.Dimensions, actual.RowCount)
	}
	if actual.BaseManifestGeneration == 0 || actual.BaseManifestChecksum == 0 || actual.BaseSchemaHash == 0 || actual.GraphSchemaHash == 0 {
		return errors.New("quantizedasset: incomplete base graph generation/checksum/schema identity")
	}
	if actual.RowCount != schema.RowCount {
		return fmt.Errorf("quantizedasset: base graph row_count=%d want schema row_count=%d", actual.RowCount, schema.RowCount)
	}
	if actual.Dimensions != schema.VectorDimensions {
		return fmt.Errorf("quantizedasset: base graph dimensions=%d want schema vector_dimensions=%d", actual.Dimensions, schema.VectorDimensions)
	}
	if actual.Metric != schema.Metric {
		return fmt.Errorf("quantizedasset: base graph metric=%q want schema metric=%q", actual.Metric, schema.Metric)
	}
	if expected == (BaseGraphIdentity{}) {
		return nil
	}
	if actual != expected {
		return fmt.Errorf("quantizedasset: base graph identity mismatch got=%+v want=%+v", actual, expected)
	}
	return nil
}

func indexParts(parts []PartImageSource) (map[string]PartImageSource, error) {
	if len(parts) == 0 {
		return nil, errors.New("quantizedasset: no typed-column part images")
	}
	out := make(map[string]PartImageSource, len(parts)*2)
	for i, part := range parts {
		if part.Image.TotalBytes() == 0 {
			return nil, fmt.Errorf("quantizedasset: part[%d] empty image", i)
		}
		if part.Image.Rows < 0 {
			return nil, fmt.Errorf("quantizedasset: part[%d] rows=%d", i, part.Image.Rows)
		}
		if part.Ref.Present {
			if part.Ref.Length != 0 && part.Ref.Length != int64(part.Image.TotalBytes()) {
				return nil, fmt.Errorf("quantizedasset: part[%d] ref length=%d image bytes=%d", i, part.Ref.Length, part.Image.TotalBytes())
			}
			if part.Ref.Checksum != 0 {
				if checksum := page.Checksum(part.Image.Bytes); checksum != part.Ref.Checksum {
					return nil, fmt.Errorf("quantizedasset: part[%d] checksum=%d want ref checksum=%d", i, checksum, part.Ref.Checksum)
				}
			}
		}
		if part.AssetBytes != 0 && part.AssetBytes != int64(part.Image.TotalBytes()) {
			return nil, fmt.Errorf("quantizedasset: part[%d] asset_bytes=%d image bytes=%d", i, part.AssetBytes, part.Image.TotalBytes())
		}
		if part.AssetID != "" {
			if _, exists := out["id:"+part.AssetID]; exists {
				return nil, fmt.Errorf("quantizedasset: duplicate part asset_id %q", part.AssetID)
			}
			out["id:"+part.AssetID] = part
		}
		if part.Ref.Present {
			key := "ref:" + part.Ref.key()
			if _, exists := out[key]; exists {
				return nil, fmt.Errorf("quantizedasset: duplicate part ref %s", part.Ref.key())
			}
			out[key] = part
		}
		if len(parts) == 1 {
			out[""] = part
		}
	}
	return out, nil
}

func resolvePart(parts map[string]PartImageSource, desc ColumnDescriptor) (PartImageSource, error) {
	if desc.AssetID != "" {
		part, ok := parts["id:"+desc.AssetID]
		if !ok {
			return PartImageSource{}, fmt.Errorf("quantizedasset: role %q missing asset_id %q", desc.Role, desc.AssetID)
		}
		return part, nil
	}
	if desc.Ref.Present {
		part, ok := parts["ref:"+desc.Ref.key()]
		if !ok {
			return PartImageSource{}, fmt.Errorf("quantizedasset: role %q missing asset ref %s", desc.Role, desc.Ref.key())
		}
		return part, nil
	}
	part, ok := parts[""]
	if !ok {
		return PartImageSource{}, fmt.Errorf("quantizedasset: role %q requires asset_id or ref when multiple part images are supplied", desc.Role)
	}
	return part, nil
}

func prepareColumn(schema SchemaDescriptor, desc ColumnDescriptor, src PartImageSource) (preparedColumn, ColumnFootprint, error) {
	rows := descriptorRowCount(schema, desc)
	if src.Image.Rows != rows {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q part rows=%d want row_count=%d", desc.Role, src.Image.Rows, rows)
	}
	if desc.Ref.Present {
		if !src.Ref.Present {
			return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q missing resolved asset ref want=%s", desc.Role, desc.Ref.key())
		}
		if desc.Ref != src.Ref {
			return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q asset ref mismatch got=%s want=%s", desc.Role, src.Ref.key(), desc.Ref.key())
		}
	}
	if desc.AssetBytes != 0 {
		assetBytes := src.AssetBytes
		if assetBytes == 0 {
			assetBytes = int64(src.Image.TotalBytes())
		}
		if desc.AssetBytes != assetBytes {
			return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q asset_bytes=%d want %d", desc.Role, assetBytes, desc.AssetBytes)
		}
	}
	if desc.SourceSchemaHash != 0 && desc.SourceSchemaHash != src.SourceSchemaHash {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q source_schema_hash=%d want %d", desc.Role, src.SourceSchemaHash, desc.SourceSchemaHash)
	}
	cert, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(src.Image)
	if err != nil {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q layout certification: %w", desc.Role, err)
	}
	certCol, ok := cert.Column(desc.Column)
	if !ok {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q missing typed-column column %q", desc.Role, desc.Column)
	}
	section, err := columnDataSection(src.Image, desc.Column)
	if err != nil {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q: %w", desc.Role, err)
	}
	if err := validateColumnShape(schema, desc, certCol, section); err != nil {
		return preparedColumn{}, ColumnFootprint{}, err
	}
	payload, err := src.Image.SectionBytes(section)
	if err != nil {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q section bytes: %w", desc.Role, err)
	}
	kind, rowBytes, elementWidth, err := preparedKindAndRowBytes(desc, certCol, schema)
	if err != nil {
		return preparedColumn{}, ColumnFootprint{}, err
	}
	wantBytes, err := checkedMul(rowBytes, rows)
	if err != nil {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q payload size: %w", desc.Role, err)
	}
	if len(payload) != wantBytes {
		return preparedColumn{}, ColumnFootprint{}, fmt.Errorf("quantizedasset: role %q payload bytes=%d want rows=%d*row_bytes=%d", desc.Role, len(payload), rows, rowBytes)
	}
	if kind == preparedColumnPackedUint {
		if err := validatePackedColumnPadding(desc.Role, rows, certCol.FixedWidthElements, certCol.BitsPerElement, rowBytes, payload); err != nil {
			return preparedColumn{}, ColumnFootprint{}, err
		}
	}
	col := preparedColumn{
		role:              desc.Role,
		column:            desc.Column,
		kind:              kind,
		logicalType:       certCol.LogicalType,
		typeName:          certCol.Type,
		encoding:          certCol.Encoding,
		rows:              rows,
		payload:           payload,
		bytesPerRow:       rowBytes,
		elementsPerRow:    certCol.FixedWidthElements,
		bitsPerElement:    certCol.BitsPerElement,
		elementWidthBytes: elementWidth,
	}
	assetBytes := src.AssetBytes
	if assetBytes == 0 {
		assetBytes = int64(src.Image.TotalBytes())
	}
	cf := ColumnFootprint{
		Role:              desc.Role,
		Column:            desc.Column,
		AssetID:           desc.AssetID,
		LogicalType:       certCol.LogicalType,
		Type:              certCol.Type,
		Encoding:          certCol.Encoding,
		Rows:              rows,
		ElementsPerRow:    certCol.FixedWidthElements,
		BytesPerRow:       rowBytes,
		BitsPerElement:    certCol.BitsPerElement,
		ElementWidthBytes: elementWidth,
		SectionBytes:      int64(section.Length),
		AssetBytes:        assetBytes,
	}
	if schema.RowCount > 0 {
		cf.BytesPerVector = float64(section.Length) / float64(schema.RowCount)
	}
	return col, cf, nil
}

func descriptorRowCount(schema SchemaDescriptor, desc ColumnDescriptor) int {
	if desc.RowCount != 0 {
		return desc.RowCount
	}
	switch desc.Role {
	case RoleScalarU8Alpha, RoleGranuleRowCount:
		return schema.GranuleCount
	default:
		return schema.RowCount
	}
}

func validatePackedColumnPadding(role Role, rows, elementsPerRow, bitsPerElement, rowBytes int, payload []byte) error {
	logicalBits, err := checkedMul(elementsPerRow, bitsPerElement)
	if err != nil {
		return fmt.Errorf("quantizedasset: role %q packed logical bits: %w", role, err)
	}
	paddingBits := rowBytes*8 - logicalBits
	if paddingBits == 0 {
		return nil
	}
	if paddingBits < 0 || paddingBits >= 8 {
		return fmt.Errorf("quantizedasset: role %q packed padding bits=%d", role, paddingBits)
	}
	wantBytes, err := checkedMul(rows, rowBytes)
	if err != nil {
		return fmt.Errorf("quantizedasset: role %q packed padding payload size: %w", role, err)
	}
	if len(payload) != wantBytes {
		return fmt.Errorf("quantizedasset: role %q packed padding payload bytes=%d want rows=%d*row_bytes=%d", role, len(payload), rows, rowBytes)
	}
	validBits := 8 - paddingBits
	validMask := byte((1 << uint(validBits)) - 1)
	for row := 0; row < rows; row++ {
		finalByte := payload[row*rowBytes+rowBytes-1]
		if finalByte&^validMask != 0 {
			return fmt.Errorf("quantizedasset: role %q packed padding: row %d: non-zero padding bits final_byte=0x%02x valid_mask=0x%02x", role, row, finalByte, validMask)
		}
	}
	return nil
}

func validateColumnShape(schema SchemaDescriptor, desc ColumnDescriptor, cert typedcolumn.ColumnPartLayoutContractColumn, section typedcolumn.ColumnPartImageSection) error {
	rows := descriptorRowCount(schema, desc)
	if cert.Rows != rows {
		return fmt.Errorf("quantizedasset: role %q cert rows=%d want %d", desc.Role, cert.Rows, rows)
	}
	if cert.Name != desc.Column {
		return fmt.Errorf("quantizedasset: role %q cert column=%q want %q", desc.Role, cert.Name, desc.Column)
	}
	if err := validateRoleType(desc.Role, cert); err != nil {
		return err
	}
	if !cert.DirectViewCertified {
		return fmt.Errorf("quantizedasset: role %q column %q is not direct-view certified", desc.Role, desc.Column)
	}
	if cert.Compression != typedcolumn.CompressionNone || section.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("quantizedasset: role %q compressed column cert/section=(%s,%s)", desc.Role, cert.Compression, section.Compression)
	}
	if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
		return fmt.Errorf("quantizedasset: role %q nullable/default wrappers are unsupported", desc.Role)
	}
	if section.Kind != typedcolumn.ColumnPartImageSectionColumnData || section.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumns || section.Column != desc.Column {
		return fmt.Errorf("quantizedasset: role %q invalid data section kind/category/column=(%s,%s,%q)", desc.Role, section.Kind, section.Category, section.Column)
	}
	if section.Rows != rows {
		return fmt.Errorf("quantizedasset: role %q section rows=%d want %d", desc.Role, section.Rows, rows)
	}
	if section.Offset != cert.Section.Offset || section.Length != cert.Section.Length {
		return fmt.Errorf("quantizedasset: role %q section offset/length=(%d,%d) want cert=(%d,%d)", desc.Role, section.Offset, section.Length, cert.Section.Offset, cert.Section.Length)
	}
	if desc.LogicalType != "" && cert.LogicalType != desc.LogicalType {
		return fmt.Errorf("quantizedasset: role %q logical_type=%q want %q", desc.Role, cert.LogicalType, desc.LogicalType)
	}
	if desc.Type != "" && cert.Type != desc.Type {
		return fmt.Errorf("quantizedasset: role %q type=%s want %s", desc.Role, cert.Type, desc.Type)
	}
	if desc.Encoding != 0 && cert.Encoding != desc.Encoding {
		return fmt.Errorf("quantizedasset: role %q encoding=%s want %s", desc.Role, cert.Encoding, desc.Encoding)
	}
	if section.Encoding != cert.Encoding {
		return fmt.Errorf("quantizedasset: role %q section encoding=%s want cert %s", desc.Role, section.Encoding, cert.Encoding)
	}
	if desc.ElementsPerRow != 0 && cert.FixedWidthElements != desc.ElementsPerRow {
		return fmt.Errorf("quantizedasset: role %q elements_per_row=%d want %d", desc.Role, cert.FixedWidthElements, desc.ElementsPerRow)
	}
	if desc.BytesPerRow != 0 && cert.BytesPerRow != desc.BytesPerRow {
		return fmt.Errorf("quantizedasset: role %q bytes_per_row=%d want %d", desc.Role, cert.BytesPerRow, desc.BytesPerRow)
	}
	if desc.BitsPerElement != 0 && cert.BitsPerElement != desc.BitsPerElement {
		return fmt.Errorf("quantizedasset: role %q bits_per_element=%d want %d", desc.Role, cert.BitsPerElement, desc.BitsPerElement)
	}
	return nil
}

func validateRoleType(role Role, cert typedcolumn.ColumnPartLayoutContractColumn) error {
	switch role {
	case RoleCodes:
		if cert.Type == typedcolumn.ColumnTypeFixedBytes && cert.Encoding == typedcolumn.EncodingRawFixedBytes && cert.LogicalType == string(columnsemantics.LogicalByteVector) {
			return nil
		}
		if wantLogical, wantEncoding, ok := unsignedDenseCodePhysical(cert.Type); ok {
			if cert.Encoding == wantEncoding && cert.LogicalType == wantLogical {
				return nil
			}
		}
		return fmt.Errorf("quantizedasset: role %q type/logical/encoding=(%s,%q,%s) is not a fixed-byte or unsigned dense code column", role, cert.Type, cert.LogicalType, cert.Encoding)
	case RolePackedCodes:
		bits, ok := typedcolumn.PackedUintVectorBits(cert.Type)
		wantEncoding, encOK := typedcolumn.PackedUintVectorEncoding(cert.Type)
		wantLogical, logicalOK := packedLogicalTypeForPhysical(cert.Type)
		if ok && encOK && logicalOK && cert.Encoding == wantEncoding && cert.BitsPerElement == bits && cert.LogicalType == wantLogical {
			return nil
		}
		return fmt.Errorf("quantizedasset: role %q type/logical/encoding=(%s,%q,%s) is not a packed-code column", role, cert.Type, cert.LogicalType, cert.Encoding)
	case RoleNorm, RoleStep, RoleLower, RoleCodeSum, RoleNorm2, RoleCentroidDistance, RoleQuantizedDotProductInv, RoleCentroidDotProduct, RoleScalarU8Alpha:
		if cert.Type == typedcolumn.ColumnTypeFloat32 && cert.Encoding == typedcolumn.EncodingRawFloat32 && cert.LogicalType == string(columnsemantics.LogicalFloat32) {
			return nil
		}
		return fmt.Errorf("quantizedasset: role %q type/logical/encoding=(%s,%q,%s) want float32/raw_float32", role, cert.Type, cert.LogicalType, cert.Encoding)
	case RoleCodeCount, RoleCentroidID, RoleListID, RoleGranuleRowCount:
		if cert.Type == typedcolumn.ColumnTypeUint32 && cert.Encoding == typedcolumn.EncodingRawUint32 && cert.LogicalType == string(columnsemantics.LogicalUint32) {
			return nil
		}
		if cert.Type == typedcolumn.ColumnTypeUint64 && cert.Encoding == typedcolumn.EncodingRawUint64 && cert.LogicalType == string(columnsemantics.LogicalUint64) {
			return nil
		}
		return fmt.Errorf("quantizedasset: role %q type/logical/encoding=(%s,%q,%s) want uint32/raw_uint32 or uint64/raw_uint64", role, cert.Type, cert.LogicalType, cert.Encoding)
	default:
		return fmt.Errorf("quantizedasset: unknown role %q", role)
	}
}

func preparedKindAndRowBytes(desc ColumnDescriptor, cert typedcolumn.ColumnPartLayoutContractColumn, schema SchemaDescriptor) (preparedColumnKind, int, int, error) {
	switch cert.Type {
	case typedcolumn.ColumnTypeFloat32:
		return preparedColumnScalarFloat32, 4, 4, nil
	case typedcolumn.ColumnTypeUint32:
		return preparedColumnScalarUint32, 4, 4, nil
	case typedcolumn.ColumnTypeUint64:
		return preparedColumnScalarUint64, 8, 8, nil
	case typedcolumn.ColumnTypeFixedBytes:
		if cert.BytesPerRow <= 0 {
			return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q bytes_per_row=%d", desc.Role, cert.BytesPerRow)
		}
		if desc.Role == RoleCodes {
			if schema.CodeDimensions != 0 && cert.BytesPerRow != schema.CodeDimensions {
				return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q bytes_per_row=%d want code_dimensions=%d", desc.Role, cert.BytesPerRow, schema.CodeDimensions)
			}
			if schema.CodeWidthBits != 0 && schema.CodeWidthBits != 8 {
				return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q fixed-byte element_width_bits=8 want code_width_bits=%d", desc.Role, schema.CodeWidthBits)
			}
		}
		return preparedColumnFixedBytes, cert.BytesPerRow, 1, nil
	case typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
		bits, _ := typedcolumn.PackedUintVectorBits(cert.Type)
		rowBytes, err := typedcolumn.PackedUintRowBytes(cert.FixedWidthElements, bits)
		if err != nil {
			return preparedColumnUnknown, 0, 0, err
		}
		if desc.Role == RolePackedCodes {
			if schema.CodeDimensions != 0 && cert.FixedWidthElements != schema.CodeDimensions {
				return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q elements_per_row=%d want code_dimensions=%d", desc.Role, cert.FixedWidthElements, schema.CodeDimensions)
			}
			if schema.CodeWidthBits != 0 && bits != schema.CodeWidthBits {
				return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q bits_per_element=%d want code_width_bits=%d", desc.Role, bits, schema.CodeWidthBits)
			}
		}
		return preparedColumnPackedUint, rowBytes, 1, nil
	default:
		width, ok := typedcolumn.DenseFixedWidthVectorElementWidth(cert.Type)
		if ok && cert.Type != typedcolumn.ColumnTypeFloat32Vector {
			if cert.FixedWidthElements <= 0 {
				return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q elements_per_row=%d", desc.Role, cert.FixedWidthElements)
			}
			rowBytes, err := checkedMul(cert.FixedWidthElements, width)
			if err != nil {
				return preparedColumnUnknown, 0, 0, err
			}
			if desc.Role == RoleCodes {
				if schema.CodeDimensions != 0 && cert.FixedWidthElements != schema.CodeDimensions {
					return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q elements_per_row=%d want code_dimensions=%d", desc.Role, cert.FixedWidthElements, schema.CodeDimensions)
				}
				if schema.CodeWidthBits != 0 && width*8 != schema.CodeWidthBits {
					return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q element_width_bits=%d want code_width_bits=%d", desc.Role, width*8, schema.CodeWidthBits)
				}
			}
			return preparedColumnDenseBytes, rowBytes, width, nil
		}
	}
	return preparedColumnUnknown, 0, 0, fmt.Errorf("quantizedasset: role %q unsupported type %s", desc.Role, cert.Type)
}

func columnDataSection(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, error) {
	var out typedcolumn.ColumnPartImageSection
	found := false
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			if found {
				return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("duplicate column data section %q", column)
			}
			out = section
			found = true
		}
	}
	if !found {
		return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("missing column data section %q", column)
	}
	return out, nil
}

// KnownRole reports whether role is part of the #1932 quantized schema contract.
func KnownRole(role Role) bool {
	switch role {
	case RoleCodes, RolePackedCodes, RoleNorm, RoleStep, RoleLower, RoleCodeSum, RoleNorm2, RoleCodeCount, RoleCentroidDistance, RoleQuantizedDotProductInv, RoleCentroidDotProduct, RoleCentroidID, RoleListID, RoleScalarU8Alpha, RoleGranuleRowCount:
		return true
	default:
		return false
	}
}

func unsignedDenseCodePhysical(t typedcolumn.ColumnType) (logical string, encoding typedcolumn.Encoding, ok bool) {
	switch t {
	case typedcolumn.ColumnTypeUint8Vector:
		return string(columnsemantics.LogicalUint8Vector), typedcolumn.EncodingRawUint8Vector, true
	case typedcolumn.ColumnTypeUint16Vector:
		return string(columnsemantics.LogicalUint16Vector), typedcolumn.EncodingRawUint16Vector, true
	case typedcolumn.ColumnTypeUint32Vector:
		return string(columnsemantics.LogicalUint32Vector), typedcolumn.EncodingRawUint32Vector, true
	case typedcolumn.ColumnTypeUint64Vector:
		return string(columnsemantics.LogicalUint64Vector), typedcolumn.EncodingRawUint64Vector, true
	default:
		return "", 0, false
	}
}

func packedLogicalTypeForPhysical(t typedcolumn.ColumnType) (string, bool) {
	switch t {
	case typedcolumn.ColumnTypePackedBitVector:
		return string(columnsemantics.LogicalPackedBitVector), true
	case typedcolumn.ColumnTypePackedUint2Vector:
		return string(columnsemantics.LogicalPackedUint2Vector), true
	case typedcolumn.ColumnTypePackedUint4Vector:
		return string(columnsemantics.LogicalPackedUint4Vector), true
	default:
		return "", false
	}
}

// Rows returns the graph/vector ordinal row count validated during prepare.
func (p *Prepared) Rows() int {
	if p == nil {
		return 0
	}
	return p.rows
}

// HasRole reports whether role is prepared.
func (p *Prepared) HasRole(role Role) bool {
	if p == nil {
		return false
	}
	_, ok := p.columns[role]
	return ok
}

// RoleRows returns the validated row count for a prepared role. Most code roles
// use the vector row count; side metadata roles may use a smaller row count such
// as one row per storage granule.
func (p *Prepared) RoleRows(role Role) (int, bool) {
	col, ok := p.preparedColumn(role)
	if !ok {
		return 0, false
	}
	return col.rows, true
}

// Footprint returns a copy of prepare-time byte accounting.
func (p *Prepared) Footprint() Footprint {
	if p == nil {
		return Footprint{}
	}
	out := p.footprint
	out.Columns = append([]ColumnFootprint(nil), p.footprint.Columns...)
	return out
}

// CodeRowView returns a zero-copy fixed-width row view for fixed-byte,
// packed-code, and dense unsigned code-vector roles. Role/kind/shape/payload
// checks happen once when the view is created; per-row access only validates the
// ordinal before slicing the immutable prepared payload.
func (p *Prepared) CodeRowView(role Role) (CodeRowView, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || !col.isCode() || col.rows < 0 || col.bytesPerRow <= 0 {
		return CodeRowView{}, false
	}
	wantBytes, err := checkedMul(col.rows, col.bytesPerRow)
	if err != nil || len(col.payload) != wantBytes {
		return CodeRowView{}, false
	}
	return CodeRowView{role: role, rows: col.rows, payload: col.payload, bytesPerRow: col.bytesPerRow, elementsPerRow: col.elementsPerRow}, true
}

// CodeRowBytes returns a zero-copy row byte view for fixed-byte, packed-code,
// and dense unsigned code-vector roles. The slice aliases the prepared image.
func (p *Prepared) CodeRowBytes(role Role, ordinal int) ([]byte, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || !col.isCode() || !col.validOrdinal(ordinal) {
		return nil, false
	}
	start := ordinal * col.bytesPerRow
	end := start + col.bytesPerRow
	if start < 0 || end < start || end > len(col.payload) {
		return nil, false
	}
	return col.payload[start:end], true
}

// Float32 returns a scalar side-array value for role/ordinal.
func (p *Prepared) Float32(role Role, ordinal int) (float32, bool) {
	payload, ok := p.Float32Payload(role)
	if !ok || ordinal < 0 || ordinal >= len(payload)/4 {
		return 0, false
	}
	start := ordinal * 4
	return math.Float32frombits(binary.LittleEndian.Uint32(payload[start : start+4])), true
}

// Float32Payload returns the immutable little-endian scalar float32 payload for
// role. The slice aliases prepared image bytes and must be treated as read-only.
func (p *Prepared) Float32Payload(role Role) ([]byte, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || col.kind != preparedColumnScalarFloat32 || col.rows < 0 {
		return nil, false
	}
	want, err := checkedMul(col.rows, 4)
	if err != nil || len(col.payload) != want {
		return nil, false
	}
	return col.payload, true
}

// Uint32 returns a uint32 scalar side-array value for role/ordinal.
func (p *Prepared) Uint32(role Role, ordinal int) (uint32, bool) {
	payload, ok := p.Uint32Payload(role)
	if !ok || ordinal < 0 || ordinal >= len(payload)/4 {
		return 0, false
	}
	start := ordinal * 4
	return binary.LittleEndian.Uint32(payload[start : start+4]), true
}

// Uint32Payload returns the immutable little-endian scalar uint32 payload for
// role. The slice aliases prepared image bytes and must be treated as read-only.
func (p *Prepared) Uint32Payload(role Role) ([]byte, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || col.kind != preparedColumnScalarUint32 || col.rows < 0 {
		return nil, false
	}
	want, err := checkedMul(col.rows, 4)
	if err != nil || len(col.payload) != want {
		return nil, false
	}
	return col.payload, true
}

// Uint64 returns a uint64 scalar side-array value for role/ordinal.
func (p *Prepared) Uint64(role Role, ordinal int) (uint64, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || col.kind != preparedColumnScalarUint64 || !col.validOrdinal(ordinal) {
		return 0, false
	}
	start := ordinal * 8
	if start < 0 || start > len(col.payload)-8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(col.payload[start : start+8]), true
}

// RowWords returns the code row as little-endian uint64 words. It uses caller
// scratch unless a future implementation can prove a direct word view; direct is
// currently false. If scratch is too small the method allocates.
func (p *Prepared) RowWords(role Role, ordinal int, scratch []uint64) (words []uint64, direct bool, ok bool) {
	row, ok := p.CodeRowBytes(role, ordinal)
	if !ok {
		return nil, false, false
	}
	needed := (len(row) + 7) / 8
	if cap(scratch) < needed {
		scratch = make([]uint64, needed)
	} else {
		scratch = scratch[:needed]
	}
	full := len(row) / 8
	for i := 0; i < full; i++ {
		scratch[i] = binary.LittleEndian.Uint64(row[i*8 : i*8+8])
	}
	if rem := len(row) % 8; rem != 0 {
		var last [8]byte
		copy(last[:], row[full*8:full*8+rem])
		scratch[full] = binary.LittleEndian.Uint64(last[:])
	}
	return scratch, false, true
}

// PackedElements unpacks a packed-code row into caller scratch. If scratch is
// too small the method allocates; pass at least ElementsPerRow(role) bytes for
// steady-state allocation-free access.
func (p *Prepared) PackedElements(role Role, ordinal int, scratch []uint8) ([]uint8, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || col.kind != preparedColumnPackedUint || !col.validOrdinal(ordinal) {
		return nil, false
	}
	row, ok := p.CodeRowBytes(role, ordinal)
	if !ok {
		return nil, false
	}
	if cap(scratch) < col.elementsPerRow {
		scratch = make([]uint8, col.elementsPerRow)
	} else {
		scratch = scratch[:col.elementsPerRow]
	}
	mask := uint16((1 << uint(col.bitsPerElement)) - 1)
	for i := 0; i < col.elementsPerRow; i++ {
		bit := i * col.bitsPerElement
		byteIndex := bit / 8
		shift := uint(bit % 8)
		value := uint16(row[byteIndex]) >> shift
		if shift+uint(col.bitsPerElement) > 8 && byteIndex+1 < len(row) {
			value |= uint16(row[byteIndex+1]) << (8 - shift)
		}
		scratch[i] = uint8(value & mask)
	}
	return scratch, true
}

// DenseUint32Row decodes a uint32_vector code row into caller scratch. If
// scratch is too small the method allocates.
func (p *Prepared) DenseUint32Row(role Role, ordinal int, scratch []uint32) ([]uint32, bool) {
	col, ok := p.preparedColumn(role)
	if !ok || col.kind != preparedColumnDenseBytes || col.typeName != typedcolumn.ColumnTypeUint32Vector || !col.validOrdinal(ordinal) {
		return nil, false
	}
	row, ok := p.CodeRowBytes(role, ordinal)
	if !ok {
		return nil, false
	}
	if cap(scratch) < col.elementsPerRow {
		scratch = make([]uint32, col.elementsPerRow)
	} else {
		scratch = scratch[:col.elementsPerRow]
	}
	for i := 0; i < col.elementsPerRow; i++ {
		off := i * 4
		scratch[i] = binary.LittleEndian.Uint32(row[off : off+4])
	}
	return scratch, true
}

// Role returns the code role validated for this view.
func (v CodeRowView) Role() Role {
	return v.role
}

// Valid reports whether the view was produced by Prepared.CodeRowView.
func (v CodeRowView) Valid() bool {
	return v.rows >= 0 && v.bytesPerRow > 0 && len(v.payload) == v.rows*v.bytesPerRow
}

// Rows returns the number of graph/vector ordinal rows covered by the view.
func (v CodeRowView) Rows() int {
	if !v.Valid() {
		return 0
	}
	return v.rows
}

// BytesPerRow returns the validated physical bytes per code row.
func (v CodeRowView) BytesPerRow() int {
	if !v.Valid() {
		return 0
	}
	return v.bytesPerRow
}

// ElementsPerRow returns the validated code elements per row.
func (v CodeRowView) ElementsPerRow() int {
	if !v.Valid() {
		return 0
	}
	return v.elementsPerRow
}

// PayloadBytes returns the validated row-major payload backing this view. The
// slice aliases immutable prepared image bytes and must be treated as read-only.
func (v CodeRowView) PayloadBytes() ([]byte, bool) {
	if !v.Valid() {
		return nil, false
	}
	return v.payload, true
}

// RowBytes returns a zero-copy row byte slice by graph/vector ordinal. The
// slice aliases immutable prepared image bytes and must be treated as read-only.
func (v CodeRowView) RowBytes(ordinal int) ([]byte, bool) {
	if ordinal < 0 || ordinal >= v.rows || v.bytesPerRow <= 0 {
		return nil, false
	}
	start := ordinal * v.bytesPerRow
	return v.payload[start : start+v.bytesPerRow], true
}

// ElementsPerRow returns the fixed vector/code elements per row for role.
func (p *Prepared) ElementsPerRow(role Role) (int, bool) {
	col, ok := p.preparedColumn(role)
	if !ok {
		return 0, false
	}
	return col.elementsPerRow, true
}

// BytesPerRow returns the physical bytes per row for role.
func (p *Prepared) BytesPerRow(role Role) (int, bool) {
	col, ok := p.preparedColumn(role)
	if !ok {
		return 0, false
	}
	return col.bytesPerRow, true
}

func (p *Prepared) preparedColumn(role Role) (preparedColumn, bool) {
	if p == nil {
		return preparedColumn{}, false
	}
	col, ok := p.columns[role]
	return col, ok
}

func (c preparedColumn) validOrdinal(ordinal int) bool {
	return ordinal >= 0 && ordinal < c.rows
}

func (c preparedColumn) isCode() bool {
	return c.kind == preparedColumnFixedBytes || c.kind == preparedColumnPackedUint || c.kind == preparedColumnDenseBytes
}

func (a AssetRefIdentity) key() string {
	return fmt.Sprintf("%s/%s/%d/%d/%d/%d/%d/%d", a.Kind, a.Namespace, a.Generation, a.PartID, a.FileID, a.Offset, a.Length, a.Checksum)
}

func partSourceKey(src PartImageSource) string {
	if src.AssetID != "" {
		return "id:" + src.AssetID
	}
	if src.Ref.Present {
		return "ref:" + src.Ref.key()
	}
	return fmt.Sprintf("image:%p:%d", &src.Image.Bytes[0], len(src.Image.Bytes))
}

func checkedMul(a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("quantizedasset: negative multiplication %d*%d", a, b)
	}
	if a != 0 && b > math.MaxInt/a {
		return 0, fmt.Errorf("quantizedasset: multiplication overflow %d*%d", a, b)
	}
	return a * b, nil
}
