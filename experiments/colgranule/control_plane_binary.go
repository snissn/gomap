package colgranule

import (
	"encoding/binary"
	"fmt"
	crc32 "github.com/snissn/go-crc32-asm"
	"math"
	"unicode/utf8"
)

const (
	columnCollectionManifestBinaryMagic   = "TCC2"
	columnWorkspaceManifestBinaryMagic    = "TCW2"
	columnWorkspacePreparedBinaryMagic    = "TCP2"
	columnCollectionManifestBinaryVersion = 2
	columnWorkspaceManifestBinaryVersion  = 2
	columnWorkspacePreparedBinaryVersion  = 2
	columnControlPlaneBinaryHeaderLen     = 32
)

var maxColumnBinaryInt = int64(int(^uint(0) >> 1))

type columnBinaryWriter struct {
	buf []byte
	err error
}

func encodeColumnCollectionManifestBinaryEnvelope(manifest ColumnCollectionManifest) ([]byte, error) {
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return nil, err
	}
	w := columnBinaryWriter{}
	writeColumnCollectionManifestBinary(&w, manifest)
	if w.err != nil {
		return nil, w.err
	}
	return encodeColumnControlPlaneEnvelope(columnCollectionManifestBinaryMagic, columnCollectionManifestBinaryVersion, w.buf)
}

func decodeColumnCollectionManifestBinaryEnvelope(data []byte) (ColumnCollectionManifest, error) {
	body, err := decodeColumnControlPlaneEnvelope(data, columnCollectionManifestBinaryMagic, columnCollectionManifestBinaryVersion, "collection manifest")
	if err != nil {
		return ColumnCollectionManifest{}, err
	}
	r := columnBinaryReader{data: body, label: "collection manifest"}
	manifest := readColumnCollectionManifestBinary(&r)
	if err := r.done(); err != nil {
		return ColumnCollectionManifest{}, err
	}
	if err := validateColumnCollectionManifest(manifest); err != nil {
		return ColumnCollectionManifest{}, err
	}
	return manifest, nil
}

func encodeColumnWorkspaceManifestBinaryEnvelope(manifest ColumnWorkspaceManifest) ([]byte, error) {
	if err := validateColumnWorkspaceManifest(manifest); err != nil {
		return nil, err
	}
	w := columnBinaryWriter{}
	writeColumnWorkspaceManifestBinary(&w, manifest)
	if w.err != nil {
		return nil, w.err
	}
	return encodeColumnControlPlaneEnvelope(columnWorkspaceManifestBinaryMagic, columnWorkspaceManifestBinaryVersion, w.buf)
}

func decodeColumnWorkspaceManifestBinaryEnvelope(data []byte) (ColumnWorkspaceManifest, error) {
	body, err := decodeColumnControlPlaneEnvelope(data, columnWorkspaceManifestBinaryMagic, columnWorkspaceManifestBinaryVersion, "workspace manifest")
	if err != nil {
		return ColumnWorkspaceManifest{}, err
	}
	r := columnBinaryReader{data: body, label: "workspace manifest"}
	manifest := readColumnWorkspaceManifestBinary(&r)
	if err := r.done(); err != nil {
		return ColumnWorkspaceManifest{}, err
	}
	if err := validateColumnWorkspaceManifest(manifest); err != nil {
		return ColumnWorkspaceManifest{}, err
	}
	return manifest, nil
}

func encodeColumnPreparedAssetRegistryBinaryEnvelope(registry ColumnPreparedAssetRegistry) ([]byte, error) {
	if err := validateColumnPreparedAssetRegistry(registry); err != nil {
		return nil, err
	}
	w := columnBinaryWriter{}
	writeColumnPreparedAssetRegistryBinary(&w, registry)
	if w.err != nil {
		return nil, w.err
	}
	return encodeColumnControlPlaneEnvelope(columnWorkspacePreparedBinaryMagic, columnWorkspacePreparedBinaryVersion, w.buf)
}

func decodeColumnPreparedAssetRegistryBinaryEnvelope(data []byte) (ColumnPreparedAssetRegistry, error) {
	body, err := decodeColumnControlPlaneEnvelope(data, columnWorkspacePreparedBinaryMagic, columnWorkspacePreparedBinaryVersion, "prepared registry")
	if err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	r := columnBinaryReader{data: body, label: "prepared registry"}
	registry := readColumnPreparedAssetRegistryBinary(&r)
	if err := r.done(); err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	if err := validateColumnPreparedAssetRegistry(registry); err != nil {
		return ColumnPreparedAssetRegistry{}, err
	}
	return registry, nil
}

func encodeColumnControlPlaneEnvelope(magic string, version uint16, body []byte) ([]byte, error) {
	if len(magic) != 4 {
		return nil, fmt.Errorf("colgranule: binary envelope magic %q is not 4 bytes", magic)
	}
	if len(body) > math.MaxUint32 && uint64(len(body)) > math.MaxUint64-uint64(columnControlPlaneBinaryHeaderLen) {
		return nil, fmt.Errorf("colgranule: binary envelope body too large")
	}
	out := make([]byte, columnControlPlaneBinaryHeaderLen+len(body))
	copy(out[0:4], magic)
	binary.LittleEndian.PutUint16(out[4:], version)
	binary.LittleEndian.PutUint16(out[6:], columnControlPlaneBinaryHeaderLen)
	binary.LittleEndian.PutUint32(out[8:], 0)
	binary.LittleEndian.PutUint64(out[12:], uint64(len(body)))
	binary.LittleEndian.PutUint32(out[20:], crc32.ChecksumIEEE(body))
	copy(out[columnControlPlaneBinaryHeaderLen:], body)
	return out, nil
}

func decodeColumnControlPlaneEnvelope(data []byte, magic string, version uint16, label string) ([]byte, error) {
	if !isColumnControlPlaneBinary(data, magic) {
		return nil, fmt.Errorf("colgranule: invalid %s binary magic", label)
	}
	if len(data) < columnControlPlaneBinaryHeaderLen {
		return nil, fmt.Errorf("colgranule: truncated %s binary header", label)
	}
	gotVersion := binary.LittleEndian.Uint16(data[4:])
	if gotVersion != version {
		return nil, fmt.Errorf("colgranule: unsupported %s binary version %d", label, gotVersion)
	}
	headerLen := binary.LittleEndian.Uint16(data[6:])
	if headerLen != columnControlPlaneBinaryHeaderLen {
		return nil, fmt.Errorf("colgranule: %s binary header length=%d want %d", label, headerLen, columnControlPlaneBinaryHeaderLen)
	}
	bodyLen := binary.LittleEndian.Uint64(data[12:])
	if bodyLen > uint64(len(data)-columnControlPlaneBinaryHeaderLen) {
		return nil, fmt.Errorf("colgranule: %s binary body length=%d exceeds file bytes=%d", label, bodyLen, len(data)-columnControlPlaneBinaryHeaderLen)
	}
	if uint64(len(data)-columnControlPlaneBinaryHeaderLen) != bodyLen {
		return nil, fmt.Errorf("colgranule: %s binary file has trailing bytes", label)
	}
	body := data[columnControlPlaneBinaryHeaderLen:]
	checksum := crc32.ChecksumIEEE(body)
	want := binary.LittleEndian.Uint32(data[20:])
	if checksum != want {
		return nil, fmt.Errorf("colgranule: %s binary checksum=%08x want %08x", label, checksum, want)
	}
	return body, nil
}

func isColumnControlPlaneBinary(data []byte, magic string) bool {
	return len(data) >= 4 && len(magic) == 4 && string(data[:4]) == magic
}

func writeColumnCollectionManifestBinary(w *columnBinaryWriter, manifest ColumnCollectionManifest) {
	w.string(manifest.Collection)
	w.u32(manifest.SchemaVersion)
	w.u8(mustColumnSchemaModeCode(w, manifest.SchemaMode))
	w.stringSlice(manifest.LogicalPrimaryKey.Columns)
	writeSortKeyColumnsBinary(w, manifest.SortKey)
	writeColumnDefinitionsBinary(w, manifest.DeclaredColumns)
	w.u64(manifest.ActiveGeneration)
	writeColumnCollectionAttachmentBinary(w, manifest.Attachment)
	writeColumnPartSetManifestBinary(w, manifest.PartSet)
	writeColumnCollectionByteAccountingBinary(w, manifest.ByteAccounting)
	w.i64(manifest.CreatedUnix)
	w.i64(manifest.UpdatedUnix)
}

func readColumnCollectionManifestBinary(r *columnBinaryReader) ColumnCollectionManifest {
	manifest := ColumnCollectionManifest{
		Magic:         columnCollectionManifestMagic,
		Version:       columnCollectionManifestVersion,
		Collection:    r.string(),
		SchemaVersion: r.u32(),
		SchemaMode:    columnSchemaModeFromCode(r.u8(), r),
	}
	manifest.LogicalPrimaryKey.Columns = r.stringSlice()
	manifest.SortKey = readSortKeyColumnsBinary(r)
	manifest.DeclaredColumns = readColumnDefinitionsBinary(r)
	manifest.ActiveGeneration = r.u64()
	manifest.Attachment = readColumnCollectionAttachmentBinary(r)
	manifest.PartSet = readColumnPartSetManifestBinary(r)
	manifest.ByteAccounting = readColumnCollectionByteAccountingBinary(r)
	manifest.CreatedUnix = r.i64()
	manifest.UpdatedUnix = r.i64()
	return manifest
}

func writeColumnWorkspaceManifestBinary(w *columnBinaryWriter, manifest ColumnWorkspaceManifest) {
	w.string(manifest.Collection)
	w.u64(manifest.Generation)
	w.u64(manifest.PublishID)
	w.i64(manifest.CreatedUnix)
	w.i64(manifest.UpdatedUnix)
	w.count(len(manifest.Parts), "workspace parts")
	for i := range manifest.Parts {
		writeColumnWorkspacePartManifestBinary(w, manifest.Parts[i])
	}
}

func readColumnWorkspaceManifestBinary(r *columnBinaryReader) ColumnWorkspaceManifest {
	manifest := ColumnWorkspaceManifest{
		Magic:       columnWorkspaceManifestMagic,
		Version:     columnWorkspaceManifestVersion,
		Collection:  r.string(),
		Generation:  r.u64(),
		PublishID:   r.u64(),
		CreatedUnix: r.i64(),
		UpdatedUnix: r.i64(),
	}
	n := r.count("workspace parts")
	manifest.Parts = make([]ColumnWorkspacePartManifest, n)
	for i := range manifest.Parts {
		manifest.Parts[i] = readColumnWorkspacePartManifestBinary(r)
	}
	return manifest
}

func writeColumnPreparedAssetRegistryBinary(w *columnBinaryWriter, registry ColumnPreparedAssetRegistry) {
	w.string(registry.Collection)
	w.u64(registry.PublishID)
	w.u64(registry.GenerationID)
	w.i64(registry.UpdatedUnix)
	w.count(len(registry.Assets), "prepared assets")
	for i := range registry.Assets {
		writeColumnPreparedAssetBinary(w, registry.Assets[i])
	}
}

func readColumnPreparedAssetRegistryBinary(r *columnBinaryReader) ColumnPreparedAssetRegistry {
	registry := ColumnPreparedAssetRegistry{
		Magic:        columnWorkspacePreparedMagic,
		Version:      columnWorkspacePreparedVersion,
		Collection:   r.string(),
		PublishID:    r.u64(),
		GenerationID: r.u64(),
		UpdatedUnix:  r.i64(),
	}
	n := r.count("prepared assets")
	registry.Assets = make([]ColumnPreparedAsset, n)
	for i := range registry.Assets {
		registry.Assets[i] = readColumnPreparedAssetBinary(r)
	}
	return registry
}

func writeColumnCollectionAttachmentBinary(w *columnBinaryWriter, attachment ColumnCollectionAttachment) {
	w.string(attachment.Model)
	w.string(attachment.SystemMetadataKey)
	w.string(attachment.RowPrimaryRoot)
	w.string(attachment.ManifestRootRef)
	w.string(attachment.LocatorRootRef)
	w.stringSlice(attachment.SecondaryRoots)
}

func readColumnCollectionAttachmentBinary(r *columnBinaryReader) ColumnCollectionAttachment {
	return ColumnCollectionAttachment{
		Model:             r.string(),
		SystemMetadataKey: r.string(),
		RowPrimaryRoot:    r.string(),
		ManifestRootRef:   r.string(),
		LocatorRootRef:    r.string(),
		SecondaryRoots:    r.stringSlice(),
	}
}

func writeColumnPartSetManifestBinary(w *columnBinaryWriter, partSet ColumnPartSetManifest) {
	w.count(len(partSet.BaseParts), "base parts")
	for i := range partSet.BaseParts {
		writeColumnManifestPartRefBinary(w, partSet.BaseParts[i])
	}
	w.count(len(partSet.DeltaParts), "delta parts")
	for i := range partSet.DeltaParts {
		writeColumnManifestPartRefBinary(w, partSet.DeltaParts[i])
	}
	w.count(len(partSet.Tombstones), "tombstones")
	for i := range partSet.Tombstones {
		writeColumnTombstoneBinary(w, partSet.Tombstones[i])
	}
}

func readColumnPartSetManifestBinary(r *columnBinaryReader) ColumnPartSetManifest {
	baseCount := r.count("base parts")
	partSet := ColumnPartSetManifest{BaseParts: make([]ColumnManifestPartRef, baseCount)}
	for i := range partSet.BaseParts {
		partSet.BaseParts[i] = readColumnManifestPartRefBinary(r)
	}
	deltaCount := r.count("delta parts")
	partSet.DeltaParts = make([]ColumnManifestPartRef, deltaCount)
	for i := range partSet.DeltaParts {
		partSet.DeltaParts[i] = readColumnManifestPartRefBinary(r)
	}
	tombstoneCount := r.count("tombstones")
	partSet.Tombstones = make([]ColumnTombstone, tombstoneCount)
	for i := range partSet.Tombstones {
		partSet.Tombstones[i] = readColumnTombstoneBinary(r)
	}
	return partSet
}

func writeColumnManifestPartRefBinary(w *columnBinaryWriter, ref ColumnManifestPartRef) {
	w.u8(mustColumnPartRoleCode(w, ref.Role))
	w.u64(ref.GenerationID)
	writeColumnPartCoverageDescriptorBinary(w, ref.Coverage)
	writeColumnWorkspacePartManifestBinary(w, ref.Part)
}

func readColumnManifestPartRefBinary(r *columnBinaryReader) ColumnManifestPartRef {
	return ColumnManifestPartRef{
		Role:         columnPartRoleFromCode(r.u8(), r),
		GenerationID: r.u64(),
		Coverage:     readColumnPartCoverageDescriptorBinary(r),
		Part:         readColumnWorkspacePartManifestBinary(r),
	}
}

func writeColumnPartCoverageDescriptorBinary(w *columnBinaryWriter, coverage ColumnPartCoverageDescriptor) {
	w.u8(mustColumnPartRoleCode(w, coverage.Role))
	w.u64(coverage.GenerationID)
	w.u8(coverage.CompactionLevel)
	w.count(len(coverage.SourceParts), "source parts")
	for i := range coverage.SourceParts {
		w.u64(coverage.SourceParts[i].PartID)
		w.u64(coverage.SourceParts[i].GenerationID)
	}
	w.u64(coverage.SourceRowRootGeneration)
	w.u64(coverage.SourceRowVersionLower)
	w.u64(coverage.SourceRowVersionUpper)
	w.i64(coverage.PrimaryIDLower)
	w.i64(coverage.PrimaryIDUpperExclusive)
	w.stringSlice(coverage.SortKeyColumns)
	w.i64Slice(coverage.SortKeyLower)
	w.i64Slice(coverage.SortKeyUpperExclusive)
	w.bool(coverage.SortKeyUpperUnbounded)
	w.intValue(coverage.Rows, "coverage rows")
	w.intValue(coverage.VisibleRows, "coverage visible rows")
	w.intValue(coverage.DeletedRows, "coverage deleted rows")
	w.count(len(coverage.AssetRefs), "coverage asset refs")
	for i := range coverage.AssetRefs {
		writeColumnAssetRefBinary(w, coverage.AssetRefs[i])
	}
	w.u32Slice(coverage.Checksums)
}

func readColumnPartCoverageDescriptorBinary(r *columnBinaryReader) ColumnPartCoverageDescriptor {
	coverage := ColumnPartCoverageDescriptor{
		Role:            columnPartRoleFromCode(r.u8(), r),
		GenerationID:    r.u64(),
		CompactionLevel: r.u8(),
	}
	sourceCount := r.count("source parts")
	coverage.SourceParts = make([]ColumnSourcePartGeneration, sourceCount)
	for i := range coverage.SourceParts {
		coverage.SourceParts[i] = ColumnSourcePartGeneration{PartID: r.u64(), GenerationID: r.u64()}
	}
	coverage.SourceRowRootGeneration = r.u64()
	coverage.SourceRowVersionLower = r.u64()
	coverage.SourceRowVersionUpper = r.u64()
	coverage.PrimaryIDLower = r.i64()
	coverage.PrimaryIDUpperExclusive = r.i64()
	coverage.SortKeyColumns = r.stringSlice()
	coverage.SortKeyLower = r.i64Slice()
	coverage.SortKeyUpperExclusive = r.i64Slice()
	coverage.SortKeyUpperUnbounded = r.bool()
	coverage.Rows = r.intValue("coverage rows")
	coverage.VisibleRows = r.intValue("coverage visible rows")
	coverage.DeletedRows = r.intValue("coverage deleted rows")
	assetCount := r.count("coverage asset refs")
	coverage.AssetRefs = make([]ColumnAssetRef, assetCount)
	for i := range coverage.AssetRefs {
		coverage.AssetRefs[i] = readColumnAssetRefBinary(r)
	}
	coverage.Checksums = r.u32Slice()
	return coverage
}

func writeColumnWorkspacePartManifestBinary(w *columnBinaryWriter, part ColumnWorkspacePartManifest) {
	w.u64(part.PartID)
	w.intValue(part.Rows, "part rows")
	w.intValue(part.VisibleRows, "part visible rows")
	w.u32(part.SchemaVersion)
	writeSortKeyColumnsBinary(w, part.SortKey)
	writeColumnWorkspacePartCoverageBinary(w, part.Coverage)
	writeColumnAssetRefBinary(w, part.AssetRef)
	writeTCS1PartRecordBinary(w, part.TCS1)
	w.intValue(part.ImageBytes, "image bytes")
	w.intValue(part.ManifestBytes, "manifest bytes")
	w.intValue(part.Sections, "sections")
	w.intValue(part.AssetBytes, "asset bytes")
	w.i64(part.PublishedUnix)
}

func readColumnWorkspacePartManifestBinary(r *columnBinaryReader) ColumnWorkspacePartManifest {
	return ColumnWorkspacePartManifest{
		PartID:        r.u64(),
		Rows:          r.intValue("part rows"),
		VisibleRows:   r.intValue("part visible rows"),
		SchemaVersion: r.u32(),
		SortKey:       readSortKeyColumnsBinary(r),
		Coverage:      readColumnWorkspacePartCoverageBinary(r),
		AssetRef:      readColumnAssetRefBinary(r),
		TCS1:          readTCS1PartRecordBinary(r),
		ImageBytes:    r.intValue("image bytes"),
		ManifestBytes: r.intValue("manifest bytes"),
		Sections:      r.intValue("sections"),
		AssetBytes:    r.intValue("asset bytes"),
		PublishedUnix: r.i64(),
	}
}

func writeColumnWorkspacePartCoverageBinary(w *columnBinaryWriter, coverage ColumnWorkspacePartCoverage) {
	w.i64(coverage.PrimaryIDLower)
	w.i64(coverage.PrimaryIDUpperExclusive)
	w.stringSlice(coverage.SortKeyColumns)
	w.i64Slice(coverage.SortKeyLower)
	w.i64Slice(coverage.SortKeyUpperExclusive)
	w.bool(coverage.SortKeyUpperUnbounded)
}

func readColumnWorkspacePartCoverageBinary(r *columnBinaryReader) ColumnWorkspacePartCoverage {
	return ColumnWorkspacePartCoverage{
		PrimaryIDLower:          r.i64(),
		PrimaryIDUpperExclusive: r.i64(),
		SortKeyColumns:          r.stringSlice(),
		SortKeyLower:            r.i64Slice(),
		SortKeyUpperExclusive:   r.i64Slice(),
		SortKeyUpperUnbounded:   r.bool(),
	}
}

func writeColumnAssetRefBinary(w *columnBinaryWriter, ref ColumnAssetRef) {
	w.u8(mustColumnAssetKindCode(w, ref.Kind))
	w.u32(ref.FileID)
	w.i64(ref.Offset)
	w.i64(ref.Length)
	w.u32(ref.Checksum)
}

func readColumnAssetRefBinary(r *columnBinaryReader) ColumnAssetRef {
	return ColumnAssetRef{
		Kind:     columnAssetKindFromCode(r.u8(), r),
		FileID:   r.u32(),
		Offset:   r.i64(),
		Length:   r.i64(),
		Checksum: r.u32(),
	}
}

func writeTCS1PartRecordBinary(w *columnBinaryWriter, record TCS1PartRecord) {
	w.u16(record.Version)
	w.u16(record.Kind)
	w.u32(record.Flags)
	w.u64(record.PartID)
	w.intValue(record.Rows, "tcs1 rows")
	w.u16(record.ImageVersion)
	w.intValue(record.PayloadBytes, "payload bytes")
	w.intValue(record.TotalBytes, "total bytes")
	w.u32(record.PayloadCRC32)
	writeColumnAssetRefBinary(w, record.AssetRef)
}

func readTCS1PartRecordBinary(r *columnBinaryReader) TCS1PartRecord {
	return TCS1PartRecord{
		Version:      r.u16(),
		Kind:         r.u16(),
		Flags:        r.u32(),
		PartID:       r.u64(),
		Rows:         r.intValue("tcs1 rows"),
		ImageVersion: r.u16(),
		PayloadBytes: r.intValue("payload bytes"),
		TotalBytes:   r.intValue("total bytes"),
		PayloadCRC32: r.u32(),
		AssetRef:     readColumnAssetRefBinary(r),
	}
}

func writeColumnTombstoneBinary(w *columnBinaryWriter, tombstone ColumnTombstone) {
	w.i64(tombstone.PrimaryID)
	w.u64(tombstone.GenerationID)
	w.string(tombstone.Reason)
	w.intValue(tombstone.PreparedBytes, "prepared bytes")
}

func readColumnTombstoneBinary(r *columnBinaryReader) ColumnTombstone {
	return ColumnTombstone{
		PrimaryID:     r.i64(),
		GenerationID:  r.u64(),
		Reason:        r.string(),
		PreparedBytes: r.intValue("prepared bytes"),
	}
}

func writeColumnPreparedAssetBinary(w *columnBinaryWriter, asset ColumnPreparedAsset) {
	writeColumnAssetRefBinary(w, asset.Ref)
	w.intValue(asset.Bytes, "prepared bytes")
	w.u64(asset.PublishID)
	w.u64(asset.GenerationID)
	w.string(asset.Reason)
}

func readColumnPreparedAssetBinary(r *columnBinaryReader) ColumnPreparedAsset {
	return ColumnPreparedAsset{
		Ref:          readColumnAssetRefBinary(r),
		Bytes:        r.intValue("prepared bytes"),
		PublishID:    r.u64(),
		GenerationID: r.u64(),
		Reason:       r.string(),
	}
}

func writeColumnCollectionByteAccountingBinary(w *columnBinaryWriter, accounting ColumnCollectionByteAccounting) {
	w.intValue(accounting.Parts, "accounting parts")
	w.intValue(accounting.BaseParts, "accounting base parts")
	w.intValue(accounting.DeltaParts, "accounting delta parts")
	w.intValue(accounting.Rows, "accounting rows")
	w.intValue(accounting.VisibleRows, "accounting visible rows")
	w.intValue(accounting.Tombstones, "accounting tombstones")
	w.intValue(accounting.DeclaredColumns, "accounting declared columns")
	w.intValue(accounting.DescriptorBytes, "accounting descriptor bytes")
	w.intValue(accounting.BaseAssetBytes, "accounting base asset bytes")
	w.intValue(accounting.DeltaAssetBytes, "accounting delta asset bytes")
	w.intValue(accounting.TotalAssetBytes, "accounting total asset bytes")
	w.intValue(accounting.ReclaimableCandidateBytes, "accounting reclaimable bytes")
}

func readColumnCollectionByteAccountingBinary(r *columnBinaryReader) ColumnCollectionByteAccounting {
	return ColumnCollectionByteAccounting{
		Parts:                     r.intValue("accounting parts"),
		BaseParts:                 r.intValue("accounting base parts"),
		DeltaParts:                r.intValue("accounting delta parts"),
		Rows:                      r.intValue("accounting rows"),
		VisibleRows:               r.intValue("accounting visible rows"),
		Tombstones:                r.intValue("accounting tombstones"),
		DeclaredColumns:           r.intValue("accounting declared columns"),
		DescriptorBytes:           r.intValue("accounting descriptor bytes"),
		BaseAssetBytes:            r.intValue("accounting base asset bytes"),
		DeltaAssetBytes:           r.intValue("accounting delta asset bytes"),
		TotalAssetBytes:           r.intValue("accounting total asset bytes"),
		ReclaimableCandidateBytes: r.intValue("accounting reclaimable bytes"),
	}
}

func writeSortKeyColumnsBinary(w *columnBinaryWriter, columns []SortKeyColumn) {
	w.count(len(columns), "sort keys")
	for i := range columns {
		w.string(columns[i].Column)
		w.u8(mustSortKeyDirectionCode(w, columns[i].Direction))
		w.u8(mustSortKeyNullOrderCode(w, columns[i].Nulls))
	}
}

func readSortKeyColumnsBinary(r *columnBinaryReader) []SortKeyColumn {
	n := r.count("sort keys")
	out := make([]SortKeyColumn, n)
	for i := range out {
		out[i] = SortKeyColumn{
			Column:    r.string(),
			Direction: sortKeyDirectionFromCode(r.u8(), r),
			Nulls:     sortKeyNullOrderFromCode(r.u8(), r),
		}
	}
	return out
}

func writeColumnDefinitionsBinary(w *columnBinaryWriter, defs []ColumnDefinition) {
	w.count(len(defs), "column definitions")
	for i := range defs {
		w.string(defs[i].Name)
		w.u8(mustColumnTypeCode(w, defs[i].Type))
		w.u8(uint8(defs[i].Encoding))
		w.u8(uint8(defs[i].Compression))
		w.u32(defs[i].Cardinality)
		w.intValue(defs[i].CodecBlockRows, "codec block rows")
	}
}

func readColumnDefinitionsBinary(r *columnBinaryReader) []ColumnDefinition {
	n := r.count("column definitions")
	out := make([]ColumnDefinition, n)
	for i := range out {
		out[i] = ColumnDefinition{
			Name:           r.string(),
			Type:           columnBinaryTypeFromCode(r.u8(), r),
			Encoding:       Encoding(r.u8()),
			Compression:    Compression(r.u8()),
			Cardinality:    r.u32(),
			CodecBlockRows: r.intValue("codec block rows"),
		}
	}
	return out
}

func (w *columnBinaryWriter) u8(v uint8) {
	if w.err != nil {
		return
	}
	w.buf = append(w.buf, v)
}

func (w *columnBinaryWriter) bool(v bool) {
	if v {
		w.u8(1)
		return
	}
	w.u8(0)
}

func (w *columnBinaryWriter) u16(v uint16) {
	if w.err != nil {
		return
	}
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], v)
	w.buf = append(w.buf, tmp[:]...)
}

func (w *columnBinaryWriter) u32(v uint32) {
	if w.err != nil {
		return
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	w.buf = append(w.buf, tmp[:]...)
}

func (w *columnBinaryWriter) u64(v uint64) {
	if w.err != nil {
		return
	}
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.buf = append(w.buf, tmp[:]...)
}

func (w *columnBinaryWriter) i64(v int64) {
	w.u64(uint64(v))
}

func (w *columnBinaryWriter) intValue(v int, field string) {
	if v < 0 {
		w.fail("%s is negative: %d", field, v)
		return
	}
	w.i64(int64(v))
}

func (w *columnBinaryWriter) count(n int, field string) {
	if n < 0 || n > math.MaxUint32 {
		w.fail("%s count=%d exceeds uint32", field, n)
		return
	}
	w.u32(uint32(n))
}

func (w *columnBinaryWriter) string(v string) {
	if !utf8.ValidString(v) {
		w.fail("invalid UTF-8 string")
		return
	}
	if len(v) > math.MaxUint32 {
		w.fail("string length=%d exceeds uint32", len(v))
		return
	}
	w.u32(uint32(len(v)))
	if w.err == nil {
		w.buf = append(w.buf, v...)
	}
}

func (w *columnBinaryWriter) stringSlice(values []string) {
	w.count(len(values), "string slice")
	for _, value := range values {
		w.string(value)
	}
}

func (w *columnBinaryWriter) i64Slice(values []int64) {
	w.count(len(values), "int64 slice")
	for _, value := range values {
		w.i64(value)
	}
}

func (w *columnBinaryWriter) u32Slice(values []uint32) {
	w.count(len(values), "uint32 slice")
	for _, value := range values {
		w.u32(value)
	}
}

func (w *columnBinaryWriter) fail(format string, args ...any) {
	if w.err == nil {
		w.err = fmt.Errorf("colgranule: binary encode: "+format, args...)
	}
}

type columnBinaryReader struct {
	data  []byte
	off   int
	label string
	err   error
}

func (r *columnBinaryReader) done() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.data) {
		return fmt.Errorf("colgranule: %s binary has %d trailing body bytes", r.label, len(r.data)-r.off)
	}
	return nil
}

func (r *columnBinaryReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.data)-r.off {
		r.err = fmt.Errorf("colgranule: truncated %s binary body", r.label)
		return nil
	}
	out := r.data[r.off : r.off+n]
	r.off += n
	return out
}

func (r *columnBinaryReader) u8() uint8 {
	b := r.bytes(1)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

func (r *columnBinaryReader) bool() bool {
	v := r.u8()
	switch v {
	case 0:
		return false
	case 1:
		return true
	default:
		r.fail("invalid bool value %d", v)
		return false
	}
}

func (r *columnBinaryReader) u16() uint16 {
	b := r.bytes(2)
	if len(b) < 2 {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (r *columnBinaryReader) u32() uint32 {
	b := r.bytes(4)
	if len(b) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (r *columnBinaryReader) u64() uint64 {
	b := r.bytes(8)
	if len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (r *columnBinaryReader) i64() int64 {
	return int64(r.u64())
}

func (r *columnBinaryReader) intValue(field string) int {
	v := r.i64()
	if v < 0 || v > maxColumnBinaryInt {
		r.fail("%s=%d exceeds host int", field, v)
		return 0
	}
	return int(v)
}

func (r *columnBinaryReader) count(field string) int {
	v := r.u32()
	if uint64(v) > uint64(len(r.data)-r.off) {
		r.fail("%s count=%d exceeds remaining body bytes=%d", field, v, len(r.data)-r.off)
		return 0
	}
	return int(v)
}

func (r *columnBinaryReader) string() string {
	n := r.u32()
	if uint64(n) > uint64(len(r.data)-r.off) {
		r.fail("string length=%d exceeds remaining body bytes=%d", n, len(r.data)-r.off)
		return ""
	}
	b := r.bytes(int(n))
	if !utf8.Valid(b) {
		r.fail("invalid UTF-8 string")
		return ""
	}
	return string(b)
}

func (r *columnBinaryReader) stringSlice() []string {
	n := r.count("string slice")
	out := make([]string, n)
	for i := range out {
		out[i] = r.string()
	}
	return out
}

func (r *columnBinaryReader) i64Slice() []int64 {
	n := r.count("int64 slice")
	out := make([]int64, n)
	for i := range out {
		out[i] = r.i64()
	}
	return out
}

func (r *columnBinaryReader) u32Slice() []uint32 {
	n := r.count("uint32 slice")
	out := make([]uint32, n)
	for i := range out {
		out[i] = r.u32()
	}
	return out
}

func (r *columnBinaryReader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("colgranule: decode %s binary: "+format, append([]any{r.label}, args...)...)
	}
}

func mustColumnSchemaModeCode(w *columnBinaryWriter, mode ColumnSchemaMode) uint8 {
	switch mode {
	case ColumnSchemaFixed:
		return 1
	default:
		w.fail("unsupported schema mode %s", mode)
		return 0
	}
}

func columnSchemaModeFromCode(code uint8, r *columnBinaryReader) ColumnSchemaMode {
	switch code {
	case 1:
		return ColumnSchemaFixed
	default:
		r.fail("unsupported schema mode code %d", code)
		return ""
	}
}

func mustColumnPartRoleCode(w *columnBinaryWriter, role ColumnPartRole) uint8 {
	switch role {
	case ColumnPartRoleBase:
		return 1
	case ColumnPartRoleDelta:
		return 2
	default:
		w.fail("unsupported part role %s", role)
		return 0
	}
}

func columnPartRoleFromCode(code uint8, r *columnBinaryReader) ColumnPartRole {
	switch code {
	case 1:
		return ColumnPartRoleBase
	case 2:
		return ColumnPartRoleDelta
	default:
		r.fail("unsupported part role code %d", code)
		return ""
	}
}

func mustColumnAssetKindCode(w *columnBinaryWriter, kind ColumnAssetKind) uint8 {
	switch kind {
	case ColumnAssetKindTCS1PartImage:
		return 1
	default:
		w.fail("unsupported asset kind %s", kind)
		return 0
	}
}

func columnAssetKindFromCode(code uint8, r *columnBinaryReader) ColumnAssetKind {
	switch code {
	case 1:
		return ColumnAssetKindTCS1PartImage
	default:
		r.fail("unsupported asset kind code %d", code)
		return ""
	}
}

func mustColumnTypeCode(w *columnBinaryWriter, typ ColumnType) uint8 {
	switch typ {
	case ColumnTypeInt64:
		return 1
	case ColumnTypeLowCardinalityCode:
		return 2
	case ColumnTypeBool:
		return 3
	default:
		w.fail("unsupported column type %s", typ)
		return 0
	}
}

func columnBinaryTypeFromCode(code uint8, r *columnBinaryReader) ColumnType {
	switch code {
	case 1:
		return ColumnTypeInt64
	case 2:
		return ColumnTypeLowCardinalityCode
	case 3:
		return ColumnTypeBool
	default:
		r.fail("unsupported column type code %d", code)
		return ""
	}
}

func mustSortKeyDirectionCode(w *columnBinaryWriter, direction SortKeyDirection) uint8 {
	switch direction {
	case "", SortKeyAsc:
		return 1
	case SortKeyDesc:
		return 2
	default:
		w.fail("unsupported sort key direction %s", direction)
		return 0
	}
}

func sortKeyDirectionFromCode(code uint8, r *columnBinaryReader) SortKeyDirection {
	switch code {
	case 1:
		return SortKeyAsc
	case 2:
		return SortKeyDesc
	default:
		r.fail("unsupported sort key direction code %d", code)
		return ""
	}
}

func mustSortKeyNullOrderCode(w *columnBinaryWriter, nulls SortKeyNullOrder) uint8 {
	switch nulls {
	case SortKeyNullsDefault:
		return 0
	case SortKeyNullsFirst:
		return 1
	case SortKeyNullsLast:
		return 2
	default:
		w.fail("unsupported sort key null order %s", nulls)
		return 0
	}
}

func sortKeyNullOrderFromCode(code uint8, r *columnBinaryReader) SortKeyNullOrder {
	switch code {
	case 0:
		return SortKeyNullsDefault
	case 1:
		return SortKeyNullsFirst
	case 2:
		return SortKeyNullsLast
	default:
		r.fail("unsupported sort key null order code %d", code)
		return ""
	}
}
