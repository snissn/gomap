package colgranule

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

func TestColumnPartImageAccountingReconcilesToSerializedBytes(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "time_us"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{aggregateMetadataTestDefinition()}
	part, err := BuildColumnPart(23, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {5, 4, 3, 2, 1},
		"time_us":   {50, 40, 30, 20, 10},
		"value":     {500, 400, 300, 200, 100},
		"kind_code": {0, 1, 1, 2, 0},
		"has_reply": {1, 1, 0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}

	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{
		Dictionaries: map[string]map[string]int64{
			"kind_code": {"commit": 1, "identity": 2},
		},
	})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	if image.TotalBytes() == 0 || image.ManifestBytes == 0 {
		t.Fatalf("bad image bytes total=%d manifest=%d", image.TotalBytes(), image.ManifestBytes)
	}
	if len(image.Sections) == 0 {
		t.Fatal("image has no sections")
	}
	for _, section := range image.Sections {
		if section.Length <= 0 {
			t.Fatalf("empty section: %+v", section)
		}
		if section.Offset < image.ManifestBytes || section.Offset+section.Length > len(image.Bytes) {
			t.Fatalf("section outside image: %+v total=%d manifest=%d", section, len(image.Bytes), image.ManifestBytes)
		}
	}

	accounting := part.ByteAccountingFromImage(image)
	if accounting.SerializedImageBytes != image.TotalBytes() {
		t.Fatalf("serialized image bytes=%d want %d", accounting.SerializedImageBytes, image.TotalBytes())
	}
	if accounting.SerializedManifestBytes != image.ManifestBytes {
		t.Fatalf("serialized manifest bytes=%d want %d", accounting.SerializedManifestBytes, image.ManifestBytes)
	}
	if image.CategoryBytes(ColumnPartImageCategoryManifest) != image.ManifestBytes {
		t.Fatalf("manifest category bytes=%d want %d", image.CategoryBytes(ColumnPartImageCategoryManifest), image.ManifestBytes)
	}
	if image.CategoryBytes(ColumnPartImageCategoryDescriptor)+image.ManifestBytes != accounting.DescriptorBytes+accounting.SerializedManifestBytes {
		t.Fatalf("descriptor/manifest split image=(%d,%d) accounting=(%d,%d)", image.CategoryBytes(ColumnPartImageCategoryDescriptor), image.ManifestBytes, accounting.DescriptorBytes, accounting.SerializedManifestBytes)
	}
	if accounting.TotalStoredBytes != image.TotalBytes() {
		t.Fatalf("total bytes=%d image bytes=%d accounting=%+v", accounting.TotalStoredBytes, image.TotalBytes(), accounting)
	}
	if accounting.DictionaryBytes == 0 {
		t.Fatalf("dictionary section was not counted: %+v", accounting)
	}
	if accounting.DeclaredColumnStoredBytes == 0 || accounting.MarkBytes == 0 || accounting.SortKeyMetadataBytes == 0 || accounting.DescriptorBytes == 0 || accounting.LocatorBytes == 0 {
		t.Fatalf("missing image accounting categories: %+v", accounting)
	}
	if accounting.AggregateMetadataBytes == 0 {
		t.Fatalf("aggregate metadata image bytes were not counted: %+v", accounting)
	}
}

func TestColumnPartImageColumnPayloadBytesMatchBlocks(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	payloadBytesByColumn := make(map[string]int)
	for _, column := range part.Columns {
		for _, block := range column.Blocks {
			payloadBytesByColumn[column.Definition.Name] += len(block.Granule.Payload)
		}
	}
	for _, section := range image.Sections {
		if section.Kind != ColumnPartImageSectionColumnData {
			continue
		}
		if section.Length != payloadBytesByColumn[section.Column] {
			t.Fatalf("column %s image bytes=%d want block payload bytes=%d", section.Column, section.Length, payloadBytesByColumn[section.Column])
		}
	}
}

func TestBuildColumnPartImageRejectsUnsupportedColumnType(t *testing.T) {
	part, _ := testColumnPartImageFixture(t, false)
	corruptPart := *part
	corruptPart.Descriptor.Columns = append([]ColumnPartColumnDescriptor(nil), part.Descriptor.Columns...)
	corruptPart.Descriptor.Columns[0].Type = ColumnType("unsupported")
	_, err := BuildColumnPartImage(&corruptPart, ColumnPartImageOptions{})
	if err == nil {
		t.Fatal("BuildColumnPartImage accepted an unsupported column type")
	}
	if !strings.Contains(err.Error(), "unsupported column type") {
		t.Fatalf("error %q does not describe unsupported column type", err)
	}
}

func TestColumnPartImageDecoderRejectsOversizedString(t *testing.T) {
	var enc columnPartImageEncoder
	enc.u32(uint32(maxColumnPartImageStringBytes + 1))
	dec := columnPartImageDecoder{data: enc.buf}
	_, err := dec.str()
	if err == nil {
		t.Fatal("decoder accepted an oversized string")
	}
	if !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("error %q does not describe oversized string", err)
	}
}

func TestBuildColumnPartImageRejectsInvalidAggregateMetadataScaledFloats(t *testing.T) {
	tests := []struct {
		name string
		edit func(*AggregateMetadata)
	}{
		{
			name: "definition NaN",
			edit: func(metadata *AggregateMetadata) {
				metadata.Definition.MaxBytesPerRow = math.NaN()
			},
		},
		{
			name: "stats Inf",
			edit: func(metadata *AggregateMetadata) {
				metadata.Stats.BytesPerPartRow = math.Inf(1)
			},
		},
		{
			name: "stats negative",
			edit: func(metadata *AggregateMetadata) {
				metadata.Stats.BytesPerMatchedRow = -1
			},
		},
		{
			name: "stats too large",
			edit: func(metadata *AggregateMetadata) {
				metadata.Stats.AdmissionMaxBytes = math.MaxFloat64
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			part, _ := testColumnPartImageFixture(t, true)
			metadata, ok := part.AggregateMetadata["test_kind_time"]
			if !ok {
				t.Fatal("missing aggregate metadata")
			}
			tt.edit(&metadata)
			part.AggregateMetadata["test_kind_time"] = metadata
			if _, err := BuildColumnPartImage(part, ColumnPartImageOptions{}); err == nil {
				t.Fatalf("BuildColumnPartImage accepted invalid aggregate metadata scaled float: %s", tt.name)
			}
		})
	}
}

func TestColumnPartWithImagePayloadsScansFromImageBytes(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	imagePart, err := part.WithImagePayloads(image)
	if err != nil {
		t.Fatalf("WithImagePayloads: %v", err)
	}
	for _, column := range imagePart.Columns {
		section, ok := image.columnDataSection(column.Definition.Name)
		if !ok {
			t.Fatalf("missing image column data section %s", column.Definition.Name)
		}
		offset := section.Offset
		for _, block := range column.Blocks {
			if block.Descriptor.StoredBytes == 0 {
				continue
			}
			if len(block.Granule.Payload) == 0 {
				t.Fatalf("column %s block %d stored bytes=%d but payload is empty", column.Definition.Name, block.Descriptor.CodecBlockOrdinal, block.Descriptor.StoredBytes)
			}
			if &block.Granule.Payload[0] != &image.Bytes[offset] {
				t.Fatalf("column %s block %d payload does not alias image bytes", column.Definition.Name, block.Descriptor.CodecBlockOrdinal)
			}
			offset += block.Descriptor.StoredBytes
		}
	}
	scan, err := imagePart.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})
	assertInt64s(t, "time_us", scan.Columns["time_us"], []int64{10, 20, 30, 40, 50})
	assertInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 1, 1, 0, 2})
	assertInt64s(t, "has_reply", scan.Columns["has_reply"], []int64{0, 1, 1, 1, 0})
}

func TestColumnPartWithImagePayloadsRejectsMismatchedImage(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	otherPart, otherImage := testColumnPartImageFixtureWithPartID(t, part.Descriptor.PartID+1, false)
	if otherPart.Descriptor.RowCount != part.Descriptor.RowCount {
		t.Fatalf("fixture row counts differ: %d/%d", otherPart.Descriptor.RowCount, part.Descriptor.RowCount)
	}
	if _, err := part.WithImagePayloads(otherImage); err == nil {
		t.Fatal("WithImagePayloads accepted an image from another part")
	}

	mismatchedRows := image
	mismatchedRows.Rows = part.Descriptor.RowCount + 1
	if _, err := part.WithImagePayloads(mismatchedRows); err == nil {
		t.Fatal("WithImagePayloads accepted mismatched image row count")
	}
}

func TestColumnPartWithImagePayloadsRejectsDescriptorMismatch(t *testing.T) {
	part, _ := testColumnPartImageFixture(t, false)
	otherPart, err := BuildColumnPart(part.Descriptor.PartID, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {900, 800, 700, 600, 500},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	otherImage, err := BuildColumnPartImage(otherPart, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	if otherImage.PartID != part.Descriptor.PartID || otherImage.Rows != part.Descriptor.RowCount {
		t.Fatalf("bad fixture image part/rows=(%d,%d), want (%d,%d)", otherImage.PartID, otherImage.Rows, part.Descriptor.PartID, part.Descriptor.RowCount)
	}
	if _, err := part.WithImagePayloads(otherImage); err == nil {
		t.Fatal("WithImagePayloads accepted descriptor-compatible part id with different block metadata")
	}
}

func TestColumnPartWithImagePayloadsAllowsFallbackCompression(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.Columns[0].Compression = CompressionLZ4
	part, err := BuildColumnPart(7, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if part.Columns["id"].Definition.Compression != CompressionLZ4 {
		t.Fatalf("id requested compression=%s want %s", part.Columns["id"].Definition.Compression, CompressionLZ4)
	}
	if part.Columns["id"].Blocks[0].Granule.Compression != CompressionNone {
		t.Fatalf("fixture did not fall back to uncompressed first block: %s", part.Columns["id"].Blocks[0].Granule.Compression)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	if _, err := part.WithImagePayloads(image); err != nil {
		t.Fatalf("WithImagePayloads rejected valid fallback-compressed image: %v", err)
	}
}

func TestColumnPartFromParsedImageScansWithoutOriginalPart(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "time_us"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{aggregateMetadataTestDefinition()}
	part, err := BuildColumnPart(23, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {5, 4, 3, 2, 1},
		"time_us":   {50, 40, 30, 20, 10},
		"value":     {500, 400, 300, 200, 100},
		"kind_code": {0, 1, 1, 2, 0},
		"has_reply": {1, 1, 0, 1, 0},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{
		Dictionaries: map[string]map[string]int64{
			"kind_code": {"zero": 0, "one": 1, "two": 2},
		},
	})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	attached, err := part.WithImagePayloads(image)
	if err != nil {
		t.Fatalf("WithImagePayloads: %v", err)
	}
	if got := attached.Columns["kind_code"].Definition.Cardinality; got != 3 {
		t.Fatalf("attached kind_code cardinality=%d want 3", got)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if parsed.TotalBytes() != image.TotalBytes() || parsed.ManifestBytes != image.ManifestBytes || len(parsed.Sections) != len(image.Sections) {
		t.Fatalf("parsed image shape total=%d/%d manifest=%d/%d sections=%d/%d", parsed.TotalBytes(), image.TotalBytes(), parsed.ManifestBytes, image.ManifestBytes, len(parsed.Sections), len(image.Sections))
	}
	dictionaries, err := parsed.Dictionaries()
	if err != nil {
		t.Fatalf("Dictionaries: %v", err)
	}
	if dictionaries["kind_code"]["zero"] != 0 || dictionaries["kind_code"]["one"] != 1 || dictionaries["kind_code"]["two"] != 2 {
		t.Fatalf("dictionary did not round trip: %+v", dictionaries["kind_code"])
	}
	imagePart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	if got := imagePart.Columns["kind_code"].Definition.Cardinality; got != 3 {
		t.Fatalf("kind_code cardinality=%d want 3", got)
	}
	scan, err := imagePart.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})
	assertInt64s(t, "time_us", scan.Columns["time_us"], []int64{10, 20, 30, 40, 50})
	assertInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 2, 1, 1, 0})
	assertInt64s(t, "has_reply", scan.Columns["has_reply"], []int64{0, 1, 0, 1, 1})

	locator, ok := imagePart.LocatePrimaryID(4)
	if !ok {
		t.Fatal("missing locator for primary id 4")
	}
	value, err := imagePart.NewScanner().ValueAt(locator, "value")
	if err != nil {
		t.Fatalf("ValueAt: %v", err)
	}
	if value != 400 {
		t.Fatalf("ValueAt primary id 4=%d want 400", value)
	}
	mayContain, constrained, err := imagePart.Marks[0].MayContainRanges([]Int64RangePredicate{{Column: "time_us", Low: 45, High: 55}})
	if err != nil {
		t.Fatalf("MayContainRanges: %v", err)
	}
	if !constrained || mayContain {
		t.Fatalf("first mark constrained/mayContain=(%v,%v) want (true,false)", constrained, mayContain)
	}
	metadata, ok := imagePart.AggregateMetadataByName("test_kind_time")
	if !ok {
		t.Fatal("missing aggregate metadata")
	}
	if !metadata.Stats.Admitted || metadata.Stats.RowsMatched != 3 || len(metadata.Granules) != len(part.Descriptor.Granules) {
		t.Fatalf("bad aggregate metadata after image parse: %+v granules=%d", metadata.Stats, len(metadata.Granules))
	}
	accounting := imagePart.ByteAccountingFromImage(parsed)
	if accounting.TotalStoredBytes != parsed.TotalBytes() {
		t.Fatalf("accounting total=%d parsed image=%d", accounting.TotalStoredBytes, parsed.TotalBytes())
	}
}

func TestColumnPartImagePersistsInferredLowCardinalityCardinality(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	for i := range opts.Columns {
		if opts.Columns[i].Name == "kind_code" {
			opts.Columns[i].Cardinality = 0
		}
	}
	part, err := BuildColumnPart(7, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	if got := part.Columns["kind_code"].Definition.Cardinality; got != 0 {
		t.Fatalf("source kind_code cardinality=%d want inferred zero in definition", got)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	attached, err := part.WithImagePayloads(image)
	if err != nil {
		t.Fatalf("WithImagePayloads: %v", err)
	}
	if got := attached.Columns["kind_code"].Definition.Cardinality; got != 0 {
		t.Fatalf("attached kind_code cardinality=%d want original inferred-zero definition", got)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	if got := imagePart.Columns["kind_code"].Definition.Cardinality; got != 3 {
		t.Fatalf("image kind_code cardinality=%d want inferred 3", got)
	}
	scan, err := imagePart.NewScanner().ScanProjected([]string{"id", "kind_code"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})
	assertInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 1, 1, 0, 2})
}

func TestParseColumnPartImageRejectsNonContiguousSections(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[manifestFirstSectionOffsetOffset(t, image):], uint64(image.ManifestBytes+1))
	if _, err := ParseColumnPartImage(corrupt); err == nil {
		t.Fatal("ParseColumnPartImage accepted a non-contiguous section layout")
	}
}

func TestParseColumnPartImageRejectsImpossibleSectionCount(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[manifestSectionCountOffset(t, image):], ^uint32(0))
	if _, err := ParseColumnPartImage(corrupt); err == nil {
		t.Fatal("ParseColumnPartImage accepted an impossible section count")
	}
}

func TestParseColumnPartImageRejectsDuplicateSingletonSections(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	imageBytes := testColumnPartImageBytes(t, part, []ColumnPartImageSection{
		{Kind: ColumnPartImageSectionDescriptor, Category: ColumnPartImageCategoryDescriptor, Name: "descriptor_a"},
		{Kind: ColumnPartImageSectionDescriptor, Category: ColumnPartImageCategoryDescriptor, Name: "descriptor_b"},
	}, [][]byte{{1}, {2}})
	if _, err := ParseColumnPartImage(imageBytes); err == nil {
		t.Fatal("ParseColumnPartImage accepted duplicate descriptor sections")
	}
}

func TestParseColumnPartImageRejectsManifestDirectorySection(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	imageBytes := testColumnPartImageBytes(t, part, []ColumnPartImageSection{
		{Kind: ColumnPartImageSectionManifest, Category: ColumnPartImageCategoryManifest, Name: "manifest"},
	}, [][]byte{{1}})
	if _, err := ParseColumnPartImage(imageBytes); err == nil {
		t.Fatal("ParseColumnPartImage accepted a manifest directory section")
	}
}

func TestColumnPartFromImageRejectsInvalidSectionBounds(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	image.Sections[0].Offset = len(image.Bytes) + 1
	if _, err := ColumnPartFromImage(image); err == nil {
		t.Fatal("ColumnPartFromImage accepted invalid section bounds")
	}
	if _, err := part.WithImagePayloads(image); err == nil {
		t.Fatal("WithImagePayloads accepted invalid section bounds")
	}
}

func TestColumnPartFromImageRejectsDescriptorManifestMismatch(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorPartIDOffset(t, image):], part.Descriptor.PartID+1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a descriptor/manifest part id mismatch")
	}
}

func TestColumnPartFromImageRejectsNegativeDescriptorRowCount(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorRowCountOffset(t, image):], ^uint64(0))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a negative descriptor row count")
	}
}

func TestColumnPartFromImageRejectsBlockRowsOutsidePart(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorFirstColumnFirstBlockRowCountOffset(t, image):], uint64(part.Descriptor.RowCount+1))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a block row range outside the part")
	}
}

func TestColumnPartFromImageRejectsOversizedBlockRawBytes(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorFirstColumnFirstBlockRawBytesOffset(t, image):], uint64(math.MaxInt64))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatalf("ColumnPartFromImage accepted oversized raw bytes for part %d", part.Descriptor.PartID)
	}
}

func TestColumnPartFromImageRejectsInvertedBlockMinMax(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	hasMinMaxOffset, minOffset, maxOffset := descriptorFirstColumnFirstBlockMinMaxOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint16(corrupt[hasMinMaxOffset:], 1)
	binary.LittleEndian.PutUint64(corrupt[minOffset:], 10)
	binary.LittleEndian.PutUint64(corrupt[maxOffset:], 1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted inverted block min/max metadata")
	}
}

func TestColumnPartFromImageRejectsNonContiguousColumnBlockRows(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorFirstColumnFirstBlockRowCountOffset(t, image):], 1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted non-contiguous column block rows")
	}
}

func TestColumnPartFromImageRejectsMismatchedBlockGranuleRange(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	firstGranuleOffset, lastGranuleOffset := descriptorFirstColumnFirstBlockGranuleRangeOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[firstGranuleOffset:], 1)
	binary.LittleEndian.PutUint64(corrupt[lastGranuleOffset:], 1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a block granule range that does not cover its row range")
	}
}

func TestColumnPartFromImageRejectsDuplicateDescriptorColumns(t *testing.T) {
	part, _ := testColumnPartImageFixture(t, false)
	corruptPart := *part
	corruptPart.Descriptor.Columns = append([]ColumnPartColumnDescriptor(nil), part.Descriptor.Columns...)
	corruptPart.Descriptor.Columns = append(corruptPart.Descriptor.Columns, part.Descriptor.Columns[0])
	image, err := BuildColumnPartImage(&corruptPart, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(image); err == nil {
		t.Fatal("ColumnPartFromImage accepted duplicate descriptor columns")
	}
}

func TestColumnPartFromImageRejectsUnsupportedDescriptorVersion(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint16(corrupt[descriptor.Offset:], columnPartDescriptorVersion+1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted an unsupported descriptor version")
	}
}

func TestColumnPartFromImageRejectsImpossibleDescriptorGranuleCount(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	offset := descriptorGranuleCountOffset(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[offset:], ^uint32(0))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted an impossible descriptor granule count")
	}
}

func TestColumnPartFromImageRejectsImpossibleRowLocatorCount(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	locators, err := image.singleSection(ColumnPartImageSectionRowLocators)
	if err != nil {
		t.Fatalf("row locators section: %v", err)
	}
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[locators.Offset:], ^uint32(0))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted an impossible row locator count")
	}
}

func TestColumnPartFromImageRejectsDuplicateRowLocators(t *testing.T) {
	part, err := BuildColumnPart(7, partTestOptions([]SortKeyColumn{{Column: "id"}}), ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	firstPrimaryIDOffset, secondPrimaryIDOffset := firstTwoRowLocatorPrimaryIDOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	firstPrimaryID := binary.LittleEndian.Uint64(corrupt[firstPrimaryIDOffset:])
	binary.LittleEndian.PutUint64(corrupt[secondPrimaryIDOffset:], firstPrimaryID)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted duplicate row locators")
	}
}

func TestColumnPartFromImageRejectsInvalidRowLocatorReferences(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	offsets := firstRowLocatorOffsets(t, image)
	originalPartRow := binary.LittleEndian.Uint32(image.Bytes[offsets.partRow:])
	mismatchedPartRow := originalPartRow + 1
	if int(mismatchedPartRow) >= part.Descriptor.RowCount {
		mismatchedPartRow = 0
	}
	if mismatchedPartRow == originalPartRow {
		t.Fatalf("could not choose mismatched part row from original=%d", originalPartRow)
	}
	tests := []struct {
		name string
		edit func([]byte)
	}{
		{
			name: "part id",
			edit: func(corrupt []byte) {
				binary.LittleEndian.PutUint64(corrupt[offsets.partID:], part.Descriptor.PartID+1)
			},
		},
		{
			name: "part row outside",
			edit: func(corrupt []byte) {
				binary.LittleEndian.PutUint32(corrupt[offsets.partRow:], uint32(part.Descriptor.RowCount))
			},
		},
		{
			name: "granule ordinal outside",
			edit: func(corrupt []byte) {
				binary.LittleEndian.PutUint32(corrupt[offsets.granuleOrdinal:], uint32(len(part.Descriptor.Granules)))
			},
		},
		{
			name: "row in granule outside",
			edit: func(corrupt []byte) {
				binary.LittleEndian.PutUint32(corrupt[offsets.rowInGranule:], uint32(part.Descriptor.RowCount))
			},
		},
		{
			name: "part row mismatch",
			edit: func(corrupt []byte) {
				binary.LittleEndian.PutUint32(corrupt[offsets.partRow:], mismatchedPartRow)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			corrupt := append([]byte(nil), image.Bytes...)
			tt.edit(corrupt)
			parsed, err := ParseColumnPartImage(corrupt)
			if err != nil {
				t.Fatalf("ParseColumnPartImage: %v", err)
			}
			if _, err := ColumnPartFromImage(parsed); err == nil {
				t.Fatalf("ColumnPartFromImage accepted invalid row locator reference: %s", tt.name)
			}
		})
	}
}

func TestColumnPartFromImageRejectsNonZeroRowLocatorReserved(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	offsets := firstRowLocatorOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[offsets.reserved:], 1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a non-zero row locator reserved field")
	}
}

func TestColumnPartFromImageRejectsRowLocatorPrimaryIDMismatchesColumn(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	first, second := firstTwoRowLocatorFieldOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	firstPrimaryID := binary.LittleEndian.Uint64(corrupt[first.primaryID:])
	secondPrimaryID := binary.LittleEndian.Uint64(corrupt[second.primaryID:])
	binary.LittleEndian.PutUint64(corrupt[first.primaryID:], secondPrimaryID)
	binary.LittleEndian.PutUint64(corrupt[second.primaryID:], firstPrimaryID)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted row locator primary IDs that do not match the primary-key column")
	}
}

func TestColumnPartFromImageRejectsIncompleteRowLocators(t *testing.T) {
	part, _ := testColumnPartImageFixture(t, false)
	corruptPart := *part
	corruptPart.Locators = make(map[int64]RowLocator, len(part.Locators)-1)
	skipped := false
	for primaryID, locator := range part.Locators {
		if !skipped {
			skipped = true
			continue
		}
		corruptPart.Locators[primaryID] = locator
	}
	image, err := BuildColumnPartImage(&corruptPart, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(image); err == nil {
		t.Fatal("ColumnPartFromImage accepted incomplete row locators")
	}
}

func TestColumnPartFromImageRejectsDuplicateRowLocatorPartRows(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	first, second := firstTwoRowLocatorFieldOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[second.partRow:], binary.LittleEndian.Uint32(corrupt[first.partRow:]))
	binary.LittleEndian.PutUint32(corrupt[second.granuleOrdinal:], binary.LittleEndian.Uint32(corrupt[first.granuleOrdinal:]))
	binary.LittleEndian.PutUint32(corrupt[second.rowInGranule:], binary.LittleEndian.Uint32(corrupt[first.rowInGranule:]))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted duplicate row locator part rows")
	}
}

func TestColumnPartFromImageRejectsDuplicateColumnDataSections(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	columnData := firstImageSectionOfKind(t, image, ColumnPartImageSectionColumnData)
	imageBytes := testColumnPartImageBytesWithAppendedSection(t, part, image, columnData, image.sectionBytes(columnData))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted duplicate column data sections")
	}
	if _, err := part.WithImagePayloads(parsed); err == nil {
		t.Fatal("WithImagePayloads accepted duplicate column data sections")
	}
}

func TestColumnPartFromImageRejectsUnknownColumnDataSections(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	columnData := firstImageSectionOfKind(t, image, ColumnPartImageSectionColumnData)
	unknownColumnData := columnData
	unknownColumnData.Column = "unknown_column"
	imageBytes := testColumnPartImageBytesWithAppendedSection(t, part, image, unknownColumnData, image.sectionBytes(columnData))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted unknown column data sections")
	}
	if _, err := part.WithImagePayloads(parsed); err == nil {
		t.Fatal("WithImagePayloads accepted unknown column data sections")
	}
}

func TestColumnPartFromImageRejectsDuplicateAggregateMetadataNames(t *testing.T) {
	part, image := testColumnPartImageFixture(t, true)
	aggregate := firstImageSectionOfKind(t, image, ColumnPartImageSectionAggregateMetadata)
	imageBytes := testColumnPartImageBytesWithAppendedSection(t, part, image, aggregate, image.sectionBytes(aggregate))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted duplicate aggregate metadata names")
	}
}

func TestColumnPartFromImageRejectsZeroAggregateMetadataEntryCount(t *testing.T) {
	_, image := testColumnPartImageFixture(t, true)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(corrupt[aggregateMetadataFirstEntryCountOffset(t, image):], 0)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted zero-count aggregate metadata entry")
	}
}

func TestColumnPartFromImageRejectsInvertedAggregateMetadataEntryBounds(t *testing.T) {
	_, image := testColumnPartImageFixture(t, true)
	minOffset, maxOffset := aggregateMetadataFirstEntryMinMaxOffsets(t, image)
	corrupt := append([]byte(nil), image.Bytes...)
	minValue := int64(binary.LittleEndian.Uint64(corrupt[minOffset:]))
	binary.LittleEndian.PutUint64(corrupt[maxOffset:], uint64(minValue-1))
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted inverted aggregate metadata entry bounds")
	}
}

func TestColumnPartFromImageRoundTripsRejectedAggregateMetadata(t *testing.T) {
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	def := aggregateMetadataTestDefinition()
	def.MaxBytesPerRow = 0.001
	opts.AggregateMetadata = []AggregateMetadataDefinition{def}
	part, err := BuildColumnPart(7, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	metadata, ok := part.AggregateMetadataByName("test_kind_time")
	if !ok {
		t.Fatal("missing aggregate metadata")
	}
	if metadata.Stats.Admitted {
		t.Fatal("test metadata was admitted")
	}
	if metadata.Stats.RowsMatched == 0 || len(metadata.Granules) != 0 {
		t.Fatalf("bad rejected metadata before image: %+v granules=%d", metadata.Stats, len(metadata.Granules))
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	decoded, ok := imagePart.AggregateMetadataByName("test_kind_time")
	if !ok {
		t.Fatal("missing decoded aggregate metadata")
	}
	if decoded.Stats.Admitted || decoded.Stats.RowsMatched != metadata.Stats.RowsMatched || len(decoded.Granules) != 0 {
		t.Fatalf("bad rejected metadata after image: %+v granules=%d", decoded.Stats, len(decoded.Granules))
	}
}

func TestColumnPartFromImageRejectsNegativeAggregateMetadataScaledFields(t *testing.T) {
	_, image := testColumnPartImageFixture(t, true)
	tests := []struct {
		name   string
		offset func(*testing.T, ColumnPartImage) int
	}{
		{name: "max bytes per row", offset: aggregateMetadataMaxBytesPerRowOffset},
		{name: "bytes per part row", offset: func(t *testing.T, image ColumnPartImage) int {
			return aggregateMetadataStatsScaledFieldOffset(t, image, "bytesPerPartRow")
		}},
		{name: "bytes per matched row", offset: func(t *testing.T, image ColumnPartImage) int {
			return aggregateMetadataStatsScaledFieldOffset(t, image, "bytesPerMatchedRow")
		}},
		{name: "admission max bytes", offset: func(t *testing.T, image ColumnPartImage) int {
			return aggregateMetadataStatsScaledFieldOffset(t, image, "admissionMaxBytes")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			corrupt := append([]byte(nil), image.Bytes...)
			binary.LittleEndian.PutUint64(corrupt[tt.offset(t, image):], ^uint64(0))
			parsed, err := ParseColumnPartImage(corrupt)
			if err != nil {
				t.Fatalf("ParseColumnPartImage: %v", err)
			}
			if _, err := ColumnPartFromImage(parsed); err == nil {
				t.Fatalf("ColumnPartFromImage accepted negative aggregate metadata %s", tt.name)
			}
		})
	}
}

func TestColumnPartFromImageRejectsInvalidSortKeyMarkBounds(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	marks := cloneSortKeyMarksForTest(part.Marks)
	prefix := &marks[0].Prefixes[0]
	prefix.UpperExclusive.Unbounded = false
	prefix.UpperExclusive.Exclusive = true
	prefix.UpperExclusive.Values = append([]int64(nil), prefix.Lower.Values...)

	imageBytes := testColumnPartImageBytesWithReplacedSection(t, part, image, firstImageSectionOfKind(t, image, ColumnPartImageSectionSortKeyMarks), encodeSortKeyMarksPayloadForTest(marks))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted an invalid sort-key mark upper bound")
	}
}

func TestColumnPartFromImageRejectsInvalidSortKeyMarkValueWidth(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	marks := cloneSortKeyMarksForTest(part.Marks)
	prefix := &marks[0].Prefixes[0]
	prefix.Lower.Values = append(prefix.Lower.Values, 0)

	imageBytes := testColumnPartImageBytesWithReplacedSection(t, part, image, firstImageSectionOfKind(t, image, ColumnPartImageSectionSortKeyMarks), encodeSortKeyMarksPayloadForTest(marks))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a sort-key mark lower bound with the wrong value width")
	}
}

func TestColumnPartFromImageRejectsMissingSortKeyMark(t *testing.T) {
	part, image := testColumnPartImageFixture(t, false)
	marks := cloneSortKeyMarksForTest(part.Marks)
	marks = marks[:len(marks)-1]

	imageBytes := testColumnPartImageBytesWithReplacedSection(t, part, image, firstImageSectionOfKind(t, image, ColumnPartImageSectionSortKeyMarks), encodeSortKeyMarksPayloadForTest(marks))
	parsed, err := ParseColumnPartImage(imageBytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted fewer sort-key marks than descriptor granules")
	}
}

func TestColumnPartFromImageRejectsOutOfOrderGranuleMarkOrdinal(t *testing.T) {
	_, image := testColumnPartImageFixture(t, false)
	corrupt := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint64(corrupt[descriptorGranuleMarkOrdinalOffset(t, image, 0):], 1)
	parsed, err := ParseColumnPartImage(corrupt)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if _, err := ColumnPartFromImage(parsed); err == nil {
		t.Fatal("ColumnPartFromImage accepted a granule mark ordinal that does not match granule order")
	}
}

func testColumnPartImageFixture(t *testing.T, withAggregateMetadata bool) (*ColumnPart, ColumnPartImage) {
	t.Helper()
	return testColumnPartImageFixtureWithPartID(t, 7, withAggregateMetadata)
}

func testColumnPartImageFixtureWithPartID(t *testing.T, partID uint64, withAggregateMetadata bool) (*ColumnPart, ColumnPartImage) {
	t.Helper()
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	if withAggregateMetadata {
		opts.AggregateMetadata = []AggregateMetadataDefinition{aggregateMetadataTestDefinition()}
	}
	part, err := BuildColumnPart(partID, opts, ColumnBatch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 5, 4},
		"time_us":   {30, 10, 20, 50, 40},
		"value":     {300, 100, 200, 500, 400},
		"kind_code": {1, 0, 1, 2, 0},
		"has_reply": {1, 0, 1, 0, 1},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	return part, image
}

func firstImageSectionOfKind(t *testing.T, image ColumnPartImage, kind ColumnPartImageSectionKind) ColumnPartImageSection {
	t.Helper()
	sections := image.sectionsByKind(kind)
	if len(sections) == 0 {
		t.Fatalf("image has no %s section", kind)
	}
	return sections[0]
}

func testColumnPartImageBytesWithAppendedSection(t *testing.T, part *ColumnPart, image ColumnPartImage, section ColumnPartImageSection, payload []byte) []byte {
	t.Helper()
	sections := append([]ColumnPartImageSection(nil), image.Sections...)
	payloads := make([][]byte, 0, len(image.Sections)+1)
	for _, existing := range image.Sections {
		payloads = append(payloads, image.sectionBytes(existing))
	}
	sections = append(sections, section)
	payloads = append(payloads, payload)
	return testColumnPartImageBytes(t, part, sections, payloads)
}

func testColumnPartImageBytesWithReplacedSection(t *testing.T, part *ColumnPart, image ColumnPartImage, target ColumnPartImageSection, payload []byte) []byte {
	t.Helper()
	sections := append([]ColumnPartImageSection(nil), image.Sections...)
	payloads := make([][]byte, 0, len(image.Sections))
	replaced := false
	for _, existing := range image.Sections {
		if existing.Kind == target.Kind && existing.Name == target.Name && existing.Column == target.Column && existing.Offset == target.Offset {
			payloads = append(payloads, payload)
			replaced = true
			continue
		}
		payloads = append(payloads, image.sectionBytes(existing))
	}
	if !replaced {
		t.Fatalf("image has no replacement target section: %+v", target)
	}
	return testColumnPartImageBytes(t, part, sections, payloads)
}

func cloneSortKeyMarksForTest(marks []SortKeyMark) []SortKeyMark {
	out := make([]SortKeyMark, len(marks))
	for i, mark := range marks {
		out[i] = SortKeyMark{
			Rows:     mark.Rows,
			Columns:  append([]string(nil), mark.Columns...),
			Prefixes: make([]SortKeyPrefixSummary, len(mark.Prefixes)),
		}
		for j, prefix := range mark.Prefixes {
			out[i].Prefixes[j] = SortKeyPrefixSummary{
				Columns: append([]string(nil), prefix.Columns...),
				Lower: SortKeyBound{
					Values:    append([]int64(nil), prefix.Lower.Values...),
					Exclusive: prefix.Lower.Exclusive,
					Unbounded: prefix.Lower.Unbounded,
				},
				UpperExclusive: SortKeyBound{
					Values:    append([]int64(nil), prefix.UpperExclusive.Values...),
					Exclusive: prefix.UpperExclusive.Exclusive,
					Unbounded: prefix.UpperExclusive.Unbounded,
				},
			}
		}
	}
	return out
}

func encodeSortKeyMarksPayloadForTest(marks []SortKeyMark) []byte {
	var enc columnPartImageEncoder
	enc.u32(uint32(len(marks)))
	for _, mark := range marks {
		enc.i64(int64(mark.Rows))
		enc.stringSlice(mark.Columns)
		enc.u32(uint32(len(mark.Prefixes)))
		for _, prefix := range mark.Prefixes {
			enc.stringSlice(prefix.Columns)
			encodeSortKeyBound(&enc, prefix.Lower)
			encodeSortKeyBound(&enc, prefix.UpperExclusive)
		}
	}
	return enc.bytes()
}

func aggregateMetadataMaxBytesPerRowOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	aggregate := firstImageSectionOfKind(t, image, ColumnPartImageSectionAggregateMetadata)
	dec := columnPartImageDecoder{data: image.sectionBytes(aggregate)}
	skipAggregateMetadataDefinitionBeforeMaxBytesPerRow(t, &dec)
	return aggregate.Offset + dec.offset
}

func aggregateMetadataStatsScaledFieldOffset(t *testing.T, image ColumnPartImage, field string) int {
	t.Helper()
	aggregate := firstImageSectionOfKind(t, image, ColumnPartImageSectionAggregateMetadata)
	dec := columnPartImageDecoder{data: image.sectionBytes(aggregate)}
	skipAggregateMetadataDefinition(t, &dec)
	if _, err := dec.boolean(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"buildNanos",
		"granules",
		"granulesWithRows",
		"rowsMatched",
		"entries",
		"valueBytes",
		"descriptorBytes",
		"totalBytes",
		"bytesPerPartRow",
		"bytesPerMatchedRow",
	} {
		if name == field {
			return aggregate.Offset + dec.offset
		}
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if field == "admissionMaxBytes" {
		return aggregate.Offset + dec.offset
	}
	t.Fatalf("unknown aggregate metadata stats field %s", field)
	return 0
}

func aggregateMetadataFirstEntryCountOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	_, count, _ := aggregateMetadataFirstEntryOffsets(t, image)
	return count
}

func aggregateMetadataFirstEntryMinMaxOffsets(t *testing.T, image ColumnPartImage) (int, int) {
	t.Helper()
	_, _, minOffset := aggregateMetadataFirstEntryOffsets(t, image)
	return minOffset, minOffset + 8
}

func aggregateMetadataFirstEntryOffsets(t *testing.T, image ColumnPartImage) (int, int, int) {
	t.Helper()
	aggregate := firstImageSectionOfKind(t, image, ColumnPartImageSectionAggregateMetadata)
	dec := columnPartImageDecoder{data: image.sectionBytes(aggregate)}
	skipAggregateMetadataDefinition(t, &dec)
	skipAggregateMetadataStats(t, &dec)
	granuleCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if granuleCount == 0 {
		t.Fatal("aggregate metadata has no granules")
	}
	for i := 0; i < 4; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	entryCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if entryCount == 0 {
		t.Fatal("aggregate metadata first granule has no entries")
	}
	groupOffset := aggregate.Offset + dec.offset
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	countOffset := aggregate.Offset + dec.offset
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	minOffset := aggregate.Offset + dec.offset
	return groupOffset, countOffset, minOffset
}

func skipAggregateMetadataDefinition(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	skipAggregateMetadataDefinitionBeforeMaxBytesPerRow(t, dec)
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
}

func skipAggregateMetadataStats(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.boolean(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
}

func skipAggregateMetadataDefinitionBeforeMaxBytesPerRow(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.stringSlice(); err != nil {
		t.Fatal(err)
	}
	measureCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < measureCount; i++ {
		if _, err := dec.str(); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.str(); err != nil {
			t.Fatal(err)
		}
	}
	predicateCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < predicateCount; i++ {
		if _, err := dec.str(); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.str(); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
}

func descriptorGranuleCountOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.stringSlice(); err != nil {
		t.Fatal(err)
	}
	return descriptor.Offset + dec.offset
}

func descriptorGranuleMarkOrdinalOffset(t *testing.T, image ColumnPartImage, index int) int {
	t.Helper()
	descriptor, dec := descriptorDecoderBeforeGranules(t, image)
	granuleCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if index < 0 || index >= int(granuleCount) {
		t.Fatalf("granule index=%d count=%d", index, granuleCount)
	}
	for i := 0; i < index; i++ {
		if _, err := decodeGranuleDescriptor(&dec); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 7; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	return descriptor.Offset + dec.offset
}

func firstTwoRowLocatorPrimaryIDOffsets(t *testing.T, image ColumnPartImage) (int, int) {
	t.Helper()
	locators, err := image.singleSection(ColumnPartImageSectionRowLocators)
	if err != nil {
		t.Fatalf("row locators section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(locators)}
	count, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if count < 2 {
		t.Fatalf("row locator count=%d want at least 2", count)
	}
	first := locators.Offset + dec.offset
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	return first, locators.Offset + dec.offset
}

type rowLocatorOffsets struct {
	primaryID      int
	partID         int
	partRow        int
	granuleOrdinal int
	rowInGranule   int
	reserved       int
}

func firstRowLocatorOffsets(t *testing.T, image ColumnPartImage) rowLocatorOffsets {
	t.Helper()
	offsets := firstNRowLocatorOffsets(t, image, 1)
	return offsets[0]
}

func firstTwoRowLocatorFieldOffsets(t *testing.T, image ColumnPartImage) (rowLocatorOffsets, rowLocatorOffsets) {
	t.Helper()
	offsets := firstNRowLocatorOffsets(t, image, 2)
	return offsets[0], offsets[1]
}

func firstNRowLocatorOffsets(t *testing.T, image ColumnPartImage, n int) []rowLocatorOffsets {
	t.Helper()
	locators, err := image.singleSection(ColumnPartImageSectionRowLocators)
	if err != nil {
		t.Fatalf("row locators section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(locators)}
	count, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if int(count) < n {
		t.Fatalf("row locator count=%d want at least %d", count, n)
	}
	out := make([]rowLocatorOffsets, 0, n)
	for i := 0; i < n; i++ {
		primaryID := locators.Offset + dec.offset
		out = append(out, rowLocatorOffsets{
			primaryID:      primaryID,
			partID:         primaryID + 8,
			partRow:        primaryID + 16,
			granuleOrdinal: primaryID + 20,
			rowInGranule:   primaryID + 24,
			reserved:       primaryID + 28,
		})
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
		if _, err := dec.u64(); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < 4; j++ {
			if _, err := dec.u32(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return out
}

func descriptorPartIDOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	return descriptor.Offset + dec.offset
}

func descriptorRowCountOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	return descriptor.Offset + dec.offset
}

func descriptorFirstColumnFirstBlockRowCountOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.stringSlice(); err != nil {
		t.Fatal(err)
	}
	granuleCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < granuleCount; i++ {
		if _, err := decodeGranuleDescriptor(&dec); err != nil {
			t.Fatal(err)
		}
	}
	columnCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if columnCount == 0 {
		t.Fatal("descriptor has no columns")
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	blockCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if blockCount == 0 {
		t.Fatal("first descriptor column has no blocks")
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	return descriptor.Offset + dec.offset
}

func descriptorFirstColumnFirstBlockGranuleRangeOffsets(t *testing.T, image ColumnPartImage) (int, int) {
	t.Helper()
	descriptor, dec := descriptorDecoderAtFirstColumnFirstBlock(t, image)
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	firstGranule := descriptor.Offset + dec.offset
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	return firstGranule, descriptor.Offset + dec.offset
}

func descriptorFirstColumnFirstBlockRawBytesOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.stringSlice(); err != nil {
		t.Fatal(err)
	}
	granuleCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < granuleCount; i++ {
		if _, err := decodeGranuleDescriptor(&dec); err != nil {
			t.Fatal(err)
		}
	}
	columnCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if columnCount == 0 {
		t.Fatal("descriptor has no columns")
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	blockCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if blockCount == 0 {
		t.Fatal("first descriptor column has no blocks")
	}
	for i := 0; i < 4; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	return descriptor.Offset + dec.offset
}

func descriptorFirstColumnFirstBlockMinMaxOffsets(t *testing.T, image ColumnPartImage) (int, int, int) {
	t.Helper()
	descriptor, dec := descriptorDecoderAtFirstColumnFirstBlock(t, image)
	for i := 0; i < 4; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := dec.i64(); err != nil {
			t.Fatal(err)
		}
	}
	hasMinMax := descriptor.Offset + dec.offset
	if _, err := dec.boolean(); err != nil {
		t.Fatal(err)
	}
	minValue := descriptor.Offset + dec.offset
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	maxValue := descriptor.Offset + dec.offset
	return hasMinMax, minValue, maxValue
}

func descriptorDecoderBeforeGranules(t *testing.T, image ColumnPartImage) (ColumnPartImageSection, columnPartImageDecoder) {
	t.Helper()
	descriptor, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		t.Fatalf("descriptor section: %v", err)
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(descriptor)}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.stringSlice(); err != nil {
		t.Fatal(err)
	}
	return descriptor, dec
}

func descriptorDecoderAtFirstColumnFirstBlock(t *testing.T, image ColumnPartImage) (ColumnPartImageSection, columnPartImageDecoder) {
	t.Helper()
	descriptor, dec := descriptorDecoderBeforeGranules(t, image)
	granuleCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < granuleCount; i++ {
		if _, err := decodeGranuleDescriptor(&dec); err != nil {
			t.Fatal(err)
		}
	}
	columnCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if columnCount == 0 {
		t.Fatal("descriptor has no columns")
	}
	if _, err := dec.str(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	blockCount, err := dec.u32()
	if err != nil {
		t.Fatal(err)
	}
	if blockCount == 0 {
		t.Fatal("first descriptor column has no blocks")
	}
	return descriptor, dec
}

func manifestSectionCountOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	dec := columnPartImageDecoder{data: image.Bytes}
	skipImageManifestHeaderBeforeSectionCount(t, &dec)
	return dec.offset
}

func manifestFirstSectionOffsetOffset(t *testing.T, image ColumnPartImage) int {
	t.Helper()
	dec := columnPartImageDecoder{data: image.Bytes}
	skipImageManifestHeaderBeforeSectionCount(t, &dec)
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	return dec.offset
}

func skipImageManifestHeaderBeforeSectionCount(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u16(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.i64(); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.u32(); err != nil {
		t.Fatal(err)
	}
}

func testColumnPartImageBytes(t *testing.T, part *ColumnPart, sections []ColumnPartImageSection, payloads [][]byte) []byte {
	t.Helper()
	if len(sections) != len(payloads) {
		t.Fatalf("sections=%d payloads=%d", len(sections), len(payloads))
	}
	sections = append([]ColumnPartImageSection(nil), sections...)
	manifestBytes := 0
	for attempt := 0; attempt < 8; attempt++ {
		manifest, err := encodeColumnPartImageManifest(part, sections, manifestBytes)
		if err != nil {
			t.Fatal(err)
		}
		offset := len(manifest)
		for i := range sections {
			sections[i].Offset = offset
			sections[i].Length = len(payloads[i])
			offset += len(payloads[i])
		}
		finalManifest, err := encodeColumnPartImageManifest(part, sections, len(manifest))
		if err != nil {
			t.Fatal(err)
		}
		if len(finalManifest) == len(manifest) {
			out := append([]byte(nil), finalManifest...)
			for _, payload := range payloads {
				out = append(out, payload...)
			}
			return out
		}
		manifestBytes = len(finalManifest)
	}
	t.Fatal("test image manifest length did not stabilize")
	return nil
}
