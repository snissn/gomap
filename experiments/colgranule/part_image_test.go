package colgranule

import "testing"

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
		for _, block := range column.Blocks {
			if block.Descriptor.StoredBytes == 0 {
				continue
			}
			offset := int(block.Granule.PayloadRef.Offset)
			if &block.Granule.Payload[0] != &image.Bytes[offset] {
				t.Fatalf("column %s block %d payload does not alias image bytes", column.Definition.Name, block.Descriptor.CodecBlockOrdinal)
			}
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
