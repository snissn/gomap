package collections

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnAdapterOptInCompressionRoundTrip1952(t *testing.T) {
	for _, compression := range []typedcolumn.Compression{typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			fields := []TypedStorageField{
				typedColumnAdapterField("count", ColumnStoreValueInt64),
				typedColumnAdapterField("flag", ColumnStoreValueBool),
				typedColumnAdapterField("kind", ColumnStoreValueString),
				typedColumnAdapterNullableField("maybe_count", ColumnStoreValueInt64),
			}
			rows := make([]typedColumnAdapterRow, 512)
			for i := range rows {
				rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
					"count":       {Type: ColumnStoreValueInt64, Present: true, Int64: 42},
					"flag":        {Type: ColumnStoreValueBool, Present: true, Bool: i%8 == 0},
					"kind":        {Type: ColumnStoreValueString, Present: true, String: "commit"},
					"maybe_count": {Type: ColumnStoreValueInt64, Present: i%5 != 0, Null: i%5 == 0, Int64: 42},
				}}
			}
			part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 512, Fields: fields, DefaultCompression: compression, DefaultCompressionSet: true}, rows)
			if err != nil {
				t.Fatalf("build compressed part: %v", err)
			}
			primary := part.Part.Columns[typedColumnAdapterPrimaryIDColumn]
			if primary.Definition.Encoding != typedcolumn.EncodingDeltaVarint || primary.Definition.Compression != compression {
				t.Fatalf("primary definition=%+v want delta_varint/%s", primary.Definition, compression)
			}
			kept := 0
			for _, block := range primary.Blocks {
				if block.Descriptor.Encoding != typedcolumn.EncodingDeltaVarint || block.Granule.Encoding != typedcolumn.EncodingDeltaVarint {
					t.Fatalf("primary block encoding=%s/%s want delta_varint", block.Descriptor.Encoding, block.Granule.Encoding)
				}
			}
			for _, column := range part.Columns {
				got := part.Part.Columns[column.Definition.Name]
				if got.Definition.Compression != compression {
					t.Fatalf("column %s requested compression=%s want %s", column.Definition.Name, got.Definition.Compression, compression)
				}
				for _, block := range got.Blocks {
					if block.Descriptor.Compression == compression {
						kept++
					}
				}
			}
			if kept == 0 {
				t.Fatalf("no block kept requested compression %s", compression)
			}
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			parsed, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: fields}, image)
			if err != nil {
				t.Fatalf("parse compressed image with default reader policy: %v", err)
			}
			for _, field := range fields {
				values, err := parsed.scanColumnValues(field.Name)
				if err != nil {
					t.Fatalf("scan %s: %v", field.Name, err)
				}
				if len(values) != len(rows) {
					t.Fatalf("scan %s rows=%d want %d", field.Name, len(values), len(rows))
				}
			}
			accounting := part.Part.ByteAccountingFromImage(image)
			if len(accounting.CompressionDetail) == 0 {
				t.Fatalf("missing compression detail")
			}
			foundRequested := false
			for _, detail := range accounting.CompressionDetail {
				if detail.RequestedCompression == compression {
					foundRequested = true
				}
			}
			if !foundRequested {
				t.Fatalf("compression detail did not include requested %s: %+v", compression, accounting.CompressionDetail)
			}
		})
	}
}

func TestTypedColumnBenchmarkPolicyCompressedDirectAggregate1952(t *testing.T) {
	for _, compression := range []string{"snappy", "lz4"} {
		t.Run(compression, func(t *testing.T) {
			t.Setenv(typedColumnBenchmarkCompressionEnv, compression)
			t.Setenv(typedColumnBenchmarkInt64EncodingEnv, "raw_int64")
			d := openTypedColumnInt64ScanDB(t)
			defer func() { _ = d.Close() }()
			cfg := testColumnStoreConfig(nil)
			cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}}
			cfg.SortKey = nil
			cfg.AggregateMetadata = nil
			cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("events")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			values := make([]int64, 1024)
			for i := range values {
				values[i] = 7
			}
			insertTypedColumnInt64ScanRows(t, col, values)

			// Readers must trust the persisted descriptor rather than requiring the
			// benchmark env vars that were active at publication time.
			t.Setenv(typedColumnBenchmarkCompressionEnv, "")
			t.Setenv(typedColumnBenchmarkInt64EncodingEnv, "")
			result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
			if err != nil {
				t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
			}
			if result.Count != int64(len(values)) || result.Sum != int64(len(values))*7 || result.Diagnostics.Fallback {
				t.Fatalf("aggregate result=%+v want compressed decode-on-scan without fallback", result)
			}
			accounting, err := col.ColumnStorePhysicalAccounting(nil, ColumnStorePhysicalAccountingOptions{DetailedSections: true, ReadIntegrity: ColumnAssetReadIntegrityVerify})
			if err != nil {
				t.Fatalf("ColumnStorePhysicalAccounting: %v", err)
			}
			found := false
			foundColumnSection := false
			for _, part := range accounting.TypedColumnParts {
				for _, detail := range part.Image.CompressionDetail {
					if detail.Column != "time_us" || detail.RequestedCompression != compression {
						continue
					}
					found = true
					if detail.ActualCompression != compression || detail.StoredBytes <= 0 || detail.EncodedRawBytes <= 0 || detail.StoredBytes >= detail.EncodedRawBytes || detail.CompressionKept == 0 {
						t.Fatalf("compression detail=%+v want kept %s smaller than raw", detail, compression)
					}
				}
				for _, section := range part.Image.SerializedSections {
					if section.Kind != string(typedcolumn.ColumnPartImageSectionColumnData) || section.Column != "time_us" {
						continue
					}
					foundColumnSection = true
					if section.RawBytes <= section.StoredBytes || section.StoredBytes != section.Bytes || section.CompressionRatio <= 0 || section.CompressionRatio >= 1 {
						t.Fatalf("column_data section accounting=%+v want encoded raw bytes above compressed stored bytes", section)
					}
				}
			}
			if !found {
				t.Fatalf("missing %s compression detail in %+v", compression, accounting.TypedColumnParts)
			}
			if !foundColumnSection {
				t.Fatalf("missing compressed column_data section accounting in %+v", accounting.TypedColumnParts)
			}
		})
	}
}

func TestTypedColumnProductionCompressionPolicyDefaultsDurable2297(t *testing.T) {
	for _, name := range []string{typedColumnBenchmarkCompressionEnv, typedColumnBenchmarkInt64EncodingEnv, typedColumnBenchmarkRowsPerGranuleEnv, typedColumnBenchmarkAdaptiveEnabledEnv, typedColumnBenchmarkAdaptiveTargetBytesEnv, typedColumnBenchmarkAdaptiveMinRowsEnv, typedColumnBenchmarkAdaptiveMaxRowsEnv} {
		old, ok := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		t.Cleanup(func() {
			if ok {
				_ = os.Setenv(name, old)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
	d := openTypedColumnInt64ScanDB(t)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if got := col.Meta().Options.ColumnStore.TypedColumnCompression; got != ColumnStoreTypedColumnCompressionLZ4 {
		t.Fatalf("typed_column_compression=%q want %q", got, ColumnStoreTypedColumnCompressionLZ4)
	}
	if got := col.Meta().Options.ColumnStore.TypedColumnSectionCompression; got != ColumnStoreTypedColumnCompressionZSTD {
		t.Fatalf("typed_column_section_compression=%q want %q", got, ColumnStoreTypedColumnCompressionZSTD)
	}
	values := make([]int64, 4096)
	for i := range values {
		values[i] = 7
	}
	insertTypedColumnInt64ScanRows(t, col, values)

	result, err := col.RunTypedColumnInt64PredicateAggregate(TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: TypedColumnInt64PredicateAll})
	if err != nil {
		t.Fatalf("RunTypedColumnInt64PredicateAggregate: %v", err)
	}
	if result.Count != int64(len(values)) || result.Sum != int64(len(values))*7 || result.Diagnostics.Fallback {
		t.Fatalf("aggregate result=%+v want production default compressed policy without fallback", result)
	}
	accounting, err := col.ColumnStorePhysicalAccounting(nil, ColumnStorePhysicalAccountingOptions{DetailedSections: true, ReadIntegrity: ColumnAssetReadIntegrityVerify})
	if err != nil {
		t.Fatalf("ColumnStorePhysicalAccounting: %v", err)
	}
	foundColumnPolicy := false
	foundLocatorSection := false
	foundPruningSection := false
	foundPrimaryID := false
	for _, part := range accounting.TypedColumnParts {
		for _, column := range part.Image.ColumnsDetail {
			if column.Column != typedColumnAdapterPrimaryIDColumn {
				continue
			}
			foundPrimaryID = true
			if column.Encoding != typedcolumn.EncodingDeltaVarint.String() || column.RequestedCompression != string(ColumnStoreTypedColumnCompressionLZ4) || column.StoredBytes >= column.LogicalValueBytes {
				t.Fatalf("primary-id column accounting=%+v want compact delta_varint/lz4 storage", column)
			}
		}
		for _, detail := range part.Image.CompressionDetail {
			if detail.Column != "time_us" || detail.RequestedCompression != string(ColumnStoreTypedColumnCompressionLZ4) {
				continue
			}
			foundColumnPolicy = true
			if detail.Blocks == 0 || detail.CompressionAttempted == 0 || detail.EncodedRawBytes == 0 || detail.StoredBytes == 0 {
				t.Fatalf("compression detail=%+v want production LZ4 attempted with byte accounting", detail)
			}
		}
		for _, section := range part.Image.SerializedSections {
			if section.Kind != string(typedcolumn.ColumnPartImageSectionRowLocators) {
				if section.Kind == string(typedcolumn.ColumnPartImageSectionPruningMetadata) {
					foundPruningSection = true
					if section.Compression != string(ColumnStoreTypedColumnCompressionZSTD) || section.RawBytes <= section.StoredBytes || section.StoredBytes != section.Bytes {
						t.Fatalf("pruning metadata section=%+v want kept zstd section compression", section)
					}
				}
				continue
			}
			foundLocatorSection = true
			if section.Compression != typedcolumn.CompressionNone.String() || section.RawBytes <= section.StoredBytes || section.StoredBytes != section.Bytes || section.Bytes > 64 {
				t.Fatalf("row locator section=%+v want compact uncompressed section below logical raw bytes", section)
			}
		}
	}
	if !foundColumnPolicy {
		t.Fatalf("missing production LZ4 compression detail in %+v", accounting.TypedColumnParts)
	}
	if !foundLocatorSection {
		t.Fatalf("missing compact row locator section in %+v", accounting.TypedColumnParts)
	}
	if !foundPruningSection {
		t.Fatalf("missing compressed pruning metadata section in %+v", accounting.TypedColumnParts)
	}
	if !foundPrimaryID {
		t.Fatalf("missing primary-id column accounting in %+v", accounting.TypedColumnParts)
	}
}

func TestTypedColumnProductionCompressionPolicyNoneIsolation2297(t *testing.T) {
	rawCfg := testColumnStoreConfig(nil)
	rawCfg.TypedColumnCompression = ColumnStoreTypedColumnCompressionNone
	rawCfg.TypedColumnSectionCompression = ColumnStoreTypedColumnCompressionNone
	normalizedCfg, err := normalizeColumnStoreConfig("events", rawCfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	cfg := *normalizedCfg
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	opts, err := typedColumnPublicationAdapterOptionsFromConfig(cfg, 2297, []TypedStorageField{field}, nil)
	if err != nil {
		t.Fatalf("typedColumnPublicationAdapterOptionsFromConfig: %v", err)
	}
	if !opts.DefaultCompressionSet || opts.DefaultCompression != typedcolumn.CompressionNone || !opts.SectionCompressionSet || opts.SectionCompression != typedcolumn.CompressionNone {
		t.Fatalf("opts=%+v want explicit none policy", opts)
	}
	rows := make([]typedColumnAdapterRow, 512)
	for i := range rows {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 42}}}
	}
	part, err := buildTypedColumnAdapterPart(opts, rows)
	if err != nil {
		t.Fatalf("build part: %v", err)
	}
	if got := part.Part.Columns["count"].Definition.Compression; got != typedcolumn.CompressionNone {
		t.Fatalf("count compression=%s want none", got)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section, ok := typedColumnImageSectionByKind(image, typedcolumn.ColumnPartImageSectionRowLocators)
	if !ok {
		t.Fatalf("missing row locator section in %+v", image.Sections)
	}
	if section.Compression != typedcolumn.CompressionNone {
		t.Fatalf("row locator section compression=%s want none", section.Compression)
	}
}

func TestTypedColumnProductionCompressionPolicySkipsUnsupportedFields2297(t *testing.T) {
	field := typedColumnAdapterField("embedding", ColumnStoreValueFloat32Vector)
	field.VectorDims = 3
	columns, err := typedColumnAdapterColumnsForFieldsWithOptions([]TypedStorageField{field}, typedColumnAdapterOptions{
		DefaultCompression:              typedcolumn.CompressionLZ4,
		DefaultCompressionSet:           true,
		DefaultCompressionOnlySupported: true,
	})
	if err != nil {
		t.Fatalf("typedColumnAdapterColumnsForFieldsWithOptions production skip: %v", err)
	}
	if got := columns[0].Definition.Compression; got != typedcolumn.CompressionNone {
		t.Fatalf("vector compression=%s want none when production policy skips unsupported fields", got)
	}
	_, err = typedColumnAdapterColumnsForFieldsWithOptions([]TypedStorageField{field}, typedColumnAdapterOptions{
		DefaultCompression:    typedcolumn.CompressionLZ4,
		DefaultCompressionSet: true,
	})
	if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "compression lz4 is unsupported") {
		t.Fatalf("forced vector compression err=%v want fail-closed unsupported compression", err)
	}

	fixedWidthInt64 := typedColumnAdapterField("raw_count", ColumnStoreValueInt64)
	fixedWidthInt64.FixedWidthEncoding = ColumnFixedWidthEncodingLittleEndian
	columns, err = typedColumnAdapterColumnsForFieldsWithOptions([]TypedStorageField{fixedWidthInt64}, typedColumnAdapterOptions{
		DefaultCompression:              typedcolumn.CompressionLZ4,
		DefaultCompressionSet:           true,
		DefaultCompressionOnlySupported: true,
	})
	if err != nil {
		t.Fatalf("typedColumnAdapterColumnsForFieldsWithOptions fixed-width int64 skip: %v", err)
	}
	if got := columns[0].Definition.Compression; got != typedcolumn.CompressionNone {
		t.Fatalf("fixed-width int64 compression=%s want none when production policy skips unsupported fields", got)
	}
	_, err = typedColumnAdapterColumnsForFieldsWithOptions([]TypedStorageField{fixedWidthInt64}, typedColumnAdapterOptions{
		DefaultCompression:    typedcolumn.CompressionLZ4,
		DefaultCompressionSet: true,
	})
	if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "compression lz4 is unsupported for fixed-width field") {
		t.Fatalf("forced fixed-width int64 compression err=%v want fail-closed unsupported compression", err)
	}
}

func TestTypedColumnAdapterOptInLocatorSectionCompression1952(t *testing.T) {
	for _, compression := range []typedcolumn.Compression{typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			field := typedColumnAdapterField("count", ColumnStoreValueInt64)
			rows := make([]typedColumnAdapterRow, 4096)
			for i := range rows {
				rows[i] = typedColumnAdapterRow{PrimaryID: int64((i + 1) * 10), Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: int64(i % 4)}}}
			}
			part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 256, Fields: []TypedStorageField{field}, SectionCompression: compression, SectionCompressionSet: true}, rows)
			if err != nil {
				t.Fatalf("build part: %v", err)
			}
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			section, ok := typedColumnImageSectionByKind(image, typedcolumn.ColumnPartImageSectionRowLocators)
			if !ok {
				t.Fatalf("missing row locator section in %+v", image.Sections)
			}
			if section.Compression != compression {
				t.Fatalf("row locator compression=%s want %s section=%+v", section.Compression, compression, section)
			}
			if section.Length <= 0 || section.Length >= 4+len(rows)*32 {
				t.Fatalf("row locator stored bytes=%d want compressed below raw", section.Length)
			}
			reopened, err := typedcolumn.ParseColumnPartImage(image.Bytes)
			if err != nil {
				t.Fatalf("ParseColumnPartImage compressed locators: %v", err)
			}
			reopenedSection, ok := typedColumnImageSectionByKind(reopened, typedcolumn.ColumnPartImageSectionRowLocators)
			if !ok || reopenedSection.Compression != compression || reopenedSection.Length != section.Length || reopenedSection.Rows != len(rows) {
				t.Fatalf("reopened locator section=%+v found=%v want compression=%s length=%d rows=%d", reopenedSection, ok, compression, section.Length, len(rows))
			}
			parsed, err := typedcolumn.ColumnPartFromImage(reopened)
			if err != nil {
				t.Fatalf("ColumnPartFromImage compressed locators: %v", err)
			}
			if len(parsed.Locators) != len(rows) {
				t.Fatalf("locators=%d want %d", len(parsed.Locators), len(rows))
			}
			sections := image.SectionByteAccounting()
			foundAccounting := false
			for _, accounting := range sections {
				if accounting.Kind != typedcolumn.ColumnPartImageSectionRowLocators {
					continue
				}
				foundAccounting = true
				if accounting.Compression != compression || accounting.RawBytes <= accounting.StoredBytes || accounting.StoredBytes != section.Length {
					t.Fatalf("locator section accounting=%+v section=%+v", accounting, section)
				}
			}
			if !foundAccounting {
				t.Fatalf("missing locator section accounting in %+v", sections)
			}
		})
	}
}

func TestTypedColumnAdapterOptInDictionarySectionCompression2300(t *testing.T) {
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	field.Dictionary = true
	const cardinality = 128
	const rowsN = 1024
	values := make([]string, cardinality)
	for i := range values {
		values[i] = fmt.Sprintf("%s%03d", strings.Repeat("atlas://did:plc:jsonbench-storage-parity/", 4), i)
	}
	rows := make([]typedColumnAdapterRow, rowsN)
	for i := range rows {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{
			"kind": {Type: ColumnStoreValueString, Present: true, String: values[i%cardinality]},
		}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{
		PartID:                2300,
		RowsPerGranule:        rowsN,
		Fields:                []TypedStorageField{field},
		SectionCompression:    typedcolumn.CompressionLZ4,
		SectionCompressionSet: true,
	}, rows)
	if err != nil {
		t.Fatalf("build part: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	section, ok := typedColumnImageSectionByKind(image, typedcolumn.ColumnPartImageSectionDictionaries)
	if !ok {
		t.Fatalf("missing dictionary section in %+v", image.Sections)
	}
	if section.Compression != typedcolumn.CompressionLZ4 || section.RawBytes <= section.Length {
		t.Fatalf("dictionary section=%+v want lz4 compressed below raw", section)
	}
	parsed, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: []TypedStorageField{field}}, image)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	got, err := parsed.scanColumnValues("kind")
	if err != nil {
		t.Fatalf("scan kind: %v", err)
	}
	if len(got) != len(rows) || got[0].String != values[0] || got[cardinality+1].String != values[1] {
		t.Fatalf("scan values len=%d first=%q second-cycle=%q", len(got), got[0].String, got[cardinality+1].String)
	}
}

func TestTypedColumnAdapterCompressedLocatorCorruptionFailsClosed1952(t *testing.T) {
	for _, compression := range []typedcolumn.Compression{typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4, typedcolumn.CompressionZSTD} {
		t.Run(compression.String(), func(t *testing.T) {
			field := typedColumnAdapterField("count", ColumnStoreValueInt64)
			rows := make([]typedColumnAdapterRow, 1024)
			for i := range rows {
				rows[i] = typedColumnAdapterRow{PrimaryID: int64((i + 1) * 10), Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: int64(i)}}}
			}
			part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 128, Fields: []TypedStorageField{field}, SectionCompression: compression, SectionCompressionSet: true}, rows)
			if err != nil {
				t.Fatalf("build part: %v", err)
			}
			image, err := part.buildImage()
			if err != nil {
				t.Fatalf("buildImage: %v", err)
			}
			section, ok := typedColumnImageSectionByKind(image, typedcolumn.ColumnPartImageSectionRowLocators)
			if !ok || section.Compression != compression || section.Length == 0 {
				t.Fatalf("compressed locator section=%+v found=%v want %s", section, ok, compression)
			}
			corrupt := image
			corrupt.Bytes = append([]byte(nil), image.Bytes...)
			corrupt.Bytes[section.Offset+section.Length/2] ^= 0xff
			_, err = typedcolumn.ColumnPartFromImage(corrupt)
			if err == nil || !strings.Contains(err.Error(), "row locator") {
				t.Fatalf("ColumnPartFromImage corrupt locator err=%v want row locator fail-closed", err)
			}
		})
	}
}

func TestTypedColumnAdapterCompressedLocatorRowMismatchFailsClosed1952(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	rows := make([]typedColumnAdapterRow, 256)
	for i := range rows {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64((i + 1) * 10), Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: int64(i)}}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 64, Fields: []TypedStorageField{field}, SectionCompression: typedcolumn.CompressionLZ4, SectionCompressionSet: true}, rows)
	if err != nil {
		t.Fatalf("build part: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	corrupt := image
	corrupt.Sections = append([]typedcolumn.ColumnPartImageSection(nil), image.Sections...)
	for i := range corrupt.Sections {
		if corrupt.Sections[i].Kind == typedcolumn.ColumnPartImageSectionRowLocators {
			corrupt.Sections[i].Rows++
		}
	}
	_, err = typedcolumn.ColumnPartFromImage(corrupt)
	if err == nil || !strings.Contains(err.Error(), "raw bytes") {
		t.Fatalf("ColumnPartFromImage locator row mismatch err=%v want fail-closed raw bytes diagnostic", err)
	}
}

func TestTypedColumnAdapterDictionarySectionCompressionRawLengthMismatchFailsClosed1952(t *testing.T) {
	field := typedColumnAdapterField("kind", ColumnStoreValueString)
	rows := []typedColumnAdapterRow{
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"kind": {Type: ColumnStoreValueString, Present: true, String: "commit"}}},
		{PrimaryID: 2, Values: map[string]columnDeclaredValue{"kind": {Type: ColumnStoreValueString, Present: true, String: "handle"}}},
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, rows)
	if err != nil {
		t.Fatalf("build part: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	for i := range image.Sections {
		if image.Sections[i].Kind == typedcolumn.ColumnPartImageSectionDictionaries {
			image.Sections[i].Compression = typedcolumn.CompressionSnappy
		}
	}
	_, err = image.Dictionaries()
	if err == nil || !strings.Contains(err.Error(), "dictionaries") || !strings.Contains(err.Error(), "decoded length") {
		t.Fatalf("Dictionaries compressed section err=%v want fail-closed decoded-length diagnostic", err)
	}
}

func TestTypedColumnAdapterDictionarySectionZSTDCapsDeclaredRawBytes1952(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd encoder: %v", err)
	}
	defer enc.Close()
	stored := enc.EncodeAll([]byte(strings.Repeat("dictionary-zstd-cap-", 512)), nil)
	section := typedcolumn.ColumnPartImageSection{
		Kind:        typedcolumn.ColumnPartImageSectionDictionaries,
		Compression: typedcolumn.CompressionZSTD,
		RawBytes:    16,
	}

	_, err = decodeTypedColumnPreparedDictionarySectionBytes(section, stored)
	if err == nil || !strings.Contains(err.Error(), "zstd decode") {
		t.Fatalf("decode prepared dictionary zstd err=%v want capped zstd decode failure", err)
	}
}

func typedColumnImageSectionByKind(image typedcolumn.ColumnPartImage, kind typedcolumn.ColumnPartImageSectionKind) (typedcolumn.ColumnPartImageSection, bool) {
	for _, section := range image.Sections {
		if section.Kind == kind {
			return section, true
		}
	}
	return typedcolumn.ColumnPartImageSection{}, false
}

func TestTypedColumnAdapterOptInCompressionRejectsZSTDDict1952(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	_, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 1952, RowsPerGranule: 8, Fields: []TypedStorageField{field}, DefaultCompression: typedcolumn.CompressionZSTDDict, DefaultCompressionSet: true}, []typedColumnAdapterRow{{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 1}}}})
	if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "unsupported compression zstd_dict") {
		t.Fatalf("err=%v want explicit zstd_dict rejection", err)
	}
}

func TestTypedColumnAdapterAdaptiveRowsPerGranule1952(t *testing.T) {
	field := typedColumnAdapterField("count", ColumnStoreValueInt64)
	rows := make([]typedColumnAdapterRow, 100)
	for i := range rows {
		rows[i] = typedColumnAdapterRow{PrimaryID: int64(i + 1), Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: int64(i)}}}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{
		PartID:         1952,
		RowsPerGranule: 100,
		Fields:         []TypedStorageField{field},
		AdaptiveMarkSizing: typedcolumn.ColumnAdaptiveMarkSizing{
			Enabled:     true,
			TargetBytes: 64,
			MinRows:     4,
			MaxRows:     16,
		},
	}, rows)
	if err != nil {
		t.Fatalf("build adaptive part: %v", err)
	}
	if got := part.Part.Options.PartPolicy.RowsPerGranule; got <= 0 || got > 16 {
		t.Fatalf("adaptive rows_per_granule=%d want <=16", got)
	}
	if granules := len(part.Part.Descriptor.Granules); granules <= 1 {
		t.Fatalf("granules=%d want adaptive split", granules)
	}
}

func TestTypedColumnBenchmarkPolicyEnvRequiresBenchmarkProfile1952(t *testing.T) {
	t.Setenv(typedColumnBenchmarkCompressionEnv, "snappy")
	t.Setenv(typedColumnBenchmarkSectionCompressionEnv, "zstd")
	t.Setenv(typedColumnBenchmarkLocatorCompressionEnv, "lz4")
	t.Setenv(typedColumnBenchmarkDictionaryCompressionEnv, "zstd")
	t.Setenv(typedColumnBenchmarkPruningCompressionEnv, "none")
	cfg := ColumnStoreConfig{ProfileSupport: ColumnStoreProfileDurableOnly}
	var opts typedColumnAdapterOptions
	if err := applyTypedColumnBenchmarkPolicyFromEnv(cfg, &opts); err == nil || !strings.Contains(err.Error(), string(ColumnStoreProfileBenchmarkRelaxed)) {
		t.Fatalf("err=%v want profile support requirement", err)
	}
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	if err := applyTypedColumnBenchmarkPolicyFromEnv(cfg, &opts); err != nil {
		t.Fatalf("benchmark relaxed policy: %v", err)
	}
	if !opts.DefaultCompressionSet || opts.DefaultCompression != typedcolumn.CompressionSnappy {
		t.Fatalf("opts=%+v want snappy", opts)
	}
	if !opts.SectionCompressionSet || opts.SectionCompression != typedcolumn.CompressionZSTD {
		t.Fatalf("opts=%+v want zstd section compression", opts)
	}
	if !opts.LocatorSectionCompressionSet || opts.LocatorSectionCompression != typedcolumn.CompressionLZ4 {
		t.Fatalf("opts=%+v want lz4 locator compression", opts)
	}
	if !opts.DictionarySectionCompressionSet || opts.DictionarySectionCompression != typedcolumn.CompressionZSTD {
		t.Fatalf("opts=%+v want zstd dictionary compression", opts)
	}
	if !opts.PruningSectionCompressionSet || opts.PruningSectionCompression != typedcolumn.CompressionNone {
		t.Fatalf("opts=%+v want none pruning compression", opts)
	}
}

func TestTypedColumnBenchmarkPolicyEnvAcceptsZSTD1952(t *testing.T) {
	t.Setenv(typedColumnBenchmarkCompressionEnv, "zstd")
	t.Setenv(typedColumnBenchmarkSectionCompressionEnv, "zstd")
	var opts typedColumnAdapterOptions
	if err := applyTypedColumnBenchmarkPolicyFromEnv(ColumnStoreConfig{ProfileSupport: ColumnStoreProfileBenchmarkRelaxed}, &opts); err != nil {
		t.Fatalf("zstd env policy: %v", err)
	}
	if !opts.DefaultCompressionSet || opts.DefaultCompression != typedcolumn.CompressionZSTD {
		t.Fatalf("opts=%+v want zstd", opts)
	}
	if !opts.SectionCompressionSet || opts.SectionCompression != typedcolumn.CompressionZSTD {
		t.Fatalf("opts=%+v want zstd section compression", opts)
	}
}

func TestTypedColumnBenchmarkPolicyEnvAcceptsPerSectionCompression1952(t *testing.T) {
	t.Setenv(typedColumnBenchmarkCompressionEnv, "lz4")
	t.Setenv(typedColumnBenchmarkLocatorCompressionEnv, "none")
	t.Setenv(typedColumnBenchmarkDictionaryCompressionEnv, "zstd")
	t.Setenv(typedColumnBenchmarkPruningCompressionEnv, "lz4")
	var opts typedColumnAdapterOptions
	if err := applyTypedColumnBenchmarkPolicyFromEnv(ColumnStoreConfig{ProfileSupport: ColumnStoreProfileBenchmarkRelaxed}, &opts); err != nil {
		t.Fatalf("per-section env policy: %v", err)
	}
	if !opts.DefaultCompressionSet || opts.DefaultCompression != typedcolumn.CompressionLZ4 {
		t.Fatalf("opts=%+v want lz4 default compression", opts)
	}
	if !opts.SectionCompressionSet || opts.SectionCompression != typedcolumn.CompressionLZ4 {
		t.Fatalf("opts=%+v want lz4 section compression inherited from combined env", opts)
	}
	if !opts.LocatorSectionCompressionSet || opts.LocatorSectionCompression != typedcolumn.CompressionNone {
		t.Fatalf("opts=%+v want none locator compression", opts)
	}
	if !opts.DictionarySectionCompressionSet || opts.DictionarySectionCompression != typedcolumn.CompressionZSTD {
		t.Fatalf("opts=%+v want zstd dictionary compression", opts)
	}
	if !opts.PruningSectionCompressionSet || opts.PruningSectionCompression != typedcolumn.CompressionLZ4 {
		t.Fatalf("opts=%+v want lz4 pruning compression", opts)
	}
}

func TestTypedColumnBenchmarkPolicyEnvEmptyDoesNotRequireProfile1952(t *testing.T) {
	for _, name := range []string{typedColumnBenchmarkCompressionEnv, typedColumnBenchmarkSectionCompressionEnv, typedColumnBenchmarkLocatorCompressionEnv, typedColumnBenchmarkDictionaryCompressionEnv, typedColumnBenchmarkPruningCompressionEnv, typedColumnBenchmarkInt64EncodingEnv, typedColumnBenchmarkRowsPerGranuleEnv, typedColumnBenchmarkAdaptiveEnabledEnv, typedColumnBenchmarkAdaptiveTargetBytesEnv, typedColumnBenchmarkAdaptiveMinRowsEnv, typedColumnBenchmarkAdaptiveMaxRowsEnv} {
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	var opts typedColumnAdapterOptions
	if err := applyTypedColumnBenchmarkPolicyFromEnv(ColumnStoreConfig{}, &opts); err != nil {
		t.Fatalf("empty env policy: %v", err)
	}
}
