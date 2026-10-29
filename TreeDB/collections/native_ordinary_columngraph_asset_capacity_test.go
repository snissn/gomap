package collections

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Construct both images through the maintained generic part/image builders.
// Only dictionary metadata compression changes: document-ID data stays raw in
// both, so checking only selected data sections would miss the second case.
func TestNativeOrdinaryColumnGraphAssetReadCapacityCompression(t *testing.T) {
	const rows = 32
	ordinals := make([]int64, rows)
	documentIDs := typedcolumn.RawBytesOffsets{Rows: rows, Offsets: make([]uint64, rows+1)}
	for i := range ordinals {
		ordinals[i] = int64(i)
		documentIDs.Values = append(documentIDs.Values, []byte(fmt.Sprintf("document-%03d", i))...)
		documentIDs.Offsets[i+1] = uint64(len(documentIDs.Values))
	}
	part, err := typedcolumn.BuildColumnPart(71, typedcolumn.Options{
		SchemaVersion: 3,
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{Name: "ordinal", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: columnVectorGraphDocumentIDStateColumnName, Type: typedcolumn.ColumnTypeBytes, Encoding: typedcolumn.EncodingRawBytesOffsets, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{"ordinal"}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: "ordinal"}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{Rows: rows, Columns: map[string][]int64{"ordinal": ordinals}, BytesColumns: map[string]typedcolumn.RawBytesOffsets{columnVectorGraphDocumentIDStateColumnName: documentIDs}})
	if err != nil {
		t.Fatal(err)
	}
	metadata := make(map[string]int64, 64)
	for i := range 64 {
		metadata[strings.Repeat("compressible-metadata-", 4)+fmt.Sprintf("%04d", i)] = int64(i)
	}
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		t.Fatal(err)
	}
	if fixed == 0 {
		t.Fatal("fixture requires the maintained nonzero decoder envelope")
	}
	var rawCapacity uint64
	var rawPart *typedcolumn.ColumnPart
	for _, tc := range []struct {
		name        string
		compression typedcolumn.Compression
	}{{"all_raw", typedcolumn.CompressionNone}, {"compressed_metadata_raw_document_ids", typedcolumn.CompressionLZ4}} {
		t.Run(tc.name, func(t *testing.T) {
			image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
				Dictionaries:                    map[string]map[string]int64{"asset.metadata": metadata},
				LayoutLogicalTypes:              map[string]string{"ordinal": "int64", columnVectorGraphDocumentIDStateColumnName: "bytes"},
				DictionarySectionCompression:    tc.compression,
				DictionarySectionCompressionSet: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			compressedMetadata, rawIDSections := 0, 0
			for _, section := range image.Sections {
				if section.Column == columnVectorGraphDocumentIDStateColumnName && (section.Kind == typedcolumn.ColumnPartImageSectionColumnOffsets || section.Kind == typedcolumn.ColumnPartImageSectionColumnValues) {
					if section.Compression != typedcolumn.CompressionNone {
						t.Fatalf("document-ID data was compressed: %+v", section)
					}
					rawIDSections++
				}
				if section.Compression != typedcolumn.CompressionNone {
					if section.Kind != typedcolumn.ColumnPartImageSectionDictionaries || section.Compression != tc.compression || section.RawBytes <= section.Length {
						t.Fatalf("unexpected compressed section: %+v", section)
					}
					compressedMetadata++
				}
			}
			if rawIDSections != 2 {
				t.Fatalf("raw document-ID offsets/values sections=%d want=2", rawIDSections)
			}
			if tc.compression == typedcolumn.CompressionNone && compressedMetadata != 0 {
				t.Fatal("raw fixture contains compressed metadata")
			}
			if tc.compression != typedcolumn.CompressionNone && compressedMetadata != 1 {
				t.Fatalf("actual compressed dictionary witnesses=%d want=1", compressedMetadata)
			}

			root := t.TempDir()
			cfg := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{Kind: ColumnAssetManagerValueLogShaped, IsolatedNamespace: true, Namespace: "capacity-control"}}
			ref, err := writeColumnAssetToManager(root, cfg, image.Bytes, ColumnAssetKindTCS1TypedColumnPart, 1, image.PartID)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := readColumnPhysicalAssetFromManager(root, ref)
			if err != nil || !bytes.Equal(stored, image.Bytes) {
				t.Fatalf("asset-manager round trip: equal=%t err=%v", bytes.Equal(stored, image.Bytes), err)
			}
			parsed, err := typedcolumn.ParseColumnPartImage(stored)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := typedcolumn.ColumnPartFromImage(parsed)
			if err != nil {
				t.Fatalf("full parser rejected maintained %s image: %v", tc.name, err)
			}
			if decoded.Descriptor.RowCount != rows {
				t.Fatalf("decoded rows=%d want=%d", decoded.Descriptor.RowCount, rows)
			}
			if rawPart == nil {
				rawPart = decoded
			} else if !reflect.DeepEqual(decoded.Descriptor, rawPart.Descriptor) || !reflect.DeepEqual(decoded.Columns, rawPart.Columns) {
				t.Fatal("metadata compression changed decoded document-ID part")
			}

			asset := columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleDocumentIDs, RowCount: rows, Ref: ref, AssetBytes: ref.Length}
			calls := 0
			capacity, err := nativeColumnGraphAssetReadCapacity(root, asset, func(n uint64) error {
				calls++
				if n != uint64(ref.Length)*16 {
					t.Fatalf("prefix admission=%d want=%d", n, uint64(ref.Length)*16)
				}
				return nil
			})
			if err != nil || calls != 1 {
				t.Fatalf("capacity=%d prefix calls=%d err=%v", capacity, calls, err)
			}
			if tc.compression == typedcolumn.CompressionNone {
				rawCapacity = capacity
				if capacity >= fixed/2 {
					t.Fatalf("entirely raw capacity=%d must fit below decoder envelope/2=%d", capacity, fixed/2)
				}
			} else if capacity < fixed || capacity <= rawCapacity {
				t.Fatalf("compressed metadata lost decoder envelope: raw=%d compressed=%d fixed=%d", rawCapacity, capacity, fixed)
			}

			// The actual asset reference is valid, but this root has never been
			// created. Only admission-before-read can return this exact sentinel.
			refused := errors.New("prefix admission refused")
			calls = 0
			_, err = nativeColumnGraphAssetReadCapacity(filepath.Join(root, "absent-root"), asset, func(n uint64) error {
				calls++
				if n != uint64(ref.Length)*16 {
					t.Fatalf("refusal prefix=%d", n)
				}
				return refused
			})
			if !errors.Is(err, refused) || calls != 1 {
				t.Fatalf("read preceded prefix refusal: calls=%d err=%v", calls, err)
			}
			t.Logf("asset=%d capacity=%d decoder_fixed=%d compressed_metadata=%d raw_id_sections=%d", ref.Length, capacity, fixed, compressedMetadata, rawIDSections)
		})
	}
}
