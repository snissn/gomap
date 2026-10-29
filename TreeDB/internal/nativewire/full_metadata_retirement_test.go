package nativewire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestDeterministicFullMetadataSemanticErrors(t *testing.T) {
	meta := collections.CollectionMeta{
		Name: "articles", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph}},
	}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := commitlog.DecodeCatalogCreateCollectionPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, from, to string
		unsupported    bool
	}{
		{name: "modern"},
		{name: "retired_strategy", from: `"strategy":"column_graph"`, to: `"strategy":"native_runtime"`, unsupported: true},
		{name: "unsupported_metric", from: `"metric":"cosine"`, to: `"metric":"l2"`, unsupported: true},
		{name: "unsupported_encoding", from: `"strategy":"column_graph"`, to: `"encoding":"int8","strategy":"column_graph"`, unsupported: true},
		{name: "unsupported_bson", from: `"options":{`, to: `"options":{"document_format":"bson",`, unsupported: true},
		{name: "malformed_strategy_type", from: `"strategy":"column_graph"`, to: `"strategy":17`},
		{name: "invalid_dimensions", from: `"dimensions":2`, to: `"dimensions":-1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := catalog.Metadata
			if tc.from != "" {
				if bytes.Count(disk, []byte(tc.from)) != 1 {
					t.Fatalf("fixture must have exactly one %s", tc.from)
				}
				disk = bytes.Replace(disk, []byte(tc.from), []byte(tc.to), 1)
			}
			payload, err := commitlog.EncodeCatalogCreateCollectionPayload(meta.Name, disk)
			if err != nil {
				t.Fatal(err)
			}
			raw := append(binary.AppendUvarint(nil, 7), payload...)
			sections := []Section{
				{ID: SectionCommandHeader, Bytes: AppendCommandHeader(nil, CommandHeader{ID: CommandCreateCollection, Version: 1})},
				{ID: SectionIdempotencyKey, Bytes: []byte("full-metadata")},
				{ID: SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 0)},
				{ID: SectionCollectionMeta, Bytes: raw},
			}
			cmd, err := MustV1Registry().ValidateRequestSections(sections)
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []struct {
				name string
				run  func() error
			}{
				{"append", func() error { _, err := AppendDeterministicEntry(nil, cmd); return err }},
				{"decode", func() error {
					_, err := DecodeDeterministicEntry(deterministicMetadataEntryRaw(CommandCreateCollection, SectionCollectionMeta, raw), Limits{})
					return err
				}},
			} {
				t.Run(operation.name, func(t *testing.T) {
					err := operation.run()
					if tc.from == "" {
						if err != nil {
							t.Fatalf("modern full metadata: %v", err)
						}
						return
					}
					want := ErrInvalidCommand
					if tc.unsupported {
						want = ErrUnsupportedFeature
					}
					if codeOf(err) != want {
						t.Errorf("code=%d want=%d: %v", codeOf(err), want, err)
					}
					if errors.Is(err, backenddb.ErrNativeSemanticUnsupported) != tc.unsupported {
						t.Errorf("semantic cause preserved=%t want=%t: %v", errors.Is(err, backenddb.ErrNativeSemanticUnsupported), tc.unsupported, err)
					}
				})
			}
		})
	}
}
