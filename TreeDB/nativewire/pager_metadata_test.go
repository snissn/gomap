package nativewire

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestPagerMetadataReportingPreservesCreatePolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version byte
		set     func(*collections.CollectionMeta)
	}{
		{"v6 column control", 6, func(meta *collections.CollectionMeta) {
			meta.Options.ColumnStore.ControlRootStoragePolicy = collections.RootStoragePager
		}},
		{"v7 column manifest", 7, func(meta *collections.CollectionMeta) {
			meta.Options.ColumnStore.ManifestRoot = &collections.ColumnManifestRootDescriptor{StoragePolicy: collections.RootStoragePager}
		}},
		{"v7 data root", 7, func(meta *collections.CollectionMeta) {
			meta.Options.DataRootStoragePolicy = collections.RootStoragePager
		}},
		{"v7 index state root", 7, func(meta *collections.CollectionMeta) {
			meta.Options.IndexStateStoragePolicy = collections.RootStoragePager
		}},
		{"v7 scalar index", 7, func(meta *collections.CollectionMeta) {
			meta.Indexes[0].StoragePolicy = collections.RootStoragePager
		}},
		{"v7 text index", 7, func(meta *collections.CollectionMeta) {
			meta.TextIndexes[0].StoragePolicy = collections.RootStoragePager
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV1)
			if tc.version == 6 {
				meta.TextIndexes = nil
			}
			meta.Indexes = []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}}
			meta.VectorIndexes = []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: collections.VectorMetricCosine,
				Dimensions: 2, Strategy: collections.VectorIndexStrategyColumnGraph, Encoding: collections.VectorIndexEncodingFloat32}}
			meta.Options.ColumnStore = &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding",
				Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}
			tc.set(&meta)
			raw, err := encodeCollectionMeta(meta)
			if err != nil || len(raw) == 0 || raw[0] != tc.version {
				t.Fatalf("metadata encoding: %x, %v", raw, err)
			}
			got, err := decodeCollectionMeta(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.Options.DataRootStoragePolicy != meta.Options.DataRootStoragePolicy ||
				got.Options.IndexStateStoragePolicy != meta.Options.IndexStateStoragePolicy ||
				got.Indexes[0].StoragePolicy != meta.Indexes[0].StoragePolicy ||
				got.Options.ColumnStore == nil || len(got.VectorIndexes) != 1 || got.VectorIndexes[0].Dimensions != 2 {
				t.Fatalf("metadata response changed declared roles: %+v", got)
			}
			if tc.version == 7 && got.TextIndexes[0].StoragePolicy != meta.TextIndexes[0].StoragePolicy {
				t.Fatalf("text storage policy changed: %+v", got.TextIndexes)
			}
			column := meta.Options.ColumnStore
			if (column.ControlRootStoragePolicy == collections.RootStoragePager || column.ManifestRoot != nil) &&
				(got.Options.ColumnStore.ControlRootStoragePolicy != collections.RootStoragePager ||
					got.Options.ColumnStore.ManifestRoot.StoragePolicy != collections.RootStoragePager) {
				t.Fatalf("Pager column policy lost: %+v", got.Options.ColumnStore)
			}
			if _, err := decodeCollectionMetaForPurpose(raw, true); err == nil || !strings.Contains(err.Error(), "pager") {
				t.Fatalf("create-purpose decoder: got %v, want explicit Pager refusal", err)
			}
			sections := []iwire.Section{
				{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandCreateCollection, Version: 1})},
				{ID: iwire.SectionIdempotencyKey, Bytes: []byte("pager-create-rejection")},
				{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 0)},
				{ID: iwire.SectionCollectionMeta, Bytes: raw},
			}
			validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := iwire.AppendDeterministicEntry(nil, validated); err == nil || !strings.Contains(err.Error(), "pager") {
				t.Fatalf("deterministic create: got %v, want explicit Pager refusal", err)
			}
		})
	}
}
