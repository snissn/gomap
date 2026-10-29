package nativewire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestFullMetadataSemanticErrors(t *testing.T) {
	meta := textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV2)
	meta.VectorIndexes = []collections.VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph}}
	raw, err := encodeCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	version, off, err := readUvarint(raw)
	if err != nil || version != 7 {
		t.Fatalf("fixture must use full metadata v7: version=%d err=%v", version, err)
	}
	catalog, err := commitlog.DecodeCatalogCreateCollectionPayload(raw[off:])
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
			for _, create := range []bool{false, true} {
				t.Run(fmt.Sprintf("create=%t", create), func(t *testing.T) {
					_, err := decodeCollectionMetaForPurpose(raw, create)
					if tc.from == "" {
						if err != nil {
							t.Fatalf("modern full metadata: %v", err)
						}
						return
					}
					want := iwire.ErrInvalidCommand
					if tc.unsupported {
						want = iwire.ErrUnsupportedFeature
					}
					if nativeCodeOf(err) != want {
						t.Errorf("code=%d want=%d: %v", nativeCodeOf(err), want, err)
					}
					if errors.Is(err, backenddb.ErrNativeSemanticUnsupported) != tc.unsupported {
						t.Errorf("semantic cause preserved=%t want=%t: %v", errors.Is(err, backenddb.ErrNativeSemanticUnsupported), tc.unsupported, err)
					}
				})
			}
		})
	}
}
