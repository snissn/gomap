package raftapply

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestCreateCollectionMetaV7LowersFullTextDeclaration(t *testing.T) {
	meta := collections.CollectionMeta{Name: "articles", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON, DisableBufferedIndexedAsyncFlush: true}, TextIndexes: []collections.TextIndexDefinition{{Name: "content", Fields: []collections.TextIndexField{{Field: "title", Weight: 2}}, Analyzer: collections.TextAnalyzerSimple, AnalyzerOptions: &collections.TextAnalyzerOptions{StopWords: []string{"the"}}, Version: collections.TextIndexVersionV2, Rollout: collections.TextIndexRolloutPrimary, StorePositions: true, StoreOffsets: true, SchemaGeneration: 9, StoragePolicy: collections.RootStorageCompressed}}}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	raw := append(binary.AppendUvarint(nil, 7), payload...)
	decoded, err := decodeCreateCollectionMetaV1(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.TextIndexes, meta.TextIndexes) || decoded.Options.DocumentFormat != meta.Options.DocumentFormat || !decoded.Options.DisableBufferedIndexedAsyncFlush {
		t.Fatalf("lowering lost declaration: %+v", decoded)
	}
	for _, hostile := range [][]byte{{7}, append(append([]byte(nil), raw...), 0), append([]byte(nil), raw[:len(raw)-1]...)} {
		if _, err := decodeCreateCollectionMetaV1(hostile); err == nil {
			t.Fatalf("malformed V7 accepted: %x", hostile)
		}
	}
}
