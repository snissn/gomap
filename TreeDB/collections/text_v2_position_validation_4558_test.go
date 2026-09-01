package collections

import (
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextV2StorageStatsPositionValidationUsesScanTables4558(t *testing.T) {
	const documents = 128
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids := make([][]byte, documents)
	docs := make([][]byte, documents)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(`{"body":"shared phrase"}`)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}
	col := createTextSearchCollection2627(t, d, "docs", def, ids, docs)

	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		status, ok, err := readTextV2StatusAtRoot(snap, catalog, collectionTextV2GenerationsRootName("docs", "lexical"))
		if err != nil || !ok {
			t.Fatalf("read status ok=%v err=%v", ok, err)
		}
		postings := newTextV2PositionValidation()
		docMapStats := TextIndexStorageStats{Version: TextIndexVersionV2}
		if err := inspectTextV2Root(snap, catalog, def, collectionTextV2DocMapRootName("docs", "lexical"), textV2RootFamilyDocMap, status, &docMapStats, nil, postings); err != nil {
			t.Fatalf("inspect docmap root: %v", err)
		}
		if got, want := len(postings.docMaps), documents; got != want {
			t.Fatalf("docmap table entries=%d want %d", got, want)
		}
		stats := TextIndexStorageStats{Version: TextIndexVersionV2}
		if err := inspectTextV2Root(snap, catalog, def, collectionTextV2PostingBlocksRootName("docs", "lexical"), textV2RootFamilyPostingBlocks, status, &stats, nil, postings); err != nil {
			t.Fatalf("inspect posting root: %v", err)
		}
		if got, want := len(postings.postings), 2*documents; got != want {
			t.Fatalf("posting table entries=%d want %d", got, want)
		}
		positionStats := TextIndexStorageStats{Version: TextIndexVersionV2}
		if err := inspectTextV2Root(snap, catalog, def, collectionTextV2PositionsRootName("docs", "lexical"), textV2RootFamilyPositions, status, &positionStats, nil, postings); err != nil {
			t.Fatalf("inspect positions root: %v", err)
		}
		if got, want := positionStats.V2PositionEntries, uint64(2*documents); got != want {
			t.Fatalf("validated positions=%d want %d", got, want)
		}

		var legacyScanned int
		for ordinal := uint64(1); ordinal <= documents; ordinal++ {
			_, found, scanned, err := readTextV2PositionPostingAtRootCounted(snap, catalog, collectionTextV2PostingBlocksRootName("docs", "lexical"), "shared", ordinal, 1, len(def.Fields))
			if err != nil || !found {
				t.Fatalf("legacy posting ordinal=%d found=%v err=%v", ordinal, found, err)
			}
			legacyScanned += scanned
			if _, found, duplicate := postings.lookup("shared", ordinal, 1); !found || duplicate {
				t.Fatalf("posting table missing ordinal=%d", ordinal)
			}
		}
		if legacyScanned < documents*documents {
			t.Fatalf("legacy rescans=%d want at least %d", legacyScanned, documents*documents)
		}
	})
	if _, err := col.TextIndexStorageStats("lexical"); err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
}

func TestTextV2PositionPostingValidationDefersDuplicateFailure4558(t *testing.T) {
	postings := newTextV2PositionValidation()
	entry := textV2PostingBlockEntry{Ordinal: 7, Generation: 3, TermFrequency: 1, FieldFrequencies: []uint32{1}}
	if err := postings.add("shared", entry, 1); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := postings.add("shared", entry, 1); err != nil {
		t.Fatalf("duplicate add: %v", err)
	}
	if _, found, duplicate := postings.lookup("shared", entry.Ordinal, entry.Generation); !found || !duplicate {
		t.Fatalf("lookup found=%v duplicate=%v want duplicate entry", found, duplicate)
	}
}

func TestTextV2StorageStatsRejectsNoncanonicalDocMapBlockWithPositions4560(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}
	col := createTextSearchCollection2627(t, d, "docs", def, [][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"shared"}`), []byte(`{"body":"shared"}`)})

	var entry textV2DocMapEntry
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		block := textV2DocMapRootBlock(t, snap, catalog, "docs", "lexical", 1)
		var ok bool
		entry, ok = block.find(2)
		if !ok {
			t.Fatal("missing canonical docmap entry")
		}
	})
	corruptTextRootValue(t, d, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(2), encodeTextV2DocMapBlockValue(textV2DocMapBlockValue{BlockStart: 2, BlockSize: textV2DefaultDocMapBlockSize, Entries: []textV2DocMapEntry{entry}}))
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("TextIndexStorageStats err=%v want storage corruption", err)
	}
}

func BenchmarkTextV2StorageStatsPositionValidation4558(b *testing.B) {
	const documents = 1024
	d := openTextV2TestDB(b, b.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids := make([][]byte, documents)
	docs := make([][]byte, documents)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
		docs[i] = []byte(`{"body":"shared phrase"}`)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}
	col := createTextSearchCollection2627(b, d, "docs", def, ids, docs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.TextIndexStorageStats("lexical"); err != nil {
			b.Fatalf("TextIndexStorageStats: %v", err)
		}
	}
}
