package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextV2StorageStatsPositionValidationUsesPostingTableAndDocMapBlockCache4558(t *testing.T) {
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
		current, err := postings.docMapCurrentAtRoot(snap, catalog, collectionTextV2DocMapRootName("docs", "lexical"), 1, 1)
		if err != nil || !current || postings.docMap == nil {
			t.Fatalf("first docmap lookup current=%v cache=%v err=%v", current, postings.docMap != nil, err)
		}
		cached := postings.docMap
		current, err = postings.docMapCurrentAtRoot(snap, catalog, collectionTextV2DocMapRootName("docs", "lexical"), 2, 1)
		if err != nil || !current || postings.docMap != cached {
			t.Fatalf("second docmap lookup current=%v reused=%v err=%v", current, postings.docMap == cached, err)
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

func TestTextV2StorageStatsEmptyPositionsKeepsDocMapCacheEmpty4560(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}
	col := createTextSearchCollection2627(t, d, "docs", def, [][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"other":"one"}`), []byte(`{"other":"two"}`)})
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		status, ok, err := readTextV2StatusAtRoot(snap, catalog, collectionTextV2GenerationsRootName("docs", "lexical"))
		if err != nil || !ok {
			t.Fatalf("read status ok=%v err=%v", ok, err)
		}
		validation := newTextV2PositionValidation()
		stats := TextIndexStorageStats{Version: TextIndexVersionV2}
		if err := inspectTextV2Root(snap, catalog, def, collectionTextV2DocMapRootName("docs", "lexical"), textV2RootFamilyDocMap, status, &stats, nil, validation); err != nil {
			t.Fatalf("inspect docmap root: %v", err)
		}
		if validation.docMap != nil {
			t.Fatal("docmap cache populated without positions")
		}
		if err := inspectTextV2Root(snap, catalog, def, collectionTextV2PositionsRootName("docs", "lexical"), textV2RootFamilyPositions, status, &stats, nil, validation); err != nil {
			t.Fatalf("inspect positions root: %v", err)
		}
		if validation.docMap != nil {
			t.Fatal("docmap cache populated for empty positions root")
		}
	})
	if _, err := col.TextIndexStorageStats("lexical"); err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
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
