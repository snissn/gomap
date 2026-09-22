package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestTextV2StorageCodecsRoundTripAndFailClosed2624(t *testing.T) {
	format := textV2RootFormatValue{FormatVersion: textV2FormatVersion, Family: textV2RootFamilyNormBlocks, DocMapBlockSize: textV2DefaultDocMapBlockSize, NormBlockSize: textV2DefaultNormBlockSize, Fields: []string{"body", "title"}}
	decodedFormat, err := decodeTextV2RootFormatValue(encodeTextV2RootFormatValue(format))
	if err != nil {
		t.Fatalf("decode format: %v", err)
	}
	if decodedFormat.Family != format.Family || !slices.Equal(decodedFormat.Fields, format.Fields) {
		t.Fatalf("format=%+v want %+v", decodedFormat, format)
	}

	status := textV2IndexStatusValue{FormatVersion: textV2FormatVersion, RootGeneration: 7, StatsGeneration: 7, DocMapGeneration: 7, NormGeneration: 7, TermGeneration: 7, NextOrdinal: 42, LiveDocuments: 39, DeletedDocuments: 2}
	decodedStatus, err := decodeTextV2IndexStatusValue(encodeTextV2IndexStatusValue(status))
	if err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if decodedStatus != status {
		t.Fatalf("status=%+v want %+v", decodedStatus, status)
	}

	docID := []byte{'d', 0, '1'}
	docKey := encodeTextV2DocIDKey(docID)
	decodedDocID, err := decodeTextV2DocIDKey(docKey)
	if err != nil || !bytes.Equal(decodedDocID, docID) {
		t.Fatalf("docid key=%q err=%v want %q", decodedDocID, err, docID)
	}
	docValue := textV2DocIDValue{Ordinal: 9, Generation: 3, Flags: textV2DocFlagTombstone}
	decodedDocValue, err := decodeTextV2DocIDValue(encodeTextV2DocIDValue(docValue))
	if err != nil || decodedDocValue != docValue || !decodedDocValue.tombstoned() {
		t.Fatalf("doc value=%+v err=%v want %+v", decodedDocValue, err, docValue)
	}

	blockKey := encodeTextV2BlockKey(129)
	if blockStart, err := decodeTextV2BlockKey(blockKey); err != nil || blockStart != 129 {
		t.Fatalf("block start=%d err=%v want 129", blockStart, err)
	}
	docMap := textV2DocMapBlockValue{BlockStart: 129, BlockSize: textV2DefaultDocMapBlockSize, Entries: []textV2DocMapEntry{{Ordinal: 130, Generation: 1, DocumentID: []byte("d130")}, {Ordinal: 129, Generation: 2, Flags: textV2DocFlagTombstone, DocumentID: []byte("d129")}}}
	decodedDocMap, err := decodeTextV2DocMapBlockValue(encodeTextV2DocMapBlockValue(docMap))
	if err != nil {
		t.Fatalf("decode docmap: %v", err)
	}
	if len(decodedDocMap.Entries) != 2 || decodedDocMap.Entries[0].Ordinal != 129 || !decodedDocMap.Entries[0].tombstoned() || !bytes.Equal(decodedDocMap.Entries[1].DocumentID, []byte("d130")) {
		t.Fatalf("docmap=%+v", decodedDocMap)
	}

	norm := textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 2, Entries: []textV2NormBlockEntry{{Ordinal: 2, Generation: 1, FieldLengths: []uint32{3, 5}}, {Ordinal: 1, Generation: 2, Flags: textV2DocFlagTombstone, FieldLengths: []uint32{0, 0}}}}
	decodedNorm, err := decodeTextV2NormBlockValue(encodeTextV2NormBlockValue(norm))
	if err != nil {
		t.Fatalf("decode norm: %v", err)
	}
	if len(decodedNorm.Entries) != 2 || decodedNorm.Entries[0].Ordinal != 1 || !decodedNorm.Entries[0].tombstoned() || !slices.Equal(decodedNorm.Entries[1].FieldLengths, []uint32{3, 5}) {
		t.Fatalf("norm=%+v", decodedNorm)
	}

	corpus := textV2CorpusStatsValue{StatsGeneration: 7, DocumentCount: 39}
	if got, err := decodeTextV2CorpusStatsValue(encodeTextV2CorpusStatsValue(corpus)); err != nil || got != corpus {
		t.Fatalf("corpus=%+v err=%v", got, err)
	}
	term := textV2TermStatsValue{StatsGeneration: 7, DocumentFrequency: 11, TotalTermFrequency: 23, PostingBlockCount: 5}
	if got, err := decodeTextV2TermStatsValue(encodeTextV2TermStatsValue(term)); err != nil || got != term {
		t.Fatalf("term=%+v err=%v", got, err)
	}
	field := textV2FieldStatsValue{StatsGeneration: 7, DocumentCount: 31, TotalTokenCount: 111}
	if got, err := decodeTextV2FieldStatsValue(encodeTextV2FieldStatsValue(field)); err != nil || got != field {
		t.Fatalf("field=%+v err=%v", got, err)
	}
	for _, tc := range []struct {
		name string
		key  []byte
		want textV2StatsKey
	}{
		{name: "corpus", key: encodeTextV2CorpusStatsKey(), want: textV2StatsKey{Kind: textV2KeyKindCorpusStats}},
		{name: "field", key: encodeTextV2FieldStatsKey("body"), want: textV2StatsKey{Kind: textV2KeyKindFieldStats, Value: "body"}},
		{name: "term", key: encodeTextV2TermStatsKey("refund"), want: textV2StatsKey{Kind: textV2KeyKindTermStats, Value: "refund"}},
	} {
		got, err := decodeTextV2StatsKey(tc.key)
		if err != nil || got != tc.want {
			t.Fatalf("stats key %s=%+v err=%v want %+v", tc.name, got, err, tc.want)
		}
	}

	malformed := []struct {
		name string
		err  error
	}{
		{name: "format version", err: func() error { _, err := decodeTextV2RootFormatValue([]byte{99}); return err }()},
		{name: "status zero generation", err: func() error {
			raw := encodeTextV2IndexStatusValue(status)
			raw[2] = 0
			_, err := decodeTextV2IndexStatusValue(raw)
			return err
		}()},
		{name: "status component generation exceeds root", err: func() error {
			bad := status
			bad.NormGeneration = bad.RootGeneration + 1
			_, err := decodeTextV2IndexStatusValue(encodeTextV2IndexStatusValue(bad))
			return err
		}()},
		{name: "docid key version", err: func() error { _, err := decodeTextV2DocIDKey([]byte{99, textV2KeyKindDocID, 'd'}); return err }()},
		{name: "docid zero ordinal", err: func() error { _, err := decodeTextV2DocIDValue([]byte{textV2DocIDValueVersion, 0, 1, 0}); return err }()},
		{name: "block key zero", err: func() error {
			_, err := decodeTextV2BlockKey([]byte{textV2KeyVersion, textV2KeyKindBlock, 0, 0, 0, 0, 0, 0, 0, 0})
			return err
		}()},
		{name: "docmap out of order", err: func() error {
			_, err := decodeTextV2DocMapBlockValue([]byte{textV2DocMapValueVersion, 1, 128, 2, 2, 1, 0, 1, 'b', 1, 1, 0, 1, 'a'})
			return err
		}()},
		{name: "norm truncated field lengths", err: func() error {
			_, err := decodeTextV2NormBlockValue([]byte{textV2NormBlockValueVersion, 1, 128, 2, 1, 1, 1, 0, 3})
			return err
		}()},
		{name: "norm zero field count", err: func() error {
			_, err := decodeTextV2NormBlockValue([]byte{textV2NormBlockValueVersion, 1, 128, 0, 0})
			return err
		}()},
		{name: "term zero generation", err: func() error {
			_, err := decodeTextV2TermStatsValue([]byte{textV2StatsValueVersion, 0, 1, 1, 0})
			return err
		}()},
		{name: "stats key trailing bytes", err: func() error {
			_, err := decodeTextV2StatsKey([]byte{textV2KeyVersion, textV2KeyKindCorpusStats, 1})
			return err
		}()},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("err=%v want ErrTextIndexStorageCorrupt", tc.err)
			}
		})
	}
}

func TestCreateCollectionInlineTextV2PublishesRootState2624(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	meta, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:    "lexical",
			Version: TextIndexVersionV2,
			Fields:  []TextIndexField{{Field: "body"}},
		}},
	})
	if err != nil {
		t.Fatalf("CreateCollection v2 metadata: %v", err)
	}
	if len(meta.TextIndexes) != 1 || meta.TextIndexes[0].Version != TextIndexVersionV2 {
		t.Fatalf("metadata text indexes=%+v want v2", meta.TextIndexes)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertDefaultV2StatusAndEmptyRoots2690(t, d, col, "lexical")
}

func TestCollectionCreateTextV2IndexBackfillsOrdinalsNormsStatsAndReopens2624(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := [][]byte{[]byte("d2"), []byte("d1"), []byte("d3")}
	docs := [][]byte{
		[]byte(`{"body":"shipping policy","title":"Other"}`),
		[]byte(`{"body":"refund failed refund","title":"Refund Policy"}`),
		[]byte(`{"body":"---","title":"Empty"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	_, backfill, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}}, StoragePolicy: RootStorageFast})
	if err != nil {
		t.Fatalf("CreateTextIndex v2: %v", err)
	}
	if backfill.DocumentsScanned != 3 || backfill.V2DocIDEntries != 3 || backfill.V2DocMapBlocks != 1 || backfill.V2NormBlocks != 1 || backfill.V2StatusRecords != 1 || backfill.V2FormatRecords != 7 || backfill.V2NextOrdinal != 4 {
		t.Fatalf("backfill=%+v want v2 roots/docs/next ordinal", backfill)
	}
	status, err := col.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus: %v", err)
	}
	if status.Version != TextIndexVersionV2 || !status.Ready || !status.Readable || !status.Writable || status.FailClosed || status.FailClosedReason != "" {
		t.Fatalf("status=%+v want v2 root-ready/readable/writable score-only", status)
	}
	if !slices.Equal(status.ActiveRootNames, collectionTextV2RootNames("docs", "lexical")) {
		t.Fatalf("active roots=%q", status.ActiveRootNames)
	}
	for _, rootName := range collectionTextV2RootNames("docs", "lexical") {
		if rootID := requireCollectionRootDescriptor2624(t, d, rootName); rootID == 0 {
			t.Fatalf("root %q descriptor is zero", rootName)
		}
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Version != TextIndexVersionV2 || stats.Documents != 3 || stats.V2LiveDocuments != 3 || stats.V2DeletedDocs != 0 || stats.V2NextOrdinal != 4 || stats.V2DocIDEntries != 3 || stats.V2TermStats == 0 {
		t.Fatalf("stats=%+v want v2 docs/stats", stats)
	}
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		d1 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d1"))
		d2 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d2"))
		if d1.Ordinal != 1 || d2.Ordinal != 2 || d1.Generation != 1 || d2.Generation != 1 {
			t.Fatalf("ordinals d1=%+v d2=%+v want primary-key order", d1, d2)
		}
		block := textV2NormRootBlock(t, snap, catalog, "docs", "lexical", 1)
		entry, ok := block.find(d1.Ordinal)
		if !ok || !slices.Equal(entry.FieldLengths, []uint32{3, 2}) {
			t.Fatalf("d1 norm entry=%+v ok=%v want body/title lengths 3/2", entry, ok)
		}
		state := textDocumentStateValueFromAnalysis(mustAnalyzeTextV2Doc2624(t, TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}}}, docs[1]))
		if !slices.Equal(entry.FieldLengths, textV2FieldLengthsFromState(TextIndexDefinition{Fields: []TextIndexField{{Field: "body"}, {Field: "title"}}}, state)) {
			t.Fatalf("norm lengths=%v not equal v1 state=%+v", entry.FieldLengths, state.Fields)
		}
		termRaw := textRootValue(t, snap, catalog, collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2TermStatsKey("refund"))
		term, err := decodeTextV2TermStatsValue(termRaw)
		if err != nil {
			t.Fatalf("decode refund term: %v", err)
		}
		if term.DocumentFrequency != 1 || term.TotalTermFrequency != 3 {
			t.Fatalf("refund term=%+v want df=1 tf=3", term)
		}
	})
	search, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10})
	if err != nil {
		t.Fatalf("v2 SearchText: %v", err)
	}
	if len(search.Results) != 1 || string(search.Results[0].DocumentID) != "d1" || len(search.Results[0].TextMatches) == 0 || search.Stats.TextMatchDetailsBuilt != uint64(len(search.Results)) || search.Stats.TextStateLookups != 0 {
		t.Fatalf("v2 SearchText response=%+v want lazy detailed d1 without text-state work", search)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedStats, err := reopenedCol.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("reopened TextIndexStorageStats: %v", err)
	}
	if reopenedStats != stats {
		t.Fatalf("reopened stats=%+v want %+v", reopenedStats, stats)
	}
}

func TestTextV2MutationsKeepOrdinalsGenerationsTombstonesAfterReopen2624(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"shipping policy"}`)}); err != nil {
		t.Fatalf("InsertBatch setup: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund refund updated"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch d2 deleted=%d err=%v", deleted, err)
	}
	if _, err := col.Insert([]byte("d2"), []byte(`{"body":"shipping returns"}`)); err != nil {
		t.Fatalf("reinsert d2: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	withTextCatalog(t, reopened, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		d1 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d1"))
		d2 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d2"))
		if d1.Ordinal != 1 || d1.Generation != 2 || d1.tombstoned() {
			t.Fatalf("d1=%+v want same ordinal generation 2 live", d1)
		}
		if d2.Ordinal != 3 || d2.Generation != 1 || d2.tombstoned() {
			t.Fatalf("d2=%+v want fresh non-reused ordinal 3 live", d2)
		}
		oldBlock := textV2DocMapRootBlock(t, snap, catalog, "docs", "lexical", 1)
		oldD2, ok := oldBlock.find(2)
		if !ok || !oldD2.tombstoned() || oldD2.Generation != 2 || !bytes.Equal(oldD2.DocumentID, []byte("d2")) {
			t.Fatalf("old d2 docmap entry=%+v ok=%v want tombstone generation 2", oldD2, ok)
		}
		newBlock := textV2DocMapRootBlock(t, snap, catalog, "docs", "lexical", 1)
		newD2, ok := newBlock.find(3)
		if !ok || newD2.tombstoned() || !bytes.Equal(newD2.DocumentID, []byte("d2")) {
			t.Fatalf("new d2 docmap entry=%+v ok=%v want live fresh ordinal", newD2, ok)
		}
		status := textV2StatusRootValue(t, snap, catalog, "docs", "lexical")
		if status.NextOrdinal != 4 || status.LiveDocuments != 2 || status.DeletedDocuments != 1 || status.RootGeneration < 4 {
			t.Fatalf("status=%+v want next=4 live=2 deleted=1 generation advanced", status)
		}
		termRaw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2TermStatsKey("policy"), nil)
		if err != nil {
			t.Fatalf("read policy term: %v", err)
		}
		if ok {
			term, err := decodeTextV2TermStatsValue(termRaw)
			if err != nil {
				t.Fatalf("decode policy term: %v", err)
			}
			if term.DocumentFrequency != 0 || term.TotalTermFrequency != 0 {
				t.Fatalf("policy term should have been removed or zero, got %+v", term)
			}
		}
	})
}

func TestTextV2StorageFailsClosedOnVersionMismatchAndStatsNormInconsistency2624(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	corruptTextRootValue(t, d, "docs", collectionTextV2NormBlocksRootName("docs", "lexical"), encodeTextV2FormatKey(), []byte{99})
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("corrupt norm format err=%v want ErrTextIndexStorageCorrupt", err)
	}

	d2 := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d2.Close() }()
	mgr2 := NewCollectionManager(d2)
	if _, err := mgr2.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection 2: %v", err)
	}
	col2, err := mgr2.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection 2: %v", err)
	}
	if _, err := col2.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert 2: %v", err)
	}
	if _, _, err := col2.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex 2: %v", err)
	}
	corruptTextRootValue(t, d2, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2CorpusStatsKey(), encodeTextV2CorpusStatsValue(textV2CorpusStatsValue{StatsGeneration: 99, DocumentCount: 1}))
	if _, err := col2.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("corrupt stats generation err=%v want ErrTextIndexStorageCorrupt", err)
	}

	d3 := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d3.Close() }()
	mgr3 := NewCollectionManager(d3)
	if _, err := mgr3.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection 3: %v", err)
	}
	col3, err := mgr3.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection 3: %v", err)
	}
	if _, err := col3.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert 3: %v", err)
	}
	if _, _, err := col3.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex 3: %v", err)
	}
	badNorm := textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 1, Entries: []textV2NormBlockEntry{{Ordinal: 1, Generation: 99, FieldLengths: []uint32{2}}}}
	corruptTextRootValue(t, d3, "docs", collectionTextV2NormBlocksRootName("docs", "lexical"), encodeTextV2BlockKey(1), encodeTextV2NormBlockValue(badNorm))
	if _, err := col3.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("corrupt norm generation err=%v want ErrTextIndexStorageCorrupt", err)
	}

	d4 := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d4.Close() }()
	mgr4 := NewCollectionManager(d4)
	if _, err := mgr4.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection 4: %v", err)
	}
	col4, err := mgr4.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection 4: %v", err)
	}
	if _, err := col4.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert 4: %v", err)
	}
	if _, _, err := col4.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex 4: %v", err)
	}
	clearTextRootDescriptor2624(t, d4, collectionTextV2DocMapRootName("docs", "lexical"))
	if _, err := col4.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("missing docmap root err=%v want ErrTextIndexStorageCorrupt", err)
	}
}

func TestTextV2FieldStatsFailClosedOnMissingUnknownAndOutOfRange2624(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(t *testing.T, d *backenddb.DB)
	}{
		{
			name: "missing field stats",
			corrupt: func(t *testing.T, d *backenddb.DB) {
				deleteTextRootValue2624(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2FieldStatsKey("body"))
			},
		},
		{
			name: "unknown field stats",
			corrupt: func(t *testing.T, d *backenddb.DB) {
				corruptTextRootValue(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2FieldStatsKey("unknown"), encodeTextV2FieldStatsValue(textV2FieldStatsValue{StatsGeneration: 1, DocumentCount: 1, TotalTokenCount: 1}))
			},
		},
		{
			name: "out of range field doc count",
			corrupt: func(t *testing.T, d *backenddb.DB) {
				corruptTextRootValue(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2FieldStatsKey("body"), encodeTextV2FieldStatsValue(textV2FieldStatsValue{StatsGeneration: 1, DocumentCount: 2, TotalTokenCount: 1}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := openTextV2TestDB(t, t.TempDir(), false)
			defer func() { _ = d.Close() }()
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
				t.Fatalf("Insert: %v", err)
			}
			if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
				t.Fatalf("CreateTextIndex: %v", err)
			}
			tc.corrupt(t, d)
			if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("TextIndexStorageStats err=%v want ErrTextIndexStorageCorrupt", err)
			}
		})
	}
}

func TestTextV2RootDescriptorsReachValueLogMaintenance2624(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, true)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, 32)
	docs := make([][]byte, 32)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund policy customer number %d repeated repeated repeated","title":"ticket %d"}`, i, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}, {Field: "title"}}, StoragePolicy: RootStorageFast}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	for _, rootName := range collectionTextV2RootNames("docs", "lexical") {
		if rootID := requireCollectionRootDescriptor2624(t, d, rootName); rootID == 0 {
			t.Fatalf("root %q descriptor zero before GC", rootName)
		}
	}
	normBefore := textV2ReadNormBlockBytes2624(t, d, "docs", "lexical", 1)
	docMapBefore := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(1))
	termsBefore := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2CorpusStatsKey())
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	normAfter := textV2ReadNormBlockBytes2624(t, d, "docs", "lexical", 1)
	docMapAfter := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(1))
	termsAfter := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2CorpusStatsKey())
	if !bytes.Equal(normBefore, normAfter) || !bytes.Equal(docMapBefore, docMapAfter) || !bytes.Equal(termsBefore, termsAfter) {
		t.Fatalf("v2 root values changed after ValueLogGC")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, true)
	defer func() { _ = reopened.Close() }()
	if got := textV2ReadNormBlockBytes2624(t, reopened, "docs", "lexical", 1); !bytes.Equal(got, normBefore) {
		t.Fatalf("norm block not reachable after reopen/GC")
	}
	if got := textV2ReadRootBytes2624(t, reopened, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(1)); !bytes.Equal(got, docMapBefore) {
		t.Fatalf("docmap block not reachable after reopen/GC")
	}
	if got := textV2ReadRootBytes2624(t, reopened, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2CorpusStatsKey()); !bytes.Equal(got, termsBefore) {
		t.Fatalf("terms corpus not reachable after reopen/GC")
	}
}

func BenchmarkTextV2NormBlockCodec2624(b *testing.B) {
	block := textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 3, Entries: make([]textV2NormBlockEntry, 0, textV2DefaultNormBlockSize)}
	for i := uint64(0); i < uint64(textV2DefaultNormBlockSize); i++ {
		block.Entries = append(block.Entries, textV2NormBlockEntry{Ordinal: i + 1, Generation: 1, FieldLengths: []uint32{uint32(i % 17), uint32(i % 23), uint32(i % 31)}})
	}
	encoded := encodeTextV2NormBlockValue(block)
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "bytes/block")
		for i := 0; i < b.N; i++ {
			_ = encodeTextV2NormBlockValue(block)
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "bytes/block")
		for i := 0; i < b.N; i++ {
			if _, err := decodeTextV2NormBlockValue(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTextV2DocIDMappingCodec2624(b *testing.B) {
	docID := []byte("doc-000123")
	docValue := textV2DocIDValue{Ordinal: 123, Generation: 7}
	docMap := textV2DocMapBlockValue{BlockStart: 1, BlockSize: textV2DefaultDocMapBlockSize, Entries: make([]textV2DocMapEntry, 0, textV2DefaultDocMapBlockSize)}
	for i := uint64(0); i < uint64(textV2DefaultDocMapBlockSize); i++ {
		docMap.Entries = append(docMap.Entries, textV2DocMapEntry{Ordinal: i + 1, Generation: 1, DocumentID: []byte(fmt.Sprintf("doc-%06d", i+1))})
	}
	encodedDocMap := encodeTextV2DocMapBlockValue(docMap)
	b.Run("docid_key_value_encode_decode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encodeTextV2DocIDKey(docID))+len(encodeTextV2DocIDValue(docValue))), "bytes/docid_pair")
		for i := 0; i < b.N; i++ {
			key := encodeTextV2DocIDKey(docID)
			if _, err := decodeTextV2DocIDKey(key); err != nil {
				b.Fatal(err)
			}
			value := encodeTextV2DocIDValue(docValue)
			if _, err := decodeTextV2DocIDValue(value); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("docmap_block_decode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encodedDocMap)), "bytes/docmap_block")
		for i := 0; i < b.N; i++ {
			if _, err := decodeTextV2DocMapBlockValue(encodedDocMap); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func openTextV2TestDB(t testing.TB, dir string, forcePointers bool) *backenddb.DB {
	t.Helper()
	opts := backenddb.Options{Dir: dir, DisableBackgroundPrune: true}
	if forcePointers {
		opts.ValueLog = backenddb.ValueLogOptions{PointerThreshold: 1, ForcePointers: true}
	}
	d, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return d
}

func requireCollectionRootDescriptor2624(t *testing.T, d *backenddb.DB, rootName string) uint64 {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	raw, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
	if err != nil {
		t.Fatalf("get root descriptor %q: %v", rootName, err)
	}
	if !ok {
		t.Fatalf("missing root descriptor %q", rootName)
	}
	rootID, err := decodeRootID(raw)
	if err != nil {
		t.Fatalf("decode root descriptor %q: %v", rootName, err)
	}
	return rootID
}

func deleteTextRootValue2624(t *testing.T, d *backenddb.DB, collection, rootName string, key []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	baseRoot := catalog.rootID(rootName)
	policy, err := collectionRootStoragePolicyForDB(d, catalog.meta, rootName)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("root policy: %v", err)
	}
	_ = snap.Close()
	table := newCollectionRunTable(1)
	table.DeleteSteal(bytes.Clone(key))
	table.Freeze()
	defer resetCollectionRunTable(table)
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	ordered := []backenddb.OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: iter, StoragePolicy: policy}}
	_, rootIDs, err := d.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("unexpected root ids %d", len(rootIDs))
		}
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{systemCollectionRootKey(rootName): encodeRootID(rootIDs[0])})
	})
	if err != nil {
		t.Fatalf("publish deleted root value: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
}

func clearTextRootDescriptor2624(t *testing.T, d *backenddb.DB, rootName string) {
	t.Helper()
	current := d.AcquireSnapshot()
	if current == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = current.Close() }()
	iter, err := buildSystemTargetIterator(current, map[string][]byte{systemCollectionRootKey(rootName): encodeRootID(0)})
	if err != nil {
		t.Fatalf("build cleared root descriptor %q: %v", rootName, err)
	}
	defer func() { _ = iter.Close() }()
	if _, err := d.PublishSystemRootIterator(iter); err != nil {
		t.Fatalf("publish cleared root descriptor %q: %v", rootName, err)
	}
}

func textV2DocIDRootValue(t *testing.T, snap *backenddb.Snapshot, catalog *collectionCatalog, collection, index string, documentID []byte) textV2DocIDValue {
	t.Helper()
	raw := textRootValue(t, snap, catalog, collectionTextV2DocIDRootName(collection, index), encodeTextV2DocIDKey(documentID))
	value, err := decodeTextV2DocIDValue(raw)
	if err != nil {
		t.Fatalf("decode text-v2 docid %q: %v", string(documentID), err)
	}
	return value
}

func textV2StatusRootValue(t *testing.T, snap *backenddb.Snapshot, catalog *collectionCatalog, collection, index string) textV2IndexStatusValue {
	t.Helper()
	raw := textRootValue(t, snap, catalog, collectionTextV2GenerationsRootName(collection, index), encodeTextV2StatusKey())
	value, err := decodeTextV2IndexStatusValue(raw)
	if err != nil {
		t.Fatalf("decode text-v2 status: %v", err)
	}
	return value
}

func textV2DocMapRootBlock(t *testing.T, snap *backenddb.Snapshot, catalog *collectionCatalog, collection, index string, blockStart uint64) textV2DocMapBlockValue {
	t.Helper()
	raw := textRootValue(t, snap, catalog, collectionTextV2DocMapRootName(collection, index), encodeTextV2BlockKey(blockStart))
	value, err := decodeTextV2DocMapBlockValue(raw)
	if err != nil {
		t.Fatalf("decode text-v2 docmap block: %v", err)
	}
	return value
}

func textV2NormRootBlock(t *testing.T, snap *backenddb.Snapshot, catalog *collectionCatalog, collection, index string, blockStart uint64) textV2NormBlockValue {
	t.Helper()
	raw := textRootValue(t, snap, catalog, collectionTextV2NormBlocksRootName(collection, index), encodeTextV2BlockKey(blockStart))
	value, err := decodeTextV2NormBlockValue(raw)
	if err != nil {
		t.Fatalf("decode text-v2 norm block: %v", err)
	}
	return value
}

func textV2ReadNormBlockBytes2624(t *testing.T, d *backenddb.DB, collection, index string, blockStart uint64) []byte {
	t.Helper()
	return textV2ReadRootBytes2624(t, d, collection, collectionTextV2NormBlocksRootName(collection, index), encodeTextV2BlockKey(blockStart))
}

func textV2ReadRootBytes2624(t *testing.T, d *backenddb.DB, collection, rootName string, key []byte) []byte {
	t.Helper()
	var out []byte
	withTextCatalog(t, d, collection, func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		out = textRootValue(t, snap, catalog, rootName, key)
	})
	return out
}

func mustAnalyzeTextV2Doc2624(t *testing.T, def TextIndexDefinition, document []byte) textAnalyzedDocument {
	t.Helper()
	analysis, err := analyzeTextIndexDocument(def, document)
	if err != nil {
		t.Fatalf("analyze doc: %v", err)
	}
	return analysis
}
