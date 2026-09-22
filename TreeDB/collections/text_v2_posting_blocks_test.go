package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var textV2PostingBlockBenchSink2625 uint64

type textV2PostingParityPosting2625 struct {
	tf     uint32
	fields []uint32
}

func TestTextV2PostingBatchBuilderTransfersCompletedFieldFrequencies4566(t *testing.T) {
	def := TextIndexDefinition{Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}}
	sharedTitle := &textAnalyzedTerm{Term: "shared", Frequency: 2}
	sharedBody := &textAnalyzedTerm{Term: "shared", Frequency: 3}
	analysis := textAnalyzedDocument{Fields: []textAnalyzedField{
		{Field: "title", Terms: map[string]*textAnalyzedTerm{"shared": sharedTitle}},
		{Field: "body", Terms: map[string]*textAnalyzedTerm{"shared": sharedBody, "body-only": {Term: "body-only", Frequency: 1}}},
	}}
	var builder textV2PostingBatchBuilder
	if err := builder.addDocument(def, 7, 11, analysis); err != nil {
		t.Fatalf("add document: %v", err)
	}
	sharedTitle.Frequency = 99
	sharedBody.Frequency = 99

	shared := builder.byTerm["shared"]
	if len(shared) != 1 || shared[0].Ordinal != 7 || shared[0].Generation != 11 || shared[0].TermFrequency != 5 || !slices.Equal(shared[0].FieldFrequencies, []uint32{2, 3}) {
		t.Fatalf("shared postings=%+v", shared)
	}
	bodyOnly := builder.byTerm["body-only"]
	if len(bodyOnly) != 1 || bodyOnly[0].TermFrequency != 1 || !slices.Equal(bodyOnly[0].FieldFrequencies, []uint32{0, 1}) {
		t.Fatalf("body-only postings=%+v", bodyOnly)
	}
}

func TestTextV2PreparedStateWritersMatchAnalysis4592(t *testing.T) {
	def := TextIndexDefinition{
		Fields:         []TextIndexField{{Field: "body"}, {Field: "title"}},
		StorePositions: true,
		StoreOffsets:   true,
	}
	analysis := textAnalyzedDocument{Fields: []textAnalyzedField{
		{Field: "body", Length: 3, Terms: map[string]*textAnalyzedTerm{
			"body-only": {Term: "body-only", Frequency: 1, Positions: []uint32{2}, Offsets: []textTokenOffset{{Start: 8, End: 12}}},
			"shared":    {Term: "shared", Frequency: 2, Positions: []uint32{0, 1}, Offsets: []textTokenOffset{{Start: 0, End: 3}, {Start: 4, End: 7}}},
		}},
		{Field: "title", Length: 1, Terms: map[string]*textAnalyzedTerm{
			"shared": {Term: "shared", Frequency: 1, Positions: []uint32{0}, Offsets: []textTokenOffset{{Start: 0, End: 3}}},
		}},
	}}
	state := textDocumentStateValueFromAnalysis(analysis)

	var analysisBuilder, stateBuilder textV2PostingBatchBuilder
	if err := analysisBuilder.addDocument(def, 7, 11, analysis); err != nil {
		t.Fatalf("add analysis document: %v", err)
	}
	if err := stateBuilder.addDocumentState(def, 7, 11, state); err != nil {
		t.Fatalf("add state document: %v", err)
	}
	if !reflect.DeepEqual(stateBuilder, analysisBuilder) {
		t.Fatalf("state postings=%+v want analysis postings=%+v", stateBuilder, analysisBuilder)
	}

	analysisTable := newCollectionRunTable(2)
	stateTable := newCollectionRunTable(2)
	analysisEntries, analysisBytes, err := addTextV2PositionEntriesForDocument(analysisTable, def, 7, 11, analysis)
	if err != nil {
		t.Fatalf("add analysis positions: %v", err)
	}
	stateEntries, stateBytes, err := addTextV2PositionEntriesForState(stateTable, def, 7, 11, state)
	if err != nil {
		t.Fatalf("add state positions: %v", err)
	}
	if stateEntries != analysisEntries || stateBytes != analysisBytes {
		t.Fatalf("state positions entries=%d bytes=%d want entries=%d bytes=%d", stateEntries, stateBytes, analysisEntries, analysisBytes)
	}
	for _, term := range []string{"body-only", "shared"} {
		key := encodeTextV2PositionKey(7, term)
		got, gotOK, gotDeleted := stateTable.Get(key)
		want, wantOK, wantDeleted := analysisTable.Get(key)
		if gotOK != wantOK || gotDeleted != wantDeleted || !bytes.Equal(got, want) {
			t.Fatalf("state position %q=(%x,%v,%v) want (%x,%v,%v)", term, got, gotOK, gotDeleted, want, wantOK, wantDeleted)
		}
	}
}

func TestTextV2PostingBlockCodecRoundTripAndFailClosed2625(t *testing.T) {
	for _, term := range []string{"", strings.Repeat("term", 512)} {
		key := encodeTextV2PostingBlockKey(term, 1, 7)
		decoded, err := decodeTextV2PostingBlockKey(key)
		if err != nil {
			t.Fatalf("decode key term len=%d: %v", len(term), err)
		}
		if decoded.Term != term || decoded.BlockStart != 1 || decoded.BlockID != 7 {
			t.Fatalf("decoded key=%+v want term len=%d start=1 id=7", decoded, len(term))
		}
		prefix := encodeTextV2PostingBlockTermPrefix(term)
		decoded, err = decodeTextV2PostingBlockKeyForPrefix(key, prefix)
		if err != nil || decoded.Term != term {
			t.Fatalf("decode key for prefix=%+v err=%v", decoded, err)
		}
		blockStart, blockID, err := decodeTextV2PostingBlockKeySuffixForPrefix(key, prefix)
		if err != nil || blockStart != 1 || blockID != 7 {
			t.Fatalf("decode key suffix start=%d id=%d err=%v", blockStart, blockID, err)
		}
	}
	if _, err := decodeTextV2PostingBlockKey([]byte{textV2KeyVersion, textV2KeyKindPostingBlock, 0}); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("short key err=%v want ErrTextIndexStorageCorrupt", err)
	}
	if _, err := decodeTextV2PostingBlockKey(encodeTextV2PostingBlockKey("refund", 0, 1)); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("zero blockStart err=%v want ErrTextIndexStorageCorrupt", err)
	}

	entries := []textV2PostingBlockEntry{
		{Ordinal: 1, Generation: 1, TermFrequency: 3, FieldFrequencies: []uint32{2, 1}},
		{Ordinal: 1<<40 + 9, Generation: 7, TermFrequency: math.MaxUint32, FieldFrequencies: []uint32{math.MaxUint32, 0}},
		{Ordinal: 1<<40 + 29, Generation: 8, TermFrequency: 5, FieldFrequencies: []uint32{2, 3}},
	}
	for _, kind := range []textV2PostingBlockKind{textV2PostingBlockKindSealed, textV2PostingBlockKindDelta, textV2PostingBlockKindMicro} {
		block, err := newTextV2PostingBlockValue(kind, entries, 2, 5)
		if err != nil {
			t.Fatalf("new block kind=%s: %v", kind, err)
		}
		encoded := encodeTextV2PostingBlockValue(block)
		decoded, err := decodeTextV2PostingBlockValue(encoded)
		if err != nil {
			t.Fatalf("decode kind=%s: %v", kind, err)
		}
		if decoded.Kind != kind || decoded.BlockStart != 1 || decoded.BlockID != 5 || decoded.Summary.DocCount != 3 || decoded.Summary.MaxTermFrequency != math.MaxUint32 || !slices.Equal(decoded.Summary.MaxFieldTermFrequencies, []uint32{math.MaxUint32, 3}) {
			t.Fatalf("decoded block=%+v", decoded)
		}
		if len(decoded.Entries) != len(entries) || decoded.Entries[1].Ordinal != entries[1].Ordinal || !slices.Equal(decoded.Entries[2].FieldFrequencies, []uint32{2, 3}) {
			t.Fatalf("decoded entries=%+v want %+v", decoded.Entries, entries)
		}
	}

	block, err := newTextV2PostingBlockValue(textV2PostingBlockKindSealed, entries, 2, 5)
	if err != nil {
		t.Fatalf("new block: %v", err)
	}
	encoded := encodeTextV2PostingBlockValue(block)
	malformed := []struct {
		name string
		raw  []byte
	}{
		{name: "value version", raw: mutatePostingBlockRaw2625(encoded, 0, 99)},
		{name: "format version", raw: mutatePostingBlockRaw2625(encoded, 1, 99)},
		{name: "kind", raw: mutatePostingBlockRaw2625(encoded, 2, 99)},
		{name: "value flags", raw: mutatePostingBlockRaw2625(encoded, 3, 1)},
		{name: "summary doc count", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue { b := block; b.Summary.DocCount = 2; return b }())},
		{name: "summary max tf", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue { b := block; b.Summary.MaxTermFrequency = 3; return b }())},
		{name: "summary field max", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue {
			b := block
			b.Summary.MaxFieldTermFrequencies = []uint32{2, 3}
			return b
		}())},
		{name: "unsupported upper bound", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue { b := block; b.Summary.UpperBoundKind = 99; return b }())},
		{name: "entry payload too short", raw: encodeTextV2PostingBlockHeaderWithoutEntries2728()},
		{name: "entry flags", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue {
			b := block
			b.Entries = cloneTextV2PostingBlockEntries(block.Entries)
			b.Entries[0].Flags = 1
			return b
		}())},
		{name: "field sum mismatch", raw: encodeTextV2PostingBlockValue(func() textV2PostingBlockValue {
			b := block
			b.Entries = cloneTextV2PostingBlockEntries(block.Entries)
			b.Entries[0].FieldFrequencies[0] = 1
			return b
		}())},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeTextV2PostingBlockValue(tc.raw)
			if !errors.Is(err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("err=%v want ErrTextIndexStorageCorrupt", err)
			}
		})
	}
}

func TestTextV2PostingBlockScannerScratchReuseAllocGuard2876(t *testing.T) {
	blocks, err := buildTextV2PostingBlockKVs("refund", syntheticTextV2PostingEntries2625(int(textV2PostingBlockTargetPostings), 2), 2, textV2PostingBlockBuildOptions{})
	if err != nil {
		t.Fatalf("build blocks: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("blocks=%d want 1", len(blocks))
	}
	var scratch []uint32
	var scanner textV2PostingBlockEntryScanner
	scan := func() uint64 {
		s, err := initTextV2PostingBlockEntryScanner(&scanner, blocks[0].Value, scratch)
		if err != nil {
			panic(err)
		}
		var sum uint64
		var entry textV2PostingBlockEntry
		for s.Next(&entry) {
			sum += uint64(entry.TermFrequency)
		}
		if err := s.Err(); err != nil {
			panic(err)
		}
		scratch = s.scratch
		return sum
	}
	if sum := scan(); sum == 0 {
		t.Fatal("warm scan produced zero term frequency")
	}
	var sink uint64
	allocs := testing.AllocsPerRun(100, func() {
		sink += scan()
	})
	if sink == 0 {
		t.Fatal("scan sink stayed zero")
	}
	if allocs != 0 {
		t.Fatalf("scanner warm scratch allocs/run=%v want 0", allocs)
	}
}

func TestTextV2PostingBlockBuilderRejectsDuplicateOrdinalAcrossDefaultBoundary2625(t *testing.T) {
	entries := syntheticTextV2PostingEntries2625(int(textV2PostingBlockTargetPostings)+1, 2)
	entries[textV2PostingBlockTargetPostings].Ordinal = uint64(textV2PostingBlockTargetPostings)
	_, err := buildTextV2PostingBlockKVs("refund", entries, 2, textV2PostingBlockBuildOptions{})
	if !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("duplicate boundary ordinal err=%v want ErrTextIndexStorageCorrupt", err)
	}
}

func TestTextV2PostingBlockIteratorOrderAcrossTerms2625(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2PostingBlockCollection2625(t, d, 4, RootStorageFast)
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	refundEntries := []textV2PostingBlockEntry{
		{Ordinal: 1, Generation: 1, TermFrequency: 2, FieldFrequencies: []uint32{2, 0}},
		{Ordinal: 2, Generation: 1, TermFrequency: 1, FieldFrequencies: []uint32{1, 0}},
		{Ordinal: 4, Generation: 1, TermFrequency: 3, FieldFrequencies: []uint32{3, 0}},
	}
	refundBlocks, err := buildTextV2PostingBlockKVs("refund", refundEntries, 2, textV2PostingBlockBuildOptions{TargetPostings: 1})
	if err != nil {
		t.Fatalf("build refund blocks: %v", err)
	}
	shippingBlocks, err := buildTextV2PostingBlockKVs("shipping", []textV2PostingBlockEntry{{Ordinal: 3, Generation: 1, TermFrequency: 1, FieldFrequencies: []uint32{1, 0}}}, 2, textV2PostingBlockBuildOptions{})
	if err != nil {
		t.Fatalf("build shipping blocks: %v", err)
	}
	for _, kv := range append(refundBlocks, shippingBlocks...) {
		corruptTextRootValue(t, d, "docs", rootName, kv.Key, kv.Value)
	}

	var blockStarts []uint64
	var ordinals []uint64
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		err = scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "refund", func(key textV2PostingBlockKey, summary textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
			blockStarts = append(blockStarts, key.BlockStart)
			if key.Term != "refund" || summary.FirstOrdinal != key.BlockStart || summary.DocCount != 1 {
				return fmt.Errorf("bad block key=%+v summary=%+v", key, summary)
			}
			var entry textV2PostingBlockEntry
			for scanner.Next(&entry) {
				ordinals = append(ordinals, entry.Ordinal)
			}
			return scanner.Err()
		})
	})
	if err != nil {
		t.Fatalf("scan refund: %v", err)
	}
	if !slices.Equal(blockStarts, []uint64{1, 2, 4}) || !slices.Equal(ordinals, []uint64{1, 2, 4}) {
		t.Fatalf("blockStarts=%v ordinals=%v want refund blocks only in ordinal order", blockStarts, ordinals)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.V2PostingBlocks < 4 {
		t.Fatalf("posting blocks=%d want at least manually inserted blocks", stats.V2PostingBlocks)
	}
}

func TestTextV2PostingBlocksReconstructV1EquivalentPostings2625(t *testing.T) {
	def := TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}}}
	docs := [][]byte{
		[]byte(`{"body":"refund refund policy","title":"Refund"}`),
		[]byte(`{"body":"shipping policy","title":"Policy"}`),
		[]byte(`{"body":"refund shipping refund","title":"Shipping refund"}`),
		[]byte(`{"body":"other","title":""}`),
	}
	want := make(map[string]map[uint64]textV2PostingParityPosting2625)
	byTerm := make(map[string][]textV2PostingBlockEntry)
	for i, doc := range docs {
		analysis, err := analyzeTextIndexDocument(def, doc)
		if err != nil {
			t.Fatalf("analyze doc %d: %v", i, err)
		}
		ordinal := uint64(i + 1)
		perDoc := make(map[string]*textV2PostingBlockEntry)
		for fieldIndex, field := range analysis.Fields {
			for term, analyzed := range field.Terms {
				entry := perDoc[term]
				if entry == nil {
					entry = &textV2PostingBlockEntry{Ordinal: ordinal, Generation: 1, FieldFrequencies: make([]uint32, len(def.Fields))}
					perDoc[term] = entry
				}
				entry.TermFrequency += analyzed.Frequency
				entry.FieldFrequencies[fieldIndex] += analyzed.Frequency
			}
		}
		for term, entry := range perDoc {
			byTerm[term] = append(byTerm[term], *entry)
			if want[term] == nil {
				want[term] = make(map[uint64]textV2PostingParityPosting2625)
			}
			want[term][ordinal] = textV2PostingParityPosting2625{tf: entry.TermFrequency, fields: append([]uint32(nil), entry.FieldFrequencies...)}
		}
	}
	for term, entries := range byTerm {
		blocks, err := buildTextV2PostingBlockKVs(term, entries, uint32(len(def.Fields)), textV2PostingBlockBuildOptions{TargetPostings: 2})
		if err != nil {
			t.Fatalf("build term %q: %v", term, err)
		}
		got := make(map[uint64]textV2PostingParityPosting2625)
		for _, kv := range blocks {
			scanner, err := newTextV2PostingBlockEntryScanner(kv.Value, nil)
			if err != nil {
				t.Fatalf("scanner term %q: %v", term, err)
			}
			var entry textV2PostingBlockEntry
			for scanner.Next(&entry) {
				got[entry.Ordinal] = textV2PostingParityPosting2625{tf: entry.TermFrequency, fields: append([]uint32(nil), entry.FieldFrequencies...)}
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("scan term %q: %v", term, err)
			}
		}
		if !postingParityEqual2625(got, want[term]) {
			t.Fatalf("term %q got=%+v want=%+v", term, got, want[term])
		}
	}
}

func TestTextV2PostingBlocksReachValueLogMaintenance2625(t *testing.T) {
	dir := t.TempDir()
	d, closeDB := openTextV2PostingBlockCompressedDB2625(t, dir)
	col := createTextV2PostingBlockCollection2625(t, d, 32, RootStorageCompressed)
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	blockKey := firstTextV2PostingBlockKeyForTerm2626(t, d, "docs", rootName, "refund")
	before := textV2ReadRootBytes2624(t, d, "docs", rootName, blockKey)
	if stats, err := col.TextIndexStorageStats("lexical"); err != nil || stats.V2PostingBlocks == 0 {
		t.Fatalf("TextIndexStorageStats before GC=%+v err=%v want emitted posting blocks", stats, err)
	}
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if after := textV2ReadRootBytes2624(t, d, "docs", rootName, blockKey); !bytes.Equal(after, before) {
		t.Fatalf("posting block changed after ValueLogGC")
	}
	if err := closeDB(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, closeReopened := openTextV2PostingBlockCompressedDB2625(t, dir)
	defer func() { _ = closeReopened() }()
	if after := textV2ReadRootBytes2624(t, reopened, "docs", rootName, blockKey); !bytes.Equal(after, before) {
		t.Fatalf("posting block not reachable after reopen/GC")
	}
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	stats, err := reopenedCol.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("reopened TextIndexStorageStats: %v", err)
	}
	if stats.V2PostingBlocks == 0 {
		t.Fatalf("reopened posting blocks=%d want emitted blocks", stats.V2PostingBlocks)
	}
}

func firstTextV2PostingBlockKeyForTerm2626(t *testing.T, d *backenddb.DB, collection, rootName, term string) []byte {
	t.Helper()
	stop := errors.New("posting block located")
	var blockKey []byte
	var scanErr error
	withTextCatalog(t, d, collection, func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		scanErr = scanTextV2PostingBlocksForTerm(snap, catalog, rootName, term, func(key textV2PostingBlockKey, _ textV2PostingBlockSummary, _ *textV2PostingBlockEntryScanner) error {
			blockKey = encodeTextV2PostingBlockKey(key.Term, key.BlockStart, key.BlockID)
			return stop
		})
	})
	if scanErr != nil && !errors.Is(scanErr, stop) {
		t.Fatalf("scan posting blocks for %q: %v", term, scanErr)
	}
	if len(blockKey) == 0 {
		t.Fatalf("posting block for %q not found", term)
	}
	return blockKey
}

func TestTextV2PostingBlocksEmittedByBackfillAndMutations2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2PostingBlockCollection2625(t, d, 2, RootStorageFast)
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after backfill: %v", err)
	}
	if stats.V2PostingBlocks == 0 {
		t.Fatalf("v2 posting blocks after M3 backfill=%d want emitted blocks", stats.V2PostingBlocks)
	}
	backfillBlocks := stats.V2PostingBlocks
	if _, _, err := col.Update([]byte("doc-000001"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund updated","title":"updated"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("doc-000002")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if _, err := col.Insert([]byte("doc-000003"), []byte(`{"body":"refund new","title":"new"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	stats, err = col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats after mutations: %v", err)
	}
	if stats.V2PostingBlocks <= backfillBlocks {
		t.Fatalf("v2 posting blocks after M3 mutations=%d want > backfill %d", stats.V2PostingBlocks, backfillBlocks)
	}
}

func TestTextV2WritePathLiteralBackslashUnindexedRootKeyDoesNotAbort2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.Insert([]byte("backfill"), []byte(`{"a\\q":"ignored","body":"refund backfill"}`)); err != nil {
		t.Fatalf("Insert before backfill: %v", err)
	}
	if _, backfill, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex with literal-backslash unindexed key: %v", err)
	} else if backfill.DocumentsScanned != 1 || backfill.V2PostingBlocks == 0 {
		t.Fatalf("backfill=%+v want one scanned document and v2 posting blocks", backfill)
	}
	if _, err := col.Insert([]byte("write"), []byte(`{"a\\q":"ignored","body":"refund write"}`)); err != nil {
		t.Fatalf("Insert after v2 index with literal-backslash unindexed key: %v", err)
	}
	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	if stats.Documents != 2 || stats.V2PostingBlocks == 0 {
		t.Fatalf("stats=%+v want two indexed documents and v2 posting blocks", stats)
	}
}

func TestTextV2WritePathBackfillPostingParity2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}
	docs := [][]byte{
		[]byte(`{"body":"refund refund policy","title":"Refund"}`),
		[]byte(`{"body":"shipping policy","title":"Policy"}`),
		[]byte(`{"body":"refund shipping refund","title":"Shipping refund"}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, backfill, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}, {Field: "title", Weight: 2}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	} else if backfill.V2PostingBlocks == 0 {
		t.Fatalf("backfill=%+v want emitted posting blocks", backfill)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	got := make(map[uint64]textV2PostingParityPosting2625)
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		err = scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "refund", func(key textV2PostingBlockKey, summary textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
			if key.Term != "refund" || summary.DocCount == 0 {
				return fmt.Errorf("bad refund block key=%+v summary=%+v", key, summary)
			}
			var entry textV2PostingBlockEntry
			for scanner.Next(&entry) {
				got[entry.Ordinal] = textV2PostingParityPosting2625{tf: entry.TermFrequency, fields: append([]uint32(nil), entry.FieldFrequencies...)}
			}
			return scanner.Err()
		})
		termRaw := textRootValue(t, snap, catalog, collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2TermStatsKey("refund"))
		term, decodeErr := decodeTextV2TermStatsValue(termRaw)
		if decodeErr != nil {
			t.Fatalf("decode refund term: %v", decodeErr)
		}
		if term.DocumentFrequency != 2 || term.TotalTermFrequency != 6 || term.PostingBlockCount == 0 {
			t.Fatalf("refund term=%+v want df=2 tf=6 block count", term)
		}
	})
	if err != nil {
		t.Fatalf("scan refund: %v", err)
	}
	want := map[uint64]textV2PostingParityPosting2625{
		1: {tf: 3, fields: []uint32{2, 1}},
		3: {tf: 3, fields: []uint32{2, 1}},
	}
	if !postingParityEqual2625(got, want) {
		t.Fatalf("refund postings=%+v want %+v", got, want)
	}
}

func TestTextV2WritePathMutationsAppendMicroBlocksAndReopen2626(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2PostingBlockDB2625(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"refund shipping"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	insertStats, err := col.TextIndexStorageStats("lexical")
	if err != nil || insertStats.V2PostingBlocks == 0 {
		t.Fatalf("stats after insert=%+v err=%v want posting blocks", insertStats, err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund updated refund"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2PostingBlockDB2625(t, dir, false)
	defer func() { _ = reopened.Close() }()
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	var generations []uint64
	withTextCatalog(t, reopened, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		d1 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d1"))
		d2 := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d2"))
		if d1.Ordinal != 1 || d1.Generation != 2 || d1.tombstoned() {
			t.Fatalf("d1=%+v want generation 2 live", d1)
		}
		if d2.Ordinal != 2 || d2.Generation != 2 || !d2.tombstoned() {
			t.Fatalf("d2=%+v want generation 2 tombstone", d2)
		}
		err = scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "refund", func(key textV2PostingBlockKey, summary textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
			if key.BlockID < 2 || summary.DocCount == 0 {
				return fmt.Errorf("bad mutation block key=%+v summary=%+v", key, summary)
			}
			var entry textV2PostingBlockEntry
			for scanner.Next(&entry) {
				if entry.Ordinal == 1 {
					generations = append(generations, entry.Generation)
				}
			}
			return scanner.Err()
		})
	})
	if err != nil {
		t.Fatalf("scan refund after reopen: %v", err)
	}
	slices.Sort(generations)
	if !slices.Equal(generations, []uint64{1, 2}) {
		t.Fatalf("d1 refund generations=%v want stale+current generations", generations)
	}
}

func TestTextV2WritePathMutationBlockIDsDoNotOverwriteSealedBlocks2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	docs := int(textV2PostingBlockTargetPostings) + 1
	ids := make([][]byte, docs)
	values := make([][]byte, docs)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i+1))
		values[i] = []byte(`{"body":"refund"}`)
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	secondBlockStart := uint64(textV2PostingBlockTargetPostings) + 1
	sealedKey := encodeTextV2PostingBlockKey("refund", secondBlockStart, 2)
	sealedBefore := textV2ReadRootBytes2624(t, d, "docs", rootName, sealedKey)
	if _, _, err := col.Update(ids[textV2PostingBlockTargetPostings], func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund refund changed"}`), true, nil
	}); err != nil {
		t.Fatalf("Update second block start doc: %v", err)
	}
	sealedAfter := textV2ReadRootBytes2624(t, d, "docs", rootName, sealedKey)
	if !bytes.Equal(sealedBefore, sealedAfter) {
		t.Fatalf("sealed posting block at second block start was overwritten by mutation")
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	var refundBlocks int
	var sawSealedSecondBlock bool
	var sawMutationSecondBlock bool
	if err := scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "refund", func(key textV2PostingBlockKey, _ textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
		refundBlocks++
		if key.BlockStart == secondBlockStart && key.BlockID == 2 {
			sawSealedSecondBlock = true
		}
		if key.BlockStart == secondBlockStart && key.BlockID >= textV2PostingBlockMutationIDBase {
			sawMutationSecondBlock = true
		}
		return nil
	}); err != nil {
		t.Fatalf("scan refund posting blocks: %v", err)
	}
	if !sawSealedSecondBlock || !sawMutationSecondBlock {
		t.Fatalf("second block start sealed=%v mutation=%v refundBlocks=%d", sawSealedSecondBlock, sawMutationSecondBlock, refundBlocks)
	}
	termStatsRaw := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2TermStatsKey("refund"))
	termStats, err := decodeTextV2TermStatsValue(termStatsRaw)
	if err != nil {
		t.Fatalf("decode refund term stats: %v", err)
	}
	if termStats.PostingBlockCount != uint64(refundBlocks) {
		t.Fatalf("refund posting block count=%d want scanned blocks %d", termStats.PostingBlockCount, refundBlocks)
	}
}

func TestTextV2WritePathMutationMicroBlockIDsDoNotOverwritePriorMicroChunks2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	docs := int(2*textV2PostingBlockMicroPostings) + 1
	ids := make([][]byte, docs)
	values := make([][]byte, docs)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i+1))
		values[i] = []byte(`{"body":"refund"}`)
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	secondMicroStart := uint64(textV2PostingBlockMicroPostings) + 1
	firstMutationBlockID, err := textV2PostingBlockMutationBlockIDStart(2)
	if err != nil {
		t.Fatalf("first mutation block id: %v", err)
	}
	priorMicroKey := encodeTextV2PostingBlockKey("refund", secondMicroStart, firstMutationBlockID)
	priorMicroBefore := textV2ReadRootBytes2624(t, d, "docs", rootName, priorMicroKey)
	if _, _, err := col.Update(ids[textV2PostingBlockMicroPostings], func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund refund changed"}`), true, nil
	}); err != nil {
		t.Fatalf("Update second micro chunk start doc: %v", err)
	}
	priorMicroAfter := textV2ReadRootBytes2624(t, d, "docs", rootName, priorMicroKey)
	if !bytes.Equal(priorMicroBefore, priorMicroAfter) {
		t.Fatalf("prior micro posting block at second chunk start was overwritten by later mutation")
	}
	secondMutationBlockID, err := textV2PostingBlockMutationBlockIDStart(3)
	if err != nil {
		t.Fatalf("second mutation block id: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	var refundBlocks int
	var sawPriorMicro bool
	var sawLaterMicro bool
	if err := scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "refund", func(key textV2PostingBlockKey, _ textV2PostingBlockSummary, scanner *textV2PostingBlockEntryScanner) error {
		refundBlocks++
		if key.BlockStart == secondMicroStart && key.BlockID == firstMutationBlockID {
			sawPriorMicro = true
		}
		if key.BlockStart == secondMicroStart && key.BlockID == secondMutationBlockID {
			sawLaterMicro = true
		}
		return nil
	}); err != nil {
		t.Fatalf("scan refund posting blocks: %v", err)
	}
	if !sawPriorMicro || !sawLaterMicro {
		t.Fatalf("second micro start prior=%v later=%v refundBlocks=%d", sawPriorMicro, sawLaterMicro, refundBlocks)
	}
	termStatsRaw := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2TermsRootName("docs", "lexical"), encodeTextV2TermStatsKey("refund"))
	termStats, err := decodeTextV2TermStatsValue(termStatsRaw)
	if err != nil {
		t.Fatalf("decode refund term stats: %v", err)
	}
	if termStats.PostingBlockCount != uint64(refundBlocks) {
		t.Fatalf("refund posting block count=%d want scanned blocks %d", termStats.PostingBlockCount, refundBlocks)
	}
}

func TestTextV2WritePathSnapshotVisibility2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog before update: %v", err)
	}
	before, err := inspectTextV2IndexStorage(snap, catalog, def)
	if err != nil {
		t.Fatalf("inspect before: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"refund updated"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	after, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("stats after: %v", err)
	}
	oldView, err := inspectTextV2IndexStorage(snap, catalog, def)
	if err != nil {
		t.Fatalf("inspect old snapshot: %v", err)
	}
	if oldView.V2PostingBlocks != before.V2PostingBlocks || after.V2PostingBlocks <= before.V2PostingBlocks {
		t.Fatalf("snapshot before=%+v old=%+v after=%+v", before, oldView, after)
	}
	var updatedBlocks int
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	if err := scanTextV2PostingBlocksForTerm(snap, catalog, rootName, "updated", func(textV2PostingBlockKey, textV2PostingBlockSummary, *textV2PostingBlockEntryScanner) error {
		updatedBlocks++
		return nil
	}); err != nil {
		t.Fatalf("scan old updated term: %v", err)
	}
	if updatedBlocks != 0 {
		t.Fatalf("old snapshot saw %d updated-term posting blocks", updatedBlocks)
	}
}

func TestTextV2WritePathDeleteSnapshotVisibility2626(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	def := TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		t.Fatalf("load catalog before delete: %v", err)
	}
	before, err := inspectTextV2IndexStorage(snap, catalog, def)
	if err != nil {
		t.Fatalf("inspect before: %v", err)
	}
	beforeDoc := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d1"))
	if beforeDoc.Ordinal != 1 || beforeDoc.Generation != 1 || beforeDoc.tombstoned() {
		t.Fatalf("snapshot before doc=%+v want live generation 1", beforeDoc)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d1")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	after, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("stats after delete: %v", err)
	}
	oldView, err := inspectTextV2IndexStorage(snap, catalog, def)
	if err != nil {
		t.Fatalf("inspect old snapshot after delete: %v", err)
	}
	oldDoc := textV2DocIDRootValue(t, snap, catalog, "docs", "lexical", []byte("d1"))
	if oldView.V2LiveDocuments != before.V2LiveDocuments || oldView.V2DeletedDocs != before.V2DeletedDocs || oldDoc.tombstoned() {
		t.Fatalf("old snapshot before=%+v old=%+v oldDoc=%+v", before, oldView, oldDoc)
	}
	if after.V2LiveDocuments != 0 || after.V2DeletedDocs != 1 || after.V2PostingBlocks != before.V2PostingBlocks {
		t.Fatalf("after delete stats=%+v before=%+v want tombstone without posting rewrite", after, before)
	}
	withTextCatalog(t, d, "docs", func(current *backenddb.Snapshot, currentCatalog *collectionCatalog) {
		currentDoc := textV2DocIDRootValue(t, current, currentCatalog, "docs", "lexical", []byte("d1"))
		if currentDoc.Ordinal != 1 || currentDoc.Generation != 2 || !currentDoc.tombstoned() {
			t.Fatalf("current doc=%+v want tombstone generation 2", currentDoc)
		}
	})
}

func TestTextV2WritePathPostingBlocksGCCompactReopen2626(t *testing.T) {
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
	ids := make([][]byte, 48)
	docs := make([][]byte, 48)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%03d", i+1))
		docs[i] = []byte(fmt.Sprintf(`{"body":"refund refund policy common common %d","title":"ticket %d"}`, i, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}, {Field: "title"}}, StoragePolicy: RootStorageFast}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	blockKey := firstTextV2PostingBlockKeyForTerm2626(t, d, "docs", rootName, "refund")
	postingBefore := textV2ReadRootBytes2624(t, d, "docs", rootName, blockKey)
	normBefore := textV2ReadNormBlockBytes2624(t, d, "docs", "lexical", 1)
	docMapBefore := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(1))
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if _, err := d.CompactStorage(context.Background(), backenddb.CompactStorageOptions{}); err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if got := textV2ReadRootBytes2624(t, d, "docs", rootName, blockKey); !bytes.Equal(got, postingBefore) {
		t.Fatalf("posting block changed after GC/compact")
	}
	if got := textV2ReadNormBlockBytes2624(t, d, "docs", "lexical", 1); !bytes.Equal(got, normBefore) {
		t.Fatalf("norm block changed after GC/compact")
	}
	if got := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2DocMapRootName("docs", "lexical"), encodeTextV2BlockKey(1)); !bytes.Equal(got, docMapBefore) {
		t.Fatalf("docmap block changed after GC/compact")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened := openTextV2TestDB(t, dir, true)
	defer func() { _ = reopened.Close() }()
	if got := textV2ReadRootBytes2624(t, reopened, "docs", rootName, blockKey); !bytes.Equal(got, postingBefore) {
		t.Fatalf("posting block not reachable after reopen")
	}
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	if stats, err := reopenedCol.TextIndexStorageStats("lexical"); err != nil || stats.V2PostingBlocks == 0 || stats.V2NormBlocks == 0 || stats.V2DocMapBlocks == 0 {
		t.Fatalf("reopened stats=%+v err=%v want v2 roots", stats, err)
	}
}

func TestTextV2PostingBlockStorageFailsClosedOnFieldLaneMismatch2625(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2PostingBlockCollection2625(t, d, 2, RootStorageFast)
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	block, err := newTextV2PostingBlockValue(textV2PostingBlockKindSealed, []textV2PostingBlockEntry{{Ordinal: 1, Generation: 1, TermFrequency: 1, FieldFrequencies: []uint32{1}}}, 1, 1)
	if err != nil {
		t.Fatalf("new block: %v", err)
	}
	corruptTextRootValue(t, d, "docs", rootName, encodeTextV2PostingBlockKey("refund", block.BlockStart, block.BlockID), encodeTextV2PostingBlockValue(block))
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("field-lane mismatch stats err=%v want ErrTextIndexStorageCorrupt", err)
	}
}

func TestTextV2PostingBlockStorageFailsClosedOnCorruption2625(t *testing.T) {
	d := openTextV2PostingBlockDB2625(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextV2PostingBlockCollection2625(t, d, 2, RootStorageFast)
	rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
	block, err := newTextV2PostingBlockValue(textV2PostingBlockKindSealed, []textV2PostingBlockEntry{{Ordinal: 1, Generation: 1, TermFrequency: 1, FieldFrequencies: []uint32{1, 0}}}, 2, 1)
	if err != nil {
		t.Fatalf("new block: %v", err)
	}
	bad := block
	bad.Summary.MaxTermFrequency = 0
	corruptTextRootValue(t, d, "docs", rootName, encodeTextV2PostingBlockKey("refund", block.BlockStart, block.BlockID), encodeTextV2PostingBlockValue(bad))
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("corrupt posting block stats err=%v want ErrTextIndexStorageCorrupt", err)
	}
}

func BenchmarkTextV2PostingBlockCodec2625(b *testing.B) {
	entries := syntheticTextV2PostingEntries2625(128, 2)
	block, err := newTextV2PostingBlockValue(textV2PostingBlockKindSealed, entries, 2, 1)
	if err != nil {
		b.Fatalf("new block: %v", err)
	}
	encoded := encodeTextV2PostingBlockValue(block)
	b.Run("encode_128", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded))/float64(len(entries)), "bytes/posting")
		for i := 0; i < b.N; i++ {
			textV2PostingBlockBenchSink2625 += uint64(len(encodeTextV2PostingBlockValue(block)))
		}
	})
	b.Run("decode_128", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded))/float64(len(entries)), "bytes/posting")
		for i := 0; i < b.N; i++ {
			decoded, err := decodeTextV2PostingBlockValue(encoded)
			if err != nil {
				b.Fatal(err)
			}
			textV2PostingBlockBenchSink2625 += uint64(len(decoded.Entries))
		}
	})
	b.Run("scan_128_reused_entry", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded))/float64(len(entries)), "bytes/posting")
		for i := 0; i < b.N; i++ {
			scanner, err := newTextV2PostingBlockEntryScanner(encoded, nil)
			if err != nil {
				b.Fatal(err)
			}
			var entry textV2PostingBlockEntry
			var count uint64
			for scanner.Next(&entry) {
				count += uint64(entry.TermFrequency)
			}
			if err := scanner.Err(); err != nil {
				b.Fatal(err)
			}
			textV2PostingBlockBenchSink2625 += count
		}
	})
}

func BenchmarkTextV2PostingBlockRangeScanVsV1Synthetic2625(b *testing.B) {
	for _, docs := range []int{8, 10_000} {
		name := "rare"
		if docs > 100 {
			name = "common"
		}
		v1 := syntheticTextV1PostingRows2625("refund", docs)
		v2Blocks, err := buildTextV2PostingBlockKVs("refund", syntheticTextV2PostingEntries2625(docs, 2), 2, textV2PostingBlockBuildOptions{})
		if err != nil {
			b.Fatalf("build v2 docs=%d: %v", docs, err)
		}
		var v2Bytes int
		for _, kv := range v2Blocks {
			v2Bytes += len(kv.Key) + len(kv.Value)
		}
		b.Run(fmt.Sprintf("%s_%d/v1_per_doc_decode", name, docs), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(v1)), "postings/op")
			for i := 0; i < b.N; i++ {
				var sum uint64
				prefix := encodeTextPostingTermPrefix("refund")
				for _, row := range v1 {
					if _, err := decodeTextPostingKeyDocumentIDForPrefix(row.key, prefix); err != nil {
						b.Fatal(err)
					}
					posting, err := decodeTextPostingValueForSearch(row.value, []string{"body", "title"})
					if err != nil {
						b.Fatal(err)
					}
					sum += uint64(posting.TermFrequency)
				}
				textV2PostingBlockBenchSink2625 += sum
			}
		})
		b.Run(fmt.Sprintf("%s_%d/v2_block_scan", name, docs), func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(docs), "postings/op")
			b.ReportMetric(float64(len(v2Blocks)), "blocks/op")
			b.ReportMetric(float64(v2Bytes)/float64(docs), "bytes/posting")
			for i := 0; i < b.N; i++ {
				var sum uint64
				var scratch []uint32
				for _, kv := range v2Blocks {
					scanner, err := newTextV2PostingBlockEntryScanner(kv.Value, scratch)
					if err != nil {
						b.Fatal(err)
					}
					var entry textV2PostingBlockEntry
					for scanner.Next(&entry) {
						sum += uint64(entry.TermFrequency)
					}
					if err := scanner.Err(); err != nil {
						b.Fatal(err)
					}
					scratch = scanner.scratch
				}
				textV2PostingBlockBenchSink2625 += sum
			}
		})
	}
}

func BenchmarkTextV2PostingBlockEncodeUpdate2625(b *testing.B) {
	const docs = 100_000
	initial, err := buildTextV2PostingBlockKVs("refund", syntheticTextV2PostingEntries2625(docs, 2), 2, textV2PostingBlockBuildOptions{})
	if err != nil {
		b.Fatalf("build initial: %v", err)
	}
	var initialBytes int
	for _, kv := range initial {
		initialBytes += len(kv.Key) + len(kv.Value)
	}
	b.Run("build_sealed_100k", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(docs), "postings/op")
		b.ReportMetric(float64(len(initial)), "blocks_emitted/op")
		b.ReportMetric(float64(initialBytes)/float64(docs), "bytes/posting")
		for i := 0; i < b.N; i++ {
			blocks, err := buildTextV2PostingBlockKVs("refund", syntheticTextV2PostingEntries2625(docs, 2), 2, textV2PostingBlockBuildOptions{})
			if err != nil {
				b.Fatal(err)
			}
			textV2PostingBlockBenchSink2625 += uint64(len(blocks))
		}
	})
	b.Run("append_microblock_one_doc", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(1, "blocks_emitted/update")
		b.ReportMetric(1, "postings_reencoded/update")
		b.ReportMetric(float64(docs), "prior_df/not_reencoded")
		for i := 0; i < b.N; i++ {
			entry := textV2PostingBlockEntry{Ordinal: uint64(docs + 1 + i), Generation: 1, TermFrequency: 2, FieldFrequencies: []uint32{1, 1}}
			blocks, err := buildTextV2PostingBlockKVs("refund", []textV2PostingBlockEntry{entry}, 2, textV2PostingBlockBuildOptions{Kind: textV2PostingBlockKindMicro, TargetPostings: 1, BlockIDStart: uint64(i) + 1_000_000})
			if err != nil {
				b.Fatal(err)
			}
			textV2PostingBlockBenchSink2625 += uint64(len(blocks[0].Value))
		}
	})
}

func encodeTextV2PostingBlockHeaderWithoutEntries2728() []byte {
	out := []byte{textV2PostingBlockValueVersion}
	out = appendTextUvarint(out, uint64(textV2FormatVersion))
	out = append(out, byte(textV2PostingBlockKindSealed), 0)
	out = appendTextUvarint(out, 1) // block start
	out = appendTextUvarint(out, 1) // block id
	out = appendTextUvarint(out, 1) // first ordinal
	out = appendTextUvarint(out, 1) // last ordinal
	out = appendTextUvarint(out, 1) // doc count
	out = appendTextUvarint(out, 1) // max term frequency
	out = appendTextUvarint(out, 2) // field count
	out = appendTextUvarint(out, 1) // max field frequency[0]
	out = appendTextUvarint(out, 0) // max field frequency[1]
	out = append(out, textV2PostingUpperBoundKindBM25FLaneMax)
	out = appendTextUvarint(out, 1) // entry count, intentionally no entry payload follows.
	var checksum [textV2PostingBlockChecksumBytes]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(out))
	return append(out, checksum[:]...)
}

func mutatePostingBlockRaw2625(raw []byte, offset int, value byte) []byte {
	out := bytes.Clone(raw)
	out[offset] = value
	return out
}

func openTextV2PostingBlockDB2625(t *testing.T, dir string, forcePointers bool) *backenddb.DB {
	t.Helper()
	return openTextV2TestDB(t, dir, forcePointers)
}

func openTextV2PostingBlockCompressedDB2625(t *testing.T, dir string) (*backenddb.DB, func() error) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableBackgroundPrune = true
	opts.IndexOuterLeavesInValueLog = true
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open compressed db: %v", err)
	}
	return d, cleanup
}

func createTextV2PostingBlockCollection2625(t *testing.T, d *backenddb.DB, docs int, storage RootStoragePolicy) *Collection {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, docs)
	values := make([][]byte, docs)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i+1))
		values[i] = []byte(fmt.Sprintf(`{"body":"refund policy shipping %d","title":"ticket refund %d"}`, i, i))
	}
	if _, err := col.InsertBatch(ids, values); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}, {Field: "title"}}, StoragePolicy: storage}); err != nil {
		t.Fatalf("CreateTextIndex v2: %v", err)
	}
	return col
}

func postingParityEqual2625(a, b map[uint64]textV2PostingParityPosting2625) bool {
	if len(a) != len(b) {
		return false
	}
	for ordinal, av := range a {
		bv, ok := b[ordinal]
		if !ok || av.tf != bv.tf || !slices.Equal(av.fields, bv.fields) {
			return false
		}
	}
	return true
}

type textV1SyntheticPostingRow2625 struct {
	key   []byte
	value []byte
}

func syntheticTextV1PostingRows2625(term string, docs int) []textV1SyntheticPostingRow2625 {
	rows := make([]textV1SyntheticPostingRow2625, 0, docs)
	for i := 0; i < docs; i++ {
		docID := []byte(fmt.Sprintf("doc-%08d", i+1))
		body := uint32(1 + i%3)
		title := uint32(i % 2)
		rows = append(rows, textV1SyntheticPostingRow2625{
			key: encodeTextPostingKey(term, docID),
			value: encodeTextPostingValue(textPostingValue{TermFrequency: body + title, Fields: []textPostingFieldValue{
				{Field: "body", Frequency: body},
				{Field: "title", Frequency: title},
			}}),
		})
	}
	return rows
}

func syntheticTextV2PostingEntries2625(docs int, fieldCount uint32) []textV2PostingBlockEntry {
	entries := make([]textV2PostingBlockEntry, 0, docs)
	for i := 0; i < docs; i++ {
		fields := make([]uint32, fieldCount)
		var tf uint32
		for j := range fields {
			fields[j] = uint32((i+j)%3 + 1)
			tf += fields[j]
		}
		entries = append(entries, textV2PostingBlockEntry{Ordinal: uint64(i + 1), Generation: 1, TermFrequency: tf, FieldFrequencies: fields})
	}
	return entries
}
