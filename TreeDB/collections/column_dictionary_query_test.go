package collections

import (
	"testing"
)

// Tests for columnDictionaryCodeDistinctSeenWords (new utility function in column_dictionary_query.go).

func TestColumnDictionaryCodeDistinctSeenWordsZeroGroupsM1634(t *testing.T) {
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for 0 groups")
	}
	_ = wordsPerGroup
	_ = totalWords
}

func TestColumnDictionaryCodeDistinctSeenWordsZeroDistinctM1634(t *testing.T) {
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(10, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for 0 distinct values")
	}
	_ = wordsPerGroup
	_ = totalWords
}

func TestColumnDictionaryCodeDistinctSeenWordsSmallM1634(t *testing.T) {
	// Small groups and distinct: should succeed.
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(4, 64)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for small groups/distinct")
	}
	// 64 distinct values -> 1 word (64 bits).
	if wordsPerGroup != 1 {
		t.Fatalf("expected wordsPerGroup=1 for 64 distinct, got %d", wordsPerGroup)
	}
	if totalWords != 4*1 {
		t.Fatalf("expected totalWords=4, got %d", totalWords)
	}
}

func TestColumnDictionaryCodeDistinctSeenWordsPartialWordM1634(t *testing.T) {
	// 65 distinct values -> 2 words (needs rounding up).
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(2, 65)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if wordsPerGroup != 2 {
		t.Fatalf("expected wordsPerGroup=2 for 65 distinct, got %d", wordsPerGroup)
	}
	if totalWords != 2*2 {
		t.Fatalf("expected totalWords=4, got %d", totalWords)
	}
}

func TestColumnDictionaryCodeDistinctSeenWordsBelowMaxM1634(t *testing.T) {
	// Just below the max words limit should succeed.
	groups := 1
	distinct := 64 * columnDictionaryCodeDistinctMaxSeenWords
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(groups, distinct)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true when just at/below limit")
	}
	_ = wordsPerGroup
	_ = totalWords
}

func TestColumnDictionaryCodeDistinctSeenWordsExceedsMaxM1634(t *testing.T) {
	// Many groups with many distinct values should exceed the word limit.
	// columnDictionaryCodeDistinctMaxSeenWords = 1<<20 words
	// so: 2 groups * ((1<<20/2+1)*64) distinct -> 2 * ((1<<19+1)) words > max
	groups := 2
	// Make wordsPerGroup > max/2 so totalWords > max.
	distinct := (columnDictionaryCodeDistinctMaxSeenWords/2+1)*64 + 1
	wordsPerGroup, totalWords, ok, err := columnDictionaryCodeDistinctSeenWords(groups, distinct)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false when totalWords=%d exceeds max=%d (wordsPerGroup=%d)", totalWords, columnDictionaryCodeDistinctMaxSeenWords, wordsPerGroup)
	}
}

func TestColumnDictionaryCodeDistinctSeenWordsNoErrorM1634(t *testing.T) {
	// The function should never return non-nil error for normal inputs.
	_, _, _, err := columnDictionaryCodeDistinctSeenWords(1, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, _, _, err = columnDictionaryCodeDistinctSeenWords(100, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Tests for columnDictionaryCodeSnapshotBytes (utility function in column_dictionary_query.go).

func TestColumnDictionaryCodeSnapshotBytesEmptyM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{}
	got := columnDictionaryCodeSnapshotBytes(view, byPart)
	if got != 0 {
		t.Fatalf("expected 0 bytes for empty view, got %d", got)
	}
}

func TestColumnDictionaryCodeSnapshotBytesMissingPartM1634(t *testing.T) {
	// A part referenced in AssetRefs but not in byPart should return 0.
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 1, PartID: 1, Length: 100}},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{}
	got := columnDictionaryCodeSnapshotBytes(view, byPart)
	if got != 0 {
		t.Fatalf("expected 0 bytes for missing part, got %d", got)
	}
}

func TestColumnDictionaryCodeSnapshotBytesAccumulatesM1634(t *testing.T) {
	part1 := [2]uint64{1, 1}
	part2 := [2]uint64{1, 2}
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 1, PartID: 1}},
			{Ref: ColumnAssetRef{Generation: 1, PartID: 2}},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{
		part1: {AssetRef: ColumnAssetRef{Length: 50}},
		part2: {AssetRef: ColumnAssetRef{Length: 75}},
	}
	got := columnDictionaryCodeSnapshotBytes(view, byPart)
	if got != 125 {
		t.Fatalf("expected 125 bytes, got %d", got)
	}
}

// Tests for columnDictionaryCodeSnapshotsByPart (utility function in column_dictionary_query.go).

func TestColumnDictionaryCodeSnapshotsByPartFiltersColumnM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{
		DictionaryCodes: []columnManifestDictionaryCodesSnapshot{
			{ColumnName: "kind", AssetRef: ColumnAssetRef{Generation: 1, PartID: 1}},
			{ColumnName: "did", AssetRef: ColumnAssetRef{Generation: 1, PartID: 1}},
			{ColumnName: "kind", AssetRef: ColumnAssetRef{Generation: 1, PartID: 2}},
		},
	}
	byPart := columnDictionaryCodeSnapshotsByPart(view, "kind")
	if len(byPart) != 2 {
		t.Fatalf("expected 2 entries for column='kind', got %d", len(byPart))
	}
	_, ok := byPart[[2]uint64{1, 1}]
	if !ok {
		t.Fatal("expected entry for generation=1 part_id=1")
	}
	_, ok = byPart[[2]uint64{1, 2}]
	if !ok {
		t.Fatal("expected entry for generation=1 part_id=2")
	}
}

func TestColumnDictionaryCodeSnapshotsByPartEmptyM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{}
	byPart := columnDictionaryCodeSnapshotsByPart(view, "kind")
	if len(byPart) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(byPart))
	}
}

func TestColumnDictionaryCodeSnapshotsByPartNoMatchM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{
		DictionaryCodes: []columnManifestDictionaryCodesSnapshot{
			{ColumnName: "did", AssetRef: ColumnAssetRef{Generation: 1, PartID: 1}},
		},
	}
	byPart := columnDictionaryCodeSnapshotsByPart(view, "kind")
	if len(byPart) != 0 {
		t.Fatalf("expected empty map for unmatched column, got %d entries", len(byPart))
	}
}

// Tests for columnDictionaryCodeSnapshotsCoverParts (utility function).

func TestColumnDictionaryCodeSnapshotsCoverPartsAllCoveredM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 1, PartID: 1}, Reason: ColumnPublishOperationInsert},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{
		{1, 1}: {AssetRef: ColumnAssetRef{Generation: 1, PartID: 1}},
	}
	if !columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
		t.Fatal("expected covered=true when all parts are in byPart")
	}
}

func TestColumnDictionaryCodeSnapshotsCoverPartsEmptyAssetRefsM1634(t *testing.T) {
	// Empty AssetRefs => false (len == 0 check).
	view := columnPhysicalScanSnapshotView{}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{}
	if columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
		t.Fatal("expected covered=false for empty AssetRefs")
	}
}

func TestColumnDictionaryCodeSnapshotsCoverPartsNonInsertReasonM1634(t *testing.T) {
	// Non-insert reason parts should cause covered=false.
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 1, PartID: 1}, Reason: ColumnPublishOperationUpdate},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{
		{1, 1}: {},
	}
	if columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
		t.Fatal("expected covered=false for non-insert reason")
	}
}

func TestColumnDictionaryCodeSnapshotsCoverPartsMissingPartM1634(t *testing.T) {
	view := columnPhysicalScanSnapshotView{
		AssetRefs: []columnManifestAssetRefForScan{
			{Ref: ColumnAssetRef{Generation: 1, PartID: 1}, Reason: ColumnPublishOperationInsert},
			{Ref: ColumnAssetRef{Generation: 1, PartID: 2}, Reason: ColumnPublishOperationInsert},
		},
	}
	byPart := map[[2]uint64]columnManifestDictionaryCodesSnapshot{
		{1, 1}: {},
		// Part 2 is missing.
	}
	if columnDictionaryCodeSnapshotsCoverParts(view, byPart) {
		t.Fatal("expected covered=false when a part is missing from byPart")
	}
}