package collections

import (
	"bytes"
	"reflect"
	"sort"
	"testing"
)

func TestTextV2PositionValueV2RoundTripAndLegacyCompatibility2835(t *testing.T) {
	value := textV2PositionValue{
		FormatVersion: textV2FormatVersion,
		Ordinal:       7,
		Generation:    3,
		Term:          "refund-policy-2835",
		Fields: []textV2PositionFieldValue{
			{FieldIndex: 1, Frequency: 4, Positions: []uint32{3, 130, 131, 4096}, Offsets: []textTokenOffset{{Start: 6, End: 12}, {Start: 260, End: 266}, {Start: 268, End: 274}, {Start: 8192, End: 8198}}},
			{FieldIndex: 3, Frequency: 2, Positions: []uint32{0, 2048}, Offsets: []textTokenOffset{{Start: 0, End: 6}, {Start: 4096, End: 4102}}},
		},
	}

	encoded := encodeTextV2PositionValue(value)
	if encoded[0] != textV2PositionValueVersion {
		t.Fatalf("position value version=%d want %d", encoded[0], textV2PositionValueVersion)
	}
	if _, err := decodeTextV2PositionValue(encoded); err == nil {
		t.Fatalf("decode v2 position without key term succeeded; want fail-closed key-bound decode")
	}
	decoded, err := decodeTextV2PositionValueForTerm(encoded, value.Term)
	if err != nil {
		t.Fatalf("decode v2 position: %v", err)
	}
	if !reflect.DeepEqual(decoded, value) {
		t.Fatalf("decoded v2 position=%+v want %+v", decoded, value)
	}

	legacy := encodeTextV2PositionValueV1ForTest(value)
	legacyDecoded, err := decodeTextV2PositionValueForTerm(legacy, value.Term)
	if err != nil {
		t.Fatalf("decode legacy v1 position: %v", err)
	}
	if !reflect.DeepEqual(legacyDecoded, value) {
		t.Fatalf("decoded legacy position=%+v want %+v", legacyDecoded, value)
	}
	if len(encoded) >= len(legacy) {
		t.Fatalf("v2 encoded bytes=%d want smaller than legacy=%d", len(encoded), len(legacy))
	}
	if _, err := decodeTextV2PositionValueForTerm(encoded, "wrong-term"); err == nil {
		t.Fatalf("decode v2 position with mismatched key term succeeded; want corruption")
	}
	if _, err := decodeTextV2PositionValueForTerm(legacy, "wrong-term"); err == nil {
		t.Fatalf("decode legacy v1 with mismatched key term succeeded; want corruption")
	}
}

func TestTextV2PositionValueV2RejectsCorruptDeltas2835(t *testing.T) {
	var raw []byte
	raw = append(raw, textV2PositionValueVersion)
	raw = appendTextUvarint(raw, uint64(textV2FormatVersion))
	raw = appendTextUvarint(raw, 1) // ordinal
	raw = appendTextUvarint(raw, 1) // generation
	raw = appendTextV2PositionTermBinding(raw, "refund")
	raw = appendTextUvarint(raw, 1) // field count
	raw = appendTextUvarint(raw, 0) // field index
	raw = appendTextUvarint(raw, 2) // frequency
	raw = appendTextUvarint(raw, 2) // position count
	raw = appendTextUvarint(raw, 7) // first absolute position
	raw = appendTextUvarint(raw, 0) // invalid second delta: non-increasing
	raw = appendTextUvarint(raw, 0) // offsets count
	if _, err := decodeTextV2PositionValueForTerm(raw, "refund"); err == nil {
		t.Fatalf("decode corrupt v2 position deltas succeeded; want corruption")
	}
}

func TestTextV2StorageStatsLaneBytesAndPositionSavings2835(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, StoreOffsets: true, Fields: []TextIndexField{{Field: "title"}, {Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2")}, [][]byte{
		[]byte(`{"title":"refundpolicytwenty835longtoken policy","body":"refundpolicytwenty835longtoken policy support refundpolicytwenty835longtoken policy support refundpolicytwenty835longtoken policy support"}`),
		[]byte(`{"title":"shippingpolicytwenty835longtoken policy","body":"shippingpolicytwenty835longtoken support policy refundpolicytwenty835longtoken support policy refundpolicytwenty835longtoken"}`),
	})

	stats, err := col.TextIndexStorageStats("lexical")
	if err != nil {
		t.Fatalf("TextIndexStorageStats: %v", err)
	}
	laneBytes := stats.V2DocIDBytes + stats.V2DocMapBytes + stats.V2PostingBlockBytes + stats.V2NormBlockBytes + stats.V2PositionBytes + stats.V2TermStatsBytes + stats.V2StatusFormatBytes
	if laneBytes != stats.EncodedBytes {
		t.Fatalf("lane bytes=%d encoded=%d stats=%+v", laneBytes, stats.EncodedBytes, stats)
	}
	if stats.V2PostingBlockBytes == 0 || stats.V2NormBlockBytes == 0 || stats.V2DocMapBytes == 0 || stats.V2PositionBytes == 0 || stats.V2TermStatsBytes == 0 || stats.V2StatusFormatBytes == 0 {
		t.Fatalf("stats=%+v want non-zero v2 lane byte accounting", stats)
	}
	legacyPositionBytes := textV2LegacyPositionRootBytes2835(t, col)
	if legacyPositionBytes <= stats.V2PositionBytes {
		t.Fatalf("legacy position bytes=%d current=%d want current smaller", legacyPositionBytes, stats.V2PositionBytes)
	}
	legacyIndexBytes := stats.EncodedBytes - stats.V2PositionBytes + legacyPositionBytes
	if legacyIndexBytes <= stats.EncodedBytes {
		t.Fatalf("legacy index bytes=%d current=%d want current smaller", legacyIndexBytes, stats.EncodedBytes)
	}
}

func BenchmarkTextV2PositionFootprint2835(b *testing.B) {
	docs := textV2ContractEnvInt2623("TREEDB_TEXT_V2_FOOTPRINT_DOCS", 1024)
	if docs < int(textV2PostingBlockTargetPostings)*3 {
		docs = int(textV2PostingBlockTargetPostings) * 3
	}
	d, col, _ := openTextV2PhraseBenchFixture2733(b, docs, nil)
	defer func() { _ = d.Close() }()
	stats := textV2ContractStorageStats2623(b, col)
	legacyPositionBytes := textV2LegacyPositionRootBytes2835(b, col)
	legacyIndexBytes := stats.EncodedBytes - stats.V2PositionBytes + legacyPositionBytes
	docsDivisor := float64(maxTextV2ContractInt2623(docs, 1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.TextIndexStorageStats(textV2ContractIndexName2623); err != nil {
			b.Fatalf("TextIndexStorageStats: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(docs), "docs_fixture")
	b.ReportMetric(float64(stats.EncodedBytes), "index_bytes")
	b.ReportMetric(float64(legacyIndexBytes), "legacy_index_bytes")
	b.ReportMetric(float64(stats.EncodedBytes)/docsDivisor, "index_bytes/doc")
	b.ReportMetric(float64(legacyIndexBytes)/docsDivisor, "legacy_index_bytes/doc")
	b.ReportMetric(float64(stats.V2PositionBytes), "v2_position_bytes")
	b.ReportMetric(float64(legacyPositionBytes), "legacy_v2_position_bytes")
	b.ReportMetric(float64(stats.V2PositionBytes)/docsDivisor, "v2_position_bytes/doc")
	b.ReportMetric(float64(legacyPositionBytes)/docsDivisor, "legacy_v2_position_bytes/doc")
	if legacyPositionBytes > 0 {
		b.ReportMetric(100*(float64(legacyPositionBytes)-float64(stats.V2PositionBytes))/float64(legacyPositionBytes), "position_bytes_saved_pct")
	}
	textV2ContractReportStorageLaneBytes2623(b, docs, stats)
}

func textV2LegacyPositionRootBytes2835(tb testing.TB, col *Collection) uint64 {
	tb.Helper()
	if col == nil || col.db == nil {
		return 0
	}
	snap := col.db.AcquireSnapshot()
	if snap == nil {
		tb.Fatalf("snapshot nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalog snapshot: %v", err)
	}
	rootName := collectionTextV2PositionsRootName(catalog.meta.Name, textV2ContractIndexName2623)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
	if err != nil || it == nil {
		tb.Fatalf("positions iterator: %v", err)
	}
	defer func() { _ = it.Close() }()
	var total uint64
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key := it.UnsafeKey()
		if bytes.Equal(key, encodeTextV2FormatKey()) {
			it.Next()
			continue
		}
		_, term, err := decodeTextV2PositionKey(key)
		if err != nil {
			tb.Fatalf("decode position key: %v", err)
		}
		value, err := decodeTextV2PositionValueForTerm(it.ValueCopy(nil), term)
		if err != nil {
			tb.Fatalf("decode position value: %v", err)
		}
		legacy := encodeTextV2PositionValueV1ForTest(value)
		total += uint64(len(key) + len(legacy))
		it.Next()
	}
	if err := it.Error(); err != nil {
		tb.Fatalf("positions iterator error: %v", err)
	}
	return total
}

func encodeTextV2PositionValueV1ForTest(value textV2PositionValue) []byte {
	fields := cloneTextV2PositionFields(value.Fields)
	sort.Slice(fields, func(i, j int) bool { return fields[i].FieldIndex < fields[j].FieldIndex })
	out := make([]byte, 0, 1+binaryMaxVarintLen64ForTest2835*4+len(value.Term))
	out = append(out, textV2PositionValueVersionV1)
	out = appendTextUvarint(out, uint64(value.FormatVersion))
	out = appendTextUvarint(out, value.Ordinal)
	out = appendTextUvarint(out, value.Generation)
	out = appendTextString(out, value.Term)
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		out = appendTextUvarint(out, uint64(field.FieldIndex))
		out = appendTextUvarint(out, uint64(field.Frequency))
		out = appendTextUint32Slice(out, field.Positions)
		out = appendTextOffsetSlice(out, field.Offsets)
	}
	return out
}

const binaryMaxVarintLen64ForTest2835 = 10
