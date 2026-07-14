package collections

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// These tests deliberately keep the established jsonparser collector as the
// comparison oracle. The cursor is private and is only eligible for the
// generic semantic-stream path, so every test config uses at least one nested
// declared path and therefore bypasses the root fast path.
func TestColumnRetainedSemanticStreamV1JSONCursorMatchesCollector(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	for _, document := range [][]byte{
		[]byte(`{"declared":{"i":-12,"s":"snowman \u2603","v":[1,-2.5,3e1],"keep":{"z":2,"a":1}},"z":0,"a":{"same":1,"same":2},"\u0062":"escaped-key","b":"last-decoded-key","empty":{},"array":[{},[],{"x":1}]}`),
		[]byte(` { "declared" : { "i" : 0 , "s" : "","v":[0,1.5,-2] }, "deep" : { "a" : { "b" : { "c" : {} } } }, "raw" : "a\\\"b\\\\c" } `),
		// jsonparser/gjson's established acceptance includes non-UTF-8 bytes in
		// an otherwise well-formed quoted string; preserve that behavior.
		{'{', '"', 'b', 'a', 'd', '"', ':', '"', 0xff, '"', ',', '"', 'd', 'e', 'c', 'l', 'a', 'r', 'e', 'd', '"', ':', '{', '"', 'i', '"', ':', '1', ',', '"', 's', '"', ':', '"', 'o', 'k', '"', ',', '"', 'v', '"', ':', '[', '1', ',', '2', ',', '3', ']', '}', '}'},
	} {
		columnRetainedSemanticStreamV1RequireJSONCursorMatchesCollector(t, cfg, document)
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorRejectsInvalidAndSkippedSubtrees(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	for _, document := range [][]byte{
		[]byte(``),
		[]byte(`{`),
		[]byte(`{"x":`),
		[]byte(`{"x":"unterminated}`),
		[]byte(`{"x":1} trailing`),
		[]byte(`[]`),
		[]byte(`{"declared":{"i":1x,"s":"ok","v":[1,2,3]},"keep":true}`),
		[]byte(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"skip":{"broken":truX}}`),
		[]byte(`{"x":01}`),
		[]byte(`{"x":1.}`),
		[]byte(`{"x":1e}`),
		[]byte(`{"x":+1}`),
		[]byte(`{"x":--1}`),
		[]byte(`{"x":"\\u12x4"}`),
		// gjson accepts the JSON grammar, but the established jsonparser
		// collector rejects an escaped lone surrogate while decoding object keys.
		[]byte(`{"\uD800":"lone-surrogate-key","declared":{"i":1,"s":"ok","v":[1,2,3]}}`),
		[]byte("{\"x\":\"control\ncharacter\"}"),
	} {
		oracleErr := columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg, document, nil)
		if oracleErr == nil {
			t.Fatalf("collector unexpectedly accepted invalid document %q", document)
		}
		cursorErr := columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg, document, nil)
		if cursorErr == nil {
			t.Fatalf("cursor accepted invalid document %q; collector error=%v", document, oracleErr)
		}
	}

	// Every JSON grammar form accepted by the collector must also be accepted
	// by the structural cursor without changing the retained bytes.
	for _, number := range []string{"0", "-0", "1", "-1", "12.34", "1e2", "1E+2", "1e-2"} {
		document := []byte(fmt.Sprintf(`{"number":%s,"declared":{"i":1,"s":"ok","v":[1,2,3]}}`, number))
		columnRetainedSemanticStreamV1RequireJSONCursorMatchesCollector(t, cfg, document)
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorExactRawDuplicateAndDeclaredValues(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	document := []byte(`{"declared":{"i":1,"i":-7,"s":"first","s":"last \u2603","v":[9,9,9],"v":[1,-2.5,3e1]},"outer":{"dup":"first","dup":"last","nested":{"k":1,"k":2}},"\u0061":"first","a":"last","quoted":"a\\\"b\\\\c","nil":null}`)

	streams, values, err := columnRetainedSemanticStreamV1JSONCursorCollectCandidateWithResult(cfg, document)
	if err != nil {
		t.Fatalf("cursor collect: %v", err)
	}
	if len(values) != len(cfg.Columns) {
		t.Fatalf("declared values=%d want %d", len(values), len(cfg.Columns))
	}
	if got := values[0]; !got.Present || got.Int64 != -7 {
		t.Fatalf("declared int=%+v want present -7", got)
	}
	if got := values[1]; !got.Present || got.String != "last ☃" {
		t.Fatalf("declared string=%+v want decoded duplicate-last string", got)
	}
	if got := values[2].Float32Vector; !reflect.DeepEqual(got, []float32{1, -2.5, 30}) {
		t.Fatalf("declared vector=%v want [1 -2.5 30]", got)
	}
	for _, tc := range []struct {
		path []string
		want string
	}{
		{[]string{"outer", "dup"}, `"last"`},
		{[]string{"outer", "nested", "k"}, "2"},
		{[]string{"a"}, `"last"`},
		{[]string{"quoted"}, `"a\\\"b\\\\c"`},
		{[]string{"nil"}, "null"},
	} {
		stream := streams.byKey[columnRetainedSemanticStreamPathKey(tc.path)]
		if stream == nil || stream.entryCount() != 1 {
			t.Fatalf("retained path %q stream=%+v", tc.path, stream)
		}
		if got := string(stream.rawValues[0]); got != tc.want {
			t.Fatalf("retained path %q raw=%q want exact %q", tc.path, got, tc.want)
		}
	}
	for _, path := range [][]string{{"declared", "i"}, {"declared", "s"}, {"declared", "v"}} {
		if stream := streams.byKey[columnRetainedSemanticStreamPathKey(path)]; stream != nil {
			t.Fatalf("declared path %q leaked into retained streams: %+v", path, stream)
		}
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorPropagatesDeclaredOnlyKeyErrors(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	// The established collector visits every member in this declared-only
	// object, including unselected keys. Keep its escaped-key error behavior.
	document := []byte(`{"declared":{"i":1,"s":"ok","v":[1,2,3],"\uD800":"unselected"},"payload":true}`)
	oracleErr := columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg, document, nil)
	cursorErr := columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg, document, nil)
	if oracleErr == nil || cursorErr == nil {
		t.Fatalf("declared-only malformed key acceptance collector=%v cursor=%v", oracleErr, cursorErr)
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorMissingNullAndIntegratedBlockEquivalence(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	for i := range cfg.Columns {
		cfg.Columns[i].Nullable = true
	}
	ids := [][]byte{[]byte("doc-0"), []byte("doc-1"), []byte("doc-2")}
	documents := [][]byte{
		[]byte(`{"declared":{"i":1,"v":[1,2,3]},"payload":{"z":1}}`),
		[]byte(`{"declared":{"i":null,"s":null,"v":null},"payload":{"z":2}}`),
		[]byte(`{"declared":{"i":3,"s":"three","v":[3,2,1]},"payload":{"z":3}}`),
	}

	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, ids, documents)
	if err != nil {
		t.Fatalf("prepare cursor-integrated documents: %v", err)
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)
	if !prepared.declaredRowsReady {
		t.Fatal("cursor-integrated declared rows not ready")
	}
	if got := prepared.declaredRows[0].Values[1]; got.Present || !got.Null {
		t.Fatalf("missing nullable string=%+v want absent null", got)
	}
	for colIdx, value := range prepared.declaredRows[1].Values {
		if !value.Present || !value.Null {
			t.Fatalf("explicit null declared row column %d=%+v want present null", colIdx, value)
		}
	}

	oracleStreams := newColumnRetainedSemanticStreamStreams()
	oracleInterner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	declaredTrie, ok := columnRetainedSemanticStreamV1DeclaredPathTrieForConfig(cfg, ids, len(documents))
	if !ok {
		t.Fatal("declared trie unavailable for oracle")
	}
	oracleRows := make([]columnDeclaredRow, len(documents))
	oracleStringInterner := &columnDeclaredStringInterner{}
	for row, document := range documents {
		values, err := collectColumnRetainedSemanticStreamV1RetainedJSONParserDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, uint64(row), len(documents), oracleStreams, oracleInterner, declaredTrie, make([]columnDeclaredValue, len(cfg.Columns)), oracleStringInterner)
		if err != nil {
			t.Fatalf("collector oracle row %d: %v", row, err)
		}
		oracleRows[row] = columnDeclaredRow{ID: bytes.Clone(ids[row]), Values: values}
	}
	oracleBlock, err := encodeColumnRetainedSemanticStreamV1BlockFromStreams(len(documents), oracleStreams)
	if err != nil {
		t.Fatalf("encode collector oracle block: %v", err)
	}
	if !reflect.DeepEqual(prepared.declaredRows, oracleRows) {
		t.Fatalf("declared rows differ\ncollector=%+v\ncursor=%+v", oracleRows, prepared.declaredRows)
	}

	iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	if !iter.Valid() {
		t.Fatal("cursor-integrated semantic block table is empty")
	}
	stored := iter.UnsafeValue()
	if !bytes.Equal(stored, oracleBlock) {
		t.Fatalf("integrated stored block differs\ncollector=%x\ncursor=%x", oracleBlock, stored)
	}
	if iter.Next(); iter.Valid() {
		t.Fatal("cursor-integrated semantic block table has more than one block")
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate cursor-integrated semantic block table: %v", err)
	}
	hash := sha256.Sum256(oracleBlock)
	for row, locator := range prepared.documents {
		want := encodeColumnRetainedSemanticStreamV1Locator(hash[:], uint64(row))
		if !bytes.Equal(locator, want) {
			t.Fatalf("row %d locator=%x want=%x", row, locator, want)
		}
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorValidatesSkippedTerminalSubtree(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "skipped", Path: "skipped", ValueType: ColumnStoreValueString, Nullable: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	document := []byte(`{"skipped":{"broken":truX},"kept":1}`)
	if err := columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg, document, nil); err == nil {
		t.Fatal("collector accepted malformed terminal skipped subtree")
	}
	if err := columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg, document, nil); err == nil {
		t.Fatal("cursor accepted malformed terminal skipped subtree")
	}
}

func TestColumnRetainedSemanticStreamV1JSONCursorDepthLimitFallsBackToCollector(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	// The cursor's block-local arena is intentionally bounded. The production
	// seam must retain current behavior by routing excessively deep documents to
	// the established collector rather than emitting partial cursor output.
	document := columnRetainedSemanticStreamV1JSONCursorDeepDocument(columnRetainedSemanticStreamV1JSONCursorMaxDepth + 1)
	if err := columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg, document, nil); err != nil {
		t.Fatalf("collector rejected bounded-depth compatibility document: %v", err)
	}
	if err := columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg, document, nil); !errors.Is(err, errColumnRetainedSemanticStreamV1JSONCursorDepth) {
		t.Fatalf("cursor depth error=%v want %v", err, errColumnRetainedSemanticStreamV1JSONCursorDepth)
	}
	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, [][]byte{[]byte("deep")}, [][]byte{document})
	if err != nil {
		t.Fatalf("production prepare did not fall back to collector: %v", err)
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)
}

func TestColumnRetainedSemanticStreamV1JSONCursorValidationOnlyArrayUsesNoDescriptors(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	var document bytes.Buffer
	document.WriteString(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"array":[`)
	for i := 0; i < columnRetainedSemanticStreamV1JSONCursorMaxDescriptors*2; i++ {
		if i != 0 {
			document.WriteByte(',')
		}
		document.WriteString(`12.34`)
	}
	document.WriteString(`]}`)

	cursor := &columnRetainedSemanticStreamV1JSONCursor{}
	if _, err := cursor.parseDocument(document.Bytes(), &columnRetainedSemanticStreamV1PathSegmentInterner{}); err != nil {
		t.Fatalf("cursor parse validation-only array: %v", err)
	}
	if got := len(cursor.nodes); got > 8 {
		t.Fatalf("validation-only array retained %d descriptors; want only object/value structure", got)
	}
	columnRetainedSemanticStreamV1RequireJSONCursorMatchesCollector(t, cfg, document.Bytes())
}

func TestColumnRetainedSemanticStreamV1JSONCursorDescriptorBudgetFallsBackToCollector(t *testing.T) {
	cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
	document := columnRetainedSemanticStreamV1JSONCursorWideDocument(columnRetainedSemanticStreamV1JSONCursorMaxDescriptors + 16)
	if err := columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg, document, nil); err != nil {
		t.Fatalf("collector rejected wide compatibility document: %v", err)
	}
	if err := columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg, document, nil); !errors.Is(err, errColumnRetainedSemanticStreamV1JSONCursorScratch) {
		t.Fatalf("cursor descriptor error=%v want %v", err, errColumnRetainedSemanticStreamV1JSONCursorScratch)
	}
	prepared, err := prepareColumnRetainedSemanticStreamV1StorageDocumentsWithIDs(cfg, [][]byte{[]byte("wide")}, [][]byte{document})
	if err != nil {
		t.Fatalf("production prepare did not fall back after descriptor budget: %v", err)
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)
	columnRetainedSemanticStreamV1RequirePreparedCursorMatchesCollector(t, cfg, [][]byte{[]byte("wide")}, [][]byte{document}, prepared)
}

func TestColumnRetainedSemanticStreamV1JSONCursorDropsOversizedUnescapeScratch(t *testing.T) {
	cursor := &columnRetainedSemanticStreamV1JSONCursor{}
	largeEscapedKey := strings.Repeat(`\u0061`, columnRetainedSemanticStreamV1JSONCursorMaxRetainedUnescapeScratch+1)
	largeDocument := []byte(`{"` + largeEscapedKey + `":1}`)
	root, err := cursor.parseDocument(largeDocument, &columnRetainedSemanticStreamV1PathSegmentInterner{})
	if err != nil {
		t.Fatalf("parse large escaped key: %v", err)
	}
	if err := cursor.collectObject(root, nil, 0, 1, nil, newColumnRetainedSemanticStreamStreams(), nil, nil); err != nil {
		t.Fatalf("collect large escaped key: %v", err)
	}
	if cap(cursor.unescapeScratch) <= columnRetainedSemanticStreamV1JSONCursorMaxRetainedUnescapeScratch {
		t.Fatalf("large escaped key scratch cap=%d did not exercise retention bound", cap(cursor.unescapeScratch))
	}
	root, err = cursor.parseDocument([]byte(`{"\u0062":2}`), &columnRetainedSemanticStreamV1PathSegmentInterner{})
	if err != nil {
		t.Fatalf("parse document after large escaped key: %v", err)
	}
	if err := cursor.collectObject(root, nil, 0, 1, nil, newColumnRetainedSemanticStreamStreams(), nil, nil); err != nil {
		t.Fatalf("collect document after large escaped key: %v", err)
	}
	if cap(cursor.unescapeScratch) > columnRetainedSemanticStreamV1JSONCursorMaxRetainedUnescapeScratch {
		t.Fatalf("oversized escaped-key scratch retained cap=%d", cap(cursor.unescapeScratch))
	}
}

func FuzzColumnRetainedSemanticStreamV1JSONCursorMatchesCollector(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"x":true}`),
		[]byte(`{"declared":{"i":-1,"s":"\u2603","v":[0,-0,1e2]},"nested":{"a":{},"b":[1,{"x":2}]}}`),
		[]byte(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"x":01}`),
		[]byte(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"x":"\uD800"}`),
		[]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, document []byte) {
		cfg := columnRetainedSemanticStreamV1JSONCursorTestConfig()
		oracleStreams, oracleValues, oracleErr := columnRetainedSemanticStreamV1JSONCursorCollect(cfg, document, false)
		cursorStreams, cursorValues, cursorErr := columnRetainedSemanticStreamV1JSONCursorCollect(cfg, document, true)
		if (oracleErr == nil) != (cursorErr == nil) {
			t.Fatalf("acceptance mismatch document=%q collector=%v cursor=%v", document, oracleErr, cursorErr)
		}
		if oracleErr != nil {
			return
		}
		columnRetainedSemanticStreamV1RequireEquivalentCursorOutput(t, oracleStreams, oracleValues, cursorStreams, cursorValues)
		columnRetainedSemanticStreamV1RequireEquivalentCursorBlocks(t, oracleStreams, cursorStreams)
	})
}

func BenchmarkColumnRetainedPayloadSemanticStreamV1PrepareJSONBenchShapeFourBlocks(b *testing.B) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "event", Path: "commit.collection", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true},
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerColumnPart},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids, docs := columnRetainedSemanticStreamV1JSONCursorBenchmarkDocuments(columnRetainedSemanticStreamV1BlockRows * 4)
	var inputBytes int64
	for _, document := range docs {
		inputBytes += int64(len(document))
	}
	b.ReportAllocs()
	b.SetBytes(inputBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
		if err != nil {
			b.Fatalf("prepare four-block JSONBench-shaped documents: %v", err)
		}
		if !prepared.declaredRowsReady || len(prepared.declaredRows) != len(docs) || prepared.semanticStreamBlocks == nil {
			b.Fatalf("unexpected prepared result declared-ready=%t rows=%d blocks=%v", prepared.declaredRowsReady, len(prepared.declaredRows), prepared.semanticStreamBlocks != nil)
		}
		if i == 0 {
			b.StopTimer()
			reportColumnRetainedSemanticStreamBenchmarkStoredBlockBytes(b, prepared.semanticStreamBlocks, inputBytes)
			b.StartTimer()
		}
		resetCollectionRunTable(prepared.semanticStreamBlocks)
	}
}

func columnRetainedSemanticStreamV1JSONCursorTestConfig() ColumnStoreConfig {
	return ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "int", Path: "declared.i", ValueType: ColumnStoreValueInt64},
			{Name: "string", Path: "declared.s", ValueType: ColumnStoreValueString, Dictionary: true, Nullable: true},
			{Name: "vector", Path: "declared.v", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
}

func columnRetainedSemanticStreamV1RequireJSONCursorMatchesCollector(t *testing.T, cfg ColumnStoreConfig, document []byte) {
	t.Helper()
	oracleStreams, oracleValues, oracleErr := columnRetainedSemanticStreamV1JSONCursorCollect(cfg, document, false)
	cursorStreams, cursorValues, cursorErr := columnRetainedSemanticStreamV1JSONCursorCollect(cfg, document, true)
	if oracleErr != nil || cursorErr != nil {
		t.Fatalf("document=%q collector=%v cursor=%v", document, oracleErr, cursorErr)
	}
	columnRetainedSemanticStreamV1RequireEquivalentCursorOutput(t, oracleStreams, oracleValues, cursorStreams, cursorValues)
	columnRetainedSemanticStreamV1RequireEquivalentCursorBlocks(t, oracleStreams, cursorStreams)
}

func columnRetainedSemanticStreamV1JSONCursorCollectOracle(cfg ColumnStoreConfig, document []byte, streams *columnRetainedSemanticStreamStreams) error {
	_, _, err := columnRetainedSemanticStreamV1JSONCursorCollectInto(cfg, document, streams, false)
	return err
}

func columnRetainedSemanticStreamV1JSONCursorCollectCandidate(cfg ColumnStoreConfig, document []byte, streams *columnRetainedSemanticStreamStreams) error {
	_, _, err := columnRetainedSemanticStreamV1JSONCursorCollectInto(cfg, document, streams, true)
	return err
}

func columnRetainedSemanticStreamV1JSONCursorCollectCandidateWithResult(cfg ColumnStoreConfig, document []byte) (*columnRetainedSemanticStreamStreams, []columnDeclaredValue, error) {
	return columnRetainedSemanticStreamV1JSONCursorCollect(cfg, document, true)
}

func columnRetainedSemanticStreamV1JSONCursorCollect(cfg ColumnStoreConfig, document []byte, cursor bool) (*columnRetainedSemanticStreamStreams, []columnDeclaredValue, error) {
	return columnRetainedSemanticStreamV1JSONCursorCollectInto(cfg, document, nil, cursor)
}

func columnRetainedSemanticStreamV1JSONCursorCollectInto(cfg ColumnStoreConfig, document []byte, streams *columnRetainedSemanticStreamStreams, cursor bool) (*columnRetainedSemanticStreamStreams, []columnDeclaredValue, error) {
	if streams == nil {
		streams = newColumnRetainedSemanticStreamStreams()
	}
	interner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	declared, ok := columnRetainedSemanticStreamV1DeclaredPathTrieForConfig(cfg, [][]byte{[]byte("doc")}, 1)
	if !ok {
		return nil, nil, errors.New("test configuration cannot use declared trie")
	}
	values := make([]columnDeclaredValue, len(cfg.Columns))
	if !cursor {
		values, err := collectColumnRetainedSemanticStreamV1RetainedJSONParserDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, 0, 1, streams, interner, declared, values, &columnDeclaredStringInterner{})
		return streams, values, err
	}
	values, err := collectColumnRetainedSemanticStreamV1JSONCursorDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, 0, 1, streams, interner, declared, values, &columnDeclaredStringInterner{}, &columnRetainedSemanticStreamV1JSONCursor{})
	return streams, values, err
}

func columnRetainedSemanticStreamV1RequireEquivalentCursorOutput(t *testing.T, oracleStreams *columnRetainedSemanticStreamStreams, oracleValues []columnDeclaredValue, cursorStreams *columnRetainedSemanticStreamStreams, cursorValues []columnDeclaredValue) {
	t.Helper()
	if !reflect.DeepEqual(oracleValues, cursorValues) {
		t.Fatalf("declared values differ\ncollector=%+v\ncursor=%+v", oracleValues, cursorValues)
	}
	if len(oracleStreams.byKey) != len(cursorStreams.byKey) {
		t.Fatalf("stream count collector=%d cursor=%d", len(oracleStreams.byKey), len(cursorStreams.byKey))
	}
	keys := make([]string, 0, len(oracleStreams.byKey))
	for key := range oracleStreams.byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		oracle := oracleStreams.byKey[key]
		cursor := cursorStreams.byKey[key]
		if cursor == nil {
			t.Fatalf("cursor missing stream %q", oracle.segments)
		}
		if !reflect.DeepEqual(oracle.segments, cursor.segments) || oracle.entryCount() != cursor.entryCount() {
			t.Fatalf("stream %q collector=%+v cursor=%+v", oracle.segments, oracle, cursor)
		}
		for i := range oracle.rawValues {
			if oracle.rowAt(i) != cursor.rowAt(i) || !bytes.Equal(oracle.rawValues[i], cursor.rawValues[i]) {
				t.Fatalf("stream %q entry %d collector row/raw=%d/%q cursor=%d/%q", oracle.segments, i, oracle.rowAt(i), oracle.rawValues[i], cursor.rowAt(i), cursor.rawValues[i])
			}
		}
	}
}

func columnRetainedSemanticStreamV1RequireEquivalentCursorBlocks(t *testing.T, oracleStreams, cursorStreams *columnRetainedSemanticStreamStreams) {
	t.Helper()
	oracleBlock, err := encodeColumnRetainedSemanticStreamV1BlockFromStreams(1, oracleStreams)
	if err != nil {
		t.Fatalf("encode collector block: %v", err)
	}
	cursorBlock, err := encodeColumnRetainedSemanticStreamV1BlockFromStreams(1, cursorStreams)
	if err != nil {
		t.Fatalf("encode cursor block: %v", err)
	}
	if !bytes.Equal(oracleBlock, cursorBlock) {
		t.Fatalf("stored block differs\ncollector=%x\ncursor=%x", oracleBlock, cursorBlock)
	}
	oracleHash := sha256.Sum256(oracleBlock)
	cursorHash := sha256.Sum256(cursorBlock)
	if oracleHash != cursorHash {
		t.Fatalf("block hash collector=%x cursor=%x", oracleHash, cursorHash)
	}
	oracleLocator := encodeColumnRetainedSemanticStreamV1Locator(oracleHash[:], 0)
	cursorLocator := encodeColumnRetainedSemanticStreamV1Locator(cursorHash[:], 0)
	if !bytes.Equal(oracleLocator, cursorLocator) {
		t.Fatalf("locator differs collector=%x cursor=%x", oracleLocator, cursorLocator)
	}
}

func columnRetainedSemanticStreamV1RequirePreparedCursorMatchesCollector(t *testing.T, cfg ColumnStoreConfig, ids, documents [][]byte, prepared columnRetainedPayloadStorageDocuments) {
	t.Helper()
	oracleStreams := newColumnRetainedSemanticStreamStreams()
	oracleInterner := &columnRetainedSemanticStreamV1PathSegmentInterner{}
	declaredTrie, ok := columnRetainedSemanticStreamV1DeclaredPathTrieForConfig(cfg, ids, len(documents))
	if !ok {
		t.Fatal("declared trie unavailable for prepared collector oracle")
	}
	oracleRows := make([]columnDeclaredRow, len(documents))
	oracleStringInterner := &columnDeclaredStringInterner{}
	for row, document := range documents {
		values, err := collectColumnRetainedSemanticStreamV1RetainedJSONParserDocument(cfg, columnRetainedSemanticStreamV1RetainedSkipTrieForConfig(cfg), document, uint64(row), len(documents), oracleStreams, oracleInterner, declaredTrie, make([]columnDeclaredValue, len(cfg.Columns)), oracleStringInterner)
		if err != nil {
			t.Fatalf("collector prepared oracle row %d: %v", row, err)
		}
		oracleRows[row] = columnDeclaredRow{ID: bytes.Clone(ids[row]), Values: values}
	}
	oracleBlock, err := encodeColumnRetainedSemanticStreamV1BlockFromStreams(len(documents), oracleStreams)
	if err != nil {
		t.Fatalf("encode collector prepared oracle block: %v", err)
	}
	if !reflect.DeepEqual(prepared.declaredRows, oracleRows) {
		t.Fatalf("prepared declared rows differ\ncollector=%+v\ncursor=%+v", oracleRows, prepared.declaredRows)
	}
	iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	if !iter.Valid() {
		t.Fatal("prepared semantic block table is empty")
	}
	if got := iter.UnsafeValue(); !bytes.Equal(got, oracleBlock) {
		t.Fatalf("prepared stored block differs\ncollector=%x\ncursor=%x", oracleBlock, got)
	}
	if iter.Next(); iter.Valid() {
		t.Fatal("prepared semantic block table has more than one block")
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate prepared semantic block table: %v", err)
	}
	hash := sha256.Sum256(oracleBlock)
	for row, locator := range prepared.documents {
		want := encodeColumnRetainedSemanticStreamV1Locator(hash[:], uint64(row))
		if !bytes.Equal(locator, want) {
			t.Fatalf("prepared row %d locator=%x want=%x", row, locator, want)
		}
	}
}

func columnRetainedSemanticStreamV1JSONCursorDeepDocument(depth int) []byte {
	var out bytes.Buffer
	out.WriteString(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"deep":`)
	for range depth {
		out.WriteString(`{"x":`)
	}
	out.WriteString(`0`)
	for range depth {
		out.WriteByte('}')
	}
	out.WriteByte('}')
	return out.Bytes()
}

func columnRetainedSemanticStreamV1JSONCursorWideDocument(members int) []byte {
	var out bytes.Buffer
	out.WriteString(`{"declared":{"i":1,"s":"ok","v":[1,2,3]},"wide":{`)
	for i := 0; i < members; i++ {
		if i != 0 {
			out.WriteByte(',')
		}
		fmt.Fprintf(&out, `"k%05d":12.34`, i)
	}
	out.WriteString(`}}`)
	return out.Bytes()
}

func columnRetainedSemanticStreamV1JSONCursorBenchmarkDocuments(count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	documents := make([][]byte, count)
	for row := range count {
		ids[row] = []byte(fmt.Sprintf("doc-%06d", row))
		documents[row] = []byte(fmt.Sprintf(
			`{"did":"did:plc:%06d","time_us":%d,"kind":"commit","commit":{"collection":"app.bsky.feed.post","operation":"create","cid":"bafy-test-%06d","rev":"3l-%06d","record":{"$type":"app.bsky.feed.post","createdAt":"2026-06-13T12:%02d:00Z","subject":{"uri":"at://did:plc:%06d/app.bsky.feed.post/%06d","cid":"bafy-subject-%06d"},"text":"semantic stream retained payload %03d"}}}`,
			row%19,
			1_750_000_000_000_000+row,
			row,
			row,
			row%60,
			row%19,
			row,
			row,
			row,
		))
	}
	return ids, documents
}
