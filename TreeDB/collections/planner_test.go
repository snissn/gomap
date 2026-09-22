package collections

import (
	"bytes"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func mustEncodeTestIndexScalar(t *testing.T, valueType IndexValueType, value any) []byte {
	t.Helper()
	encoded, err := encodeIndexScalar(valueType, value)
	if err != nil {
		t.Fatalf("encode index scalar: %v", err)
	}
	return encoded
}

func TestApplyCollectionRunEntriesWithFlagsBulkTombstones(t *testing.T) {
	table := newCollectionRootAccumulatorRunTable()
	err := applyCollectionRunEntriesWithFlags(table, 3, func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		switch i {
		case 0:
			return []byte("a"), []byte("old-a"), page.ValuePtr{}, node.FlagInline, nil
		case 1:
			return []byte("b"), nil, page.ValuePtr{}, node.FlagInline, nil
		default:
			return []byte("a"), nil, page.ValuePtr{}, node.FlagTombstone, nil
		}
	})
	if err != nil {
		t.Fatalf("applyCollectionRunEntriesWithFlags: %v", err)
	}
	if _, _, flags, ok := table.GetEntry([]byte("a")); !ok || flags&node.FlagTombstone == 0 {
		t.Fatalf("a entry ok=%v flags=%02x, want tombstone", ok, flags)
	}
	if got, _, flags, ok := table.GetEntry([]byte("b")); !ok || flags&node.FlagTombstone != 0 || got != nil {
		t.Fatalf("b entry value=%q flags=%02x ok=%v, want live nil inline value", got, flags, ok)
	}
	table.Freeze()
	requireFreezeSortRunIterator(t, table.NewIterator(nil, nil), []string{"a", "b"})
}

func TestInsertBatchPlanner_EmitsRootLocalRunsForPrimaryIndexStateAndSecondaryRoots(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
			{name: "city", field: "city", valueType: IndexValueString},
		},
	}

	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com","city":"hnl"}`),
			[]byte(`{"email":"ada@example.com","city":"hnl"}`),
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got, want := len(plan.runs), 4; got != want {
		t.Fatalf("runs len=%d want %d", got, want)
	}

	primary := mustFindRun(t, plan, collectionRootPrimary, "")
	primaryEntries := collectRunEntries(t, primary)
	assertEntryKeys(t, primaryEntries, "u1", "u2")
	if got, want := string(primaryEntries[0].value), `{"email":"ada@example.com","city":"hnl"}`; got != want {
		t.Fatalf("primary u1 value=%q want %q", got, want)
	}
	if got, want := string(primaryEntries[1].value), `{"email":"grace@example.com","city":"hnl"}`; got != want {
		t.Fatalf("primary u2 value=%q want %q", got, want)
	}

	state := mustFindRun(t, plan, collectionRootIndexState, "")
	stateEntries := collectRunEntries(t, state)
	assertEntryKeys(t, stateEntries, "u1", "u2")
	for _, entry := range stateEntries {
		if len(entry.value) == 0 {
			t.Fatalf("index-state entry for %q is empty", entry.key)
		}
	}

	email := mustFindRun(t, plan, collectionRootSecondary, "email")
	emailEntries := collectRunEntries(t, email)
	if got, want := len(emailEntries), 2; got != want {
		t.Fatalf("email index entries=%d want %d", got, want)
	}
	assertSortedEntries(t, emailEntries)
	if !bytes.HasSuffix(emailEntries[0].key, []byte("u1")) || !bytes.HasSuffix(emailEntries[1].key, []byte("u2")) {
		t.Fatalf("email entries should be value-prefix plus document id, got %q", entryKeys(emailEntries))
	}

	city := mustFindRun(t, plan, collectionRootSecondary, "city")
	cityEntries := collectRunEntries(t, city)
	if got, want := len(cityEntries), 2; got != want {
		t.Fatalf("city index entries=%d want %d", got, want)
	}
	assertSortedEntries(t, cityEntries)

	if got := len(plan.uniqueProbeRuns); got != 0 {
		t.Fatalf("unique probe runs=%d want 0 without persisted unique roots", got)
	}
}

func TestInsertBatchPlanner_TemplateV1SkipsIndexStateRun(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
			{name: "city", field: "city", valueType: IndexValueString},
		},
		options: collectionOptions{documentFormat: DocumentFormatTemplateV1},
	}

	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"ada@example.com", "hnl"}),
			mustTemplateV1Document(t, []string{"email", "city"}, []any{"grace@example.com", "hnl"}),
		},
	)
	if err != nil {
		t.Fatalf("plan template-v1 insert batch: %v", err)
	}
	if got, want := len(plan.runs), 4; got != want {
		t.Fatalf("runs len=%d want %d", got, want)
	}
	if idx := findRunIndex(plan, collectionRootIndexState, ""); idx >= 0 {
		t.Fatalf("template-v1 plan unexpectedly emitted index-state run at %d", idx)
	}
	if got := plan.stats.IndexStateRunBuild; got != 0 {
		t.Fatalf("template-v1 index-state run build=%s want 0", got)
	}

	_ = mustFindRun(t, plan, collectionRootPrimary, "")
	_ = mustFindRun(t, plan, collectionRootTemplate, "")
	_ = mustFindRun(t, plan, collectionRootSecondary, "email")
	_ = mustFindRun(t, plan, collectionRootSecondary, "city")
}

func TestEmitGroupedSecondaryRunTableSortsValueGroupsByDocumentID(t *testing.T) {
	items := []insertBatchItem{
		{id: []byte("u3"), state: orderedDocumentIndexState{{[]byte("s:city-01")}}},
		{id: []byte("u1"), state: orderedDocumentIndexState{{[]byte("s:city-01")}}},
		{id: []byte("u4"), state: orderedDocumentIndexState{{[]byte("s:city-00")}}},
		{id: []byte("u2"), state: orderedDocumentIndexState{{[]byte("s:city-00")}}},
	}
	order := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	entryCount, keyBytes, alreadySorted, err := secondaryEntryOrderStats(items, 0, order)
	if err != nil {
		t.Fatalf("secondary stats: %v", err)
	}
	if alreadySorted {
		t.Fatal("test data should exercise grouped unsorted construction")
	}
	table, ok, err := (insertBatchPlanner{collection: "users"}).emitGroupedSecondaryRunTable(items, 0, "city", IndexValueString, order, entryCount, keyBytes)
	if err != nil {
		t.Fatalf("grouped secondary run: %v", err)
	}
	if !ok {
		t.Fatal("expected grouped secondary construction")
	}
	entries := collectRunEntries(t, collectionRootRun{name: "city", table: table})
	assertSortedEntries(t, entries)

	want := make([][]byte, 0, 4)
	for _, pair := range []struct {
		encoded    []byte
		documentID []byte
	}{
		{[]byte("s:city-00"), []byte("u2")},
		{[]byte("s:city-00"), []byte("u4")},
		{[]byte("s:city-01"), []byte("u1")},
		{[]byte("s:city-01"), []byte("u3")},
	} {
		key, err := indexEntryKey(pair.encoded, pair.documentID)
		if err != nil {
			t.Fatalf("expected key: %v", err)
		}
		want = append(want, key)
	}
	if got := entryKeys(entries); !byteMatrixEqual(got, want) {
		t.Fatalf("grouped keys=%q want %q", got, want)
	}
}

func TestEmitSecondaryRunsPreservesInputOrderSortedFastPath(t *testing.T) {
	items := []insertBatchItem{
		{id: []byte("u2"), state: orderedDocumentIndexState{{[]byte("s:city-00")}}},
		{id: []byte("u1"), state: orderedDocumentIndexState{{[]byte("s:city-01")}}},
	}
	primaryOrder := sortedItemOrderByKey(items, func(item *insertBatchItem) []byte { return item.id })
	if primaryOrder == nil {
		t.Fatal("test data should have different primary-key order")
	}
	planner := insertBatchPlanner{collection: "users"}
	plan := &insertBatchPlan{}
	runtimes := []indexRuntime{{
		def: indexDefinition{name: "city", field: "city", valueType: IndexValueString},
	}}
	if err := planner.emitSecondaryRuns(plan, items, runtimes, primaryOrder); err != nil {
		t.Fatalf("emit secondary runs: %v", err)
	}
	if got, want := plan.stats.SecondarySortedRuns, 1; got != want {
		t.Fatalf("sorted runs=%d want %d", got, want)
	}
	if got := plan.stats.SecondaryUnsortedRuns; got != 0 {
		t.Fatalf("unsorted runs=%d want 0", got)
	}
	entries := collectRunEntries(t, mustFindRun(t, plan, collectionRootSecondary, "city"))
	assertSortedEntries(t, entries)
}

func TestInsertBatchPlanner_PreservesCallerVisibleResultOrdering(t *testing.T) {
	planner := insertBatchPlanner{collection: "users"}
	ids := [][]byte{[]byte("u3"), []byte("u1"), []byte("u2")}

	plan, err := planner.planInsertBatch(ids, [][]byte{
		[]byte(`{"name":"third"}`),
		[]byte(`{"name":"first"}`),
		[]byte(`{"name":"second"}`),
	})
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if len(plan.resultIDs) != len(ids) {
		t.Fatalf("result ids len=%d want %d", len(plan.resultIDs), len(ids))
	}
	for i := range ids {
		if !bytes.Equal(plan.resultIDs[i], ids[i]) {
			t.Fatalf("result id[%d]=%q want %q", i, plan.resultIDs[i], ids[i])
		}
	}

	primary := mustFindRun(t, plan, collectionRootPrimary, "")
	primaryEntries := collectRunEntries(t, primary)
	assertEntryKeys(t, primaryEntries, "u1", "u2", "u3")
	if got, want := plan.stats.payloadBuilds, 3; got != want {
		t.Fatalf("payload builds=%d want %d", got, want)
	}
}

func TestEncodeNormalizedDocumentIndexStateMatchesConservativeEncoder(t *testing.T) {
	state := documentIndexState{
		"city":  {[]byte("s:hnl")},
		"email": {[]byte("s:ada@example.com")},
	}
	want, err := encodeDocumentIndexState(cloneDocumentIndexState(state))
	if err != nil {
		t.Fatalf("encode conservative index state: %v", err)
	}
	got, err := encodeNormalizedDocumentIndexState(cloneDocumentIndexState(state))
	if err != nil {
		t.Fatalf("encode normalized index state: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("normalized encoding mismatch\n got: %x\nwant: %x", got, want)
	}
}

func TestAppendIndexScalarEncodesIntoArena(t *testing.T) {
	var arena []byte
	tests := []struct {
		name      string
		valueType IndexValueType
		value     any
		want      []byte
	}{
		{name: "string", valueType: IndexValueString, value: "ada", want: []byte{'a', 'd', 'a', 0x00, 0x00}},
		{name: "string nul", valueType: IndexValueString, value: "a\x00b", want: []byte{'a', 0x00, 0xff, 'b', 0x00, 0x00}},
		{name: "bool true", valueType: IndexValueBool, value: true, want: []byte{0x01}},
		{name: "bool false", valueType: IndexValueBool, value: false, want: []byte{0x00}},
		{name: "int64", valueType: IndexValueInt64, value: int64(0), want: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{name: "double zero", valueType: IndexValueDouble, value: float64(0), want: []byte{0x02, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := len(arena)
			var encoded []byte
			var err error
			arena, encoded, err = appendIndexScalar(arena, tc.valueType, tc.value)
			if err != nil {
				t.Fatalf("append index scalar: %v", err)
			}
			if !bytes.Equal(encoded, tc.want) {
				t.Fatalf("encoded=%x want %x", encoded, tc.want)
			}
			if !bytes.Equal(encoded, arena[before:]) {
				t.Fatalf("encoded slice is not backed by arena tail")
			}
		})
	}
}

func TestAppendIndexScalarRejectsPlatformInt(t *testing.T) {
	_, _, err := appendIndexScalar(nil, IndexValueInt64, int(42))
	if err == nil || !strings.Contains(err.Error(), "must be int64-compatible") {
		t.Fatalf("appendIndexScalar(int) err=%v want unsupported int", err)
	}
}

func TestTypedIndexScalarOrdering(t *testing.T) {
	requireEncodedIndexOrder(t, IndexValueString, "", "a", "a\x00", "aa", "b", "ba")
	requireEncodedIndexOrder(t, IndexValueBool, false, true)
	requireEncodedIndexOrder(t, IndexValueInt64,
		int64(math.MinInt64),
		int64(-10),
		int64(-2),
		int64(0),
		int64(2),
		int64(10),
		int64(math.MaxInt64),
	)
	requireEncodedIndexOrder(t, IndexValueDouble,
		math.NaN(),
		math.Inf(-1),
		float64(-10),
		math.Copysign(0, -1),
		float64(2),
		float64(10),
		math.Inf(1),
	)

	negZero := mustEncodeTestIndexScalar(t, IndexValueDouble, math.Copysign(0, -1))
	posZero := mustEncodeTestIndexScalar(t, IndexValueDouble, float64(0))
	if !bytes.Equal(negZero, posZero) {
		t.Fatalf("double -0 encoded %x want +0 encoding %x", negZero, posZero)
	}
	nanA := mustEncodeTestIndexScalar(t, IndexValueDouble, math.Float64frombits(0x7ff8000000000001))
	nanB := mustEncodeTestIndexScalar(t, IndexValueDouble, math.Float64frombits(0x7ff0000000000001))
	if !bytes.Equal(nanA, nanB) {
		t.Fatalf("NaN encodings differ: %x vs %x", nanA, nanB)
	}
	end := prefixEnd(mustEncodeTestIndexScalar(t, IndexValueString, "a\x00"))
	if end == nil || bytes.Compare(end, mustEncodeTestIndexScalar(t, IndexValueString, "a\x00")) <= 0 {
		t.Fatalf("prefixEnd for escaped string=%x", end)
	}
}

func TestIndexRangeScanBoundsDoubleNaN(t *testing.T) {
	start, end, empty, err := indexRangeScanBounds(IndexValueDouble, IndexRangeOptions{
		Lower: IndexRangeBound{Value: math.NaN(), Inclusive: false},
		Upper: IndexRangeBound{Unbounded: true},
	})
	if err != nil {
		t.Fatalf("> NaN bounds: %v", err)
	}
	if empty || !bytes.Equal(start, []byte{0x01}) || end != nil {
		t.Fatalf("> NaN start=%x end=%x empty=%v want [01,unbounded)", start, end, empty)
	}

	start, end, empty, err = indexRangeScanBounds(IndexValueDouble, IndexRangeOptions{
		Lower: IndexRangeBound{Value: math.NaN(), Inclusive: true},
		Upper: IndexRangeBound{Unbounded: true},
	})
	if err != nil {
		t.Fatalf(">= NaN bounds: %v", err)
	}
	if empty || !bytes.Equal(start, []byte{0x00}) || end != nil {
		t.Fatalf(">= NaN start=%x end=%x empty=%v want [00,unbounded)", start, end, empty)
	}

	start, end, empty, err = indexRangeScanBounds(IndexValueDouble, IndexRangeOptions{
		Lower: IndexRangeBound{Unbounded: true},
		Upper: IndexRangeBound{Value: math.NaN(), Inclusive: true},
	})
	if err != nil {
		t.Fatalf("<= NaN bounds: %v", err)
	}
	if empty || start != nil || !bytes.Equal(end, []byte{0x01}) {
		t.Fatalf("<= NaN start=%x end=%x empty=%v want [unbounded,01)", start, end, empty)
	}
}

func TestIndexRangeScanBoundsTypedScalars(t *testing.T) {
	encoded := func(valueType IndexValueType, value any) []byte {
		t.Helper()
		return mustEncodeTestIndexScalar(t, valueType, value)
	}
	prefixAfter := func(valueType IndexValueType, value any) []byte {
		t.Helper()
		out := prefixEnd(encoded(valueType, value))
		if out == nil {
			t.Fatalf("prefixEnd returned nil for %s %v", valueType, value)
		}
		return out
	}
	tests := []struct {
		name      string
		valueType IndexValueType
		opts      IndexRangeOptions
		wantStart []byte
		wantEnd   []byte
		wantEmpty bool
	}{
		{
			name:      "string lower inclusive",
			valueType: IndexValueString,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "m", Inclusive: true},
				Upper: IndexRangeBound{Unbounded: true},
			},
			wantStart: encoded(IndexValueString, "m"),
		},
		{
			name:      "string lower exclusive",
			valueType: IndexValueString,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "m", Inclusive: false},
				Upper: IndexRangeBound{Unbounded: true},
			},
			wantStart: prefixAfter(IndexValueString, "m"),
		},
		{
			name:      "string upper inclusive",
			valueType: IndexValueString,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: "m", Inclusive: true},
			},
			wantEnd: prefixAfter(IndexValueString, "m"),
		},
		{
			name:      "string upper exclusive",
			valueType: IndexValueString,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Unbounded: true},
				Upper: IndexRangeBound{Value: "m", Inclusive: false},
			},
			wantEnd: encoded(IndexValueString, "m"),
		},
		{
			name:      "bool closed",
			valueType: IndexValueBool,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: false, Inclusive: true},
				Upper: IndexRangeBound{Value: true, Inclusive: true},
			},
			wantStart: encoded(IndexValueBool, false),
			wantEnd:   prefixAfter(IndexValueBool, true),
		},
		{
			name:      "int64 equality",
			valueType: IndexValueInt64,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(10), Inclusive: true},
			},
			wantStart: encoded(IndexValueInt64, int64(10)),
			wantEnd:   prefixAfter(IndexValueInt64, int64(10)),
		},
		{
			name:      "int64 empty half-open equality",
			valueType: IndexValueInt64,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
				Upper: IndexRangeBound{Value: int64(10), Inclusive: false},
			},
			wantEmpty: true,
		},
		{
			name:      "double infinities",
			valueType: IndexValueDouble,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: math.Inf(-1), Inclusive: true},
				Upper: IndexRangeBound{Value: math.Inf(1), Inclusive: true},
			},
			wantStart: encoded(IndexValueDouble, math.Inf(-1)),
			wantEnd:   prefixAfter(IndexValueDouble, math.Inf(1)),
		},
		{
			name:      "double negative zero exclusive",
			valueType: IndexValueDouble,
			opts: IndexRangeOptions{
				Lower: IndexRangeBound{Value: math.Copysign(0, -1), Inclusive: false},
				Upper: IndexRangeBound{Value: float64(1), Inclusive: false},
			},
			wantStart: prefixAfter(IndexValueDouble, float64(0)),
			wantEnd:   encoded(IndexValueDouble, float64(1)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start, end, empty, err := indexRangeScanBounds(tc.valueType, tc.opts)
			if err != nil {
				t.Fatalf("indexRangeScanBounds: %v", err)
			}
			if empty != tc.wantEmpty {
				t.Fatalf("empty=%v want %v start=%x end=%x", empty, tc.wantEmpty, start, end)
			}
			if !bytes.Equal(start, tc.wantStart) || !bytes.Equal(end, tc.wantEnd) {
				t.Fatalf("bounds start=%x end=%x want start=%x end=%x", start, end, tc.wantStart, tc.wantEnd)
			}
		})
	}
}

func requireEncodedIndexOrder(t *testing.T, valueType IndexValueType, values ...any) {
	t.Helper()
	var prev []byte
	for i, value := range values {
		encoded := mustEncodeTestIndexScalar(t, valueType, value)
		if i > 0 && bytes.Compare(prev, encoded) >= 0 {
			t.Fatalf("%s encoded order values[%d]=%v encoded=%x after prev=%x", valueType, i, value, encoded, prev)
		}
		prev = encoded
	}
}

func stringSliceEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestOrderedIndexStateForDocumentHandlesScalarAndArrayValues(t *testing.T) {
	scalarRuntime := []indexRuntime{{
		def:  indexDefinition{name: "email", field: "email", valueType: IndexValueString},
		path: []string{"email"},
	}}
	scalarState, err := orderedIndexStateForDocument([]byte(`{"email":"ada@example.com"}`), scalarRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("scalar index state: %v", err)
	}
	if got, want := scalarState.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "ada@example.com")}; !byteMatrixEqual(got, want) {
		t.Fatalf("scalar values=%q want %q", got, want)
	}

	arrayRuntime := []indexRuntime{{
		def:  indexDefinition{name: "tags", field: "tags", valueType: IndexValueString, multiKey: true},
		path: []string{"tags"},
	}}
	arrayState, err := orderedIndexStateForDocument([]byte(`{"tags":["b",null,"a","a"]}`), arrayRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("array index state: %v", err)
	}
	if got, want := arrayState.valuesAt(0), [][]byte{
		mustEncodeTestIndexScalar(t, IndexValueString, "a"),
		mustEncodeTestIndexScalar(t, IndexValueString, "b"),
	}; !byteMatrixEqual(got, want) {
		t.Fatalf("array values=%q want %q", got, want)
	}

	nullArrayState, err := orderedIndexStateForDocument([]byte(`{"tags":[null,null]}`), arrayRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("null array index state: %v", err)
	}
	if got := nullArrayState.valuesAt(0); len(got) != 0 {
		t.Fatalf("null array values=%q want none", got)
	}

	nestedArrayRuntime := []indexRuntime{{
		def:  indexDefinition{name: "tags", field: "profile.tags", valueType: IndexValueString, multiKey: true},
		path: []string{"profile", "tags"},
	}}
	nestedArrayState, err := orderedIndexStateForDocument([]byte(`{"profile":{"tags":["b",null,"a","a"]}}`), nestedArrayRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("nested array index state: %v", err)
	}
	if got, want := nestedArrayState.valuesAt(0), [][]byte{
		mustEncodeTestIndexScalar(t, IndexValueString, "a"),
		mustEncodeTestIndexScalar(t, IndexValueString, "b"),
	}; !byteMatrixEqual(got, want) {
		t.Fatalf("nested array values=%q want %q", got, want)
	}

	_, err = orderedIndexStateForDocument([]byte(`{"tags":["a"]}`), []indexRuntime{{
		def:  indexDefinition{name: "tags", field: "tags", valueType: IndexValueString},
		path: []string{"tags"},
	}}, collectionOptions{})
	if err == nil || !strings.Contains(err.Error(), "array value not allowed") {
		t.Fatalf("err=%v want array value not allowed", err)
	}
}

func TestOrderedIndexStateForDocumentPreservesLargeIntegerNumbers(t *testing.T) {
	rootRuntime := []indexRuntime{{
		def:  indexDefinition{name: "big", field: "big", valueType: IndexValueInt64},
		path: []string{"big"},
	}}
	rootState, err := orderedIndexStateForDocument([]byte(`{"big":9007199254740993}`), rootRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("root large int index state: %v", err)
	}
	if got, want := rootState.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueInt64, int64(9007199254740993))}; !byteMatrixEqual(got, want) {
		t.Fatalf("root large int values=%q want %q", got, want)
	}

	nestedRuntime := []indexRuntime{{
		def:  indexDefinition{name: "big", field: "nested.big", valueType: IndexValueInt64},
		path: []string{"nested", "big"},
	}}
	nestedState, err := orderedIndexStateForDocument([]byte(`{"nested":{"big":9007199254740993}}`), nestedRuntime, collectionOptions{})
	if err != nil {
		t.Fatalf("nested large int index state: %v", err)
	}
	if got, want := nestedState.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueInt64, int64(9007199254740993))}; !byteMatrixEqual(got, want) {
		t.Fatalf("nested large int values=%q want %q", got, want)
	}
}

func TestOrderedIndexStateForDocumentJSONRootFastPathPreservesFieldSemantics(t *testing.T) {
	planner := insertBatchPlanner{
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString},
			{name: "literal", field: "a*b", valueType: IndexValueString},
		},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	state, err := orderedIndexStateForDocument([]byte(`{"email":"first","a\u002ab":"escaped","a*b":"literal","email":"second","axb":"wild"}`), runtimes, collectionOptions{})
	if err != nil {
		t.Fatalf("root fast path index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "second")}; !byteMatrixEqual(got, want) {
		t.Fatalf("duplicate email values=%q want %q", got, want)
	}
	if got, want := state.valuesAt(1), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "literal")}; !byteMatrixEqual(got, want) {
		t.Fatalf("literal field values=%q want %q", got, want)
	}
}

func TestOrderedIndexStateForDocumentJSONRootFastPathUnescapesStringValues(t *testing.T) {
	planner := insertBatchPlanner{
		indexes: []indexDefinition{{name: "email", field: "email", valueType: IndexValueString}},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	state, err := orderedIndexStateForDocument([]byte(`{"email":"ada\n@example.com"}`), runtimes, collectionOptions{})
	if err != nil {
		t.Fatalf("root fast path index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueString, "ada\n@example.com")}; !byteMatrixEqual(got, want) {
		t.Fatalf("escaped string values=%q want %q", got, want)
	}
}

func TestOrderedIndexStateForDocumentJSONRootFastPathRejectsOutOfRangeNumber(t *testing.T) {
	planner := insertBatchPlanner{
		indexes: []indexDefinition{{name: "score", field: "score", valueType: IndexValueDouble}},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	_, err = orderedIndexStateForDocument([]byte(`{"score":1e999}`), runtimes, collectionOptions{})
	if err == nil || !strings.Contains(err.Error(), "unsupported indexed JSON") {
		t.Fatalf("err=%v want unsupported indexed JSON number", err)
	}
}

func TestOrderedIndexStateForDocumentJSONRootFastPathAcceptsExponentDoubleOutsideInt64Range(t *testing.T) {
	planner := insertBatchPlanner{
		indexes: []indexDefinition{{name: "score", field: "score", valueType: IndexValueDouble}},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	state, err := orderedIndexStateForDocument([]byte(`{"score":1e20}`), runtimes, collectionOptions{})
	if err != nil {
		t.Fatalf("root fast path index state: %v", err)
	}
	if got, want := state.valuesAt(0), [][]byte{mustEncodeTestIndexScalar(t, IndexValueDouble, 1e20)}; !byteMatrixEqual(got, want) {
		t.Fatalf("exponent double values=%q want %q", got, want)
	}
}

func TestOrderedIndexStateForDocumentJSONRootFastPathRejectsInvalidJSON(t *testing.T) {
	planner := insertBatchPlanner{
		indexes: []indexDefinition{{name: "email", field: "email", valueType: IndexValueString}},
	}
	runtimes, err := planner.indexRuntimes()
	if err != nil {
		t.Fatalf("index runtimes: %v", err)
	}
	_, err = orderedIndexStateForDocument([]byte(`{"email":"ada"`), runtimes, collectionOptions{})
	if err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("err=%v want invalid JSON", err)
	}
}

func TestBuildUniqueProbeRunsMatchesEncodedPrefixOrdering(t *testing.T) {
	candidates := []uniqueProbeCandidate{
		{indexName: "email", encodedValue: []byte("s:zz"), documentID: []byte("u3")},
		{indexName: "email", encodedValue: []byte("s:a"), documentID: []byte("u1")},
		{indexName: "email", encodedValue: []byte("s:bbbb"), documentID: []byte("u2")},
	}
	runs, err := buildUniqueProbeRuns(candidates)
	if err != nil {
		t.Fatalf("build unique probe runs: %v", err)
	}
	if got, want := len(runs), 1; got != want {
		t.Fatalf("runs=%d want %d", got, want)
	}

	want := make([][]byte, 0, len(candidates))
	for _, candidate := range candidates {
		prefix, err := indexValuePrefix(candidate.encodedValue)
		if err != nil {
			t.Fatalf("index value prefix: %v", err)
		}
		want = append(want, prefix)
	}
	sort.Slice(want, func(i, j int) bool {
		return bytes.Compare(want[i], want[j]) < 0
	})
	if !byteMatrixEqual(runs[0].prefixes, want) {
		t.Fatalf("prefix order mismatch\n got: %q\nwant: %q", runs[0].prefixes, want)
	}
}

func TestInsertBatchPlanner_BuildsUniqueProbePrefixesOnlyForPersistedRoots(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
			{name: "username", field: "username", valueType: IndexValueString, unique: true},
		},
	}
	probe := &recordingRootSnapshotProbe{}
	plan, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{
			[]byte(`{"email":"ada@example.com","username":"ada"}`),
			[]byte(`{"email":"grace@example.com","username":"grace"}`),
		},
		insertBatchPreflight{
			snapshot: probe,
			uniqueIndexRootIDs: map[string]uint64{
				"email": 77,
			},
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got, want := len(plan.uniqueProbeRuns), 1; got != want {
		t.Fatalf("unique probe runs=%d want %d", got, want)
	}
	if got, want := len(plan.allUniqueProbeRuns), 2; got != want {
		t.Fatalf("all unique probe runs=%d want %d", got, want)
	}
	if got, want := plan.uniqueProbeRuns[0].indexName, "email"; got != want {
		t.Fatalf("unique probe index=%q want %q", got, want)
	}
	if got, want := len(plan.uniqueProbeRuns[0].prefixes), 2; got != want {
		t.Fatalf("unique probe prefixes=%d want %d", got, want)
	}
	for _, prefix := range plan.uniqueProbeRuns[0].prefixes {
		if bytes.Contains(prefix, []byte("u1")) || bytes.Contains(prefix, []byte("u2")) {
			t.Fatalf("unique probe prefix contains a document id: %q", prefix)
		}
	}
	if got, want := probe.hasPrefixesCalls, 1; got != want {
		t.Fatalf("HasPrefixesAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.lastHasPrefixesRootID, uint64(77); got != want {
		t.Fatalf("HasPrefixesAtRoot root=%d want %d", got, want)
	}

	noRootProbe := &recordingRootSnapshotProbe{}
	planWithoutPersistedRoots, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u3")},
		[][]byte{[]byte(`{"email":"katherine@example.com","username":"katherine"}`)},
		insertBatchPreflight{snapshot: noRootProbe},
	)
	if err != nil {
		t.Fatalf("plan insert batch without persisted roots: %v", err)
	}
	if !planWithoutPersistedRoots.allUniqueProbeRunsBuilt {
		t.Fatal("all unique probe runs were not built")
	}
	if got, want := len(planWithoutPersistedRoots.allUniqueProbeRuns), 2; got != want {
		t.Fatalf("all unique probe runs without persisted roots=%d want %d", got, want)
	}
	if noRootProbe.hasPrefixesCalls != 0 {
		t.Fatalf("HasPrefixesAtRoot calls without persisted roots=%d want 0", noRootProbe.hasPrefixesCalls)
	}
}

func TestInsertBatchPlanner_SingleDirectBufferedInsertPlanPreservesIndexedSemantics(t *testing.T) {
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
			{name: "city", field: "city", valueType: IndexValueString, multiKey: true},
		},
		buildPrimaryVal:    clonePrimaryDocument,
		directBufferedRuns: true,
	}

	plan, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"ada@example.com","city":["hnl","hnl","iad"]}`)},
		insertBatchPreflight{
			snapshot: &recordingRootSnapshotProbe{},
			uniqueIndexRootIDs: map[string]uint64{
				"email": 77,
			},
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got := len(plan.runs); got != 0 {
		t.Fatalf("runs len=%d want 0 for direct-buffered plan", got)
	}
	if plan.directBufferedInsert == nil {
		t.Fatal("missing direct-buffered insert plan")
	}
	direct := plan.directBufferedInsert
	if got, want := direct.rootNames, []string{"users/primary", "users/index-state", "users/index/email", "users/index/city"}; !stringSliceEqual(got, want) {
		t.Fatalf("root names=%v want %v", got, want)
	}
	if got, want := len(direct.primaryEntries), 1; got != want {
		t.Fatalf("primary entries=%d want %d", got, want)
	}
	if got, want := string(direct.primaryEntries[0].key), "u1"; got != want {
		t.Fatalf("primary key=%q want %q", got, want)
	}
	if got, want := string(direct.primaryEntries[0].value), `{"email":"ada@example.com","city":["hnl","hnl","iad"]}`; got != want {
		t.Fatalf("primary value=%q want %q", got, want)
	}
	if got, want := len(direct.indexStateEntries), 1; got != want {
		t.Fatalf("index-state entries=%d want %d", got, want)
	}
	if len(direct.indexStateEntries[0].value) == 0 {
		t.Fatal("index-state entry is empty")
	}
	if got, want := len(direct.secondaryRootPlans), 2; got != want {
		t.Fatalf("secondary root plans=%d want %d", got, want)
	}
	if got, want := len(direct.secondaryRootPlans[0].entries), 1; got != want {
		t.Fatalf("email secondary entries=%d want %d", got, want)
	}
	if got, want := len(direct.secondaryRootPlans[1].entries), 2; got != want {
		t.Fatalf("city secondary entries=%d want %d", got, want)
	}
	if got, want := len(plan.allUniqueProbeRuns), 1; got != want {
		t.Fatalf("all unique probe runs=%d want %d", got, want)
	}
	if got, want := len(plan.allUniqueProbeRuns[0].prefixes), 1; got != want {
		t.Fatalf("all unique probe prefixes=%d want %d", got, want)
	}
	if got, want := len(plan.uniqueProbeRuns), 1; got != want {
		t.Fatalf("preflight unique probe runs=%d want %d", got, want)
	}
	if got, want := len(direct.uniqueValueRootPlans), 1; got != want {
		t.Fatalf("unique value root plans=%d want %d", got, want)
	}
	if got, want := len(direct.uniqueValueRootPlans[0].prefixes), 1; got != want {
		t.Fatalf("unique value root prefixes=%d want %d", got, want)
	}
}

func TestInsertBatchPlanner_SingleDirectBufferedInsertPlanRecordsDocumentPreflightStats(t *testing.T) {
	planner := insertBatchPlanner{
		collection:          "users",
		buildPrimaryVal:     clonePrimaryDocument,
		directBufferedRuns:  true,
		cachedIndexRuntimes: nil,
	}
	probe := &delayedRootSnapshotProbe{delay: 2 * time.Millisecond}
	plan, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"name":"ada"}`)},
		insertBatchPreflight{
			snapshot:      probe,
			primaryRootID: 9,
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if probe.hasAnySortedCalls != 1 {
		t.Fatalf("HasAnySortedAtRoot calls=%d want 1", probe.hasAnySortedCalls)
	}
	if got := plan.stats.DuplicateDocumentPreflight; got < probe.delay {
		t.Fatalf("DuplicateDocumentPreflight=%s want at least %s", got, probe.delay)
	}
}

func TestInsertBatchPreflightCheckUniqueConflictsSkipsMissingRoots(t *testing.T) {
	probe := &recordingRootSnapshotProbe{}
	preflight := insertBatchPreflight{
		snapshot: probe,
		uniqueIndexRootIDs: map[string]uint64{
			"email": 77,
		},
	}
	err := preflight.checkUniqueConflicts([]collectionUniqueProbeRun{
		{indexName: "username", prefixes: [][]byte{[]byte("ada")}},
		{indexName: "email", prefixes: [][]byte{[]byte("ada@example.com")}},
	})
	if err != nil {
		t.Fatalf("check unique conflicts: %v", err)
	}
	if got, want := probe.hasPrefixesCalls, 1; got != want {
		t.Fatalf("HasPrefixesAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.lastHasPrefixesRootID, uint64(77); got != want {
		t.Fatalf("HasPrefixesAtRoot root=%d want %d", got, want)
	}
}

func TestCollectionDocumentsFromDirectInsertPlanUsesPrimaryOrder(t *testing.T) {
	plan := &insertBatchPlan{
		resultIDs: [][]byte{[]byte("u2"), []byte("u1")},
		directBufferedInsert: &directBufferedInsertPlan{
			primaryRootName: collectionPrimaryRootName("users"),
			primaryEntries: []directBufferedRootEntry{
				{key: []byte("u1"), value: []byte(`{"name":"Ada"}`), flags: node.FlagInline},
				{key: []byte("u2"), value: []byte(`{"name":"Grace"}`), flags: node.FlagInline},
			},
		},
	}
	docs, err := collectionDocumentsFromInsertPlan(plan, collectionPrimaryRootName("users"))
	if err != nil {
		t.Fatalf("collectionDocumentsFromInsertPlan: %v", err)
	}
	if got, want := len(docs), 2; got != want {
		t.Fatalf("docs=%d want %d", got, want)
	}
	if string(docs[0].ID) != "u1" || string(docs[1].ID) != "u2" {
		t.Fatalf("doc order=%q,%q want primary order u1,u2", docs[0].ID, docs[1].ID)
	}
}

func TestInsertBatchPlanCheckPersistedConflictsRejectsMissingInputs(t *testing.T) {
	tests := []struct {
		name    string
		plan    *insertBatchPlan
		catalog *collectionCatalog
		want    string
	}{
		{name: "plan", want: "missing plan"},
		{name: "snapshot", plan: &insertBatchPlan{}, catalog: &collectionCatalog{meta: CollectionMeta{Name: "users"}}, want: "missing snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan.checkPersistedConflicts(nil, tt.catalog)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("checkPersistedConflicts err=%v want %q", err, tt.want)
			}
		})
	}

	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()
	err = (&insertBatchPlan{}).checkPersistedConflicts(snap, nil)
	if err == nil || !strings.Contains(err.Error(), "missing catalog") {
		t.Fatalf("checkPersistedConflicts err=%v want missing catalog", err)
	}
}

func TestInsertBatchPlanCheckPersistedConflictsRejectsIncompleteDerivedInputs(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	catalog := &collectionCatalog{
		meta:  CollectionMeta{Name: "users"},
		roots: map[string]uint64{},
	}
	err = (&insertBatchPlan{resultIDs: [][]byte{[]byte("u1")}}).checkPersistedConflicts(snap, catalog)
	if err == nil || !strings.Contains(err.Error(), "missing primary keys") {
		t.Fatalf("checkPersistedConflicts err=%v want missing primary keys", err)
	}

	catalog.meta.Indexes = []IndexDefinition{{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true}}
	catalog.roots[collectionSecondaryRootName("users", "email")] = 2
	err = (&insertBatchPlan{
		resultIDs:   [][]byte{[]byte("u1")},
		primaryKeys: [][]byte{[]byte("u1")},
	}).checkPersistedConflicts(snap, catalog)
	if err == nil || !strings.Contains(err.Error(), "missing unique probe candidates") {
		t.Fatalf("checkPersistedConflicts err=%v want missing unique probe candidates", err)
	}

	err = (&insertBatchPlan{
		resultIDs:               [][]byte{[]byte("u1")},
		primaryKeys:             [][]byte{[]byte("u1")},
		allUniqueProbeRunsBuilt: true,
	}).checkPersistedConflicts(snap, catalog)
	if err != nil {
		t.Fatalf("checkPersistedConflicts with built empty probes: %v", err)
	}
}

func TestInsertBatchPlanner_FailFastDuplicatesBeforePayloadConstruction(t *testing.T) {
	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}

	_, err := planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u1")},
		[][]byte{[]byte(`{"email":"a@example.com"}`), []byte(`{"email":"b@example.com"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate document id") {
		t.Fatalf("err=%v want duplicate document id", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}

	builds = 0
	planner.indexes = []indexDefinition{{name: "email", field: "email", valueType: IndexValueString, unique: true}}
	_, err = planner.planInsertBatch(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"same@example.com"}`), []byte(`{"email":"same@example.com"}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want unique index conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_RejectsPersistedDocumentIDBeforePayloadConstruction(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	primary := newCollectionRunTable(1)
	primary.SetSteal([]byte("u1"), []byte(`{"email":"seed@example.com"}`))
	primary.Freeze()
	_, rootIDs, err := d.PublishOrderedRootGroup(nil, []backenddb.OrderedRootPublishInput{{
		Iter: primary.NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish primary seed: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}
	_, err = planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1"), []byte("u2")},
		[][]byte{[]byte(`{"email":"dup@example.com"}`), []byte(`{"email":"new@example.com"}`)},
		insertBatchPreflight{
			snapshot:      snap,
			primaryRootID: rootIDs[0],
		},
	)
	if err == nil || !strings.Contains(err.Error(), "document already exists") {
		t.Fatalf("err=%v want persisted document conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_RejectsPersistedUniqueValueBeforePayloadConstruction(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	seedKey, err := indexEntryKey(mustEncodeTestIndexScalar(t, IndexValueString, "seed@example.com"), []byte("seed"))
	if err != nil {
		t.Fatalf("seed index key: %v", err)
	}
	secondary := newCollectionRunTable(1)
	secondary.SetSteal(seedKey, nil)
	secondary.Freeze()
	_, rootIDs, err := d.PublishOrderedRootGroup(nil, []backenddb.OrderedRootPublishInput{{
		Iter: secondary.NewIterator(nil, nil),
	}})
	if err != nil {
		t.Fatalf("publish secondary seed: %v", err)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
		},
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}
	_, err = planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u1")},
		[][]byte{[]byte(`{"email":"seed@example.com"}`)},
		insertBatchPreflight{
			snapshot: snap,
			uniqueIndexRootIDs: map[string]uint64{
				"email": rootIDs[0],
			},
		},
	)
	if err == nil || !strings.Contains(err.Error(), "unique index") {
		t.Fatalf("err=%v want persisted unique conflict", err)
	}
	if builds != 0 {
		t.Fatalf("payload builds=%d want 0", builds)
	}
}

func TestInsertBatchPlanner_PreflightUsesRootBatchProbes(t *testing.T) {
	probe := &recordingRootSnapshotProbe{}
	builds := 0
	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
		},
		buildPrimaryVal: func(_, document []byte) ([]byte, error) {
			builds++
			return bytes.Clone(document), nil
		},
	}

	_, err := planner.planInsertBatchWithPreflight(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
		insertBatchPreflight{
			snapshot:      probe,
			primaryRootID: 42,
			uniqueIndexRootIDs: map[string]uint64{
				"email": 77,
			},
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}
	if got, want := probe.hasAnySortedCalls, 1; got != want {
		t.Fatalf("HasAnySortedAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.hasPrefixesCalls, 1; got != want {
		t.Fatalf("HasPrefixesAtRoot calls=%d want %d", got, want)
	}
	if got, want := probe.lastHasAnySortedRootID, uint64(42); got != want {
		t.Fatalf("HasAnySortedAtRoot root=%d want %d", got, want)
	}
	if got, want := probe.lastHasPrefixesRootID, uint64(77); got != want {
		t.Fatalf("HasPrefixesAtRoot root=%d want %d", got, want)
	}
	if got, want := byteMatrixStrings(probe.lastHasAnySortedKeys), []string{"u1", "u2"}; !equalStrings(got, want) {
		t.Fatalf("HasAnySortedAtRoot keys=%q want %q", got, want)
	}
	if got, want := len(probe.lastHasPrefixesPrefixes), 2; got != want {
		t.Fatalf("HasPrefixesAtRoot prefixes=%d want %d", got, want)
	}
	if builds != 2 {
		t.Fatalf("payload builds=%d want 2", builds)
	}
}

func TestInsertBatchPlanner_PublishesRunsThroughGroupedOrderedRootPublisher(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	planner := insertBatchPlanner{
		collection: "users",
		indexes: []indexDefinition{
			{name: "email", field: "email", valueType: IndexValueString, unique: true},
		},
	}
	plan, err := planner.planInsertBatch(
		[][]byte{[]byte("u2"), []byte("u1")},
		[][]byte{
			[]byte(`{"email":"grace@example.com"}`),
			[]byte(`{"email":"ada@example.com"}`),
		},
	)
	if err != nil {
		t.Fatalf("plan insert batch: %v", err)
	}

	rootIDs, err := plan.publishRootRuns(d, nil)
	if err != nil {
		t.Fatalf("publish root runs: %v", err)
	}
	if got, want := len(rootIDs), len(plan.runs); got != want {
		t.Fatalf("root IDs len=%d want %d", got, want)
	}

	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer func() { _ = snap.Close() }()

	primaryRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootPrimary, "")]
	entry, err := snap.GetEntryAtRoot(primaryRoot, []byte("u1"))
	if err != nil {
		t.Fatalf("primary u1 lookup: %v", err)
	}
	if got, want := string(entry.Value), `{"email":"ada@example.com"}`; got != want {
		t.Fatalf("primary u1 value=%q want %q", got, want)
	}

	stateRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootIndexState, "")]
	stateEntry, err := snap.GetEntryAtRoot(stateRoot, []byte("u2"))
	if err != nil {
		t.Fatalf("index-state u2 lookup: %v", err)
	}
	if len(stateEntry.Value) == 0 {
		t.Fatalf("index-state u2 is empty")
	}

	emailRoot := rootIDs[mustFindRunIndex(t, plan, collectionRootSecondary, "email")]
	it, err := snap.IteratorAtRoot(emailRoot, nil, nil)
	if err != nil {
		t.Fatalf("email index iterator: %v", err)
	}
	defer func() { _ = it.Close() }()
	seen := 0
	for it.Valid() {
		seen++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("email index iterator error: %v", err)
	}
	if got, want := seen, 2; got != want {
		t.Fatalf("email index entries=%d want %d", got, want)
	}
}

type runEntry struct {
	key   []byte
	value []byte
}

type recordingRootSnapshotProbe struct {
	hasAnySortedCalls       int
	hasPrefixesCalls        int
	lastHasAnySortedRootID  uint64
	lastHasPrefixesRootID   uint64
	lastHasAnySortedKeys    [][]byte
	lastHasPrefixesPrefixes [][]byte
}

type delayedRootSnapshotProbe struct {
	recordingRootSnapshotProbe
	delay time.Duration
}

func (p *delayedRootSnapshotProbe) HasAnySortedAtRoot(rootID uint64, keys [][]byte) (bool, error) {
	time.Sleep(p.delay)
	return p.recordingRootSnapshotProbe.HasAnySortedAtRoot(rootID, keys)
}

func (p *recordingRootSnapshotProbe) HasAnySortedAtRoot(rootID uint64, keys [][]byte) (bool, error) {
	p.hasAnySortedCalls++
	p.lastHasAnySortedRootID = rootID
	p.lastHasAnySortedKeys = cloneByteMatrix(keys)
	return false, nil
}

func (p *recordingRootSnapshotProbe) HasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error) {
	p.hasPrefixesCalls++
	p.lastHasPrefixesRootID = rootID
	p.lastHasPrefixesPrefixes = cloneByteMatrix(prefixes)
	return make([]bool, len(prefixes)), nil
}

func mustFindRun(t *testing.T, plan *insertBatchPlan, kind collectionRootKind, indexName string) collectionRootRun {
	t.Helper()
	for _, run := range plan.runs {
		if run.kind == kind && run.indexName == indexName {
			return run
		}
	}
	t.Fatalf("missing run kind=%d index=%q", kind, indexName)
	return collectionRootRun{}
}

func mustFindRunIndex(t *testing.T, plan *insertBatchPlan, kind collectionRootKind, indexName string) int {
	t.Helper()
	if idx := findRunIndex(plan, kind, indexName); idx >= 0 {
		return idx
	}
	t.Fatalf("missing run kind=%d index=%q", kind, indexName)
	return -1
}

func findRunIndex(plan *insertBatchPlan, kind collectionRootKind, indexName string) int {
	if plan == nil {
		return -1
	}
	for i, run := range plan.runs {
		if run.kind == kind && run.indexName == indexName {
			return i
		}
	}
	return -1
}

func collectRunEntries(t *testing.T, run collectionRootRun) []runEntry {
	t.Helper()
	if run.table == nil {
		t.Fatalf("run %q has nil table", run.name)
	}
	it := run.table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	var entries []runEntry
	for it.Valid() {
		entries = append(entries, runEntry{
			key:   it.KeyCopy(nil),
			value: it.ValueCopy(nil),
		})
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterate run %q: %v", run.name, err)
	}
	return entries
}

func assertEntryKeys(t *testing.T, entries []runEntry, want ...string) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("entries len=%d want %d keys=%q", len(entries), len(want), entryKeys(entries))
	}
	for i := range want {
		if got := string(entries[i].key); got != want[i] {
			t.Fatalf("entry key[%d]=%q want %q; all keys=%q", i, got, want[i], entryKeys(entries))
		}
	}
}

func assertSortedEntries(t *testing.T, entries []runEntry) {
	t.Helper()
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].key, entries[i].key) > 0 {
			t.Fatalf("entries not sorted: %q", entryKeys(entries))
		}
	}
}

func entryKeys(entries []runEntry) [][]byte {
	keys := make([][]byte, len(entries))
	for i := range entries {
		keys[i] = entries[i].key
	}
	return keys
}

func cloneByteMatrix(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}

func byteMatrixEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func byteMatrixStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = string(in[i])
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
