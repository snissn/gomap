package collections

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
	"unsafe"

	"github.com/buger/jsonparser"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type normalizerGrowthTestSink struct {
	admit  func(uint64) error
	tokens []TextToken
	err    error
}

func (s *normalizerGrowthTestSink) textNormalizerAdmission() func(uint64) error {
	return s.admit
}

func (s *normalizerGrowthTestSink) AddTextToken(token TextToken) error {
	if s.err != nil {
		return s.err
	}
	s.tokens = append(s.tokens, token)
	return nil
}

func TestTextNormalizerGrowthBeforeEffect(t *testing.T) {
	denied := errors.New("original normalizer admission refusal")
	for _, later := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial", true: "later"}[later], func(t *testing.T) {
			var builder strings.Builder
			if later {
				builder.Grow(1)
				for builder.Len() < builder.Cap() {
					builder.WriteByte('a')
				}
			}
			before, capacity := builder.String(), builder.Cap()
			var requested uint64
			admit := func(n uint64) error {
				requested = n
				if builder.String() != before || builder.Cap() != capacity {
					panic("normalizer grew before original admission")
				}
				return denied
			}
			if allocs := testing.AllocsPerRun(8, func() {
				if err := growTextNormalizer(&builder, '界', admit); err != denied {
					panic("normalizer refusal identity lost")
				}
			}); allocs != 0 {
				t.Fatalf("refused Builder growth allocated %g", allocs)
			}
			if requested != memtable.COWAllocationCharge(uint64(2*capacity+3)) || builder.Cap() != capacity || builder.String() != before || unsafe.StringData(builder.String()) != unsafe.StringData(before) {
				t.Fatal("refusal changed Builder backing/content or charged another target")
			}
		})
	}

	// Exercise actual Grow/WriteRune capacity at each width and many subsequent
	// growths. Admission observes the old Builder; each following write keeps
	// the admitted backing and cannot grow it independently.
	var builder strings.Builder
	var cohorts int
	for i := 0; i < 512; i++ {
		for _, r := range []rune{'a', 'é', '界', '𐐨'} {
			length, capacity := builder.Len(), builder.Cap()
			width := utf8.RuneLen(r)
			called := false
			if err := growTextNormalizer(&builder, r, func(n uint64) error {
				called = true
				cohorts++
				if builder.Len() != length || builder.Cap() != capacity || n != memtable.COWAllocationCharge(uint64(2*capacity+width)) {
					t.Fatal("growth not admitted against untouched original Builder")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if called != (capacity-length < width) {
				t.Fatal("missing or unnecessary Builder admission")
			}
			capacity = builder.Cap()
			backing := unsafe.StringData(builder.String())
			builder.WriteRune(r)
			if builder.Cap() != capacity || builder.Len() != length+width || length != 0 && unsafe.StringData(builder.String()) != backing {
				t.Fatal("WriteRune changed admitted backing")
			}
		}
	}
	if cohorts < 2 {
		t.Fatal("later Builder growth not exercised")
	}
}

func TestTextNormalizerGrowthLimits(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, dimensions := range [][3]int{
		{-1, 0, 1}, {1, 0, 1}, {0, -1, 1}, {0, 0, -1}, {0, 0, 0}, {0, 0, 5},
		{maxInt, maxInt, 1}, {maxInt / 2, maxInt / 2, 4},
	} {
		if _, err := textNormalizerGrowthCharge(dimensions[0], dimensions[1], dimensions[2]); err != memtable.ErrCOWCapacity {
			t.Fatalf("invalid or overflowing dimensions %v: %v", dimensions, err)
		}
	}
	// On 64-bit hosts an int-representable Builder target can still saturate Q.
	capacity := (maxInt - 1) / 2
	if memtable.COWAllocationCharge(uint64(2*capacity+1)) == ^uint64(0) {
		if _, err := textNormalizerGrowthCharge(capacity, capacity, 1); err != memtable.ErrCOWCapacity {
			t.Fatal("saturated allocation charge accepted")
		}
	}
	for width := 1; width <= utf8.UTFMax; width++ {
		charge, err := textNormalizerGrowthCharge(0, 0, width)
		if err != nil || charge != memtable.COWAllocationCharge(uint64(width)) {
			t.Fatalf("initial width %d charge=%d err=%v", width, charge, err)
		}
		if charge, err := textNormalizerGrowthCharge(4, 8, width); err != nil || charge != 0 {
			t.Fatalf("sufficient capacity width %d charge=%d err=%v", width, charge, err)
		}
	}
	for _, r := range []rune{-1, 0xD800, utf8.MaxRune + 1} {
		var builder strings.Builder
		if err := growTextNormalizer(&builder, r, func(uint64) error { t.Fatal("invalid rune reached admission"); return nil }); err != memtable.ErrCOWCapacity || builder.Cap() != 0 {
			t.Fatalf("invalid rune %U changed Builder: %v", r, err)
		}
	}
}

func TestTextNormalizerStoppedDuplicateAndSinkRefusal(t *testing.T) {
	denied := errors.New("original stage refusal")
	longStop := strings.Repeat("s", 80)
	for _, tc := range []struct {
		name, text string
		stopWords  []string
		refuse     int
		wantTokens int
	}{
		{"stopped-initial", "THE refund", []string{"the"}, 1, 0},
		{"stopped-later", longStop + " refund", []string{longStop}, 2, 0},
		{"duplicate", "refund refund", nil, 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var admitted uint64
			sink := &normalizerGrowthTestSink{admit: func(n uint64) error {
				calls++
				if calls == tc.refuse {
					return denied
				}
				admitted += n
				return nil
			}}
			err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, &TextAnalyzerOptions{StopWords: tc.stopWords}, tc.text, sink)
			if err != denied || calls != tc.refuse || len(sink.tokens) != tc.wantTokens || tc.refuse > 1 && admitted == 0 {
				t.Fatalf("calls=%d admitted=%d tokens=%+v err=%v", calls, admitted, sink.tokens, err)
			}
		})
	}
	var cohorts int
	var charged uint64
	sink := &normalizerGrowthTestSink{admit: func(n uint64) error { cohorts++; charged += n; return nil }}
	if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, &TextAnalyzerOptions{StopWords: []string{"the"}}, "THE THE refund refund", sink); err != nil {
		t.Fatal(err)
	}
	if cohorts != 4 || charged != 4*memtable.COWAllocationCharge(1) || !slices.Equal(sink.tokens, []TextToken{{Term: "refund", Position: 2, StartOffset: 8, EndOffset: 14}, {Term: "refund", Position: 3, StartOffset: 15, EndOffset: 21}}) {
		t.Fatalf("stopped/duplicate cohorts missing: %d/%d tokens=%+v", cohorts, charged, sink.tokens)
	}
	sink.err = errors.New("original downstream sink error")
	before := cohorts
	if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, &TextAnalyzerOptions{StopWords: []string{"the"}}, "THE refund policy", sink); err != sink.err || cohorts != before+2 {
		t.Fatalf("sink error identity or stop position lost: cohorts=%d err=%v", cohorts-before, err)
	}
}

func TestTextNormalizerOwnedTokenParity(t *testing.T) {
	options := &TextAnalyzerOptions{StopWords: []string{"THE"}}
	for _, input := range []string{"", "Ⱥa ÉCOLE 東京_9 𐐀 \xffZ_7 THE refund refund", strings.Repeat("Ⱥ", 200) + " " + strings.Repeat("A", 1024), "a\xed\xa0\x80b\xffc"} {
		raw := []byte(input)
		want, err := AnalyzeTextWithOptions(TextAnalyzerSimple, options, string(raw))
		if err != nil {
			t.Fatal(err)
		}
		for _, admitted := range []bool{false, true} {
			sink := &normalizerGrowthTestSink{}
			if admitted {
				sink.admit = func(uint64) error { return nil }
			}
			if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, options, string(raw), sink); err != nil || !slices.Equal(sink.tokens, want) {
				t.Fatalf("full token parity admitted=%v input=%q got=%+v want=%+v err=%v", admitted, input, sink.tokens, want, err)
			}
			owned := slices.Clone(sink.tokens)
			// Subsequent calls and overwritten source must not overwrite old
			// token strings (including same-length duplicates).
			if err := AnalyzeTextToSink(TextAnalyzerSimple, "replacement another", sink); err != nil || !slices.Equal(sink.tokens[:len(owned)], want) {
				t.Fatal("later analysis reused returned token backing")
			}
			clear(raw)
			if !slices.Equal(owned, want) {
				t.Fatal("returned terms borrowed source lifetime")
			}
			raw = []byte(input)
		}
	}
	var got normalizerGrowthTestSink
	got.admit = func(uint64) error { return nil }
	if err := AnalyzeTextToSink(TextAnalyzerSimple, "Ⱥa\xffÉCOLE", &got); err != nil || !slices.Equal(got.tokens, []TextToken{{Term: "ⱥa", Position: 0, StartOffset: 0, EndOffset: 3}, {Term: "école", Position: 1, StartOffset: 4, EndOffset: 10}}) {
		t.Fatalf("Unicode width expansion/invalid UTF8 offsets: %+v %v", got.tokens, err)
	}
}

func TestNativeTextNormalizerPreparedParityAndCharge(t *testing.T) {
	// Source-bound fixture ledger: all 34 short ASCII token cohorts, including
	// 31 discarded duplicate payload terms, cost Q(1) each. Input conversion
	// and owner/header promises remain independent; options are caller-admitted.
	content := "refund policy " + strings.Repeat("payload ", 32)
	def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple, Fields: []TextIndexField{{Field: "content"}}}
	values := []jsonParserIndexValue{{valueType: jsonparser.String, raw: []byte(content)}}
	var charged uint64
	state, err := nativeTextPrepareCurrent(def, values, func(n uint64) error { charged += n; return nil })
	var expected nativeTextAdmissionSum
	expected.array(1, uint64(unsafe.Sizeof(textDocumentStateValue{})))
	expected.array(1, uint64(unsafe.Sizeof(textDocumentFieldState{})))
	expected.array(uint64(len(content)), 1)
	expected.array(1, uint64(unsafe.Sizeof(textTermAccumulator{})))
	expected.add(34, memtable.COWAllocationCharge(1))
	if err != nil || len(content) != 270 || charged != expected.total || len(state.Fields) != 1 || len(state.Fields[0].Terms) != 3 || state.Fields[0].Length != 34 {
		t.Fatalf("prepared original callback ledger charged=%d expected=%d state=%+v err=%v", charged, expected.total, state, err)
	}
	for _, positions := range []bool{false, true} {
		for _, offsets := range []bool{false, true} {
			def.StorePositions, def.StoreOffsets = positions, offsets
			def.AnalyzerOptions = &TextAnalyzerOptions{StopWords: []string{strings.Repeat("S", 80), "the"}}
			content := strings.Repeat("s", 80) + " THE " + strings.Repeat("Ⱥ", 200) + " a\xffb refund refund alpha beta gamma delta epsilon zeta eta theta iota kappa"
			doc := append([]byte(`{"content":"`), []byte(content)...)
			doc = append(doc, []byte(`"}`)...)
			analysis, err := analyzeTextIndexDocument(def, doc)
			if err != nil {
				t.Fatal(err)
			}
			values := []jsonParserIndexValue{{valueType: jsonparser.String, raw: []byte(content)}}
			state, err := nativeTextPrepareCurrent(def, values, func(uint64) error { return nil })
			want := textDocumentStateValueFromAnalysis(analysis)
			wantBytes := encodeTextDocumentStateValue(want)
			if err != nil || !reflect.DeepEqual(state, want) || !bytes.Equal(encodeTextDocumentStateValue(state), wantBytes) {
				t.Fatalf("full state/bytes parity positions=%v offsets=%v err=%v", positions, offsets, err)
			}
			clear(doc)
			clear(values[0].raw)
			if !bytes.Equal(encodeTextDocumentStateValue(state), wantBytes) {
				t.Fatal("prepared term state borrowed input")
			}
		}
	}
}

func TestNativeTextNormalizerPreparedOriginalLeaseRefusal(t *testing.T) {
	for _, refuse := range []int{3, 4} {
		// Headers and input are the first two callbacks. The following calls
		// govern initial and subsequent Builder growth of a stopped token,
		// before any term/position sink can be invoked.
		budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		lease, err := budget.AcquireExternal(0)
		if err != nil {
			t.Fatal(err)
		}
		before := budget.Stats()
		var charged uint64
		calls := 0
		admit := func(n uint64) error {
			calls++
			if calls == refuse {
				return memtable.ErrCOWCapacity
			}
			if n > ^uint64(0)-charged {
				return memtable.ErrCOWCapacity
			}
			if err := lease.Resize(charged + n); err != nil {
				return err
			}
			charged += n
			return nil
		}
		stop := strings.Repeat("s", 80)
		def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple,
			Fields: []TextIndexField{{Field: "content"}}, AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{stop}}}
		// As in the native caller, options get their independent original-lease
		// promise before preparation; it is not part of the growth callback count.
		definition, err := nativeTextDefinitionCharge(def, 1)
		if err != nil || lease.Resize(definition) != nil {
			t.Fatal("could not admit options on original lease")
		}
		charged = definition
		state, err := nativeTextPrepareCurrent(def, []jsonParserIndexValue{{valueType: jsonparser.String, raw: []byte(stop + " refund")}}, admit)
		after := budget.Stats()
		if !errors.Is(err, memtable.ErrCOWCapacity) || calls != refuse || len(state.Fields) != 0 || charged <= definition || after.ExternalLeases != 1 || after.ReservedBytes != before.ReservedBytes+charged || after.ExternalBytes != before.ExternalBytes+charged {
			t.Errorf("refusal=%d calls=%d charged=%d before=%+v after=%+v state=%+v err=%v", refuse, calls, charged, before, after, state, err)
		}
		// The failed phase retains all successful cohorts until its original
		// lease closes; closure, not an error refund, releases that custody.
		lease.Close()
		budget.Close()
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("failed phase receipts at full close: %+v", stats)
		}
	}
}
