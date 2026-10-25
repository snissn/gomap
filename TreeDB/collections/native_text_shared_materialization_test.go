package collections

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeTextSharedMaterializationParity(t *testing.T) {
	for _, positions := range []bool{false, true} {
		for _, offsets := range []bool{false, true} {
			def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple,
				AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"the", "STOP"}},
				Fields:          []TextIndexField{{Field: "z"}, {Field: "a"}, {Field: "missing"}}, StorePositions: positions, StoreOffsets: offsets}
			for _, content := range []string{"The ÉCOLE école 東京 STOP _id １２ refund refund", "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu alpha", "", "a\xffb"} {
				// Raw invalid UTF-8 remains the same maintained analyzer input.
				doc := append([]byte(`{"z":"`), []byte(content)...)
				doc = append(doc, []byte(`","a":"policy policy"}`)...)
				analysis, err := analyzeTextIndexDocument(def, doc)
				if err != nil {
					t.Fatal(err)
				}
				values := make([]jsonParserIndexValue, len(def.Fields))
				if handled, err := selectTextIndexDocumentJSONRootValues(def, doc, values); !handled || err != nil {
					t.Fatalf("selection %v %v", handled, err)
				}
				var admitted uint64
				state, err := nativeTextPrepareCurrent(def, values, func(n uint64) error { admitted += n; return nil })
				if err != nil || admitted == 0 {
					t.Fatalf("prepare: %v", err)
				}
				want := encodeTextDocumentStateValue(textDocumentStateValueFromAnalysis(analysis))
				clear(doc)
				if got := encodeTextDocumentStateValue(state); !bytes.Equal(got, want) {
					t.Fatalf("state/encoder mismatch positions=%v offsets=%v input=%q", positions, offsets, content)
				}
			}
		}
	}
}

func TestNativeTextSharedMaterializationTransfer(t *testing.T) {
	for _, distinct := range []int{3, 20} {
		var charged uint64
		a := &textTermAccumulator{storePositions: true, storeOffsets: true, admit: func(n uint64) error { charged += n; return nil }}
		a.terms = a.inline[:0]
		for i := 0; i < distinct; i++ {
			if err := a.AddTextToken(TextToken{Term: string(rune('z' - i)), Position: i, StartOffset: 2 * i, EndOffset: 2*i + 1}); err != nil {
				t.Fatal(err)
			}
		}
		before := &a.terms[0]
		field := a.takeDocumentField("content")
		if &field.Terms[0] != before || a.terms != nil || a.lookup != nil || a.admit != nil || len(field.Terms) != distinct {
			t.Fatal("finalizer copied terms or retained active lookup/callback")
		}
		if !strings.HasPrefix(field.Terms[0].Term, string(rune('z'-distinct+1))) {
			t.Fatal("terms not canonical")
		}
		for _, term := range field.Terms {
			if term.Frequency != 1 || len(term.Positions) != 1 || len(term.Offsets) != 1 {
				t.Fatal("occurrence lanes lost")
			}
		}
		if charged == 0 {
			t.Fatal("growth was not admitted")
		}
	}
}

// Real token sink refusals must occur before ANY chosen growth allocation or
// mutation. This test checks existing backing identity/content and actual heap
// allocation, rather than copying the growth predicate into a parallel helper.
func TestNativeTextSharedMaterializationRefusal(t *testing.T) {
	denied := errors.New("original admission refusal")
	for _, scenario := range []struct {
		name               string
		positions, offsets bool
		unique, repeats    int
	}{
		{"position-first", true, false, 0, 0}, {"offset-first", false, true, 0, 0},
		{"position-growth", true, false, 1, 1}, {"offset-growth", false, true, 1, 1},
		{"terms-and-lookup-first", false, false, 8, 0}, {"lookup-rebuild", false, false, 16, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			a := &textTermAccumulator{storePositions: scenario.positions, storeOffsets: scenario.offsets, admit: func(uint64) error { return nil }}
			a.terms = a.inline[:0]
			for i := 0; i < scenario.unique; i++ {
				if err := a.AddTextToken(TextToken{Term: string(rune('a' + i)), Position: i}); err != nil {
					t.Fatal(err)
				}
			}
			var token TextToken
			if scenario.repeats > 0 {
				token = TextToken{Term: "a", Position: 1}
			} else {
				token = TextToken{Term: string(rune('a' + scenario.unique)), Position: scenario.unique}
			}
			before := make([]textAnalyzedTerm, len(a.terms))
			copy(before, a.terms)
			var backing *textAnalyzedTerm
			if len(a.terms) > 0 {
				backing = &a.terms[0]
			}
			length, capacity, lookupCapacity := a.length, cap(a.terms), a.lookupCapacity
			a.admit = func(uint64) error { return denied }
			if allocations := testing.AllocsPerRun(8, func() {
				if err := a.AddTextToken(token); err != denied {
					panic("refusal lost")
				}
			}); allocations != 0 {
				t.Fatalf("refused growth allocated %g", allocations)
			}
			if a.length != length || cap(a.terms) != capacity || a.lookupCapacity != lookupCapacity || !reflect.DeepEqual(a.terms, before) || backing != nil && &a.terms[0] != backing {
				t.Fatal("refused token mutated retained state")
			}
			a.admit = func(uint64) error { return nil }
			if err := a.AddTextToken(token); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeTextSharedMaterializationLimits(t *testing.T) {
	if _, err := textTermGrowthCapacity(int(^uint(0)>>1), int(^uint(0)>>1)); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("capacity overflow accepted")
	}
	for _, token := range []TextToken{{Position: -1}, {Position: 0, StartOffset: -1}, {Position: 0, StartOffset: 2, EndOffset: 1}} {
		a := &textTermAccumulator{storePositions: true, storeOffsets: true, admit: func(uint64) error { t.Fatal("invalid token reached admission"); return nil }}
		a.terms = a.inline[:0]
		if err := a.AddTextToken(token); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatal("uint32-incompatible token accepted")
		}
	}
	a := &textTermAccumulator{length: math.MaxUint32, admit: func(uint64) error { t.Fatal("length overflow reached admission"); return nil }}
	if err := a.AddTextToken(TextToken{Term: "a"}); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatal("length wrapped")
	}
}

func TestNativeTextSharedMaterializationDefinitions(t *testing.T) {
	doc := []byte(`{"content":"refund refund policy ÉCOLE"}`)
	var values [1]jsonParserIndexValue
	def := TextIndexDefinition{Version: TextIndexVersionV2, Analyzer: TextAnalyzerSimple, Fields: []TextIndexField{{Field: "content"}}, StorePositions: true, StoreOffsets: true}
	if handled, err := selectTextIndexDocumentJSONRootValues(def, doc, values[:]); !handled || err != nil {
		t.Fatal(err)
	}
	first, err := nativeTextPrepareCurrent(def, values[:], func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	firstBytes := encodeTextDocumentStateValue(first)
	def.AnalyzerOptions = &TextAnalyzerOptions{StopWords: []string{"refund"}}
	second, err := nativeTextPrepareCurrent(def, values[:], func(uint64) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	clear(doc)
	if !bytes.Equal(encodeTextDocumentStateValue(first), firstBytes) || bytes.Equal(encodeTextDocumentStateValue(second), firstBytes) {
		t.Fatal("definition-local state reused or analyzer options lost")
	}
}
