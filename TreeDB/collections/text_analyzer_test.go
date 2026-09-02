package collections

import (
	"errors"
	"slices"
	"testing"
)

func TestAnalyzeTextToSinkParity2626(t *testing.T) {
	cases := []string{
		"",
		"Refund REFUND http_500 café",
		"  punctuation!!! separates--tokens 42 ",
		"emoji🙂split snake_case MIXED_Case",
	}
	for _, text := range cases {
		t.Run(text, func(t *testing.T) {
			want, err := AnalyzeText(TextAnalyzerSimple, text)
			if err != nil {
				t.Fatalf("AnalyzeText: %v", err)
			}
			var got []TextToken
			if err := AnalyzeTextToSink(TextAnalyzerSimple, text, TextTokenSinkFunc(func(token TextToken) error {
				got = append(got, token)
				return nil
			})); err != nil {
				t.Fatalf("AnalyzeTextToSink: %v", err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("sink tokens=%+v want %+v", got, want)
			}
		})
	}
}

func TestAnalyzeTextToSinkPropagatesSinkError2626(t *testing.T) {
	want := errors.New("stop")
	err := AnalyzeTextToSink(TextAnalyzerSimple, "refund policy", TextTokenSinkFunc(func(TextToken) error {
		return want
	}))
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want %v", err, want)
	}
}

func TestAnalyzeTextIndexFieldUsesCompactAccumulator2626(t *testing.T) {
	var b []byte
	for i := 0; i < textTermAccumulatorInlineTerms+4; i++ {
		if i > 0 {
			b = append(b, ' ')
		}
		b = append(b, 't', byte('a'+i))
	}
	field, ok, err := analyzeTextIndexField(TextIndexDefinition{Analyzer: TextAnalyzerSimple, StorePositions: true, StoreOffsets: true}, "body", string(b))
	if err != nil || !ok {
		t.Fatalf("analyze field ok=%v err=%v", ok, err)
	}
	if field.Length != uint32(textTermAccumulatorInlineTerms+4) || len(field.Terms) != textTermAccumulatorInlineTerms+4 {
		t.Fatalf("field length=%d terms=%d", field.Length, len(field.Terms))
	}
	term := field.Terms["ta"]
	if term == nil || term.Frequency != 1 || !slices.Equal(term.Positions, []uint32{0}) || len(term.Offsets) != 1 {
		t.Fatalf("term ta=%+v", term)
	}
}
