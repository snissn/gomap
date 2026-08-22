package chunking

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func mustChunk(t *testing.T, parentID, text string, cfg Config) []Chunk {
	t.Helper()
	chunks, err := SplitChunks(parentID, text, cfg)
	if err != nil {
		t.Fatalf("Chunk: %v", err)
	}
	return chunks
}

func marshalMap(t *testing.T, m map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal metadata map: %v", err)
	}
	return raw
}

func TestConfigValidationFailClosed(t *testing.T) {
	valid := Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 16, Overlap: 4}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := []struct {
		name string
		cfg  Config
	}{
		{"unknown strategy", Config{Strategy: "semantic", SizeUnit: SizeUnitRunes, Size: 16}},
		{"empty strategy", Config{Strategy: "", SizeUnit: SizeUnitRunes, Size: 16}},
		{"unknown size unit", Config{Strategy: StrategyFixedWindow, SizeUnit: "characters", Size: 16}},
		{"missing size unit", Config{Strategy: StrategyFixedWindow, SizeUnit: "", Size: 16}},
		{"zero size", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 0}},
		{"negative size", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: -1}},
		{"overlap negative", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 16, Overlap: -1}},
		{"overlap equals size", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 16, Overlap: 16}},
		{"overlap exceeds size", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 16, Overlap: 32}},
		{"recursive overlap equals size", Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 16, Overlap: 16}},
	}
	for _, tc := range cases {
		if err := tc.cfg.Validate(); err == nil {
			t.Fatalf("%s: expected fail-closed error, got nil", tc.name)
		}
	}
}

func TestChunkEmptyParentIDFailsClosed(t *testing.T) {
	cfg := Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 8, Overlap: 0}
	if _, err := SplitChunks("", "hello world", cfg); err == nil {
		t.Fatal("empty parent ID: expected fail-closed error")
	}
}

func TestFixedWindowCoversInputWithOverlap(t *testing.T) {
	text := strings.Repeat("abcdefghij", 10) // 100 runes
	const size, overlap = 30, 10
	chunks := mustChunk(t, "doc1", text, Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: size, Overlap: overlap})
	if len(chunks) < 2 {
		t.Fatalf("len(chunks)=%d want multiple chunks", len(chunks))
	}
	for i, ch := range chunks {
		if ch.Ordinal != i {
			t.Fatalf("chunk %d ordinal=%d", i, ch.Ordinal)
		}
		if len([]rune(ch.Text)) > size {
			t.Fatalf("chunk %d exceeds size: %d runes", i, len([]rune(ch.Text)))
		}
		if ch.ParentID != "doc1" || ch.Kind != KindChunk || ch.ID != fmt.Sprintf("doc1#%d", i) {
			t.Fatalf("chunk %d linkage=%+v", i, ch)
		}
	}
	// Consecutive chunks overlap by exactly `overlap` runes (offset arithmetic;
	// Text is always the input slice [StartOffset,EndOffset)).
	for i := 1; i < len(chunks); i++ {
		prev := chunks[i-1]
		cur := chunks[i]
		if got := prev.EndOffset - cur.StartOffset; got != overlap {
			t.Fatalf("chunks %d/%d overlap=%d want %d", i-1, i, got, overlap)
		}
	}
	// Coverage: every rune offset of the input appears in at least one chunk.
	covered := make([]bool, len(text))
	for _, ch := range chunks {
		for off := ch.StartOffset; off < ch.EndOffset; off++ {
			covered[off] = true
		}
	}
	for off, ok := range covered {
		if !ok {
			t.Fatalf("offset %d not covered by any chunk", off)
		}
	}
	// Last chunk ends exactly at the end of the text.
	if last := chunks[len(chunks)-1]; last.EndOffset != len(text) {
		t.Fatalf("last EndOffset=%d want %d", last.EndOffset, len(text))
	}
}

func TestFixedWindowShortInputSingleChunk(t *testing.T) {
	chunks := mustChunk(t, "p", "short text", Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 64, Overlap: 8})
	if len(chunks) != 1 || chunks[0].Text != "short text" || chunks[0].Ordinal != 0 {
		t.Fatalf("chunks=%+v", chunks)
	}
}

func TestRecursiveKeepsSeparatorBoundedUnitsWhole(t *testing.T) {
	paras := []string{
		"First paragraph about apples.",
		"Second paragraph about bananas and orchards.",
		"Third paragraph about cherries in season.",
	}
	text := strings.Join(paras, "\n\n")
	chunks := mustChunk(t, "p", text, Config{
		Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes,
		Size: 60, Overlap: 0,
		Separators: DefaultSeparators(),
	})
	if len(chunks) < 2 {
		t.Fatalf("len(chunks)=%d want paragraph-per-chunk split", len(chunks))
	}
	// Paragraph boundaries are respected: no chunk straddles a "\n\n" join
	// when every paragraph fits within Size.
	for i, ch := range chunks {
		if strings.Contains(strings.Trim(ch.Text, "\n"), "\n\n") {
			t.Fatalf("chunk %d straddles paragraph separator: %q", i, ch.Text)
		}
		if !strings.Contains(text, strings.TrimSpace(ch.Text)) {
			t.Fatalf("chunk %d text is not a contiguous slice of input: %q", i, ch.Text)
		}
	}
}

func TestRecursiveOversizedUnitFallsBackToHardSplit(t *testing.T) {
	long := strings.Repeat("x", 500)
	text := long + "\n\n" + "tiny tail"
	chunks := mustChunk(t, "p", text, Config{
		Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes,
		Size: 100, Overlap: 20,
		Separators: DefaultSeparators(),
	})
	if len(chunks) < 5 {
		t.Fatalf("len(chunks)=%d want hard split of oversized unit", len(chunks))
	}
	for i, ch := range chunks {
		if n := len([]rune(ch.Text)); n > 100 {
			t.Fatalf("chunk %d has %d runes, exceeds size 100", i, n)
		}
	}
	if !strings.Contains(chunks[len(chunks)-1].Text, "tiny tail") {
		t.Fatalf("tail lost: %+v", chunks[len(chunks)-1])
	}
	covered := make([]bool, len(text))
	for _, ch := range chunks {
		for off := ch.StartOffset; off < ch.EndOffset; off++ {
			covered[off] = true
		}
	}
	for off, ok := range covered {
		if !ok {
			t.Fatalf("offset %d not covered", off)
		}
	}
}

func chunkStreamDigest(t *testing.T, parentID, text string, cfg Config) string {
	t.Helper()
	chunks := mustChunk(t, parentID, text, cfg)
	h := sha256.New()
	for _, ch := range chunks {
		fmt.Fprintf(h, "%s|%d|%d|%d|%s\n", ch.ID, ch.Ordinal, ch.StartOffset, ch.EndOffset, ch.Text)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestDeterminismSameInputAndConfigIdenticalStream(t *testing.T) {
	text := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 40)
	configs := []Config{
		{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 128, Overlap: 24},
		{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 96, Overlap: 16, Separators: DefaultSeparators()},
	}
	for i, cfg := range configs {
		first := chunkStreamDigest(t, "parent-7", text, cfg)
		for attempt := 0; attempt < 3; attempt++ {
			again := chunkStreamDigest(t, "parent-7", text, cfg)
			if again != first {
				t.Fatalf("config %d attempt %d: digest mismatch\n%s\n%s", i, attempt, first, again)
			}
		}
	}
}

func TestDeterminismDifferentParentIDChangesIDsNotText(t *testing.T) {
	text := strings.Repeat("stable corpus body. ", 30)
	cfg := Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 64, Overlap: 12}
	a := mustChunk(t, "alpha", text, cfg)
	b := mustChunk(t, "beta", text, cfg)
	if len(a) != len(b) {
		t.Fatalf("chunk count differs across parent IDs: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Text != b[i].Text {
			t.Fatalf("chunk %d text differs across parent IDs", i)
		}
		if a[i].ID != fmt.Sprintf("alpha#%d", i) || b[i].ID != fmt.Sprintf("beta#%d", i) {
			t.Fatalf("derived IDs wrong: %q %q", a[i].ID, b[i].ID)
		}
	}
}

func TestChildIDSchemeRoundTrip(t *testing.T) {
	if got := ChildDocumentID("parent-9", 42); got != "parent-9#42" {
		t.Fatalf("ChildDocumentID=%q", got)
	}
	parent, ordinal, ok := ParseChildID("parent-9#42")
	if !ok || parent != "parent-9" || ordinal != 42 {
		t.Fatalf("ParseChildID=(%q,%d,%v)", parent, ordinal, ok)
	}
	bad := []string{"", "noseparator", "#0", "p#", "p#x", "p#-1", "p#1#2"}
	for _, id := range bad {
		if _, _, ok := ParseChildID(id); ok {
			t.Fatalf("ParseChildID(%q) accepted malformed ID", id)
		}
	}
}

func TestParseChildMetaFailClosedOnMalformedMetadata(t *testing.T) {
	doc := marshalMap(t, map[string]any{
		MetaFieldParent:  "p1",
		MetaFieldOrdinal: float64(3),
		MetaFieldKind:    KindChunk,
	})
	child, err := ParseChildMeta(doc)
	if err != nil || child.ParentID != "p1" || child.Ordinal != 3 || child.Kind != KindChunk {
		t.Fatalf("ParseChildMeta=(%+v,%v)", child, err)
	}

	malformed := []map[string]any{
		{}, // no chunk fields at all: a plain document, not a chunk child
		{MetaFieldParent: "p1"},
		{MetaFieldOrdinal: float64(3)},
		{MetaFieldKind: KindChunk},
		{MetaFieldParent: "p1", MetaFieldOrdinal: float64(-2), MetaFieldKind: KindChunk},
		{MetaFieldParent: "p1", MetaFieldOrdinal: float64(3), MetaFieldKind: "mystery"},
		{MetaFieldParent: "", MetaFieldOrdinal: float64(3), MetaFieldKind: KindChunk},
		{MetaFieldParent: "p1", MetaFieldOrdinal: "three", MetaFieldKind: KindChunk},
	}
	for i, m := range malformed {
		raw := marshalMap(t, m)
		child, err := ParseChildMeta(raw)
		if i == 0 {
			if err != nil || child != nil {
				t.Fatalf("no-metadata document: child=%+v err=%v want nil,nil", child, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("malformed metadata case %d (%v): accepted as %+v", i, m, child)
		}
	}
}

func TestValidateChunkChildLinksIDAndMeta(t *testing.T) {
	doc := marshalMap(t, map[string]any{
		MetaFieldParent:  "p1",
		MetaFieldOrdinal: float64(2),
		MetaFieldKind:    KindChunk,
	})
	if err := ValidateChunkChild("p1#2", doc); err != nil {
		t.Fatalf("valid child rejected: %v", err)
	}
	if err := ValidateChunkChild("p1#7", doc); err == nil {
		t.Fatal("mismatched derived ID accepted")
	}
	if err := ValidateChunkChild("totally-unrelated", doc); err == nil {
		t.Fatal("non-child ID with chunk metadata accepted")
	}
	if err := ValidateChunkChild("plain-doc", []byte(`{"body":"no chunk fields"}`)); err != nil {
		t.Fatalf("plain document rejected: %v", err)
	}
}
