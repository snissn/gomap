// Package chunking provides the collection-native document chunking seam for
// TreeDB collections. It turns a parent source document's text into an ordered
// stream of child chunk documents linked to the parent with stable derived IDs
// (`<parentID>#<ordinal>`), ordinal, and kind metadata.
//
// # Determinism
//
// Chunk is a pure function of (parentID, text, config): the same inputs always
// produce an identical chunk stream — identical IDs, ordinals, offsets, and
// text. No randomness, clocks, map iteration, or global state participate in
// output construction. Downstream golden fixtures (RAG benchmark, C1) rely on
// this guarantee.
//
// # Size Unit
//
// The only supported size unit is runes (SizeUnitRunes). Token-based sizing is
// deliberately deferred: it would require a tokenizer dependency and a
// model-bound definition of "token". Rune counts are deterministic across
// platforms and versions, which the determinism contract above requires.
package chunking

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Strategy selects the chunking algorithm.
type Strategy string

const (
	// StrategyFixedWindow slices text into overlapping windows of exactly
	// `size` runes (the final window may be shorter).
	StrategyFixedWindow Strategy = "fixed_window"
	// StrategyRecursive splits text on structural separators (paragraphs,
	// lines, sentence ends, spaces, in that order) and keeps separator-bounded
	// units whole whenever they fit within `size`; oversized units fall back to
	// progressively finer separators and finally to overlapped hard splits.
	StrategyRecursive Strategy = "recursive"
)

// SizeUnit names the unit in which Config.Size and Config.Overlap are
// measured. Only runes are supported (see package comment).
type SizeUnit string

const SizeUnitRunes SizeUnit = "runes"

// KindChunk is the kind value carried by every generated child document.
const KindChunk = "chunk"

// Metadata field names embedded in child documents. These mirror the
// document-service conventions (`meta.chunk_kind` et al.) so service-level
// exposure can map onto the collection seam without translation.
const (
	MetaFieldParent  = "chunk_parent"
	MetaFieldOrdinal = "chunk_ordinal"
	MetaFieldKind    = "chunk_kind"
)

// childIDSep joins a parent ID and a chunk ordinal into a derived child ID.
const childIDSep = "#"

// Chunk is one derived child of a parent source document. StartOffset and
// EndOffset are rune offsets into the parent text, and Text is always exactly
// the parent text slice parentText[StartOffset:EndOffset].
type Chunk struct {
	ID          string
	ParentID    string
	Ordinal     int
	Kind        string
	Text        string
	StartOffset int
	EndOffset   int
}

// Config configures a chunker. Validate must be called (explicitly or via
// Chunk) before use; all violations are fail-closed errors.
type Config struct {
	Strategy Strategy `json:"strategy"`
	SizeUnit SizeUnit `json:"size_unit"`
	// Size is the maximum chunk length, in runes.
	Size int `json:"size"`
	// Overlap is the number of trailing runes repeated at the start of the next
	// chunk. Must satisfy 0 <= Overlap < Size.
	Overlap int `json:"overlap"`
	// Separators customizes StrategyRecursive splitting, tried in order. An
	// empty entry means a hard split by runes. When omitted,
	// DefaultSeparators is used.
	Separators []string `json:"separators,omitempty"`
}

// DefaultSeparators returns the default recursive splitting order: paragraph
// breaks, line breaks, sentence ends, word breaks, and finally a hard rune
// split ("" entry).
func DefaultSeparators() []string {
	return []string{"\n\n", "\n", ". ", " ", ""}
}

func (c Config) normalizedSeparators() []string {
	if len(c.Separators) == 0 {
		return DefaultSeparators()
	}
	return c.Separators
}

// Validate fails closed on any configuration that cannot be honored exactly.
func (c Config) Validate() error {
	switch c.Strategy {
	case StrategyFixedWindow, StrategyRecursive:
	default:
		return fmt.Errorf("chunking: unsupported strategy %q (want %q or %q)",
			string(c.Strategy), StrategyFixedWindow, StrategyRecursive)
	}
	if c.SizeUnit != SizeUnitRunes {
		return fmt.Errorf("chunking: unsupported size unit %q (only %q is supported)",
			string(c.SizeUnit), SizeUnitRunes)
	}
	if c.Size <= 0 {
		return fmt.Errorf("chunking: size must be positive, got %d", c.Size)
	}
	if c.Overlap < 0 || c.Overlap >= c.Size {
		return fmt.Errorf("chunking: overlap %d out of range [0, size=%d)", c.Overlap, c.Size)
	}
	return nil
}

// ChildDocumentID derives the stable child ID for parentID and ordinal.
func ChildDocumentID(parentID string, ordinal int) string {
	return parentID + childIDSep + strconv.Itoa(ordinal)
}

// ParseChildID decomposes a derived child ID. ok is false for IDs that are not
// chunk-child IDs (including malformed ones such as "p#", "p#x", "p#-1", or
// IDs containing more than one separator).
func ParseChildID(id string) (parentID string, ordinal int, ok bool) {
	i := strings.Index(id, childIDSep)
	if i < 0 || strings.Contains(id[i+len(childIDSep):], childIDSep) {
		return "", 0, false
	}
	parentID = id[:i]
	if parentID == "" {
		return "", 0, false
	}
	n, err := strconv.Atoi(id[i+len(childIDSep):])
	if err != nil || n < 0 {
		return "", 0, false
	}
	return parentID, n, true
}

// ChildMeta is the validated chunk linkage metadata of a child document.
type ChildMeta struct {
	ParentID string
	Ordinal  int
	Kind     string
}

// ParseChildMeta extracts chunk linkage metadata from a stored JSON document.
// A document carrying none of the chunk metadata fields parses as
// (nil, nil) — a plain, unchunked document. A document carrying partial or
// ill-typed metadata fails closed: partial metadata indicates corruption or a
// writer bug, and silently indexing such a document would orphan chunks.
func ParseChildMeta(document []byte) (*ChildMeta, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil {
		return nil, fmt.Errorf("chunking: parse child document: %w", err)
	}
	rawParent, hasParent := fields[MetaFieldParent]
	rawOrdinal, hasOrdinal := fields[MetaFieldOrdinal]
	rawKind, hasKind := fields[MetaFieldKind]
	if !hasParent && !hasOrdinal && !hasKind {
		return nil, nil
	}
	if !hasParent || !hasOrdinal || !hasKind {
		return nil, fmt.Errorf("chunking: partial chunk metadata (parent=%t ordinal=%t kind=%t)", hasParent, hasOrdinal, hasKind)
	}
	var parent string
	if err := json.Unmarshal(rawParent, &parent); err != nil {
		return nil, fmt.Errorf("chunking: field %q must be a string: %w", MetaFieldParent, err)
	}
	if parent == "" {
		return nil, fmt.Errorf("chunking: field %q must be non-empty", MetaFieldParent)
	}
	var ordinal json.Number
	if err := json.Unmarshal(rawOrdinal, &ordinal); err != nil {
		return nil, fmt.Errorf("chunking: field %q must be a number: %w", MetaFieldOrdinal, err)
	}
	n, err := strconv.ParseInt(string(ordinal), 10, 64)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("chunking: field %q must be a non-negative integer, got %s", MetaFieldOrdinal, string(rawOrdinal))
	}
	var kind string
	if err := json.Unmarshal(rawKind, &kind); err != nil {
		return nil, fmt.Errorf("chunking: field %q must be a string: %w", MetaFieldKind, err)
	}
	if kind != KindChunk {
		return nil, fmt.Errorf("chunking: field %q must be %q, got %q", MetaFieldKind, KindChunk, kind)
	}
	return &ChildMeta{ParentID: parent, Ordinal: int(n), Kind: kind}, nil
}

// ValidateChunkChild verifies that a stored document with chunk metadata is a
// well-formed child of the chunk with the given document ID: the ID must equal
// the derived `<parent>#<ordinal>` scheme implied by the metadata. Documents
// without chunk metadata pass trivially. Any mismatch fails closed.
func ValidateChunkChild(documentID string, document []byte) error {
	meta, err := ParseChildMeta(document)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}
	if want := ChildDocumentID(meta.ParentID, meta.Ordinal); documentID != want {
		return fmt.Errorf("chunking: child document ID %q does not match derived ID %q (parent=%q ordinal=%d)",
			documentID, want, meta.ParentID, meta.Ordinal)
	}
	return nil
}

// SplitChunks splits text into a deterministic, ordered stream of child chunks
// under parentID. Every rune offset of text is covered by at least one chunk,
// every chunk is at most Size runes, and consecutive chunks share exactly
// Overlap trailing/leading runes. Empty text yields no chunks.
func SplitChunks(parentID, text string, cfg Config) ([]Chunk, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if parentID == "" {
		return nil, fmt.Errorf("chunking: parent ID must be non-empty")
	}
	if text == "" {
		return nil, nil
	}
	var spans []span
	if cfg.Strategy == StrategyFixedWindow {
		spans = fixedWindowSpans([]rune(text), cfg.Size, cfg.Overlap)
	} else {
		spans = recursiveSpans([]rune(text), cfg.normalizedSeparators(), cfg.Size, cfg.Overlap)
	}
	chunks := make([]Chunk, 0, len(spans))
	for i, s := range spans {
		chunks = append(chunks, Chunk{
			ID:          ChildDocumentID(parentID, i),
			ParentID:    parentID,
			Ordinal:     i,
			Kind:        KindChunk,
			Text:        string(s.runes),
			StartOffset: s.start,
			EndOffset:   s.end,
		})
	}
	return chunks, nil
}

// span is a rune range of the parent text. runes always equals text[start:end]
// in rune space.
type span struct {
	runes      []rune
	start, end int
}

func fixedWindowSpans(runes []rune, size, overlap int) []span {
	step := size - overlap
	var spans []span
	for start := 0; start < len(runes); {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		spans = append(spans, span{runes: runes[start:end], start: start, end: end})
		if end == len(runes) {
			break
		}
		start += step
	}
	return spans
}

// recursiveSpans splits [0,len) on the first separator present; oversized
// pieces recurse with the remaining separators; sibling pieces that fit merge
// greedily without crossing a recursion boundary.
func recursiveSpans(runes []rune, seps []string, size, overlap int) []span {
	spans, _ := splitRange(runes, 0, len(runes), seps, size, overlap)
	return spans
}

func splitRange(runes []rune, lo, hi int, seps []string, size, overlap int) ([]span, bool) {
	if hi-lo <= size {
		return []span{{runes: runes[lo:hi], start: lo, end: hi}}, true
	}
	if len(seps) == 0 {
		return hardSplitSpans(runes, lo, hi, size, overlap), false
	}
	pieces := splitBySeparator(runes, lo, hi, seps[0])
	if len(pieces) <= 1 {
		return splitRange(runes, lo, hi, seps[1:], size, overlap)
	}
	var out []span
	var pending []span // adjacent leaf siblings buffered for greedy merge
	flush := func() {
		if len(pending) == 0 {
			return
		}
		out = append(out, mergeSpans(runes, pending))
		pending = nil
	}
	for _, p := range pieces {
		subs, leaf := splitRange(runes, p.lo, p.hi, seps[1:], size, overlap)
		if !leaf {
			flush()
			out = append(out, subs...)
			continue
		}
		for _, s := range subs {
			if len(pending) > 0 && s.end-pending[0].start > size {
				flush()
			}
			pending = append(pending, s)
		}
	}
	flush()
	return out, false
}

func hardSplitSpans(runes []rune, lo, hi int, size, overlap int) []span {
	step := size - overlap
	var spans []span
	for start := lo; start < hi; {
		end := start + size
		if end > hi {
			end = hi
		}
		spans = append(spans, span{runes: runes[start:end], start: start, end: end})
		if end == hi {
			break
		}
		start += step
	}
	return spans
}

// splitBySeparator cuts [lo,hi) after every occurrence of sep. A separator not
// present yields a single piece; empty pieces are dropped.
func splitBySeparator(runes []rune, lo, hi int, sep string) []struct{ lo, hi int } {
	if sep == "" {
		return []struct{ lo, hi int }{{lo, hi}}
	}
	sepr := []rune(sep)
	var pieces []struct{ lo, hi int }
	start := lo
	for i := lo; i <= hi-len(sepr); {
		if string(runes[i:i+len(sepr)]) == sep {
			if start < i+len(sepr) {
				pieces = append(pieces, struct{ lo, hi int }{start, i + len(sepr)})
			}
			start = i + len(sepr)
			i = start
			continue
		}
		i++
	}
	pieces = append(pieces, struct{ lo, hi int }{start, hi})
	return pieces
}

func mergeSpans(runes []rune, spans []span) span {
	merged := span{start: spans[0].start, end: spans[len(spans)-1].end}
	merged.runes = runes[merged.start:merged.end]
	return merged
}
