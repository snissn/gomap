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
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Strategy selects the chunking algorithm.
type Strategy string

const (
	// StrategyFixedWindow slices text into overlapping windows of exactly
	// `size` runes (the final window may be shorter).
	StrategyFixedWindow Strategy = "fixed_window"
	// StrategyRecursive fills each size window at the furthest boundary of the
	// first configured separator present, then advances by end-overlap. It
	// falls through progressively finer separators; an empty separator causes
	// an immediate hard split for that window.
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

// ParentIDErrorReason classifies an unsupported chunk parent ID.
type ParentIDErrorReason string

const (
	ParentIDEmpty             ParentIDErrorReason = "empty"
	ParentIDInvalidUTF8       ParentIDErrorReason = "invalid_utf8"
	ParentIDReservedSeparator ParentIDErrorReason = "reserved_separator"
)

// ParentIDError reports a parent ID that cannot be represented losslessly in
// JSON linkage metadata or would overlap the derived child namespace.
type ParentIDError struct {
	ID     string
	Reason ParentIDErrorReason
}

func (e *ParentIDError) Error() string {
	switch e.Reason {
	case ParentIDEmpty:
		return "chunking: parent ID must be non-empty"
	case ParentIDInvalidUTF8:
		return fmt.Sprintf("chunking: parent ID %x is not valid UTF-8", []byte(e.ID))
	case ParentIDReservedSeparator:
		return fmt.Sprintf("chunking: parent ID %q contains reserved child separator %q", e.ID, childIDSep)
	default:
		return fmt.Sprintf("chunking: unsupported parent ID %q", e.ID)
	}
}

// ValidateParentID enforces the lossless, disjoint parent/child namespace.
func ValidateParentID(parentID string) error {
	switch {
	case parentID == "":
		return &ParentIDError{ID: parentID, Reason: ParentIDEmpty}
	case !utf8.ValidString(parentID):
		return &ParentIDError{ID: parentID, Reason: ParentIDInvalidUTF8}
	case strings.Contains(parentID, childIDSep):
		return &ParentIDError{ID: parentID, Reason: ParentIDReservedSeparator}
	default:
		return nil
	}
}

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
	if err := ValidateParentID(parent); err != nil {
		return nil, fmt.Errorf("chunking: field %q: %w", MetaFieldParent, err)
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
	if err := ValidateParentID(parentID); err != nil {
		return nil, err
	}
	if text == "" {
		return nil, nil
	}
	runes := []rune(text)
	var spans []span
	if cfg.Strategy == StrategyFixedWindow {
		spans = fixedWindowSpans(runes, cfg.Size, cfg.Overlap)
	} else {
		spans = recursiveSpans(runes, cfg.normalizedSeparators(), cfg.Size, cfg.Overlap)
	}
	validText := utf8.ValidString(text)
	var byteOffsets []int
	if validText && len(runes) != len(text) {
		byteOffsets = make([]int, len(runes)+1)
		i := 0
		for offset := range text {
			byteOffsets[i] = offset
			i++
		}
		byteOffsets[len(runes)] = len(text)
	}
	chunks := make([]Chunk, 0, len(spans))
	for i, s := range spans {
		chunkText := ""
		switch {
		case !validText:
			chunkText = string(runes[s.start:s.end])
		case byteOffsets == nil:
			chunkText = text[s.start:s.end]
		default:
			chunkText = text[byteOffsets[s.start]:byteOffsets[s.end]]
		}
		chunks = append(chunks, Chunk{
			ID:          ChildDocumentID(parentID, i),
			ParentID:    parentID,
			Ordinal:     i,
			Kind:        KindChunk,
			Text:        chunkText,
			StartOffset: s.start,
			EndOffset:   s.end,
		})
	}
	return chunks, nil
}

// span is a rune range of the parent text.
type span struct {
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
		spans = append(spans, span{start: start, end: end})
		if end == len(runes) {
			break
		}
		start += step
	}
	return spans
}

// recursiveSpans chooses the furthest boundary for the first configured
// separator that occurs inside each size window. The next window starts at the
// prior end minus overlap, so every path has identical overlap semantics.
func recursiveSpans(runes []rune, seps []string, size, overlap int) []span {
	var spans []span
	for start := 0; start < len(runes); {
		capEnd := start + size
		if capEnd >= len(runes) {
			capEnd = len(runes)
		}
		end := recursiveChunkEnd(runes, start, capEnd, overlap, seps)
		spans = append(spans, span{start: start, end: end})
		if end == len(runes) {
			break
		}
		start = end - overlap
	}
	return spans
}

func recursiveChunkEnd(runes []rune, start, capEnd, overlap int, seps []string) int {
	for _, sep := range seps {
		if sep == "" {
			return capEnd
		}
		separator := []rune(sep)
		last := 0
		for i := start; i <= capEnd-len(separator); {
			if slices.Equal(runes[i:i+len(separator)], separator) {
				candidate := i + len(separator)
				if candidate > start+overlap {
					last = candidate
				}
				i = candidate
				continue
			}
			i++
		}
		if last != 0 {
			return last
		}
	}
	return capEnd
}
