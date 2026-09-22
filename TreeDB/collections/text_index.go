package collections

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ErrTextIndexUnavailable reports that a declared collection text index cannot
// safely serve a requested text-search operation. Ranked SearchText executes from
// postings in #1764 M4, but bounded candidate-generation guardrails still fail
// closed instead of silently scanning all documents or returning incomplete text
// rankings.
var ErrTextIndexUnavailable = errors.New("collections: text index unavailable")

// TextAnalyzer names a collection text analyzer. The zero value normalizes to
// TextAnalyzerSimple.
type TextAnalyzer string

const (
	TextAnalyzerSimple TextAnalyzer = "simple"
)

// TextAnalyzerOptions configures the built-in text analyzer in ways that are
// safe to persist in collection metadata. StopWords are supported for the
// simple analyzer. Stemmer and Synonyms reserve bounded extension seams and are
// rejected until their indexing/query expansion semantics are implemented.
type TextAnalyzerOptions struct {
	StopWords []string            `json:"stop_words,omitempty"`
	Stemmer   string              `json:"stemmer,omitempty"`
	Synonyms  map[string][]string `json:"synonyms,omitempty"`
}

// TextIndexVersion selects the physical text-index contract. For new text
// index declarations, the zero value normalizes to TextIndexVersionV2. Existing
// persisted v1 metadata remains v1, and callers that need the legacy root format
// can still set TextIndexVersionV1 explicitly. Unsupported versions fail closed
// rather than silently falling back to another format.
type TextIndexVersion string

const (
	TextIndexVersionDefault TextIndexVersion = ""
	TextIndexVersionV1      TextIndexVersion = "v1"
	TextIndexVersionV2      TextIndexVersion = "v2"
)

// TextIndexRolloutMode describes how a text index participates in reads/writes
// during v1/v2 coexistence. The zero value normalizes to primary. Non-primary
// modes are reserved for the text v2 rollout and fail closed until the
// corresponding dual-write/shadow-read implementation lands.
type TextIndexRolloutMode string

const (
	TextIndexRolloutDefault   TextIndexRolloutMode = ""
	TextIndexRolloutPrimary   TextIndexRolloutMode = "primary"
	TextIndexRolloutShadow    TextIndexRolloutMode = "shadow"
	TextIndexRolloutDualWrite TextIndexRolloutMode = "dual_write"
	TextIndexRolloutDisabled  TextIndexRolloutMode = "disabled"
)

// TextIndexDefinition declares a persistent collection-native inverted index.
// M2 stores and validates metadata plus versioned postings/text-state/stats
// storage, M3 maintains these roots on writes, and M4 ranks SearchText results
// from bounded postings scans.
type TextIndexDefinition struct {
	Name             string               `json:"name"`
	Fields           []TextIndexField     `json:"fields"`
	Analyzer         TextAnalyzer         `json:"analyzer,omitempty"`
	AnalyzerOptions  *TextAnalyzerOptions `json:"analyzer_options,omitempty"`
	Version          TextIndexVersion     `json:"version,omitempty"`
	Rollout          TextIndexRolloutMode `json:"rollout,omitempty"`
	StorePositions   bool                 `json:"store_positions,omitempty"`
	StoreOffsets     bool                 `json:"store_offsets,omitempty"`
	StoragePolicy    RootStoragePolicy    `json:"storage_policy,omitempty"`
	SchemaGeneration uint64               `json:"schema_generation,omitempty"`
}

// TextIndexField declares one document field in a text index. Weight defaults
// to 1.0 when omitted or set to zero.
type TextIndexField struct {
	Field  string  `json:"field"`
	Weight float64 `json:"weight,omitempty"`
}

func normalizeTextAnalyzer(analyzer TextAnalyzer) (TextAnalyzer, error) {
	switch analyzer {
	case "", TextAnalyzerSimple:
		return TextAnalyzerSimple, nil
	default:
		return "", fmt.Errorf("unsupported analyzer %q", analyzer)
	}
}

func normalizeTextAnalyzerOptions(analyzer TextAnalyzer, options *TextAnalyzerOptions) (*TextAnalyzerOptions, error) {
	if options == nil {
		return nil, nil
	}
	if _, err := normalizeTextAnalyzer(analyzer); err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.Stemmer) != "" {
		return nil, fmt.Errorf("%w: text analyzer stemmer %q is not implemented", ErrTextIndexUnavailable, options.Stemmer)
	}
	if len(options.Synonyms) != 0 {
		return nil, fmt.Errorf("%w: text analyzer synonyms are not implemented", ErrTextIndexUnavailable)
	}
	stopWords, err := normalizeTextAnalyzerStopWords(options.StopWords)
	if err != nil {
		return nil, err
	}
	if len(stopWords) == 0 {
		return nil, nil
	}
	return &TextAnalyzerOptions{StopWords: stopWords}, nil
}

func normalizeTextAnalyzerStopWords(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(values))
	for i, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		var terms []string
		if err := analyzeSimpleTextToSink(raw, TextTokenSinkFunc(func(token TextToken) error {
			terms = append(terms, token.Term)
			return nil
		})); err != nil {
			return nil, fmt.Errorf("text analyzer stop_words[%d]: %w", i, err)
		}
		if len(terms) != 1 {
			return nil, fmt.Errorf("text analyzer stop_words[%d] %q must analyze to exactly one simple token", i, raw)
		}
		seen[terms[0]] = struct{}{}
	}
	if len(seen) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(seen))
	for term := range seen {
		out = append(out, term)
	}
	sort.Strings(out)
	return out, nil
}

func textAnalyzerStopWordSet(options *TextAnalyzerOptions) map[string]struct{} {
	if options == nil || len(options.StopWords) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(options.StopWords))
	for _, term := range options.StopWords {
		set[term] = struct{}{}
	}
	return set
}

func cloneTextAnalyzerOptions(options *TextAnalyzerOptions) *TextAnalyzerOptions {
	if options == nil {
		return nil
	}
	out := &TextAnalyzerOptions{
		StopWords: append([]string(nil), options.StopWords...),
		Stemmer:   options.Stemmer,
	}
	if len(options.Synonyms) != 0 {
		out.Synonyms = make(map[string][]string, len(options.Synonyms))
		for term, expansions := range options.Synonyms {
			out.Synonyms[term] = append([]string(nil), expansions...)
		}
	}
	return out
}

func textAnalyzerOptionsEqual(a, b *TextAnalyzerOptions) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Stemmer != b.Stemmer || len(a.StopWords) != len(b.StopWords) || len(a.Synonyms) != len(b.Synonyms) {
		return false
	}
	for i := range a.StopWords {
		if a.StopWords[i] != b.StopWords[i] {
			return false
		}
	}
	for term, left := range a.Synonyms {
		right, ok := b.Synonyms[term]
		if !ok || len(left) != len(right) {
			return false
		}
		for i := range left {
			if left[i] != right[i] {
				return false
			}
		}
	}
	return true
}

func normalizeTextIndexVersion(version TextIndexVersion) (TextIndexVersion, error) {
	switch version {
	case TextIndexVersionDefault, TextIndexVersionV2:
		return TextIndexVersionV2, nil
	case TextIndexVersionV1:
		return TextIndexVersionV1, nil
	default:
		return "", fmt.Errorf("unsupported text index version %q", version)
	}
}

func normalizeTextIndexRolloutMode(mode TextIndexRolloutMode) (TextIndexRolloutMode, error) {
	switch mode {
	case TextIndexRolloutDefault, TextIndexRolloutPrimary:
		return TextIndexRolloutPrimary, nil
	case TextIndexRolloutShadow, TextIndexRolloutDualWrite, TextIndexRolloutDisabled:
		return "", fmt.Errorf("%w: text index rollout mode %q is reserved until text v2 rollout lands", ErrTextIndexUnavailable, mode)
	default:
		return "", fmt.Errorf("unsupported text index rollout mode %q", mode)
	}
}

func normalizeTextIndexDefinition(def TextIndexDefinition) (TextIndexDefinition, error) {
	if def.Name == "" {
		return TextIndexDefinition{}, errors.New("name is required")
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return TextIndexDefinition{}, err
	}
	analyzer, err := normalizeTextAnalyzer(def.Analyzer)
	if err != nil {
		return TextIndexDefinition{}, err
	}
	def.Analyzer = analyzer
	analyzerOptions, err := normalizeTextAnalyzerOptions(analyzer, def.AnalyzerOptions)
	if err != nil {
		return TextIndexDefinition{}, err
	}
	def.AnalyzerOptions = analyzerOptions
	version, err := normalizeTextIndexVersion(def.Version)
	if err != nil {
		return TextIndexDefinition{}, err
	}
	def.Version = version
	rollout, err := normalizeTextIndexRolloutMode(def.Rollout)
	if err != nil {
		return TextIndexDefinition{}, err
	}
	def.Rollout = rollout
	if _, err := backendRootStoragePolicy(def.StoragePolicy); err != nil {
		return TextIndexDefinition{}, err
	}
	if def.StoreOffsets && !def.StorePositions {
		return TextIndexDefinition{}, errors.New("store_offsets requires store_positions")
	}
	if len(def.Fields) == 0 {
		return TextIndexDefinition{}, errors.New("fields are required")
	}
	fields := append([]TextIndexField(nil), def.Fields...)
	seenFields := make(map[string]struct{}, len(fields))
	for i := range fields {
		field := fields[i].Field
		if err := ValidateIndexPath(field); err != nil {
			return TextIndexDefinition{}, fmt.Errorf("field[%d]: %w", i, err)
		}
		if _, ok := seenFields[field]; ok {
			return TextIndexDefinition{}, fmt.Errorf("duplicate field %q", field)
		}
		seenFields[field] = struct{}{}
		weight := fields[i].Weight
		if weight == 0 {
			fields[i].Weight = 1
			continue
		}
		if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
			return TextIndexDefinition{}, fmt.Errorf("field[%d] weight must be finite and non-negative", i)
		}
	}
	def.Fields = fields
	return def, nil
}

func copyTextIndexDefinitions(in []TextIndexDefinition) []TextIndexDefinition {
	if len(in) == 0 {
		return nil
	}
	out := append([]TextIndexDefinition(nil), in...)
	for i := range out {
		out[i].Fields = append([]TextIndexField(nil), out[i].Fields...)
		out[i].AnalyzerOptions = cloneTextAnalyzerOptions(out[i].AnalyzerOptions)
	}
	return out
}

func textIndexDefinitionValuesEqual(a, b TextIndexDefinition) bool {
	if a.Name != b.Name ||
		a.Analyzer != b.Analyzer ||
		!textAnalyzerOptionsEqual(a.AnalyzerOptions, b.AnalyzerOptions) ||
		a.Version != b.Version ||
		a.Rollout != b.Rollout ||
		a.StorePositions != b.StorePositions ||
		a.StoreOffsets != b.StoreOffsets ||
		a.StoragePolicy != b.StoragePolicy ||
		a.SchemaGeneration != b.SchemaGeneration ||
		len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

func findTextIndex(indexes []TextIndexDefinition, name string) (TextIndexDefinition, bool) {
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return TextIndexDefinition{}, false
}

func collectionTextIndexRootName(collection, indexName string) string {
	return collection + "/text-index/" + indexName
}

func collectionTextStateRootName(collection, indexName string) string {
	return collection + "/text-state/" + indexName
}

func collectionTextStatsRootName(collection, indexName string) string {
	return collection + "/text-stats/" + indexName
}

func collectionTextRootNames(collection, indexName string) []string {
	return []string{
		collectionTextIndexRootName(collection, indexName),
		collectionTextStateRootName(collection, indexName),
		collectionTextStatsRootName(collection, indexName),
	}
}

func collectionTextRootNamesForDefinition(collection string, def TextIndexDefinition) []string {
	version := def.Version
	if version == TextIndexVersionDefault {
		version = TextIndexVersionV2
	}
	if version == TextIndexVersionV2 {
		return collectionTextV2RootNames(collection, def.Name)
	}
	return collectionTextRootNames(collection, def.Name)
}

func collectionTextAllRootNames(collection, indexName string) []string {
	roots := collectionTextRootNames(collection, indexName)
	return append(roots, collectionTextV2RootNames(collection, indexName)...)
}

func collectionTextV2DocIDRootName(collection, indexName string) string {
	return collection + "/text-v2-docid/" + indexName
}

func collectionTextV2DocMapRootName(collection, indexName string) string {
	return collection + "/text-v2-docmap/" + indexName
}

func collectionTextV2TermsRootName(collection, indexName string) string {
	return collection + "/text-v2-terms/" + indexName
}

func collectionTextV2PostingBlocksRootName(collection, indexName string) string {
	return collection + "/text-v2-posting-blocks/" + indexName
}

func collectionTextV2NormBlocksRootName(collection, indexName string) string {
	return collection + "/text-v2-norm-blocks/" + indexName
}

func collectionTextV2PositionsRootName(collection, indexName string) string {
	return collection + "/text-v2-positions/" + indexName
}

func collectionTextV2GenerationsRootName(collection, indexName string) string {
	return collection + "/text-v2-generations/" + indexName
}

func collectionTextV2RootNames(collection, indexName string) []string {
	return []string{
		collectionTextV2DocIDRootName(collection, indexName),
		collectionTextV2DocMapRootName(collection, indexName),
		collectionTextV2TermsRootName(collection, indexName),
		collectionTextV2PostingBlocksRootName(collection, indexName),
		collectionTextV2NormBlocksRootName(collection, indexName),
		collectionTextV2PositionsRootName(collection, indexName),
		collectionTextV2GenerationsRootName(collection, indexName),
	}
}

// TextIndexStatus reports the currently supported text-index contract for one
// declared index. It is intentionally low-cardinality so callers and benchmark
// harnesses can fail closed when a requested version or rollout mode is not
// active.
type TextIndexStatus struct {
	Name                    string               `json:"name"`
	Version                 TextIndexVersion     `json:"version"`
	Rollout                 TextIndexRolloutMode `json:"rollout"`
	Ready                   bool                 `json:"ready"`
	Readable                bool                 `json:"readable"`
	Writable                bool                 `json:"writable"`
	FailClosed              bool                 `json:"fail_closed,omitempty"`
	FailClosedReason        string               `json:"fail_closed_reason,omitempty"`
	ActiveRootNames         []string             `json:"active_root_names,omitempty"`
	ReservedV2RootNames     []string             `json:"reserved_v2_root_names,omitempty"`
	RequiredCounterNames    []string             `json:"required_counter_names,omitempty"`
	RewriteMergeState       string               `json:"rewrite_merge_state,omitempty"`
	PhysicalReclamationPath string               `json:"physical_reclamation_path,omitempty"`
}

const (
	TextIndexRewriteMergeStateNotApplicable = "not_applicable"
	TextIndexPhysicalReclamationTreeDB      = "ordered_roots_value_log_gc_leaf_generation_index_vacuum_compact_storage"
)

func (c *Collection) TextIndexStatus(indexName string) (TextIndexStatus, error) {
	if c == nil {
		return TextIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return TextIndexStatus{}, errCollectionDBNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return TextIndexStatus{}, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return TextIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return TextIndexStatus{}, err
	}
	if catalog == nil {
		return TextIndexStatus{}, errCollectionNotFound
	}
	idx, ok := findTextIndex(catalog.meta.TextIndexes, indexName)
	if !ok {
		return TextIndexStatus{}, ErrIndexNotFound
	}
	return textIndexStatusForDefinition(catalog.meta.Name, idx), nil
}

func textIndexStatusForDefinition(collection string, def TextIndexDefinition) TextIndexStatus {
	version := def.Version
	if version == TextIndexVersionDefault {
		version = TextIndexVersionV2
	}
	rollout := def.Rollout
	if rollout == TextIndexRolloutDefault {
		rollout = TextIndexRolloutPrimary
	}
	status := TextIndexStatus{
		Name:                    def.Name,
		Version:                 version,
		Rollout:                 rollout,
		ReservedV2RootNames:     collectionTextV2RootNames(collection, def.Name),
		RequiredCounterNames:    TextIndexV2RequiredCounterNames(),
		RewriteMergeState:       TextIndexRewriteMergeStateNotApplicable,
		PhysicalReclamationPath: TextIndexPhysicalReclamationTreeDB,
	}
	if rollout != TextIndexRolloutPrimary {
		status.FailClosed = true
		status.FailClosedReason = "text_index_rollout_mode_unavailable"
		return status
	}
	if version == TextIndexVersionV2 {
		status.Ready = true
		status.Readable = true
		status.Writable = true
		status.ActiveRootNames = collectionTextV2RootNames(collection, def.Name)
		status.RewriteMergeState = TextIndexRewriteMergeStateReady
		return status
	}
	if version != TextIndexVersionV1 {
		status.FailClosed = true
		status.FailClosedReason = "text_index_version_unavailable"
		return status
	}
	status.Ready = true
	status.Readable = true
	status.Writable = true
	status.ActiveRootNames = collectionTextRootNames(collection, def.Name)
	return status
}

func TextIndexV2RequiredCounterNames() []string {
	return []string{
		"postings_scanned",
		"posting_blocks_visited",
		"posting_blocks_skipped",
		"blockmax_fallbacks",
		"threshold_updates",
		"wand_pivots",
		"scalar_prefilter_ids",
		"scalar_posting_blocks_skipped",
		"scalar_postings_rejected",
		"candidates_scored",
		"state_lookups",
		"norm_lookups",
		"docs_fetched",
		"match_details_built",
		"position_lookups",
		"phrase_candidates_checked",
		"phrase_candidates_matched",
		"scalar_filter_selectivity",
		"fail_closed",
		"write_amplification",
		"index_bytes_per_doc",
		"rewrite_merge_state",
	}
}
