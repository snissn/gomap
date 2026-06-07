package collections

import (
	"errors"
	"fmt"
	"math"
)

// ErrTextIndexUnavailable reports that a declared collection text index cannot
// yet be used for persistent maintenance or search. PR1 for #1764 intentionally
// lands metadata/analyzer/query contracts before postings/text-state/stats
// storage; write/search paths fail closed instead of silently scanning or
// returning incomplete text-index results.
var ErrTextIndexUnavailable = errors.New("collections: text index unavailable")

// TextAnalyzer names a collection text analyzer. The zero value normalizes to
// TextAnalyzerSimple.
type TextAnalyzer string

const (
	TextAnalyzerSimple TextAnalyzer = "simple"
)

// TextIndexDefinition declares a persistent collection-native inverted index.
// PR1 stores and validates metadata only; postings, text-state, stats, mutation
// maintenance, and SearchText execution land in later #1764 milestones.
type TextIndexDefinition struct {
	Name             string            `json:"name"`
	Fields           []TextIndexField  `json:"fields"`
	Analyzer         TextAnalyzer      `json:"analyzer,omitempty"`
	StorePositions   bool              `json:"store_positions,omitempty"`
	StoreOffsets     bool              `json:"store_offsets,omitempty"`
	StoragePolicy    RootStoragePolicy `json:"storage_policy,omitempty"`
	SchemaGeneration uint64            `json:"schema_generation,omitempty"`
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
	}
	return out
}

func textIndexDefinitionValuesEqual(a, b TextIndexDefinition) bool {
	if a.Name != b.Name ||
		a.Analyzer != b.Analyzer ||
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

func rejectTextIndexWriteUnavailable(meta CollectionMeta, operation string) error {
	if len(meta.TextIndexes) == 0 {
		return nil
	}
	if operation == "" {
		operation = "write"
	}
	return fmt.Errorf("%w: collection %q has %d declared text index(es); %s requires postings/text-state/stats maintenance that is not implemented in this milestone", ErrTextIndexUnavailable, meta.Name, len(meta.TextIndexes), operation)
}
