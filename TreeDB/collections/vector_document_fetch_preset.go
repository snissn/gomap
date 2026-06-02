package collections

import (
	"fmt"
	"strings"
)

// VectorDocumentFetchPreset is a named, opt-in final-document fetch preset for
// vector search APIs. Apply it to VectorIndexSearchOptions or
// VectorIndexSearcherSearchOptions when a vector search response should include
// top-k documents while omitting the vector payload field from those documents.
//
// The projection-oriented preset is the storage-efficient product path for
// vector-heavy document responses: use ColumnRetainedPayloadNonColumn at storage
// setup time, then use this preset at search time to fetch documents with the
// vector field excluded from the final response. The preset is deliberately
// opt-in; zero-valued search options and IncludeDocuments=true without this
// preset continue to return full documents.
type VectorDocumentFetchPreset struct {
	// IncludeDocuments should be copied to vector search options. The
	// projection-oriented preset sets this to true because projection is only
	// meaningful when callers request final documents.
	IncludeDocuments bool
	// DocumentFetchOptions should be copied to vector search options. ApplyTo*
	// always replaces IncludePaths/ExcludePaths from the target options. Scalar
	// fields use merge semantics: zero-valued preset scalars preserve the target
	// option's existing value, and non-zero preset scalars override it.
	DocumentFetchOptions DocumentFetchOptions
}

// ProjectionOrientedVectorDocumentFetchPreset returns the preferred vector
// final-document fetch preset for the field declared by def.Field. It excludes
// that vector field from materialized JSON documents and sets IncludeDocuments
// for the target vector search options.
//
// Current DocumentFetchOptions projection supports top-level JSON fields only;
// nested vector fields fail closed with a clear error instead of silently
// projecting a broader document shape.
func ProjectionOrientedVectorDocumentFetchPreset(def VectorIndexDefinition) (VectorDocumentFetchPreset, error) {
	return ProjectionOrientedVectorDocumentFetchPresetForField(def.Field)
}

// ProjectionOrientedVectorDocumentFetchPresetForField returns the preferred
// vector final-document fetch preset for a custom vector field path. The field
// path must be a supported top-level JSON projection path.
func ProjectionOrientedVectorDocumentFetchPresetForField(vectorField string) (VectorDocumentFetchPreset, error) {
	field := strings.TrimSpace(vectorField)
	if field != vectorField {
		return VectorDocumentFetchPreset{}, fmt.Errorf("collections: projection-oriented vector document fetch field %q has leading or trailing spaces", vectorField)
	}
	if err := validateDocumentProjectionPath(field); err != nil {
		return VectorDocumentFetchPreset{}, fmt.Errorf("collections: projection-oriented vector document fetch field %q: %w", vectorField, err)
	}
	return VectorDocumentFetchPreset{
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{field},
		},
	}, nil
}

// ApplyToSearchOptions applies p to collection-level SearchVectorIndex options.
// It enables IncludeDocuments, replaces projection paths from
// opts.DocumentFetchOptions, and preserves existing scalar fetch settings unless
// the preset explicitly overrides them.
func (p VectorDocumentFetchPreset) ApplyToSearchOptions(opts *VectorIndexSearchOptions) {
	if opts == nil {
		return
	}
	opts.IncludeDocuments = p.IncludeDocuments
	opts.DocumentFetchOptions = mergeDocumentFetchOptionsPreset(opts.DocumentFetchOptions, p.DocumentFetchOptions)
}

// ApplyToSearcherSearchOptions applies p to reusable VectorIndexSearcher.Search
// options. It enables IncludeDocuments, replaces projection paths from
// opts.DocumentFetchOptions, and preserves existing scalar fetch settings unless
// the preset explicitly overrides them.
func (p VectorDocumentFetchPreset) ApplyToSearcherSearchOptions(opts *VectorIndexSearcherSearchOptions) {
	if opts == nil {
		return
	}
	opts.IncludeDocuments = p.IncludeDocuments
	opts.DocumentFetchOptions = mergeDocumentFetchOptionsPreset(opts.DocumentFetchOptions, p.DocumentFetchOptions)
}

// cloneDocumentFetchOptions returns a shallow copy of opts with independent
// slice backing arrays for IncludePaths and ExcludePaths.
//
// MAINTENANCE: update this function whenever DocumentFetchOptions gains new
// reference-typed slice, map, or pointer fields.
func cloneDocumentFetchOptions(opts DocumentFetchOptions) DocumentFetchOptions {
	out := opts
	out.IncludePaths = append([]string(nil), opts.IncludePaths...)
	out.ExcludePaths = append([]string(nil), opts.ExcludePaths...)
	return out
}

// mergeDocumentFetchOptionsPreset applies preset projection paths to existing
// fetch options while preserving existing scalar settings when the preset uses
// their zero value. Non-zero preset scalar fields override the existing value.
func mergeDocumentFetchOptionsPreset(existing, preset DocumentFetchOptions) DocumentFetchOptions {
	out := cloneDocumentFetchOptions(existing)
	out.IncludePaths = append([]string(nil), preset.IncludePaths...)
	out.ExcludePaths = append([]string(nil), preset.ExcludePaths...)
	if preset.Format != "" {
		out.Format = preset.Format
	}
	if preset.ColumnAssetReadIntegrity != "" {
		out.ColumnAssetReadIntegrity = preset.ColumnAssetReadIntegrity
	}
	return out
}
