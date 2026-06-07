package collections

import (
	"errors"
	"fmt"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// TextSearchOperator controls how analyzed query terms are combined. The zero
// value currently normalizes to OR.
type TextSearchOperator string

const (
	TextSearchOperatorOR  TextSearchOperator = "or"
	TextSearchOperatorAND TextSearchOperator = "and"
)

const textSearchUnavailableExecution = "text_search_execution_unimplemented"

// TextSearchOptions configures collection-native lexical search. PR1 validates
// the query shape and fails closed before scanning because ranked text search
// execution is not implemented yet.
type TextSearchOptions struct {
	IndexName            string
	Query                string
	Operator             TextSearchOperator
	TopK                 int
	IncludeDocuments     bool
	DocumentFetchOptions DocumentFetchOptions
}

// TextSearchResult is one ranked lexical result. Later #1764 storage/search
// milestones populate these fields from persistent postings and BM25/BM25F
// scoring; PR1 returns no results while storage is unavailable.
type TextSearchResult struct {
	DocumentID    []byte
	Rank          int
	Score         float64
	MatchedTerms  []string
	MatchedFields []string
	Document      []byte
}

// TextSearchStats reports text-only search work. Counters are intentionally
// lexical/search-specific and avoid hybrid candidate/fusion terminology.
type TextSearchStats struct {
	QueryTerms        int
	PostingsScanned   uint64
	CandidatesScored  uint64
	DocumentsFetched  uint64
	Truncated         bool
	Unavailable       bool
	UnavailableReason string
}

// TextSearchResponse contains ranked text results and diagnostics.
type TextSearchResponse struct {
	Results []TextSearchResult
	Stats   TextSearchStats
}

// SearchText validates the declared text index and query shape, then fails
// closed until ranked postings/text-state/stats search execution lands in a
// later #1764 milestone. It never scans/ranks all collection documents.
func (c *Collection) SearchText(opts TextSearchOptions) (TextSearchResponse, error) {
	var response TextSearchResponse
	if c == nil {
		return response, errCollectionNil
	}
	if c.db == nil {
		return response, errCollectionDBNil
	}
	if err := ValidateIndexName(opts.IndexName); err != nil {
		return response, err
	}
	if opts.TopK <= 0 {
		return response, errors.New("collections: text search TopK must be positive")
	}
	if _, err := normalizeTextSearchOperator(opts.Operator); err != nil {
		return response, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return response, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return response, err
	}
	if catalog == nil {
		return response, errCollectionNotFound
	}
	idx, ok := findTextIndex(catalog.meta.TextIndexes, opts.IndexName)
	if !ok {
		return response, ErrIndexNotFound
	}

	terms, _, err := parseTextSearchQuery(idx.Analyzer, opts.Query, opts.Operator)
	if err != nil {
		return response, err
	}
	response.Stats.QueryTerms = len(terms)
	if len(terms) == 0 {
		response.Results = []TextSearchResult{}
		return response, nil
	}
	response.Stats.Unavailable = true
	response.Stats.UnavailableReason = textSearchUnavailableExecution
	return response, fmt.Errorf("%w: collection %q text index %q search execution is not implemented yet", ErrTextIndexUnavailable, catalog.meta.Name, idx.Name)
}

func normalizeTextSearchOperator(op TextSearchOperator) (TextSearchOperator, error) {
	switch op {
	case "", TextSearchOperatorOR:
		return TextSearchOperatorOR, nil
	case TextSearchOperatorAND:
		return TextSearchOperatorAND, nil
	default:
		return "", fmt.Errorf("collections: unsupported text search operator %q", op)
	}
}

func parseTextSearchQuery(analyzer TextAnalyzer, query string, requested TextSearchOperator) ([]string, TextSearchOperator, error) {
	if strings.ContainsAny(query, "\"()") {
		return nil, "", errors.New("collections: unsupported text query syntax: phrase and grouped queries are not implemented")
	}
	operator := requested
	var explicit TextSearchOperator
	parts := strings.Fields(query)
	terms := make([]string, 0, len(parts))
	expectTerm := true
	for _, part := range parts {
		switch {
		case strings.EqualFold(part, "AND"):
			if expectTerm {
				return nil, "", errors.New("collections: malformed text query: dangling AND")
			}
			if explicit != "" && explicit != TextSearchOperatorAND {
				return nil, "", errors.New("collections: mixed AND/OR text queries are not supported")
			}
			explicit = TextSearchOperatorAND
			expectTerm = true
			continue
		case strings.EqualFold(part, "OR"):
			if expectTerm {
				return nil, "", errors.New("collections: malformed text query: dangling OR")
			}
			if explicit != "" && explicit != TextSearchOperatorOR {
				return nil, "", errors.New("collections: mixed AND/OR text queries are not supported")
			}
			explicit = TextSearchOperatorOR
			expectTerm = true
			continue
		}
		tokens, err := AnalyzeText(analyzer, part)
		if err != nil {
			return nil, "", err
		}
		if len(tokens) == 0 {
			continue
		}
		for _, token := range tokens {
			terms = append(terms, token.Term)
		}
		expectTerm = false
	}
	if expectTerm && len(terms) > 0 {
		return nil, "", errors.New("collections: malformed text query: dangling operator")
	}
	if explicit != "" {
		if requested != "" && requested != explicit {
			return nil, "", fmt.Errorf("collections: text query operator %q conflicts with requested operator %q", explicit, requested)
		}
		operator = explicit
	}
	if operator == "" {
		operator = TextSearchOperatorOR
	}
	return terms, operator, nil
}
