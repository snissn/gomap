package collections

import "fmt"

// SearchVectorIndex searches a declared collection vector index by name. Native
// indexes use the existing runtime graph path. Explicit column_graph indexes
// load the persisted ColumnVectorGraph, run graph traversal/scoring/top-k
// without fetching full documents, then materialize documents for returned
// top-k IDs. If the selected persisted path cannot load and exact fallback is
// not disabled, the collection exact scan remains the correctness path.
func (c *Collection) SearchVectorIndex(name string, query []float32, opts VectorIndexSearchOptions) ([]VectorSearchResult, VectorIndexTrace, error) {
	trace := VectorIndexTrace{Strategy: "vector_index"}
	if c == nil {
		return nil, trace, errCollectionNil
	}
	if c.db == nil {
		return nil, trace, errCollectionDBNil
	}
	if err := ValidateIndexName(name); err != nil {
		return nil, trace, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return nil, trace, err
	}
	def, err := c.declaredVectorIndexDefinitionPreparedWithoutColumnRootValidation(name)
	if err != nil {
		return nil, trace, err
	}
	if vectorIndexDefinitionStrategy(def) == VectorIndexStrategyColumnGraph {
		return c.searchColumnGraphVectorIndex(def, query, opts)
	}
	def, err = c.declaredVectorIndexDefinitionPrepared(name)
	if err != nil {
		return nil, trace, err
	}

	index := c.registeredVectorIndex(def.Name)
	if index == nil {
		loaded, status, err := c.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			return nil, trace, err
		}
		if loaded == nil {
			return c.searchVectorIndexExactFallback(def, query, opts, trace, status.ExactFallbackReason)
		}
		index = loaded
	}
	return index.Search(query, opts)
}

func (c *Collection) searchColumnGraphVectorIndex(def VectorIndexDefinition, query []float32, opts VectorIndexSearchOptions) ([]VectorSearchResult, VectorIndexTrace, error) {
	trace := VectorIndexTrace{Strategy: columnVectorGraphStrategyCosine}
	if opts.TopK <= 0 {
		return nil, trace, fmt.Errorf("collections: vector search TopK must be positive")
	}
	if opts.Filter != nil || opts.IndexRangeFilter != nil {
		return c.searchVectorIndexExactFallback(def, query, opts, trace, "column_graph_filter_requires_exact")
	}
	graph, status, err := c.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		return nil, trace, err
	}
	if graph == nil {
		return c.searchVectorIndexExactFallback(def, query, opts, trace, status.ExactFallbackReason)
	}
	var scratch ColumnVectorGraphSearchScratch
	results, graphTrace, err := graph.SearchCosine(query, ColumnVectorGraphSearchOptions{
		TopK:     opts.TopK,
		EfSearch: opts.EfSearch,
	}, &scratch)
	if err != nil {
		return nil, trace, err
	}
	trace = graphTrace.VectorIndexTrace
	attached, err := c.attachColumnGraphVectorSearchResultDocuments(results, opts.TopK)
	if err != nil {
		return nil, trace, err
	}
	trace.ReturnedCount = len(attached)
	return attached, trace, nil
}

func (c *Collection) searchVectorIndexExactFallback(def VectorIndexDefinition, query []float32, opts VectorIndexSearchOptions, trace VectorIndexTrace, reason string) ([]VectorSearchResult, VectorIndexTrace, error) {
	if reason == "" {
		reason = "unavailable"
	}
	trace.Strategy = "vector_index_exact_fallback"
	trace.ExactFallbackReason = reason
	if opts.DisableExactFallback {
		return nil, trace, fmt.Errorf("collections: vector index %q unavailable: %s", def.Name, reason)
	}
	searchExact := c.SearchVectorsExact
	if vectorIndexDefinitionStrategy(def) == VectorIndexStrategyColumnGraph {
		searchExact = c.searchVectorsExactWithoutColumnRootValidation
	}
	exact, err := searchExact(query, VectorSearchOptions{
		Field:            def.Field,
		Metric:           def.Metric,
		TopK:             opts.TopK,
		Filter:           opts.Filter,
		IndexRangeFilter: opts.IndexRangeFilter,
	})
	if err != nil {
		return nil, trace, err
	}
	trace.ReturnedCount = len(exact)
	return exact, trace, nil
}

func (c *Collection) attachColumnGraphVectorSearchResultDocuments(ranked []VectorSearchResult, topK int) ([]VectorSearchResult, error) {
	limit := minInt(topK, len(ranked))
	if limit == 0 {
		return []VectorSearchResult{}, nil
	}
	results := make([]VectorSearchResult, 0, limit)
	for i := range ranked {
		if len(results) >= limit {
			break
		}
		result := ranked[i]
		document, err := c.Get(result.DocumentID)
		if err != nil {
			return nil, err
		}
		if document == nil {
			continue
		}
		result.DocumentID = append([]byte(nil), result.DocumentID...)
		result.Document = document
		results = append(results, result)
	}
	if len(results) == 0 {
		return []VectorSearchResult{}, nil
	}
	return results, nil
}
