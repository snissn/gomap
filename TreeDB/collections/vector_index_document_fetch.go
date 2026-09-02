package collections

// FetchDocumentsForVectorIndexSearchResults materializes documents for an
// already completed vector-index search result list, preserving result order.
// It is an explicit post-search fetch helper for callers that keep the ANN
// search/scoring path no-document (IncludeDocuments=false) and then decide to
// materialize the returned top-k IDs outside the high-QPS search boundary.
//
// The fetch is bound to this CollectionReadView's snapshot. Native buffered
// callers that require search/document consistency must open the view with
// Collection.OpenCollectionReadViewForVectorIndexSearch; an ordinary
// OpenCollectionReadView selects its own visibility point. Other search paths
// can use IncludeDocuments=true on SearchVectorIndex or
// VectorIndexSearcher.Search for one-shot materialization.
//
// Results returned by SearchVectorIndexWithBuffer alias the caller's
// VectorIndexSearchBuffer; do not reuse or reset that buffer until this helper
// returns if those IDs are the fetch input.
func (v *CollectionReadView) FetchDocumentsForVectorIndexSearchResults(results []VectorIndexSearchResult, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	ids := make([][]byte, len(results))
	for i := range results {
		ids[i] = results[i].ID
	}
	return v.FetchDocumentsByID(ids, opts)
}
