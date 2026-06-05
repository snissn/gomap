package collections

// FetchDocumentsForVectorIndexSearchResults materializes documents for an
// already completed vector-index search result list, preserving result order.
// It is an explicit post-search fetch helper for callers that keep the ANN
// search/scoring path no-document (IncludeDocuments=false) and then decide to
// materialize the returned top-k IDs outside the high-QPS search boundary.
//
// The fetch is bound to this CollectionReadView's snapshot, not implicitly to
// the snapshot used by the search call that produced results. Open the read view
// at the visibility point you want to materialize. If same-snapshot search plus
// document materialization is required, use IncludeDocuments=true on
// SearchVectorIndex or VectorIndexSearcher.Search instead and report those
// document counters as part of that explicit with-documents path.
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
