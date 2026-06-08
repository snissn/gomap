// Package documentservice exposes TreeDB's pre-alpha Haystack-style document
// service contract.
//
// The package is the Go implementation seam for the HTTP/JSON API documented in
// docs/TREEDB_DOCUMENT_SERVICE_API.md. It maps Haystack-style documents
// (id/content/embedding/meta/score) onto TreeDB collections, supports exact
// dense-vector scoring with metadata filters, and serves keyword/hybrid retrieval
// through collection-native SearchText/SearchHybrid APIs. Keyword/hybrid filters
// and unavailable indexes fail closed; the service does not scan documents as a
// fallback.
//
// The contract is pre-alpha: request/response schemas may change before TreeDB
// stabilizes. Exact dense scoring here is the correctness/MVP service path for
// Python and Haystack clients; hybrid vector sources use TreeDB's declared
// collection vector index and fail closed when it is unavailable.
package documentservice
