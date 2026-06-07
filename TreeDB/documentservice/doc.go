// Package documentservice exposes TreeDB's pre-alpha Haystack-style document
// service contract.
//
// The package is the Go implementation seam for the HTTP/JSON API documented in
// docs/TREEDB_DOCUMENT_SERVICE_API.md. It maps Haystack-style documents
// (id/content/embedding/meta/score) onto TreeDB collections, supports exact
// dense-vector scoring with metadata filters, and intentionally fails closed for
// keyword and hybrid retrieval until TreeDB's ranked text/hybrid executors are
// implemented.
//
// The contract is pre-alpha: request/response schemas may change before TreeDB
// stabilizes. Exact dense scoring here is the correctness/MVP service path for
// Python and Haystack clients; it is not TreeDB's high-QPS column_graph ANN
// serving path.
package documentservice
