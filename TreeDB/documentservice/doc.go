// Package documentservice exposes TreeDB's pre-alpha Haystack-style document
// service contract.
//
// The package is the Go implementation seam for the HTTP/JSON API and optional
// native typed transport documented in docs/TREEDB_DOCUMENT_SERVICE_API.md. It
// maps Haystack-style documents (id/content/embedding/meta/score) onto TreeDB
// collections, supports legacy exact document scans and compatible indexed
// dense search, and serves keyword/hybrid retrieval through collection-native
// SearchText/SearchHybrid APIs. Selected typed column_graph serving requires
// explicit admission and reflects typed mutations before Fold; its internal
// empty/exact/HNSW branch is reported in owned dense work proof.
//
// Keyword/hybrid filters and unavailable sources fail closed without document
// scan fallback. The contract is pre-alpha: schemas may change before TreeDB
// stabilizes. Route correctness and work evidence do not establish performance
// qualification.
package documentservice
