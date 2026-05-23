// Package typedcolumn contains the transplanted experiments/colgranule
// typed-column data plane for TreeDB.
//
// The package is intentionally non-authoritative in this PR: it can build,
// encode, decode, and read typed-column part artifacts, but it does not publish
// production collection assets, register manifest/recovery state, participate in
// WAL replay, or own logical fields through typed_storage_layout. Later TreeDB
// integration layers are expected to adapt this package to #1736 mappedresource
// handles and the production control plane without reshaping the sectioned
// column-major part-image model into the existing typed-row asset format.
package typedcolumn
