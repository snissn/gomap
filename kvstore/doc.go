// Package kvstore defines a shared, minimal key/value store API used across gomap
// engines (HashDB, TreeDB, BTreeOnHashDB) and tooling like cmd/unified_bench.
//
// Iterator semantics (performance-first):
//   - Key() and Value() may return read-only views into underlying storage.
//   - Returned slices are valid until the next Next()/Close() call on that iterator.
//   - Callers must not retain or modify returned views across iterator movement.
//   - Use KeyCopy/ValueCopy for caller-owned stable bytes.
package kvstore
