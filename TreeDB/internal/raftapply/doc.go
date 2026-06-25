// Package raftapply applies committed R3a deterministic entry bytes to a local
// TreeDB replica.
//
// This package owns the committed-bytes apply boundary, deterministic rejection
// results, the create-collection command lowering slice, LogicalDigestV1, and
// fake bounded stores used by tests while durable apply progress/result storage
// is implemented in later slices.
package raftapply
