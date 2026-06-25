// Package raftapply applies committed R3a deterministic entry bytes to a local
// TreeDB replica.
//
// This package is intentionally a skeleton. It owns the committed-bytes apply
// boundary, deterministic rejection results, and fake bounded stores used by
// tests while real durable apply progress/result storage and command lowering
// are implemented in later slices.
package raftapply
