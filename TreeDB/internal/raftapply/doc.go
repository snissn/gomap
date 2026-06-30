// Package raftapply applies committed R3a deterministic entry bytes to a local
// TreeDB replica.
//
// This package owns the committed-bytes apply boundary, deterministic rejection
// results, the create-collection command lowering slice, LogicalDigestV1, fake
// bounded stores used by tests, and append-only durable stores for apply
// progress plus result/idempotency metadata.
//
// DurableApplyProgressStore and DurableApplyResultStore use caller-owned
// metadata directories. Their default files are apply-progress-v1.log and
// apply-results-v1.log. Each file has a versioned header and checksummed frames;
// open rebuilds lookup indexes from the log and fails closed on truncation,
// corruption, unsupported versions, digest conflicts, idempotency conflicts, or
// non-monotonic progress metadata.
package raftapply
