// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open, which enables the cached write-back
// layer for improved write throughput.
//
// Durability:
// Use SetSync / Batch.WriteSync if the write must survive process crashes.
// When Options.RelaxedSync is enabled, Sync operations are crash-consistent
// only (kernel buffer flush) and may not survive power loss.
//
// Iteration:
// Iterators are point-in-time views of the DB and must be closed.
//
// Locking:
// Open acquires an exclusive (cross-process) directory lock; if the directory is
// already open, Open returns ErrLocked.

package treedb
