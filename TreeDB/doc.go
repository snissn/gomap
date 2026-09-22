// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open, which enables the cached write-back
// layer for improved write throughput.
//
// Durability:
// Use SetSync / Batch.WriteSync if the write must cross the next configured
// durability boundary. With the command WAL active, explicit sync operations
// opt up to a durable V2 prefix even when ordinary writes use
// DurabilityWALOnRelaxed. In legacy relaxed modes without the command WAL,
// Sync operations do not fsync and may not survive power loss. In
// DurabilityWALOffRelaxed specifically, recent writes may remain buffered in
// TreeDB until a later checkpoint/flush boundary even after SetSync /
// Batch.WriteSync; they remain visible to readers in the current process but
// are still part of an unsafe durability mode.
//
// Iteration:
// Iterators are point-in-time views of the DB and must be closed.
//
// Locking:
// Open acquires an exclusive (cross-process) directory lock; if the directory is
// already open, Open returns ErrLocked.

package treedb
