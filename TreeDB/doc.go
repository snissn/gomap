// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open, which enables the cached write-back
// layer for improved write throughput.
//
// Durability:
// Use SetSync / Batch.WriteSync if the write must cross the next configured
// durability boundary. In relaxed durability modes
// (Options.Durability = DurabilityWALOnRelaxed or DurabilityWALOffRelaxed),
// Sync operations do not fsync and may not survive power loss. In
// DurabilityWALOffRelaxed specifically, Checkpoint is the backend publication
// and cleanup boundary. Successful writes are still immediately visible to
// normal DB readers. Snapshots remain point-in-time views, so a write is only
// visible to snapshots acquired after that write; the relaxed behavior only
// weakens crash durability and backend publication timing.
//
// Iteration:
// Iterators are point-in-time views of the DB and must be closed.
//
// Locking:
// Open acquires an exclusive (cross-process) directory lock; if the directory is
// already open, Open returns ErrLocked.

package treedb
