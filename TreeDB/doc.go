// Package treedb provides the public TreeDB API.
//
// The recommended entrypoint is Open/OpenCached, which enables the cached
// write-back layer for improved write throughput. To open the backend-only
// engine (no caching), use OpenBackend or set Options.Mode = ModeBackend.
//
// Durability:
// Use SetSync / Batch.WriteSync if the write must survive process crashes and
// power loss. Non-Sync writes are not guaranteed durable.
//
// Iteration:
// Iterators are point-in-time views of the DB and must be closed.
//
// Locking:
// Open acquires an exclusive (cross-process) directory lock; if the directory is
// already open, Open returns ErrLocked.

package treedb
