// Package hashdb implements HashDB, a mmap-backed hash index with a slab value log.
//
// The recommended entrypoint is Open/OpenWithShards, which returns *HashDB: a
// thread-safe sharded store (formerly "gomap_distributed").
//
// For single-shard usage (not thread-safe), open DB directly or use OpenSingle.
//
// HashDB acquires an exclusive process lock on the database directory. If the
// directory is already open in another process, Open/OpenWithShards/OpenSingle
// return ErrLocked.
package hashdb
