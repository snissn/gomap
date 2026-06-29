// Package raftcluster defines the single-group Raft cluster boundary for
// TreeDB.
//
// This package is intentionally only a configuration, provider, and storage
// layout contract. It does not start a consensus loop, admit client writes,
// enable ack_policy=raft_committed, install snapshots, truncate logs, or route
// multiple groups.
//
// The local TreeDB command WAL remains node-local crash-recovery state. It is
// not a Raft log. Future providers may append deterministic command entries to
// a Raft log and then apply committed entries through R3a, but the application
// of a committed entry still has to satisfy TreeDB's local command-WAL
// recoverability rules before the node reports local durability.
//
// The TreeDB value log under maindb/value_vlog is persistent value storage.
// Raft storage must not treat value_vlog segments as temporary WAL bytes or
// delete them by age. Value-log segments are managed by reachability-based GC
// and rewrite/compaction, independent of the consensus log.
package raftcluster
