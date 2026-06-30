// Package raftcluster defines the single-group Raft cluster boundary for
// TreeDB.
//
// This package owns the configuration/storage layout contract plus the first
// single-group submit/apply bridge. It does not start a consensus loop, choose
// a Raft library, install snapshots, truncate logs, or route multiple groups.
// AckRaftCommitted is only satisfied when a commit source provides explicit
// production-consensus evidence and the local committed-entry applier reports
// recoverable local coverage.
//
// The local TreeDB command WAL remains node-local crash-recovery state. It is
// not a Raft log. Commit sources may append deterministic command entries to a
// Raft log and then apply committed entries through R3a, but the application of
// a committed entry is serialized by this single-group bridge and still has to
// satisfy TreeDB's local command-WAL recoverability rules before the node
// reports raft-committed durability.
//
// The TreeDB value log under maindb/value_vlog is persistent value storage.
// Raft storage must not treat value_vlog segments as temporary WAL bytes or
// delete them by age. Value-log segments are managed by reachability-based GC
// and rewrite/compaction, independent of the consensus log.
package raftcluster
