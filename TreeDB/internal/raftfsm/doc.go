// Package raftfsm wires the single-group R3a committed-entry apply loop.
//
// This package is intentionally narrow. It consumes already-committed
// deterministic CommandEntryV1 bytes, maps the Raft term/index directly to
// raftentry.ApplyEntryID, and delegates command execution to raftapply. It does
// not provide leader routing, failover, snapshots, read-index, or
// client-visible raft_committed semantics.
package raftfsm
