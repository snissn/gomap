// Package raftharness provides a deterministic in-process harness for the
// TreeDB single-group Raft apply substrate.
//
// The harness records and replays injected committed raftfsm entries across
// local node directories. It is integration-test scaffolding for failover,
// restart, and follower catch-up behavior around the committed-entry boundary.
// It does not run a Raft library, elect leaders, form quorums, replicate logs,
// or prove production consensus.
package raftharness
