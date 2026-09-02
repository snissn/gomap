// Package raftharness provides a deterministic in-process harness for the
// TreeDB single-group Raft apply substrate.
//
// The harness records and replays injected committed raftfsm entries across
// local node directories. It is integration-test scaffolding for failover,
// restart, follower catch-up, and logical snapshot-prefix install behavior
// around the committed-entry boundary. It does not run a Raft library, elect
// leaders, form quorums, replicate logs, transfer production snapshots, or
// prove production consensus.
package raftharness
