// Package raftplacement validates the first TreeDB multi-group placement
// catalog shape and resolves collection identities to Raft group IDs.
//
// This package is deliberately pure catalog logic. It does not route requests,
// start Raft groups, expose submitter APIs, choose leaders, maintain a meta
// Raft group, rebalance data, or implement token/ring partitioning.
package raftplacement
