// Package raftplacement validates the first TreeDB multi-group placement
// catalog shapes and resolves explicit route requests to Raft group decisions
// or token partitions through explicit helper APIs.
//
// This package is deliberately pure catalog validation, simulation, and
// route-decision logic. It does not submit routed requests, start Raft groups,
// expose submitter APIs, choose leaders, maintain a meta Raft group, rebalance
// data, or provide native-wire/Mongo server routing.
package raftplacement
