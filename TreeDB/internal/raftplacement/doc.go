// Package raftplacement validates the first TreeDB multi-group placement
// catalog shapes and resolves collection identities to Raft group IDs or
// token partitions through explicit helper APIs.
//
// This package is deliberately pure catalog validation and simulation logic. It
// does not route requests, start Raft groups, expose submitter APIs, choose
// leaders, maintain a meta Raft group, rebalance data, or enable token/ring
// production request routing.
package raftplacement
