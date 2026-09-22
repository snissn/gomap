// Package raftplacement validates the first TreeDB multi-group placement
// catalog shapes and resolves explicit route requests to Raft group decisions
// or token partitions through explicit helper APIs.
//
// This package is deliberately pure catalog validation, simulation, and
// route-decision logic. CatalogMetaAuthorityV1 additionally provides the local
// applied-state seam for a declared replicated meta Raft group; it does not
// start that group, choose leaders, rebalance data, or provide a forwarding
// protocol.
package raftplacement
