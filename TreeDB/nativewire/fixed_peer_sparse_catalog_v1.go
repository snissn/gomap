package nativewire

import (
	"fmt"
	"slices"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func fixedPeerIdentityV1(value string) bool {
	if len(value) == 0 || len(value) > 128 || strings.TrimSpace(value) != value ||
		value == "." || value == ".." || strings.ContainsAny(value, "/\\") {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func cloneFixedPeerGroupV1(group FixedPeerTCPGroupV1) FixedPeerTCPGroupV1 {
	group.Features.Required = slices.Clone(group.Features.Required)
	group.Peers = slices.Clone(group.Peers)
	for i := range group.Peers {
		group.Peers[i].Capabilities.Required = slices.Clone(group.Peers[i].Capabilities.Required)
	}
	return group
}

// This conservative JSON-size budget counts escaped string bytes and container
// overhead before copying caller input. The canonical encoded size is checked
// separately after feature normalization. No resource admission uses an
// unbounded JSON encode/decode merely to discover that the inventory is large.
func preflightFixedPeerConfigV1(c FixedPeerTCPConfigV1) error {
	invalid := func(reason string) error {
		return fmt.Errorf("%w: %s", raftcluster.ErrInvalidConfig, reason)
	}
	if len(c.Nodes) < 1 || len(c.Nodes) > fixedPeerMaxNodesV1 {
		return invalid("require 1..1024 inventory nodes")
	}
	if len(c.Groups) < 1 || len(c.Groups) > fixedPeerMaxDataGroupsV1 {
		return invalid("data-group inventory exceeds catalog capacity")
	}
	if len(c.RaftListen) > fixedPeerMaxHostedDataGroupsV1+1 {
		return invalid("too many local Raft listeners")
	}
	if !fixedPeerIdentityV1(string(c.NodeID)) || (c.ClusterID != "" && !fixedPeerIdentityV1(c.ClusterID)) {
		return invalid("invalid bounded node or cluster identity")
	}
	if len(c.DataRoot) > 4096 || len(c.RaftRoot) > 4096 || len(c.ListenAddress) > 128 {
		return invalid("local path or address exceeds byte budget")
	}
	budget := fixedPeerMaxConfigBytesV1 - 1024 - 6*(len(c.DataRoot)+len(c.RaftRoot)+len(c.ListenAddress)+len(c.NodeID)+len(c.ClusterID))
	spend := func(bytes int) bool {
		budget -= bytes
		return budget >= 0
	}
	featuresOK := func(features raftcluster.FeatureSet) bool {
		if len(features.Required) > 64 || !spend(128) {
			return false
		}
		for _, feature := range features.Required {
			if len(feature.Name) > 128 || !spend(64+6*len(feature.Name)) {
				return false
			}
		}
		return true
	}
	groupOK := func(group FixedPeerTCPGroupV1) bool {
		if !fixedPeerIdentityV1(string(group.ID)) || !fixedPeerIdentityV1(string(group.BootstrapNode)) ||
			len(group.Peers) == 0 || len(group.Peers) > fixedPeerMaxPeersV1 ||
			!spend(256+6*(len(group.ID)+len(group.BootstrapNode))) || !featuresOK(group.Features) {
			return false
		}
		for _, peer := range group.Peers {
			if !fixedPeerIdentityV1(string(peer.ID)) || len(peer.Address) > 128 ||
				!spend(128+6*(len(peer.ID)+len(peer.Address))) || !featuresOK(peer.Capabilities) {
				return false
			}
		}
		return true
	}
	for _, node := range c.Nodes {
		if !fixedPeerIdentityV1(string(node.ID)) || len(node.Address) > 128 ||
			!spend(64+6*(len(node.ID)+len(node.Address))) {
			return invalid("node inventory exceeds identity or byte budget")
		}
	}
	if !groupOK(c.Catalog) {
		return invalid("catalog group exceeds identity, member or byte budget")
	}
	for _, group := range c.Groups {
		if !groupOK(group) {
			return invalid("data group exceeds identity, member or byte budget")
		}
	}
	for group, address := range c.RaftListen {
		if !fixedPeerIdentityV1(string(group)) || len(address) > 128 || !spend(32+6*(len(group)+len(address))) {
			return invalid("local listener map exceeds identity or byte budget")
		}
	}
	return nil
}
