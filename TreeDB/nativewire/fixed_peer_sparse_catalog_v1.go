package nativewire

import (
	"fmt"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func fixedPeerIdentityV1(value string) bool {
	if len(value) == 0 || len(value) > 128 || !utf8.ValidString(value) || strings.TrimSpace(value) != value ||
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
	if len(c.DataRoot) > 4096 || len(c.RaftRoot) > 4096 || len(c.ListenAddress) > 128 ||
		!utf8.ValidString(c.DataRoot) || !utf8.ValidString(c.RaftRoot) {
		return invalid("local path or address exceeds byte budget")
	}
	budget := fixedPeerMaxConfigBytesV1 - 1024 - 6*(len(c.DataRoot)+len(c.RaftRoot)+len(c.ListenAddress)+len(c.NodeID)+len(c.ClusterID))
	if c.Credentials != nil {
		if c.ClusterID == "" {
			return invalid("TLS requires explicit stable cluster identity")
		}
		for _, path := range []string{c.Credentials.TrustRootsFile, c.Credentials.CertificateFile, c.Credentials.PrivateKeyFile} {
			if len(path) == 0 || len(path) > 4096 || !utf8.ValidString(path) {
				return invalid("TLS credential path exceeds byte budget")
			}
			budget -= 128 + 6*len(path)
		}
	}
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
	if c.Vector != nil {
		if !preflightFixedPeerVectorInventoryV1(reflect.ValueOf(c.Vector), &budget) {
			return invalid("vector inventory exceeds byte budget")
		}
		if c.Credentials != nil {
			// The public vector protocol remains local in authenticated peer
			// mode. Immutable shard serving uses the authenticated peer transport.
			loopback := func(address string) bool {
				parsed, err := netip.ParseAddrPort(address)
				return err == nil && parsed.Addr().IsLoopback()
			}
			for _, address := range c.Vector.PublicAddresses {
				if !loopback(address) {
					return invalid("authenticated vector public listener must be loopback")
				}
			}
			for _, members := range c.Vector.ShardAddresses {
				for _, address := range members {
					if c.Vector.Identity.Immutable != (raftplacement.VectorPartitionLifecycleImmutableAuthorityV1{}) {
						if !peerPrivateEndpointV1(address) {
							return invalid("authenticated immutable vector shard listener must be private or loopback")
						}
					} else if !loopback(address) {
						return invalid("authenticated vector shard listener must be loopback")
					}
				}
			}
		}
	}
	return nil
}

// Bound vector-owned containers before cloning them. The later canonical JSON
// check remains authoritative; this conservative walk is only admission.
func preflightFixedPeerVectorInventoryV1(value reflect.Value, budget *int) bool {
	spend := func(bytes int) bool {
		if bytes > *budget {
			return false
		}
		*budget -= bytes
		return true
	}
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		return spend(8) && (value.IsNil() || preflightFixedPeerVectorInventoryV1(value.Elem(), budget))
	case reflect.Struct:
		if value.NumField() > *budget/32 || !spend(32*value.NumField()) {
			return false
		}
		for i := 0; i < value.NumField(); i++ {
			if !preflightFixedPeerVectorInventoryV1(value.Field(i), budget) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if value.Len() > *budget/32 || !spend(32*value.Len()) {
			return false
		}
		for i := 0; i < value.Len(); i++ {
			if !preflightFixedPeerVectorInventoryV1(value.Index(i), budget) {
				return false
			}
		}
		return true
	case reflect.Map:
		if value.Len() > *budget/64 || !spend(64*value.Len()) {
			return false
		}
		for iter := value.MapRange(); iter.Next(); {
			if !preflightFixedPeerVectorInventoryV1(iter.Key(), budget) || !preflightFixedPeerVectorInventoryV1(iter.Value(), budget) {
				return false
			}
		}
		return true
	case reflect.String:
		return value.Len() <= *budget/6 && spend(6*value.Len())
	default:
		return spend(32)
	}
}
