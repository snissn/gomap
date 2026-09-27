package nativewire

import (
	"net"
	"net/netip"
	"sort"
	"sync/atomic"
)

type peerIPBytesV1 struct{ read, written atomic.Uint64 }

// PeerNetworkBytesV1 counts TCP stream bytes at the raw socket, including TLS
// handshakes and rejected handshakes. RemoteIP is physical attribution, NOT an
// authenticated identity. Unknown addresses share one fixed bucket. These
// counters exclude IP/TCP headers and retransmissions; use host NIC/VPC flow
// records for wire and cross-AZ accounting. Receive and send must not be added
// across both endpoints to claim unique transferred bytes.
type PeerNetworkBytesV1 struct {
	RemoteIP                string
	ReadBytes, WrittenBytes uint64
}

func peerNetworkInventoryV1(config FixedPeerTCPConfigV1) map[string]*peerIPBytesV1 {
	result := make(map[string]*peerIPBytesV1)
	add := func(endpoint string) {
		address, err := netip.ParseAddrPort(endpoint)
		if err != nil {
			return
		}
		key := address.Addr().Unmap().String()
		if result[key] == nil {
			result[key] = &peerIPBytesV1{}
		}
	}
	for _, node := range config.Nodes {
		add(node.Address)
	}
	for _, peer := range config.Catalog.Peers {
		add(peer.Address)
	}
	for _, group := range config.Groups {
		for _, peer := range group.Peers {
			add(peer.Address)
		}
	}
	return result
}

func (a *peerNodeAdmissionV1) networkForV1(address net.Addr) *peerIPBytesV1 {
	if tcp, ok := address.(*net.TCPAddr); ok {
		if ip, ok := netip.AddrFromSlice(tcp.IP); ok {
			if value := a.network[ip.Unmap().String()]; value != nil {
				return value
			}
		}
	}
	return &a.unknown
}

// NetworkStatsV1 snapshots the fixed startup inventory on explicit diagnostics,
// never on the per-request routing/status path. No remote input grows the map.
func (p *PeerTransportV1) NetworkStatsV1() []PeerNetworkBytesV1 {
	if p == nil || p.admission == nil {
		return nil
	}
	a := p.admission
	result := make([]PeerNetworkBytesV1, 0, len(a.network)+1)
	for ip, value := range a.network {
		result = append(result, PeerNetworkBytesV1{ip, value.read.Load(), value.written.Load()})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].RemoteIP < result[j].RemoteIP })
	return append(result, PeerNetworkBytesV1{"unknown", a.unknown.read.Load(), a.unknown.written.Load()})
}
