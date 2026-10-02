package nativewire

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// Use the public opener in an identical base/head harness. Directory and port
// selection, shutdown, and election waiting are outside the startup timer.
func BenchmarkFixedPeerTCPRuntimeStartupV1(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()
	for range b.N {
		var reservations []net.Listener
		address := func() string {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			reservations = append(reservations, listener)
			return listener.Addr().String()
		}
		control, catalog, data := address(), address(), address()
		features := raftcluster.DefaultFeatureSet()
		features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.Version{Major: 1}})
		root := b.TempDir()
		config := FixedPeerTCPConfigV1{
			NodeID: "node", DataRoot: filepath.Join(root, "data"), RaftRoot: filepath.Join(root, "raft"), ListenAddress: control,
			Nodes:      []FixedPeerTCPNodeV1{{ID: "node", Address: control}},
			Catalog:    FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "node", Features: features, Peers: []raftcluster.Peer{{ID: "node", Address: catalog, Capabilities: features}}},
			Groups:     []FixedPeerTCPGroupV1{{ID: "data", BootstrapNode: "node", Peers: []raftcluster.Peer{{ID: "node", Address: data}}}},
			RaftListen: map[raftcluster.GroupID]string{"meta": catalog, "data": data}, RequestTimeout: 2 * time.Second, RaftTimeout: 300 * time.Millisecond,
		}
		for _, listener := range reservations {
			_ = listener.Close()
		}
		b.StartTimer()
		runtime, err := OpenFixedPeerTCPRuntimeV1(config)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if err := runtime.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
