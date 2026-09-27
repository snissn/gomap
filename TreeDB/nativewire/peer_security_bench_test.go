package nativewire

import (
	"testing"
	"time"
)

// Compare the same public remote-owner durable writes and four live processes.
// Inventory40 still means four live processes and 36 dormant addresses.
// TLS includes the real Raft links, control forwarding and catalog proofs.
func BenchmarkPeerSecurityRemoteOwnerCreateV1(b *testing.B) {
	b.Run("Plain", func(b *testing.B) {
		benchmarkSparseCatalogRemoteOwnerCreateV1(b, nil)
	})
	b.Run("MutualTLS", func(b *testing.B) {
		benchmarkSparseCatalogRemoteOwnerCreateV1(b, func(t testing.TB, configs []FixedPeerTCPConfigV1) int {
			ca := newPeerCAFixtureV1(t)
			now := time.Now()
			for i := range configs {
				configs[i].ClusterID = "public-path-tls-cost"
				configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), now.Add(-time.Hour), now.Add(time.Hour))
			}
			// Publication requires a catalog voter identity; the measured routes
			// and writes still enter through the identical ingress consumer.
			return 0
		})
	})
}
