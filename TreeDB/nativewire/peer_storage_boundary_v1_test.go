package nativewire

import (
	"os"
	"testing"
)

func TestPeerSecurityMissingPersistentRootRefusesRebootstrapV1(t *testing.T) {
	for _, lost := range []string{"data", "raft"} {
		t.Run(lost, func(t *testing.T) {
			fixture, config := peerTransportFixtureV1(t)
			fixture.Close()
			node, err := OpenFixedPeerTCPRuntimeV1(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			path := config.DataRoot
			if lost == "raft" {
				path = config.RaftRoot
			}
			// These are test-owned temporary directories, simulating loss of
			// one persistent volume while the other remains present.
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
			if reopened, err := OpenFixedPeerTCPRuntimeV1(config); err == nil {
				reopened.Close()
				t.Fatal("missing persistent root was silently recreated/rebootstrapped")
			}
		})
	}
}
