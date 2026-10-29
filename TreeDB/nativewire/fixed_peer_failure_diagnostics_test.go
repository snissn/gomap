package nativewire

import (
	"context"
	"testing"
	"time"
)

// Retain observations from existing fixture calls; failure reporting performs
// no new requests and therefore does not change polling or publication bounds.
func fixedPeerFailureStatusRecorder(t *testing.T, nodes []*FixedPeerTCPRuntimeV1) (func(context.Context, *FixedPeerTCPRuntimeV1) (FixedPeerTCPStatusV1, error), func(), func(string)) {
	t.Helper()
	history := newFixedPeerElectionStatusHistoryV1()
	lastSuccess := make(map[*FixedPeerTCPRuntimeV1]FixedPeerTCPStatusV1)
	lastError := make(map[*FixedPeerTCPRuntimeV1]error)
	observe := func(ctx context.Context, node *FixedPeerTCPRuntimeV1) (FixedPeerTCPStatusV1, error) {
		started := time.Now()
		status, err := node.Status(ctx)
		history.observe(node.config.NodeID, started, status, err)
		if err != nil {
			lastError[node] = err
		} else {
			lastSuccess[node] = status
		}
		return status, err
	}
	report := func() {
		if !t.Failed() {
			return
		}
		history.report(t)
		for _, node := range nodes {
			if node != nil {
				t.Logf("fixture last observation node=%s request_timeout=%s raft_timeout=%s status=%+v last_error=%v", node.config.NodeID, node.config.RequestTimeout, node.config.RaftTimeout, lastSuccess[node], lastError[node])
			}
		}
	}
	return observe, report, history.setPhase
}
