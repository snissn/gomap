package nativewire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// Retain observations from existing fixture calls; failure reporting performs
// no new requests and therefore does not change polling or publication bounds.
func fixedPeerFailureStatusRecorder(t *testing.T, nodes []*FixedPeerTCPRuntimeV1) (func(context.Context, *FixedPeerTCPRuntimeV1) (FixedPeerTCPStatusV1, error), func()) {
	t.Helper()
	lastSuccess := make(map[*FixedPeerTCPRuntimeV1]FixedPeerTCPStatusV1)
	lastError := make(map[*FixedPeerTCPRuntimeV1]error)
	observe := func(ctx context.Context, node *FixedPeerTCPRuntimeV1) (FixedPeerTCPStatusV1, error) {
		status, err := node.Status(ctx)
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
		for _, node := range nodes {
			if node != nil {
				t.Logf("fixture last observation node=%s request_timeout=%s raft_timeout=%s status=%+v last_error=%v", node.config.NodeID, node.config.RequestTimeout, node.config.RaftTimeout, lastSuccess[node], lastError[node])
			}
		}
	}
	return observe, report
}

const (
	sparseCatalogSetupCheckpointLimitV1 = 6
	sparseCatalogSetupStatusLimitV1     = 4096
	sparseCatalogSetupErrorLimitV1      = 1024
	sparseCatalogSetupTruncatedV1       = "...[truncated]"
)

type sparseCatalogSetupCheckpointV1 struct {
	phase   string
	elapsed time.Duration
}

type sparseCatalogSetupObservationV1 struct {
	calls         uint64
	status        string
	statusPhase   string
	statusElapsed time.Duration
	err           string
	errorPhase    string
	errorElapsed  time.Duration
}

// This setup-only recorder owns bounded text, not status payloads or errors.
// Elapsed offsets start at helper entry, after the caller's context was created.
// Full status formatting may allocate before its retained text is truncated.
type sparseCatalogSetupFailureDiagnosticsV1 struct {
	started      time.Time
	phase        string
	checkpoints  []sparseCatalogSetupCheckpointV1
	nodes        []raftcluster.NodeID
	observations map[raftcluster.NodeID]*sparseCatalogSetupObservationV1
}

func newSparseCatalogSetupFailureDiagnosticsV1(configs []FixedPeerTCPConfigV1) *sparseCatalogSetupFailureDiagnosticsV1 {
	d := &sparseCatalogSetupFailureDiagnosticsV1{
		started:      time.Now(),
		observations: make(map[raftcluster.NodeID]*sparseCatalogSetupObservationV1, len(configs)),
	}
	for _, config := range configs {
		if _, exists := d.observations[config.NodeID]; !exists {
			d.nodes = append(d.nodes, config.NodeID)
			d.observations[config.NodeID] = &sparseCatalogSetupObservationV1{}
		}
	}
	return d
}

func (d *sparseCatalogSetupFailureDiagnosticsV1) checkpoint(phase string) {
	d.phase = phase
	if len(d.checkpoints) < sparseCatalogSetupCheckpointLimitV1 {
		d.checkpoints = append(d.checkpoints, sparseCatalogSetupCheckpointV1{phase: phase, elapsed: time.Since(d.started)})
	}
}

func sparseCatalogSetupBoundTextV1(text string, limit int) string {
	if len(text) > limit {
		return text[:limit-len(sparseCatalogSetupTruncatedV1)] + sparseCatalogSetupTruncatedV1
	}
	// Error strings may otherwise keep a much larger backing string alive.
	return strings.Clone(text)
}

func (d *sparseCatalogSetupFailureDiagnosticsV1) observe(node raftcluster.NodeID, status FixedPeerTCPStatusV1, err error) {
	observation, configured := d.observations[node]
	if !configured {
		return
	}
	observation.calls++
	elapsed := time.Since(d.started)
	if err != nil {
		observation.err = sparseCatalogSetupBoundTextV1(err.Error(), sparseCatalogSetupErrorLimitV1)
		observation.errorPhase, observation.errorElapsed = d.phase, elapsed
		return
	}
	observation.status = sparseCatalogSetupBoundTextV1(fmt.Sprintf("%+v", status), sparseCatalogSetupStatusLimitV1)
	observation.statusPhase, observation.statusElapsed = d.phase, elapsed
}

func (d *sparseCatalogSetupFailureDiagnosticsV1) report(failed bool, logf func(string, ...any)) {
	if !failed {
		return
	}
	logf("sparse catalog setup phase=%s elapsed_since_helper_entry=%s checkpoints=%+v", d.phase, time.Since(d.started), d.checkpoints)
	for _, node := range d.nodes {
		observation := d.observations[node]
		logf("sparse catalog setup node=%s status_calls=%d last_status_phase=%s last_status_elapsed=%s last_status=%s last_error_phase=%s last_error_elapsed=%s last_error=%s",
			node, observation.calls, observation.statusPhase, observation.statusElapsed, observation.status,
			observation.errorPhase, observation.errorElapsed, observation.err)
	}
}

func TestSparseCatalogSetupFailureDiagnosticsV1(t *testing.T) {
	configs := []FixedPeerTCPConfigV1{{NodeID: "configured"}, {NodeID: "unobserved"}}
	for _, phase := range []string{"catalog-leader", "catalog-publish", "all-node-readiness"} {
		t.Run(phase, func(t *testing.T) {
			d := newSparseCatalogSetupFailureDiagnosticsV1(configs)
			d.checkpoint("child-setup")
			d.checkpoint(phase)
			d.observe("configured", FixedPeerTCPStatusV1{NodeID: "configured", RecoveryState: "opened"}, nil)
			var output strings.Builder
			d.report(true, func(format string, args ...any) { fmt.Fprintf(&output, format+"\n", args...) })
			for _, want := range []string{"phase=" + phase, "elapsed_since_helper_entry=", "node=configured status_calls=1", "last_status_phase=" + phase, "RecoveryState:opened", "node=unobserved status_calls=0"} {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("failure report missing %q:\n%s", want, output.String())
				}
			}
			d.report(false, func(string, ...any) { t.Fatal("successful setup emitted diagnostics") })
		})
	}
	t.Run("observations", func(t *testing.T) {
		d := newSparseCatalogSetupFailureDiagnosticsV1(configs)
		d.checkpoint("catalog-leader")
		d.observe("configured", FixedPeerTCPStatusV1{RecoveryState: "first-success"}, nil)
		first := *d.observations["configured"]
		d.checkpoint("catalog-publish")
		d.observe("configured", FixedPeerTCPStatusV1{RecoveryState: "failed-result"}, fmt.Errorf("original status error"))
		failed := *d.observations["configured"]
		if failed.calls != 2 || failed.status != first.status || failed.statusPhase != first.statusPhase || failed.statusElapsed != first.statusElapsed || failed.err != "original status error" || failed.errorPhase != "catalog-publish" || failed.errorElapsed < first.statusElapsed {
			t.Fatalf("error replaced successful history or lost its own phase: %+v", failed)
		}
		d.checkpoint("all-node-readiness")
		d.observe("configured", FixedPeerTCPStatusV1{RecoveryState: "latest-success"}, nil)
		last := *d.observations["configured"]
		if last.calls != 3 || !strings.Contains(last.status, "latest-success") || last.statusPhase != "all-node-readiness" || last.statusElapsed < failed.errorElapsed || last.err != failed.err || last.errorPhase != failed.errorPhase || last.errorElapsed != failed.errorElapsed {
			t.Fatalf("success replaced error history or lost its own phase: %+v", last)
		}
		d.observe("outside-inventory", FixedPeerTCPStatusV1{}, fmt.Errorf("not configured"))
		if len(d.observations) != len(configs) || len(d.nodes) != len(configs) || *d.observations["configured"] != last {
			t.Fatal("unconfigured node changed retained observations")
		}
	})
	t.Run("bounds", func(t *testing.T) {
		d := newSparseCatalogSetupFailureDiagnosticsV1(append(configs, configs[0]))
		for i := 0; i < 20; i++ {
			d.checkpoint(fmt.Sprintf("phase-%d", i))
		}
		if len(d.checkpoints) != sparseCatalogSetupCheckpointLimitV1 || len(d.nodes) != len(configs) || len(d.observations) != len(configs) || d.phase != "phase-19" {
			t.Fatalf("unbounded setup history: %+v", d)
		}
		for i, checkpoint := range d.checkpoints {
			if checkpoint.phase != fmt.Sprintf("phase-%d", i) || checkpoint.elapsed < 0 || i > 0 && checkpoint.elapsed < d.checkpoints[i-1].elapsed {
				t.Fatalf("checkpoint order/offset changed: %+v", d.checkpoints)
			}
		}
		d.observe("configured", FixedPeerTCPStatusV1{RecoveryState: strings.Repeat("s", 2*sparseCatalogSetupStatusLimitV1)}, nil)
		d.observe("configured", FixedPeerTCPStatusV1{}, fmt.Errorf("%s", strings.Repeat("e", 2*sparseCatalogSetupErrorLimitV1)))
		observation := d.observations["configured"]
		if len(observation.status) != sparseCatalogSetupStatusLimitV1 || len(observation.err) != sparseCatalogSetupErrorLimitV1 || !strings.HasSuffix(observation.status, sparseCatalogSetupTruncatedV1) || !strings.HasSuffix(observation.err, sparseCatalogSetupTruncatedV1) {
			t.Fatalf("retained text exceeded bounds: status=%d error=%d", len(observation.status), len(observation.err))
		}
		if got := sparseCatalogSetupBoundTextV1("short", sparseCatalogSetupErrorLimitV1); got != "short" {
			t.Fatalf("bounded text changed: %q", got)
		}
	})
}
