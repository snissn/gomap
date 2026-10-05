package nativewire

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestPeerPopulationAuditPreMarshalBoundV1(t *testing.T) {
	p := ColocatedAuditPlanV1{Version: 1, RunID: "population", RequiredAppliedIndex: 1, Population: &collections.VectorSourcePopulationExpectationV1{Rows: math.MaxUint64, Dimensions: 4096, SHA256: strings.Repeat("a", 64), Limits: collections.VectorSourcePopulationLimitsV1{MaxRows: math.MaxUint64, MaxIDBytes: math.MaxUint64, MaxSourceRecordBytes: math.MaxUint64, MaxTotalBytes: math.MaxUint64, MaxInspected: math.MaxUint64}}}
	body := fixedPeerRequestV1{ColocatedAudit: &p}
	bound, err := preflightPeerRequestBytesV1(body)
	raw, marshalErr := json.Marshal(body)
	if err != nil || marshalErr != nil || bound < int64(len(raw)) {
		t.Fatalf("population JSON=%d bound=%d errors=%v/%v", len(raw), bound, err, marshalErr)
	}
	p.Population.SHA256 = strings.Repeat("\x00", fixedPeerMaxRPCBytesV1/6)
	if _, err := preflightPeerRequestBytesV1(body); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
		t.Fatalf("oversized population before marshal: %v", err)
	}
	p.Population.SHA256 = strings.Repeat("a", 64)
	p.Writes = make([]ColocatedAuditWriteV1, 1)
	if _, err := preflightPeerRequestBytesV1(body); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
		t.Fatalf("partial ledger admitted: %v", err)
	}
}

func TestPeerSecurityByteRefusalIsDefiniteBeforeSendV1(t *testing.T) {
	fixture, config := peerTransportFixtureV1(t)
	fixture.Close()
	node, err := fixedPeerOpenTestRuntimeV1(t, config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := node.client
	if _, err := client.Status(ctx, config.NodeID); err != nil {
		t.Fatal(err)
	}
	blocked, err := client.peerTransport.admission.acquire("control-write", peerBytesV1, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.release()
	before := client.peerTransport.ResourceStatsV1().WrittenBytes
	if _, err := client.Submit(ctx, config.NodeID, nil, ClusterRequestMetadata{}); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("pre-send byte refusal: %v", err)
	}
	if after := client.peerTransport.ResourceStatsV1().WrittenBytes; after != before {
		t.Fatalf("refused mutation sent network bytes: %d -> %d", before, after)
	}
	if _, err := client.Status(ctx, config.NodeID); err != nil {
		t.Fatalf("hot write bytes starved catalog/status reserve: %v", err)
	}
	blocked.release()
	if current := client.peerTransport.ResourceStatsV1().Current; current[peerRequestsV1] != 0 || current[peerBytesV1] != 0 {
		t.Fatalf("request leases leaked: %v", current)
	}
}

func TestPeerSecurityRequestExpansionRefusesBeforeAllocationV1(t *testing.T) {
	body := fixedPeerRequestV1{}
	body.Route.Collection = strings.Repeat("\x00", 2<<20)
	if _, err := preflightPeerRequestBytesV1(body); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
		t.Fatalf("unbounded JSON escaping accepted: %v", err)
	}
}

func TestPeerSecurityDrainKeepsAdmittedForwardingV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	a := transport.admission
	ingress, err := a.request(context.Background(), "control-write", 64<<10, peerRequestIngressV1)
	if err != nil {
		t.Fatal(err)
	}
	defer ingress.release()
	a.beginDrain()
	for _, scope := range []string{"control-forward", "control-read", "native", "shard:" + string(config.Groups[0].ID)} {
		work, err := a.request(ingress.ctx, scope, 64<<10, peerRequestDescendantV1)
		if err != nil {
			t.Fatalf("admitted %s refused: %v", scope, err)
		}
		work.release()
		if work, err := a.request(context.Background(), scope, 64<<10, peerRequestDescendantV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
			work.release()
			t.Fatalf("fresh %s accepted: %v", scope, err)
		}
	}
	// A capability never authorizes fresh ingress, even on its owning transport.
	if work, err := a.request(ingress.ctx, "native", 1, peerRequestIngressV1); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		work.release()
		t.Fatalf("fresh ingress reused capability: %v", err)
	}
}

func TestSplitProofNodeReservePreservesNestedReadsV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Credentials = &PeerCredentialsV1{}
	config.Vector = &FixedPeerTCPVectorConfigV1{}
	a, err := newPeerNodeAdmissionV1(config)
	if err != nil {
		t.Fatal(err)
	}
	defer a.cancel()
	var held []peerResourceLeaseV1
	defer func() {
		for i := range held {
			held[i].release()
		}
	}()
	// Consume actual shared leases while leaving only reserved dependency work.
	for _, kind := range []int{peerRequestsV1, peerBytesV1} {
		amount := int64(1)
		if kind == peerBytesV1 {
			amount = 1 << 16
		}
		for scope := range a.scopes {
			if scope == "control-proof" || scope == "control-read" {
				continue
			}
			for {
				lease, err := a.acquire(scope, kind, amount)
				if err != nil {
					break
				}
				held = append(held, lease)
			}
		}
		if a.shared[kind] != a.sharedLimits[kind] {
			t.Fatalf("fixture did not saturate shared resource %d: %d/%d", kind, a.shared[kind], a.sharedLimits[kind])
		}
	}
	bound, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{Entry: make([]byte, 128<<10)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		work, err := a.work("control-proof", peerRequestsV1, peerControlBytesV1(bound))
		if err != nil {
			t.Fatalf("reserved proof leg %d: %v", i, err)
		}
		defer work.release()
	}
	if work, err := a.work("control-proof", peerRequestsV1, peerControlBytesV1(bound)); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		work.release()
		t.Fatalf("third proof exceeded reserved capacity: %v", err)
	}
	read, err := a.work("control-read", peerRequestsV1, 4<<20)
	if err != nil {
		t.Fatalf("proofs starved reserved leaf read: %v", err)
	}
	read.release()
	for _, operation := range []string{"vector-split-source-proof", "vector-split-receipt", "/v1/vector-split-source-proof", "/v1/vector-split-receipt"} {
		if peerControlScopeV1(operation) != "control-proof" || peerControlRequestModeV1(operation, peerRequestIngressV1) != peerRequestInternalV1 {
			t.Fatalf("proof dependency classification changed: %s", operation)
		}
	}
}

func TestPeerSecurityColocatedRequestPreflightV1(t *testing.T) {
	audit := func() *ColocatedAuditPlanV1 {
		p := &ColocatedAuditPlanV1{Version: 1, RunID: "preflight", HighestNewCommitIndex: math.MaxUint64, RequiredAppliedIndex: math.MaxUint64, Final: []ColocatedAuditStateV1{{ID: []byte{1}, Document: []byte{2}}}}
		for i := 0; i < 6; i++ {
			r := public.InsertRequestV1{Version: math.MaxUint32, Generation: public.GenerationIDV1{Index: "\x00", Generation: math.MaxUint64}, ID: []byte{1}, IdempotencyKey: []byte{2}, Vector: []float32{math.MaxFloat32, -math.SmallestNonzeroFloat32}, Document: []byte{3}}
			w := ColocatedAuditWriteV1{Response: public.MutationResponseV1{Generation: r.Generation, OwnerGroup: "\x00", VisibilityToken: []byte{4}, CommitTerm: math.MaxUint64, CommitIndex: math.MaxUint64, AppliedIndex: math.MaxUint64, Coverage: math.MaxUint64, LiveRevision: math.MaxUint64, Matched: math.MaxUint64, Modified: math.MaxUint64, Deleted: math.MaxUint64, Counters: public.MutationCountersV1{Routes: math.MaxUint64, Forwards: math.MaxUint64, Commits: math.MaxUint64, Replications: math.MaxUint64, Applies: math.MaxUint64, VisibilityProofs: math.MaxUint64}}}
			if i%2 == 0 {
				w.Replace = &r
			} else {
				w.Delete = &public.DeleteRequestV1{Version: r.Version, Generation: r.Generation, ID: r.ID, IdempotencyKey: r.IdempotencyKey}
			}
			p.Writes = append(p.Writes, w)
		}
		return p
	}
	p := audit()
	// Conservative accounting may exceed the exact 512 KiB plan limit. It must
	// still admit the finite pre-Marshal shape; validation keeps its exact cap.
	p.Writes[0].Replace.Document = make([]byte, 300<<10)
	body := fixedPeerRequestV1{ColocatedAudit: p, VectorInsert: &VectorPartitionRoutedInsertV1{Request: *p.Writes[0].Replace}, VectorMutation: &VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: *p.Writes[0].Replace}}, VectorMutationVisibility: []byte{1}}
	bound, err := preflightPeerRequestBytesV1(body)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if bound < int64(len(raw)) || bound <= ColocatedAuditPlanMaxBytesV1 {
		t.Fatalf("undercharged JSON: bound %d actual %d", bound, len(raw))
	}
	planRaw, err := json.Marshal(p)
	if err != nil || len(planRaw) >= ColocatedAuditPlanMaxBytesV1 {
		t.Fatalf("near-limit fixture: bytes %d err %v", len(planRaw), err)
	}
	oversized := make([]byte, fixedPeerMaxRPCBytesV1/2)
	escaped := strings.Repeat("\x00", fixedPeerMaxRPCBytesV1/6)
	vectors := make([]float32, fixedPeerMaxRPCBytesV1/24)
	cases := []struct {
		name string
		grow func(*ColocatedAuditPlanV1)
	}{
		{"run", func(p *ColocatedAuditPlanV1) { p.RunID = escaped }},
		{"replace-id", func(p *ColocatedAuditPlanV1) { p.Writes[0].Replace.ID = oversized }},
		{"replace-key", func(p *ColocatedAuditPlanV1) { p.Writes[0].Replace.IdempotencyKey = oversized }},
		{"replace-document", func(p *ColocatedAuditPlanV1) { p.Writes[0].Replace.Document = oversized }},
		{"replace-vector", func(p *ColocatedAuditPlanV1) { p.Writes[0].Replace.Vector = vectors }},
		{"replace-index", func(p *ColocatedAuditPlanV1) { p.Writes[0].Replace.Generation.Index = escaped }},
		{"delete-id", func(p *ColocatedAuditPlanV1) { p.Writes[1].Delete.ID = oversized }},
		{"delete-key", func(p *ColocatedAuditPlanV1) { p.Writes[1].Delete.IdempotencyKey = oversized }},
		{"delete-index", func(p *ColocatedAuditPlanV1) { p.Writes[1].Delete.Generation.Index = escaped }},
		{"response-token", func(p *ColocatedAuditPlanV1) { p.Writes[0].Response.VisibilityToken = oversized }},
		{"response-index", func(p *ColocatedAuditPlanV1) { p.Writes[0].Response.Generation.Index = escaped }},
		{"response-owner", func(p *ColocatedAuditPlanV1) { p.Writes[0].Response.OwnerGroup = escaped }},
		{"final-id", func(p *ColocatedAuditPlanV1) { p.Final[0].ID = oversized }},
		{"final-document", func(p *ColocatedAuditPlanV1) { p.Final[0].Document = oversized }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := audit()
			tc.grow(p)
			if _, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{ColocatedAudit: p}); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
				t.Fatalf("unbounded audit accepted: %v", err)
			}
		})
	}
	for _, body := range []fixedPeerRequestV1{
		{VectorMutationVisibility: oversized},
		{VectorMutation: &VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{Document: oversized}}}},
		{VectorInsert: &VectorPartitionRoutedInsertV1{RouterModelDigest: escaped}},
		{VectorMutation: &VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{ReadySetDigest: escaped}}},
	} {
		if _, err := preflightPeerRequestBytesV1(body); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
			t.Fatalf("unbounded sibling accepted: %v", err)
		}
	}
}

func TestPeerSecurityAuditBytesRefusedBeforeSendV1(t *testing.T) {
	transport, config := peerTransportFixtureV1(t)
	defer transport.Close()
	client, err := NewFixedPeerTCPClientWithTransportV1(config, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	empty, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := transport.admission.acquire("control-diagnostics", peerBytesV1, transport.admission.scopes["control-diagnostics"].limits[peerBytesV1]-peerControlBytesV1(empty))
	if err != nil {
		t.Fatal(err)
	}
	defer blocked.release()
	p := ColocatedAuditPlanV1{RunID: "audit", Writes: make([]ColocatedAuditWriteV1, 6), Final: []ColocatedAuditStateV1{{ID: []byte("id")}}}
	for i := range p.Writes {
		p.Writes[i].Replace = &public.ReplaceRequestV1{Document: make([]byte, 64<<10)}
	}
	before := transport.ResourceStatsV1().WrittenBytes
	if _, err := client.call(context.Background(), config.NodeID, "diagnostics", fixedPeerRequestV1{ColocatedAudit: &p}, false); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("audit did not refuse at byte admission: %v", err)
	}
	if after := transport.ResourceStatsV1().WrittenBytes; after != before {
		t.Fatalf("refused audit sent bytes: %d->%d", before, after)
	}
	blocked.release()
	stats := transport.ResourceStatsV1().Current
	if stats[peerRequestsV1] != 0 || stats[peerBytesV1] != 0 {
		t.Fatalf("refused audit leaked leases: %v", stats)
	}
}
