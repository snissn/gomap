package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
	"strings"
	"testing"
	"time"
)

func TestColocatedAuditPlanBoundsV1(t *testing.T) {
	for _, p := range []ColocatedAuditPlanV1{{}, {Version: 1, RunID: "run"}, {Version: 2, RunID: "run", Writes: make([]ColocatedAuditWriteV1, 6)}} {
		if ValidateColocatedAuditPlanV1(context.Background(), p) == nil {
			t.Fatal("accepted incomplete audit plan")
		}
	}
	// Ordinary diagnostics retain no audit attachment; an explicit incomplete
	// attachment must be refused rather than downgraded to ordinary diagnostics.
	var r *FixedPeerTCPRuntimeV1
	if _, err := r.DiagnosticsWithColocatedAuditV1(context.Background(), ColocatedAuditPlanV1{}); err == nil {
		t.Fatal("accepted missing runtime authority")
	}
}

func TestFixedPeerColocatedAuditCurrentAuthorityV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 4, false, func(t *testing.T, parent context.Context, nodes []*FixedPeerTCPRuntimeV1) {
		ctx, cancel := context.WithTimeout(parent, 60*time.Second)
		defer cancel()
		node := nodes[0]
		v := node.servingVectorConfigV1()
		g := public.GenerationIDV1{Index: v.Identity.Index.IndexName, Generation: v.Identity.Generation}
		client, err := DialContext(ctx, "tcp", node.config.VectorInitialization.PublicAddresses[node.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		// Reuse the real fixture's warm-router pattern. Restore its exact original
		// canonical document with the sixth write for unchanged snapshot/tail checks.
		search := public.SearchRequestV1{Version: 1, Generation: g, Query: []float32{-1, 0}, Metric: public.MetricCosineV1, TopK: 3, Probes: 1, EfSearch: 16, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}}
		if _, err = client.VectorSearchStrictV1(ctx, search); err != nil {
			t.Fatal(err)
		}
		original, err := node.vector.collection.Get([]byte("base-minus-x"))
		if err != nil {
			t.Fatal(err)
		}
		p := ColocatedAuditPlanV1{Version: 1, RunID: "audit-test"}
		for i := 0; i < 6; i++ {
			document := []byte(fmt.Sprintf(`{"embedding":[-1,0],"kind":"audit-%d"}`, i))
			if i == 5 {
				document = original
			}
			request := public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte("base-minus-x"), IdempotencyKey: []byte(fmt.Sprintf("audit-%d", i)), Vector: []float32{-1, 0}, Document: document}
			response, err := client.VectorReplaceV1(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			p.Writes = append(p.Writes, ColocatedAuditWriteV1{Replace: &request, Response: response})
			p.HighestNewCommitIndex = response.CommitIndex
			p.RequiredAppliedIndex = response.AppliedIndex
		}
		p.Final = []ColocatedAuditStateV1{{ID: []byte("base-minus-x"), Document: original}}
		if err = ValidateColocatedAuditPlanV1(ctx, p); err != nil {
			t.Fatal(err)
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, n := range nodes {
				s, e := n.Status(ctx)
				if e != nil || len(s.Groups) != 1 || s.Groups[0].Applied.Index < p.RequiredAppliedIndex {
					return false
				}
			}
			return true
		})
		for _, n := range nodes {
			// CLI/client attachment goes through the EXISTING authenticated diagnostics
			// operation and validates actual per-voter current-FSM state.
			report, err := node.client.DiagnosticsWithColocatedAuditV1(ctx, n.config.NodeID, p)
			if err != nil || report.ColocatedAudit == nil || report.ColocatedAudit.RetainedCount != 6 || len(report.ColocatedAudit.Witnesses) != 6 || len(report.ColocatedAudit.Final) != 1 {
				t.Fatalf("audit voter%s=%+v err%v", n.config.NodeID, report.ColocatedAudit, err)
			}
		}
		raw, _ := json.Marshal(p)
		var bad ColocatedAuditPlanV1
		_ = json.Unmarshal(raw, &bad)
		token, err := decodeColocatedVectorVisibilityV1(bad.Writes[0].Response.VisibilityToken)
		if err != nil {
			t.Fatal(err)
		}
		token.Outcome.CommandDigest[0] ^= 1
		encoded, _ := json.Marshal(token)
		bad.Writes[0].Response.VisibilityToken = append([]byte(colocatedVectorVisibilityMagicV1), encoded...)
		if _, err = node.client.DiagnosticsWithColocatedAuditV1(ctx, node.config.NodeID, bad); err == nil {
			t.Fatal("forged command digest admitted")
		}
		_ = json.Unmarshal(raw, &bad)
		bad.Writes[0].Replace.Vector[0] = 1
		if _, err = node.client.DiagnosticsWithColocatedAuditV1(ctx, node.config.NodeID, bad); err == nil {
			t.Fatal("document/vector mismatch admitted")
		}
		bad = p
		bad.Writes = bad.Writes[:5]
		if _, err = node.client.DiagnosticsWithColocatedAuditV1(ctx, node.config.NodeID, bad); err == nil {
			t.Fatal("incomplete witnesses admitted")
		}
		// Matching bytes from an unrelated DB are insufficient authority. This
		// explicit wrong-current handle must fail before attempting any proof.
		stale := &FixedPeerTCPRuntimeV1{preparedVector: node.preparedVector, data: node.data, vector: &fixedPeerVectorRuntimeV1{boundDB: nodes[1].vector.boundDB, dataGroup: node.vector.dataGroup}}
		_, err = stale.colocatedAuditV1(ctx, p)
		if err == nil {
			t.Fatal("audit accepted missing current-FSM binding")
		}
	})
}

func TestColocatedAuditOrdinarySchemaAndStrictDecodeV1(t *testing.T) {
	raw, err := json.Marshal(FixedPeerDiagnosticsV1{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("ColocatedAudit")) {
		t.Fatal("ordinary diagnostics schema grew audit payload")
	}
	for _, input := range []string{`{"Version":1,"unknown":1}`, `{} {}`, strings.Repeat("x", ColocatedAuditPlanMaxBytesV1+1)} {
		if _, err := DecodeColocatedAuditPlanV1(context.Background(), strings.NewReader(input)); err == nil {
			t.Fatal("accepted malformed audit JSON")
		}
	}
}
