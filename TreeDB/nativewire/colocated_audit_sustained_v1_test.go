package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// Synthetic extension of the frozen six-outcome admission fixture. Production
// encoders bind each appended logical request and token. These are local
// validation inputs, never executed ACKs, current-FSM witnesses or qualification.
func colocatedAuditSustainedPlanV1(t testing.TB, originals int) ColocatedAuditPlanV1 {
	t.Helper()
	ctx := context.Background()
	p, err := DecodeColocatedAuditPlanV1(ctx, strings.NewReader(populationLegacyColocatedPlanGuardJSONV1))
	if err != nil {
		t.Fatal(err)
	}
	baseIndex := p.HighestNewCommitIndex
	lastToken, err := decodeColocatedVectorVisibilityV1(p.Writes[5].Response.VisibilityToken)
	if err != nil {
		t.Fatal(err)
	}
	for i := 6; i < originals; i++ {
		template := p.Writes[i%2]
		request := *template.Replace
		request.IdempotencyKey = []byte(fmt.Sprintf("%s-extra-%d", p.RunID, i))
		token, err := decodeColocatedVectorVisibilityV1(template.Response.VisibilityToken)
		if err != nil {
			t.Fatal(err)
		}
		token.Attempt = request.IdempotencyKey
		token.Outcome.Index = baseIndex + uint64(i-5)
		token.Outcome.Coverage = lastToken.Outcome.Coverage + uint64(i-5)
		token.Outcome.Revision = lastToken.Outcome.Revision + uint64(i-5)
		token.Outcome.Ordinal = uint64(i + 1)
		token.Outcome.ExpectedCatalogVersion = lastToken.Outcome.ExpectedCatalogVersion + uint64(i-5)
		routed := VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: request}}
		entry, err := fixedPeerVectorColocatedEntryV1("synthetic-audit-cost", token.Outcome.ExpectedCatalogVersion, routed, token.Scope)
		if err != nil {
			t.Fatal(err)
		}
		token.Outcome.CommandDigest = [32]byte(raftentry.CommandDigestV1ForBytes(entry, raftentry.DecodeOptions{}))
		raw, err := json.Marshal(token)
		if err != nil {
			t.Fatal(err)
		}
		response := template.Response
		response.CommitIndex, response.AppliedIndex = token.Outcome.Index, token.Outcome.Index
		response.Coverage, response.LiveRevision = token.Outcome.Coverage, token.Outcome.Revision
		response.VisibilityToken = append([]byte(colocatedVectorVisibilityMagicV1), raw...)
		p.Writes = append(p.Writes, ColocatedAuditWriteV1{Replace: &request, Response: response})
		p.HighestNewCommitIndex = response.CommitIndex
		p.RequiredAppliedIndex = response.AppliedIndex
		p.Final[0].Document = request.Document
	}
	if err := ValidateColocatedAuditPlanV1(ctx, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestColocatedAuditSustainedBoundsV1(t *testing.T) {
	ctx := context.Background()
	p := colocatedAuditSustainedPlanV1(t, 63)
	if _, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{ColocatedAudit: &p}); err != nil {
		t.Fatal("maximum pre-marshal admission:", err)
	}
	for _, count := range []int{1, 5, 64} {
		bad := p
		if count <= 63 {
			bad.Writes = p.Writes[:count]
		} else {
			bad.Writes = append(append([]ColocatedAuditWriteV1(nil), p.Writes...), p.Writes[0])
		}
		if _, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{ColocatedAudit: &bad}); err == nil {
			t.Fatalf("pre-marshal admitted%d outcomes", count)
		}
		if err := ValidateColocatedAuditPlanV1(ctx, bad); err == nil {
			t.Fatalf("validator admitted%d outcomes", count)
		}
	}
	bad := p
	bad.Writes = append([]ColocatedAuditWriteV1(nil), p.Writes...)
	duplicate := *bad.Writes[62].Replace
	duplicate.IdempotencyKey = bad.Writes[61].Replace.IdempotencyKey
	bad.Writes[62].Replace = &duplicate
	if err := ValidateColocatedAuditPlanV1(ctx, bad); err == nil {
		t.Fatal("duplicate original admitted")
	}
	oversized := p
	oversized.Writes = append([]ColocatedAuditWriteV1(nil), p.Writes...)
	large := *oversized.Writes[0].Replace
	large.Document = make([]byte, fixedPeerMaxRPCBytesV1)
	oversized.Writes[0].Replace = &large
	if _, err := preflightPeerRequestBytesV1(fixedPeerRequestV1{ColocatedAudit: &oversized}); err == nil {
		t.Fatal("oversized document passed pre-marshal byte charge")
	}
	encodedTooLarge := p
	encodedTooLarge.Writes = append([]ColocatedAuditWriteV1(nil), p.Writes...)
	large = *encodedTooLarge.Writes[0].Replace
	large.Document, _ = json.Marshal(struct {
		Embedding []float32 `json:"embedding"`
		Kind      string    `json:"kind"`
	}{large.Vector, strings.Repeat("x", ColocatedAuditPlanMaxBytesV1)})
	encodedTooLarge.Writes[0].Replace = &large
	if err := ValidateColocatedAuditPlanV1(ctx, encodedTooLarge); err == nil || !strings.Contains(err.Error(), "encoded bound") {
		t.Fatal("exact encoded plan cap not enforced:", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := ValidateColocatedAuditPlanV1(ctx, p); err == nil {
		t.Fatal("canceled maximum plan admitted")
	}
}

// Includes complete encoded ledger/token validation; excludes network, current
// FSM witness lookup and population scans. Real 63-outcome authority is tested
// by TestFixedPeerColocatedAuditVariableLengthCurrentAuthorityV2.
func BenchmarkColocatedAuditSustained58V1(b *testing.B) {
	ctx := context.Background()
	p := colocatedAuditSustainedPlanV1(b, 58)
	raw, err := json.Marshal(p)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("validate", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := ValidateColocatedAuditPlanV1(ctx, p); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := DecodeColocatedAuditPlanV1(ctx, bytes.NewReader(raw)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
