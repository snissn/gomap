package nativewire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/snissn/gomap/TreeDB/collections"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
	"math"
	"runtime"
	"strings"
	"sync"
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

// This wire-contract test deliberately uses only the pre-existing decoder and
// plan fields: absence of the new capability must be a decoder failure, never
// a compiler failure or an ordinary-diagnostics fallback.
func TestColocatedAuditPopulationOnlyDecodeV1(t *testing.T) {
	const population = `"Population":{"Rows":1,"Dimensions":2,"SHA256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","Limits":{"MaxRows":2,"MaxIDBytes":128,"MaxSourceRecordBytes":4096,"MaxTotalBytes":8192,"MaxInspected":32}}`
	raw := `{"Version":1,"RunID":"initial-population","HighestNewCommitIndex":0,"RequiredAppliedIndex":1,"Writes":[],"Final":[],` + population + `}`
	p, err := DecodeColocatedAuditPlanV1(context.Background(), strings.NewReader(raw))
	if err != nil {
		t.Fatalf("population-only diagnostics capability unavailable: %v", err)
	}
	if len(p.Writes) != 0 || len(p.Final) != 0 || p.HighestNewCommitIndex != 0 || p.RequiredAppliedIndex != 1 {
		t.Fatalf("population-only plan changed its observation boundary: %+v", p)
	}
	for i := 1; i < 6; i++ {
		partial := `{"Version":1,"RunID":"partial-population","HighestNewCommitIndex":0,"RequiredAppliedIndex":1,"Writes":[` + strings.Repeat(`{},`, i-1) + `{}],"Final":[],` + population + `}`
		if _, err := DecodeColocatedAuditPlanV1(context.Background(), strings.NewReader(partial)); err == nil {
			t.Fatalf("partial %d-outcome ledger downgraded to population-only", i)
		}
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
		population := colocatedAuditSeedPopulationV1()
		initial := ColocatedAuditPlanV1{Version: 1, RunID: "initial-population", Population: &population}
		status, err := node.Status(ctx)
		if err != nil || len(status.Groups) != 1 {
			t.Fatalf("initial applied floor: %+v %v", status, err)
		}
		initial.RequiredAppliedIndex = status.Groups[0].Applied.Index
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, n := range nodes {
				s, e := n.Status(ctx)
				if e != nil || len(s.Groups) != 1 || s.Groups[0].Applied.Index < initial.RequiredAppliedIndex {
					return false
				}
			}
			return true
		})
		for _, n := range nodes {
			report, err := node.client.DiagnosticsWithColocatedAuditV1(ctx, n.config.NodeID, initial)
			if err != nil || report.ColocatedAudit == nil || report.ColocatedAudit.Population == nil || report.ColocatedAudit.Population.SHA256 != population.SHA256 || report.ColocatedAudit.Population.Rows != 3 || len(report.ColocatedAudit.Witnesses) != 0 || len(report.ColocatedAudit.Final) != 0 || report.ColocatedAudit.AppliedIndex < initial.RequiredAppliedIndex {
				t.Fatalf("initial voter%s=%+v err%v", n.config.NodeID, report.ColocatedAudit, err)
			}
		}
		p := ColocatedAuditPlanV1{Version: 1, RunID: "audit-test", Population: &population}
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
			if err != nil || report.ColocatedAudit == nil || report.ColocatedAudit.RetainedCount != 6 || len(report.ColocatedAudit.Witnesses) != 6 || len(report.ColocatedAudit.Final) != 1 || report.ColocatedAudit.Population == nil || report.ColocatedAudit.Population.SHA256 != population.SHA256 {
				t.Fatalf("audit voter%s=%+v err%v", n.config.NodeID, report.ColocatedAudit, err)
			}
		}
		legacy := p
		legacy.Population = nil
		if report, err := node.client.DiagnosticsWithColocatedAuditV1(ctx, node.config.NodeID, legacy); err != nil || report.ColocatedAudit == nil || report.ColocatedAudit.Population != nil {
			t.Fatalf("legacy audit changed: %+v %v", report.ColocatedAudit, err)
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
		t.Run("ConcurrentFollowerApply", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			leader, err := node.client.leader(ctx, node.config.Groups[0])
			if err != nil {
				t.Fatal(err)
			}
			var follower *FixedPeerTCPRuntimeV1
			for _, n := range nodes {
				if n.config.NodeID != leader {
					follower = n
					break
				}
			}
			if follower == nil {
				t.Fatal("missing follower")
			}
			writer, err := DialContext(ctx, "tcp", v.PublicAddresses[leader])
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			paused := &colocatedAuditAdmissionContextV1{Context: ctx, admitted: make(chan struct{}), resume: make(chan struct{})}
			var resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(paused.resume) }) }
			var workers sync.WaitGroup
			defer func() {
				// Release the only test-owned pause on every exit, cancel network
				// work and join both workers before fixture shutdown. An actual
				// uninterruptible production lock cycle consumes the outer Go test
				// timeout; it cannot be detached and mistaken for a clean test exit.
				cancel()
				resume()
				workers.Wait()
			}()
			type auditResult struct {
				receipt ColocatedAuditReceiptV1
				err     error
			}
			audited := make(chan auditResult, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				receipt, err := follower.colocatedAuditV1(paused, p)
				audited <- auditResult{receipt, err}
			}()
			select {
			case <-paused.admitted:
			case result := <-audited:
				t.Fatalf("audit never reached prepared admission: %v", result.err)
			case <-ctx.Done():
				t.Fatal("audit admission rendezvous canceled")
			}
			// A new exact same-content outcome changes durable metadata but leaves
			// the fixture's canonical source/live membership and revision intact.
			request := public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte("base-minus-x"), IdempotencyKey: []byte("audit-concurrent-noop"), Vector: []float32{-1, 0}, Document: original}
			type writeResult struct {
				response public.MutationResponseV1
				err      error
			}
			written := make(chan writeResult, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				response, err := writer.VectorReplaceV1(ctx, request)
				written <- writeResult{response, err}
			}()
			// Observe the actual lock acquisition, not a sleep-based arrival guess.
			// Apply holds f.mu while blocked on the admission retained by this audit.
			fixedPeerWaitV1(t, ctx, colocatedAuditApplyWaitingForAdmissionV1)
			resume()
			select {
			case result := <-audited:
				if result.err == nil || result.receipt.Version != 0 {
					t.Fatalf("concurrent applied-state drift produced audit receipt: %+v err%v", result.receipt, result.err)
				}
			case <-ctx.Done():
				t.Fatal("audit and follower apply deadlocked")
			}
			var response public.MutationResponseV1
			select {
			case result := <-written:
				response = result.response
				if result.err != nil || response.Matched != 1 || response.Modified != 0 || response.LiveRevision != p.Writes[5].Response.LiveRevision {
					t.Fatalf("concurrent no-op response %+v err%v", response, result.err)
				}
			case <-ctx.Done():
				t.Fatal("concurrent public apply did not finish")
			}
			fixedPeerWaitV1(t, ctx, func() bool {
				s, err := follower.Status(ctx)
				return err == nil && len(s.Groups) == 1 && s.Groups[0].Applied.Index >= response.CommitIndex
			})
		})
		t.Run("UntouchedCurrentPopulation", func(t *testing.T) {
			originalY, err := node.vector.collection.Get([]byte("base-minus-y"))
			if err != nil {
				t.Fatal(err)
			}
			check := func(floor uint64, want bool) {
				t.Helper()
				observation := initial
				observation.RequiredAppliedIndex = floor
				fixedPeerWaitV1(t, ctx, func() bool {
					for _, n := range nodes {
						s, e := n.Status(ctx)
						if e != nil || len(s.Groups) != 1 || s.Groups[0].Applied.Index < floor {
							return false
						}
					}
					return true
				})
				for _, n := range nodes {
					report, err := node.client.DiagnosticsWithColocatedAuditV1(ctx, n.config.NodeID, observation)
					if want {
						if err != nil || report.ColocatedAudit == nil || report.ColocatedAudit.Population == nil || report.ColocatedAudit.Population.SHA256 != population.SHA256 {
							t.Fatalf("restored voter%s %+v %v", n.config.NodeID, report.ColocatedAudit, err)
						}
					} else if err == nil || report.ColocatedAudit != nil {
						t.Fatalf("untouched drift voter%s produced receipt %+v %v", n.config.NodeID, report.ColocatedAudit, err)
					}
				}
			}
			stale := public.ReplaceRequestV1{Version: 1, Generation: g, ID: []byte("base-minus-y"), IdempotencyKey: []byte("population-stale"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"seed"}`)}
			changed, err := client.VectorReplaceV1(ctx, stale)
			if err != nil {
				t.Fatal(err)
			}
			check(changed.AppliedIndex, false)
			stale.IdempotencyKey = []byte("population-restored")
			stale.Vector = []float32{0, -1}
			stale.Document = originalY
			restored, err := client.VectorReplaceV1(ctx, stale)
			if err != nil {
				t.Fatal(err)
			}
			check(restored.AppliedIndex, true)
			extra := public.InsertRequestV1{Version: 1, Generation: g, ID: []byte("population-extra"), IdempotencyKey: []byte("population-extra"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"extra"}`)}
			inserted, err := client.VectorInsertV1(ctx, extra)
			if err != nil {
				t.Fatal(err)
			}
			check(inserted.AppliedIndex, false)
			deleted, err := client.VectorDeleteV1(ctx, public.DeleteRequestV1{Version: 1, Generation: g, ID: extra.ID, IdempotencyKey: []byte("population-delete-extra")})
			if err != nil {
				t.Fatal(err)
			}
			check(deleted.AppliedIndex, true)
			missing, err := client.VectorDeleteV1(ctx, public.DeleteRequestV1{Version: 1, Generation: g, ID: []byte("base-minus-y"), IdempotencyKey: []byte("population-missing")})
			if err != nil {
				t.Fatal(err)
			}
			check(missing.AppliedIndex, false)
			back, err := client.VectorInsertV1(ctx, public.InsertRequestV1{Version: 1, Generation: g, ID: []byte("base-minus-y"), IdempotencyKey: []byte("population-back"), Vector: []float32{0, -1}, Document: originalY})
			if err != nil {
				t.Fatal(err)
			}
			check(back.AppliedIndex, true)
		})
	})
}

func colocatedAuditSeedPopulationV1() collections.VectorSourcePopulationExpectationV1 {
	h := sha256.New()
	var word [4]byte
	for _, row := range []struct {
		id string
		v  []float32
	}{{"base-minus-x", []float32{-1, 0}}, {"base-minus-y", []float32{0, -1}}, {"base-x", []float32{1, 0}}} {
		binary.LittleEndian.PutUint32(word[:], uint32(len(row.id)))
		h.Write(word[:])
		h.Write([]byte(row.id))
		for _, v := range row.v {
			binary.LittleEndian.PutUint32(word[:], math.Float32bits(v))
			h.Write(word[:])
		}
	}
	return collections.VectorSourcePopulationExpectationV1{Rows: 3, Dimensions: 2, SHA256: hex.EncodeToString(h.Sum(nil)), Limits: collections.VectorSourcePopulationLimitsV1{MaxRows: 16, MaxIDBytes: 128, MaxSourceRecordBytes: 4096, MaxTotalBytes: 32 << 20, MaxInspected: 65536}}
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

// Pause only at the existing logical-state verifier's context check under the
// audit's prepared admission. This is test-only scheduling of a real callback;
// no production hook, fabricated FSM result, or network response is involved.
type colocatedAuditAdmissionContextV1 struct {
	context.Context
	admitted, resume chan struct{}
	once             sync.Once
}

func (c *colocatedAuditAdmissionContextV1) Err() error {
	var pcs [16]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	logical, audit := false, false
	for {
		frame, more := frames.Next()
		logical = logical || strings.HasSuffix(frame.Function, ".(*Collection).colocatedVectorMutationLogicalStateV1")
		audit = audit || strings.Contains(frame.Function, ".(*FixedPeerTCPRuntimeV1).colocatedAuditV1.func")
		if !more {
			break
		}
	}
	if logical && audit {
		c.once.Do(func() {
			close(c.admitted)
			select {
			case <-c.resume:
			case <-c.Context.Done():
			}
		})
	}
	return c.Context.Err()
}

func colocatedAuditApplyWaitingForAdmissionV1() bool {
	// Bound diagnostic storage to this small RF4 fixture. A truncated dump cannot
	// establish arrival; the outer fixture/context budget fails the test instead.
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	if n == len(buf) {
		return false
	}
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		// A fresh replay manager may first need the same admission's read lock
		// while opening its collection; a cached handle reaches prepared write
		// admission. Both are real f.mu -> native-admission apply paths.
		admission := strings.Contains(stack, ".(*CollectionManager).openCollectionWithCommandWALIntent(") || strings.Contains(stack, ".(*Collection).lockVectorIndexCoverageMutationWithColdCarrier(")
		if admission && strings.Contains(stack, ".(*FSM).ApplyCommittedEntryV1(") && strings.Contains(stack, "runtime_SemacquireRWMutex") {
			return true
		}
	}
	return false
}
