package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type colocatedPublicCostVoterV1 struct {
	NodeID                                                  string
	AppliedIndex, PhysicalAppliedLSN                        uint64
	RetainedOutcomeCount, RetainedOutcomeBytes              uint64
	OutcomeChain                                            string
	CommandWALFileBytes, ResultFileBytes, ProgressFileBytes int64
}

type colocatedPublicCostSampleV1 struct {
	ID                                                        string
	LatencyNs                                                 int64
	OwnerGroup                                                string
	CommitTerm, CommitIndex, AppliedIndex, Coverage, Revision uint64
	Matched, Modified, Deleted                                uint64
	ProductionConsensus                                       bool
	VisibilityTokenBytes                                      int
	Counters                                                  public.MutationCountersV1
}

// This measures public submit through the visible ACK, including TCP, real
// loopback RF4 consensus and disk-backed WAL/result/progress durability. It is
// neither internal commit-to-visible timing nor sustained two-host throughput.
// The unchanged preparation fixture subsequently snapshots and reopens all
// voters; this callback restores its original population before those checks.
func TestFixedPeerColocatedMutationPublicCostV1(t *testing.T) {
	const n = 32
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 4, false, func(t *testing.T, parent context.Context, nodes []*FixedPeerTCPRuntimeV1) {
		ctx, cancel := context.WithTimeout(parent, 90*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		node := nodes[0]
		group := node.config.Groups[0]
		generation := public.GenerationIDV1{Index: node.config.VectorInitialization.IndexDefinition.Name, Generation: node.config.VectorInitialization.Generation}
		client, err := DialContext(ctx, "tcp", node.config.VectorInitialization.PublicAddresses[node.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		search := public.SearchRequestV1{Version: 1, Generation: generation, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: 3, Probes: 1, EfSearch: 16, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}, Deadline: deadline}
		if _, err := client.VectorSearchStrictV1(ctx, search); err != nil {
			t.Fatal(err)
		}
		var lastIndex uint64
		currentCollection := func(replica *FixedPeerTCPRuntimeV1) (*collections.Collection, *backenddb.DB) {
			t.Helper()
			c, db, err := replica.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: replica.config.NodeID, GroupID: group.ID, MinAppliedIndex: lastIndex}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			if err := c.EnsureVectorPartitionLiveBindingV1(ctx, replica.servingVectorConfigV1().Manifest); err != nil {
				t.Fatal(err)
			}
			return c, db
		}
		c, _ := currentCollection(node)
		original, err := c.Get([]byte("base-minus-x"))
		if err != nil || len(original) == 0 {
			t.Fatalf("original base document=%q err=%v", original, err)
		}
		catchup := func() {
			t.Helper()
			fixedPeerWaitV1(t, ctx, func() bool {
				for _, replica := range nodes {
					status, err := replica.Status(ctx)
					if err != nil || len(status.Groups) != 1 || status.Groups[0].Applied.Index < lastIndex {
						return false
					}
				}
				return true
			})
		}
		captureVoters := func() []colocatedPublicCostVoterV1 {
			t.Helper()
			catchup()
			out := make([]colocatedPublicCostVoterV1, len(nodes))
			for i, replica := range nodes {
				c, db := currentCollection(replica)
				state, ok := db.StateToken()
				if !ok {
					t.Fatal("missing current DB state")
				}
				status, err := replica.Status(ctx)
				if err != nil || len(status.Groups) != 1 || status.Groups[0].Applied.Index < lastIndex {
					t.Fatalf("voter status=%+v err=%v", status, err)
				}
				v := &out[i]
				v.NodeID, v.AppliedIndex, v.PhysicalAppliedLSN = string(replica.config.NodeID), status.Groups[0].Applied.Index, state.AppliedCommandLSN
				logical, err := c.VectorPartitionColocatedMutationLogicalStateV1(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(logical) != 0 {
					if len(logical) != 2 || len(logical[1]) != 56 || binary.LittleEndian.Uint64(logical[1]) != 1 {
						t.Fatalf("malformed retained summary on %s", replica.config.NodeID)
					}
					v.RetainedOutcomeCount, v.RetainedOutcomeBytes = binary.LittleEndian.Uint64(logical[1][8:]), binary.LittleEndian.Uint64(logical[1][16:])
					v.OutcomeChain = hex.EncodeToString(logical[1][24:])
				}
				resolved, err := raftcluster.Validate(raftcluster.Config{Dir: filepath.Join(replica.config.DataRoot, string(group.ID)), ClusterDir: replica.config.RaftRoot, DisableSideStores: true, NodeID: replica.config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features})
				if err != nil {
					t.Fatal(err)
				}
				fileBytes := func(path string) int64 {
					t.Helper()
					info, err := os.Stat(path)
					if err != nil || !info.Mode().IsRegular() {
						t.Fatalf("durability file %s: info=%v err=%v", path, info, err)
					}
					return info.Size()
				}
				v.ResultFileBytes = fileBytes(raftapply.DurableApplyResultStorePath(resolved.Layout.ApplyDir))
				v.ProgressFileBytes = fileBytes(raftapply.DurableApplyProgressStorePath(resolved.Layout.ApplyDir))
				// Shallow filename/stat inventory only: no frame or population scan.
				walDir := backenddb.WALDirPath(db.Dir())
				entries, err := os.ReadDir(walDir)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if commitlog.IsCommandSegmentName(entry.Name()) {
						v.CommandWALFileBytes += fileBytes(filepath.Join(walDir, entry.Name()))
					}
				}
				if i != 0 && (v.RetainedOutcomeCount != out[0].RetainedOutcomeCount || v.RetainedOutcomeBytes != out[0].RetainedOutcomeBytes || v.OutcomeChain != out[0].OutcomeChain) {
					t.Fatalf("retained summaries disagree: %+v", out)
				}
			}
			return out
		}
		prove := func(id, document []byte, vector []float32, deletion bool, response public.MutationResponseV1) {
			t.Helper()
			// The visible ACK covers the owner. Wait outside its clock before
			// asking the ingress voter's current FSM DB for an exact local proof.
			catchup()
			search.Query, search.VisibilityToken = vector, response.VisibilityToken
			hits, err := client.VectorSearchStrictV1(ctx, search)
			if err != nil || hits.Generation != generation || hits.Counters.Retries != 0 || hits.Counters.Redirects != 0 {
				t.Fatalf("strict visibility search=%+v err=%v", hits, err)
			}
			found := false
			for _, hit := range hits.Neighbors {
				if hit.ID == string(id) {
					found = true
					if deletion || hit.Score != 1 {
						t.Fatalf("unexpected canonical neighbor=%+v deletion=%t", hit, deletion)
					}
				}
			}
			if !deletion && !found {
				t.Fatalf("replacement absent from canonical search: %q", id)
			}
			c, _ := currentCollection(node)
			err = c.WithPreparedCommandWALMutation(func(owner *collections.CommandWALAdmittedCollection) error {
				matched := int64(1)
				if deletion {
					matched = 0
				}
				live, err := owner.ProveVectorPartitionColocatedMutationV1(ctx, node.servingVectorConfigV1().Manifest, id, document, deletion, matched, 1)
				if err == nil && (live.Coverage < response.Coverage || live.Revision < response.LiveRevision) {
					return fmt.Errorf("source/live proof below ACK: %+v", live)
				}
				return err
			})
			if err != nil {
				t.Fatalf("exact source/live membership proof for %q: %v", id, err)
			}
		}
		runWindow := func(deletion bool) {
			t.Helper()
			operation := "replace-existing"
			if deletion {
				operation = "delete-existing"
			}
			beforeVoters := captureVoters()
			runtime.GC() // Untimed, consistent retained-heap boundary after catchup.
			before := sparseCatalogReadProcessMetricsV1(0)
			samples := make([]colocatedPublicCostSampleV1, n)
			latencies := make([]int64, n)
			var totalNs int64
			maxSampledRSS := before.RSSBytes
			windowStart := time.Now()
			for i := range samples {
				id, vector, document := []byte("base-minus-x"), []float32{0, 1}, []byte(`{"embedding":[0,1],"kind":"cost-y"}`)
				if i%2 != 0 {
					vector, document = []float32{1, 0}, []byte(`{"embedding":[1,0],"kind":"cost-x"}`)
				}
				var response public.MutationResponseV1
				var elapsed time.Duration
				if deletion {
					id, vector, document = []byte(fmt.Sprintf("cost-delete-%02d", i)), []float32{0, 1}, nil
					request := public.DeleteRequestV1{Version: 1, Generation: generation, ID: id, IdempotencyKey: []byte(fmt.Sprintf("cost-delete-key-%02d", i)), Deadline: deadline}
					start := time.Now()
					response, err = client.VectorDeleteV1(ctx, request)
					elapsed = time.Since(start)
				} else {
					request := public.ReplaceRequestV1{Version: 1, Generation: generation, ID: id, Vector: vector, Document: document, IdempotencyKey: []byte(fmt.Sprintf("cost-replace-key-%02d", i)), Deadline: deadline}
					start := time.Now()
					response, err = client.VectorReplaceV1(ctx, request)
					elapsed = time.Since(start)
				}
				if err != nil {
					t.Fatalf("%s sample %d (no retry): %v", operation, i, err)
				}
				if err := public.ValidateMutationResponseV1(generation, deletion, response); err != nil || response.CommitIndex <= lastIndex || (!deletion && (response.Matched != 1 || response.Modified != 1)) || (deletion && response.Deleted != 1) {
					t.Fatalf("%s sample %d outcome=%+v err=%v", operation, i, response, err)
				}
				lastIndex = response.CommitIndex
				latencies[i] = elapsed.Nanoseconds()
				totalNs += latencies[i]
				samples[i] = colocatedPublicCostSampleV1{ID: string(id), LatencyNs: latencies[i], OwnerGroup: response.OwnerGroup, CommitTerm: response.CommitTerm, CommitIndex: response.CommitIndex, AppliedIndex: response.AppliedIndex, Coverage: response.Coverage, Revision: response.LiveRevision, Matched: response.Matched, Modified: response.Modified, Deleted: response.Deleted, ProductionConsensus: response.ProductionConsensus, VisibilityTokenBytes: len(response.VisibilityToken), Counters: response.Counters}
				prove(id, document, vector, deletion, response) // Outside the call clock.
				metrics := sparseCatalogReadProcessMetricsV1(uint64(i + 1))
				maxSampledRSS = max(maxSampledRSS, metrics.RSSBytes)
			}
			afterVoters := captureVoters()
			runtime.GC()
			after := sparseCatalogReadProcessMetricsV1(n + 1)
			maxSampledRSS = max(maxSampledRSS, after.RSSBytes)
			growth := make([]colocatedPublicCostVoterV1, len(nodes))
			for i := range growth {
				b, a := beforeVoters[i], afterVoters[i]
				if a.RetainedOutcomeCount != b.RetainedOutcomeCount+n || a.RetainedOutcomeBytes <= b.RetainedOutcomeBytes {
					t.Fatalf("%s retained witness count/bytes: before=%+v after=%+v", operation, b, a)
				}
				growth[i] = colocatedPublicCostVoterV1{NodeID: a.NodeID, RetainedOutcomeCount: a.RetainedOutcomeCount - b.RetainedOutcomeCount, RetainedOutcomeBytes: a.RetainedOutcomeBytes - b.RetainedOutcomeBytes, CommandWALFileBytes: a.CommandWALFileBytes - b.CommandWALFileBytes, ResultFileBytes: a.ResultFileBytes - b.ResultFileBytes, ProgressFileBytes: a.ProgressFileBytes - b.ProgressFileBytes}
			}
			sorted := slices.Clone(latencies)
			slices.Sort(sorted)
			packet := map[string]any{
				"version": 1, "operation": operation, "n": n, "layout": "one-process-loopback-RF4",
				"clock": "public-submit-to-visible-ACK", "latencyNs": latencies, "samples": samples,
				"sumLatencyNs": totalNs, "meanLatencyNs": float64(totalNs) / n,
				"latencyDerivedSequentialCallsPerSecond": float64(n) * 1e9 / float64(totalNs),
				"p50Ns":                                  sorted[15], "p95Ns": sorted[30], "maxNs": sorted[31], "quantileMethod": "nearest-rank,32-samples",
				"windowWallNsIncludingUntimedProofAndBookkeeping": time.Since(windowStart).Nanoseconds(),
				"processBefore": before, "processAfter": after,
				"aggregateProcessAllocBytes": after.TotalAlloc - before.TotalAlloc, "aggregateProcessMallocs": after.Mallocs - before.Mallocs,
				"signedRetainedHeapAllocBytes": int64(after.HeapAlloc) - int64(before.HeapAlloc), "signedRetainedHeapInuseBytes": int64(after.HeapInuse) - int64(before.HeapInuse),
				"signedRSSBytes": int64(after.RSSBytes) - int64(before.RSSBytes), "maxSampledRSSBytes": maxSampledRSS,
				"votersBefore": beforeVoters, "votersAfter": afterVoters, "voterGrowth": growth,
				"limits": "No retry/discard. Process allocation/heap/RSS include all four voters, client, background work and untimed proof/bookkeeping/capture/GC; not request-attributed B/op. VmHWM includes setup; sampled RSS is not exact transient peak. File-length deltas are observed command-WAL/result/progress growth, not total storage or a separate fsync proof. Sequential latency-derived rate is not sustained cluster throughput. Seeding/restoration are outside both windows.",
			}
			raw, err := json.Marshal(packet)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("COLOCATED_PUBLIC_COST_V1 %s", raw)
		}
		runWindow(false)
		restore := public.ReplaceRequestV1{Version: 1, Generation: generation, ID: []byte("base-minus-x"), IdempotencyKey: []byte("cost-restore-base"), Vector: []float32{-1, 0}, Document: original, Deadline: deadline}
		restored, err := client.VectorReplaceV1(ctx, restore)
		if err != nil || public.ValidateMutationResponseV1(generation, false, restored) != nil || restored.Matched != 1 || restored.Modified != 1 || restored.CommitIndex <= lastIndex {
			t.Fatalf("untimed restore=%+v err=%v", restored, err)
		}
		lastIndex = restored.CommitIndex
		prove(restore.ID, original, restore.Vector, false, restored)
		for i := 0; i < n; i++ {
			request := public.InsertRequestV1{Version: 1, Generation: generation, ID: []byte(fmt.Sprintf("cost-delete-%02d", i)), IdempotencyKey: []byte(fmt.Sprintf("cost-seed-key-%02d", i)), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"cost-delete-seed"}`), Deadline: deadline}
			response, err := client.VectorInsertV1(ctx, request)
			if err != nil || public.ValidateInsertResponseV1(request, response) != nil || response.CommitIndex <= lastIndex {
				t.Fatalf("untimed delete seed %d=%+v err=%v", i, response, err)
			}
			lastIndex = response.CommitIndex
		}
		runWindow(true)
		for _, replica := range nodes {
			c, _ := currentCollection(replica)
			actual, err := c.Get(restore.ID)
			if err != nil || !bytes.Equal(actual, original) {
				t.Fatalf("final canonical base document=%q original=%q err=%v", actual, original, err)
			}
			for i := 0; i < n; i++ {
				actual, err := c.Get([]byte(fmt.Sprintf("cost-delete-%02d", i)))
				if err != nil || actual != nil {
					t.Fatalf("final delete cohort %d=%q err=%v", i, actual, err)
				}
			}
		}
	})
}
