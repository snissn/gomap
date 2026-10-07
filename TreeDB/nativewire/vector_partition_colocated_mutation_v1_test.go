package nativewire

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type colocatedWireBackendV1 struct {
	public.BackendV1
	calls    int
	badProof bool
}

func (b *colocatedWireBackendV1) ReplaceVectorPartitionV1(_ context.Context, r public.ReplaceRequestV1) (public.MutationResponseV1, error) {
	b.calls++
	return b.response(r.Generation, false), nil
}
func (b *colocatedWireBackendV1) DeleteVectorPartitionV1(_ context.Context, r public.DeleteRequestV1) (public.MutationResponseV1, error) {
	b.calls++
	return b.response(r.Generation, true), nil
}
func (b *colocatedWireBackendV1) response(g public.GenerationIDV1, deletion bool) public.MutationResponseV1 {
	r := public.MutationResponseV1{Generation: g, OwnerGroup: "owner", CommitTerm: 3, CommitIndex: 9, AppliedIndex: 9, ProductionConsensus: true, Coverage: 4, LiveRevision: 2, VisibilityToken: []byte("scoped-floor"), Counters: public.MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
	if deletion {
		r.Deleted = 1
	} else {
		r.Matched, r.Modified = 1, 1
	}
	if b.badProof {
		r.AppliedIndex = 8
	}
	return r
}

func TestVectorPartitionColocatedNativeWireContractV1(t *testing.T) {
	backend := &colocatedWireBackendV1{}
	service, err := public.NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	config := public.ConservativeOperationsConfigV1()
	config.Enabled = true
	operations, err := public.NewOperationsV1(service, config, func(context.Context) (public.OperationsHealthV1, error) {
		return public.OperationsHealthV1{Ready: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cluster := &fakeClusterSubmitter{}
	server := NewServer(ServerOptions{ClusterSubmitter: cluster, VectorPartitionOperations: operations})
	defer server.Close()
	client, _, err := NewInProcessClient(t.Context(), server)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := public.ReplaceRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7}, ID: []byte("doc"), IdempotencyKey: []byte("replace"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(time.Minute)}
	if got, err := client.VectorReplaceV1(t.Context(), request); err != nil || got.Modified != 1 || got.CommitIndex != 9 {
		t.Fatalf("replace=%+v err=%v", got, err)
	}
	deletion := public.DeleteRequestV1{Version: 1, Generation: request.Generation, ID: request.ID, IdempotencyKey: []byte("delete"), Deadline: request.Deadline}
	if got, err := client.VectorDeleteV1(t.Context(), deletion); err != nil || got.Deleted != 1 {
		t.Fatalf("delete=%+v err=%v", got, err)
	}
	if backend.calls != 2 || len(cluster.snapshot()) != 0 {
		t.Fatalf("dedicated calls=%d generic=%d", backend.calls, len(cluster.snapshot()))
	}
	tooLarge := request
	tooLarge.Document = make([]byte, commitlog.ColocatedVectorMutationMaxDocumentBytesV1+1)
	if _, err := client.VectorReplaceV1(t.Context(), tooLarge); err == nil || backend.calls != 2 {
		t.Fatalf("oversize reached backend err=%v calls=%d", err, backend.calls)
	}
	backend.badProof = true
	if _, err := client.VectorReplaceV1(t.Context(), request); !hasPublicErrorCodeV1(err, public.ErrorCommitAmbiguousV1) {
		t.Fatalf("unproven success=%v", err)
	}
}

func TestFixedPeerEntryRouteRejectsColocatedScopeV1(t *testing.T) {
	scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: "embedding", Generation: 7, OwnerGroup: "owner", Digest: [32]byte{1}}
	request := VectorPartitionRoutedMutationV1{VectorPartitionRoutedInsertV1: VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{ID: []byte("doc"), IdempotencyKey: []byte("attempt"), Document: []byte(`{"embedding":[1,0]}`)}}}
	for _, deletion := range []bool{false, true} {
		request.Delete = deletion
		entry, err := fixedPeerVectorColocatedEntryV1("docs", 7, request, scope)
		if err != nil {
			t.Fatal(err)
		}
		before := bytes.Clone(entry)
		request.Request.IdempotencyKey[0] ^= 1
		if !bytes.Equal(entry, before) {
			t.Fatal("encoded entry retained caller-owned idempotency bytes")
		}
		request.Request.IdempotencyKey[0] ^= 1
		if err := validateFixedPeerEntryRouteV1(entry, raftentry.RequestMetadataV1{}); !errors.Is(err, raftcluster.ErrRouteTargetUnsupported) {
			t.Fatalf("generic peer admitted dedicated scope: %v", err)
		}
		decoded, err := iwire.DecodeDeterministicEntry(entry, iwire.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		cluster := &fakeClusterSubmitter{}
		server := NewServer(ServerOptions{ClusterSubmitter: cluster})
		client, _, err := NewInProcessClient(t.Context(), server)
		if err != nil {
			t.Fatal(err)
		}
		sections := make([]iwire.Section, 0, len(decoded.Sections))
		for _, section := range decoded.Sections {
			if section.ID != iwire.SectionCommandHeader {
				sections = append(sections, section)
			}
		}
		_, err = client.commandSections(t.Context(), decoded.CommandID, sections...)
		if err == nil || len(cluster.snapshot()) != 0 {
			t.Fatalf("generic public admitted dedicated scope err=%v calls=%d", err, len(cluster.snapshot()))
		}
		_ = client.Close()
		_ = server.Close()
	}
}

func TestFixedPeerColocatedExactIDMutationsRF4RealRaftV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 4, false, func(t *testing.T, parent context.Context, nodes []*FixedPeerTCPRuntimeV1) {
		ctx, cancel := context.WithTimeout(parent, 90*time.Second)
		defer cancel()
		node := nodes[0]
		generation := public.GenerationIDV1{Index: node.config.VectorInitialization.IndexDefinition.Name, Generation: node.config.VectorInitialization.Generation}
		client, err := DialContext(ctx, "tcp", node.config.VectorInitialization.PublicAddresses[node.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		search := public.SearchRequestV1{Version: 1, Generation: generation, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: 3, Probes: 1, EfSearch: 16, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}, Deadline: time.Now().Add(90 * time.Second)}
		// Warm the actual coordinator/router before writes, as in production fixtures.
		if _, err := client.VectorSearchStrictV1(ctx, search); err != nil {
			t.Fatal(err)
		}
		replacement := public.ReplaceRequestV1{Version: 1, Generation: generation, ID: []byte("base-minus-x"), IdempotencyKey: []byte("colocated-replace"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"changed"}`), Deadline: search.Deadline}
		first, err := client.VectorReplaceV1(ctx, replacement)
		if err != nil || first.Matched != 1 || first.Modified != 1 {
			t.Fatalf("replace=%+v err=%v", first, err)
		}
		search.Query, search.VisibilityToken = []float32{0, 1}, first.VisibilityToken
		found, err := client.VectorSearchStrictV1(ctx, search)
		if err != nil || len(found.Neighbors) == 0 || found.Neighbors[0].ID != "base-minus-x" || found.Neighbors[0].Score != 1 {
			t.Fatalf("visible move=%+v err=%v", found, err)
		}
		later := replacement
		later.IdempotencyKey = []byte("colocated-restore")
		later.Vector = []float32{-1, 0}
		later.Document = []byte(`{"embedding":[-1,0],"kind":"seed"}`)
		restored, err := client.VectorReplaceV1(ctx, later)
		if err != nil || restored.Modified != 1 {
			t.Fatalf("restore=%+v err=%v", restored, err)
		}
		retry, err := client.VectorReplaceV1(ctx, replacement)
		if err != nil || retry.CommitTerm != first.CommitTerm || retry.CommitIndex != first.CommitIndex || retry.Matched != 1 || retry.Modified != 1 || !bytes.Equal(retry.VisibilityToken, first.VisibilityToken) {
			t.Fatalf("old replace outcome=%+v first=%+v err=%v", retry, first, err)
		}
		noop := later
		noop.IdempotencyKey = []byte("colocated-same-content")
		unchanged, err := client.VectorReplaceV1(ctx, noop)
		if err != nil || unchanged.Matched != 1 || unchanged.Modified != 0 || unchanged.LiveRevision != restored.LiveRevision {
			t.Fatalf("same content=%+v err=%v", unchanged, err)
		}
		missing := later
		missing.ID = []byte("missing-replace")
		missing.IdempotencyKey = []byte("colocated-missing-replace")
		absent, err := client.VectorReplaceV1(ctx, missing)
		if err != nil || absent.Matched != 0 || absent.Modified != 0 || absent.LiveRevision != restored.LiveRevision {
			t.Fatalf("missing replacement=%+v err=%v", absent, err)
		}
		deletion := public.DeleteRequestV1{Version: 1, Generation: generation, ID: []byte("base-minus-y"), IdempotencyKey: []byte("colocated-delete"), Deadline: search.Deadline}
		removed, err := client.VectorDeleteV1(ctx, deletion)
		if err != nil || removed.Deleted != 1 {
			t.Fatalf("delete=%+v err=%v", removed, err)
		}
		search.Query, search.VisibilityToken = []float32{0, -1}, removed.VisibilityToken
		hits, err := client.VectorSearchStrictV1(ctx, search)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range hits.Neighbors {
			if hit.ID == "base-minus-y" {
				t.Fatal("deleted base leaked through ANN")
			}
		}
		insert := public.InsertRequestV1{Version: 1, Generation: generation, ID: deletion.ID, IdempotencyKey: []byte("colocated-reinsert"), Vector: []float32{0, -1}, Document: []byte(`{"embedding":[0,-1],"kind":"seed"}`), Deadline: search.Deadline}
		if _, err := client.VectorInsertV1(ctx, insert); err != nil {
			t.Fatal(err)
		}
		oldDelete, err := client.VectorDeleteV1(ctx, deletion)
		if err != nil || oldDelete.Deleted != 1 || oldDelete.CommitIndex != removed.CommitIndex || !bytes.Equal(oldDelete.VisibilityToken, removed.VisibilityToken) {
			t.Fatalf("old delete=%+v original=%+v err=%v", oldDelete, removed, err)
		}
		// Old token is a floor and remains useful after a later reinsert; it does not
		// assert that the old postimage is the present document.
		hits, err = client.VectorSearchStrictV1(ctx, search)
		if err != nil || len(hits.Neighbors) == 0 || hits.Neighbors[0].ID != "base-minus-y" {
			t.Fatalf("reinsert lost on old retry=%+v err=%v", hits, err)
		}
		missingDelete := deletion
		missingDelete.ID = []byte("never-present")
		missingDelete.IdempotencyKey = []byte("colocated-missing-delete")
		nothing, err := client.VectorDeleteV1(ctx, missingDelete)
		if err != nil || nothing.Deleted != 0 {
			t.Fatalf("missing delete=%+v err=%v", nothing, err)
		}
		// An inserted overlay is replaced and deleted without changing the final
		// fixture population; source absence alone must not leave a live node.
		overlay := insert
		overlay.ID, overlay.IdempotencyKey = []byte("colocated-overlay"), []byte("overlay-insert")
		if _, err := client.VectorInsertV1(ctx, overlay); err != nil {
			t.Fatal(err)
		}
		overlayReplace := public.ReplaceRequestV1(overlay)
		overlayReplace.IdempotencyKey, overlayReplace.Vector = []byte("overlay-replace"), []float32{0, 1}
		overlayReplace.Document = []byte(`{"embedding":[0,1],"kind":"overlay"}`)
		if result, err := client.VectorReplaceV1(ctx, overlayReplace); err != nil || result.Modified != 1 {
			t.Fatalf("overlay replace=%+v err=%v", result, err)
		}
		overlayDelete := deletion
		overlayDelete.ID, overlayDelete.IdempotencyKey = overlay.ID, []byte("overlay-delete")
		overlayRemoved, err := client.VectorDeleteV1(ctx, overlayDelete)
		if err != nil || overlayRemoved.Deleted != 1 {
			t.Fatalf("overlay delete=%+v err=%v", overlayRemoved, err)
		}
		overlaySearch := search
		overlaySearch.Query, overlaySearch.VisibilityToken = []float32{0, 1}, overlayRemoved.VisibilityToken
		overlayHits, err := client.VectorSearchStrictV1(ctx, overlaySearch)
		if err != nil {
			t.Fatal(err)
		}
		for _, hit := range overlayHits.Neighbors {
			if hit.ID == string(overlay.ID) {
				t.Fatal("deleted overlay leaked through ANN")
			}
		}
		// Invalid generation is refused before a new Raft entry or local WAL.
		before, err := node.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stale := replacement
		stale.Generation.Generation++
		stale.IdempotencyKey = []byte("stale-generation")
		if _, err := client.VectorReplaceV1(ctx, stale); err == nil {
			t.Fatal("stale generation admitted")
		}
		after, err := node.Status(ctx)
		if err != nil || before.Groups[0].LastIndex != after.Groups[0].LastIndex {
			t.Fatalf("stale generation appended before=%+v after=%+v err=%v", before, after, err)
		}
		forged := search
		forged.VisibilityToken = bytes.Clone(search.VisibilityToken)
		forged.VisibilityToken[len(forged.VisibilityToken)-2] ^= 1
		if _, err := client.VectorSearchStrictV1(ctx, forged); err == nil {
			t.Fatal("forged token admitted")
		}
		conflict := replacement
		conflict.Document = later.Document
		conflict.Vector = later.Vector
		if _, err := client.VectorReplaceV1(ctx, conflict); err == nil {
			t.Fatal("same key accepted changed command")
		}
		// Every voter must cover the exact supported prefix before the harness takes
		// its real provider snapshot and reopens the snapshot plus later Raft tail.
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, replica := range nodes {
				status, err := replica.Status(ctx)
				if err != nil || len(status.Groups) != 1 || status.Groups[0].Applied.Index < overlayRemoved.AppliedIndex {
					return false
				}
			}
			return true
		})
		// Gracefully retire the real data leader, elect from the remaining RF4
		// voters, and recover the original reply through a different coordinator.
		// This is a leader-loss/rejoin control, not power-loss qualification.
		leaderIndex := -1
		for i, replica := range nodes {
			status, err := replica.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if status.Groups[0].LeaderID == replica.config.NodeID {
				leaderIndex = i
			}
		}
		if leaderIndex < 0 {
			t.Fatal("no data leader before retirement")
		}
		retired := nodes[leaderIndex]
		retiredConfig := retired.config
		if err := retired.Close(); err != nil {
			t.Fatal(err)
		}
		nodes[leaderIndex] = nil
		defer func() {
			if nodes[leaderIndex] == nil {
				var err error
				nodes[leaderIndex], err = fixedPeerOpenTestRuntimeV1(t, retiredConfig)
				if err != nil {
					t.Errorf("rejoin retired voter: %v", err)
				}
			}
		}()
		var survivor *FixedPeerTCPRuntimeV1
		fixedPeerWaitV1(t, ctx, func() bool {
			// Retiring a runtime stops both voters. Their elections are
			// independent; discovery must no longer advertise the retired node.
			var dataLeader *FixedPeerTCPRuntimeV1
			var catalogLeader raftcluster.NodeID
			var statuses []FixedPeerTCPStatusV1
			for _, replica := range nodes {
				if replica == nil {
					continue
				}
				status, err := replica.Status(ctx)
				if err != nil || len(status.Groups) != 1 {
					return false
				}
				statuses = append(statuses, status)
				if status.Groups[0].State == "Leader" && status.Groups[0].LeaderID == replica.config.NodeID {
					dataLeader = replica
				}
				if status.CatalogRaft.State == "Leader" && status.CatalogRaft.LeaderID == replica.config.NodeID {
					catalogLeader = replica.config.NodeID
				}
			}
			if dataLeader == nil || catalogLeader == "" {
				return false
			}
			for _, status := range statuses {
				if status.Groups[0].LeaderID != dataLeader.config.NodeID || status.CatalogRaft.LeaderID != catalogLeader {
					return false
				}
			}
			survivor = dataLeader
			return true
		})
		newClient, err := DialContext(ctx, "tcp", survivor.config.VectorInitialization.PublicAddresses[survivor.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer newClient.Close()
		if _, err := newClient.VectorSearchStrictV1(ctx, search); err != nil {
			t.Fatal(err)
		}
		afterLeaderLoss, err := newClient.VectorReplaceV1(ctx, replacement)
		if err != nil || afterLeaderLoss.CommitTerm != first.CommitTerm || afterLeaderLoss.CommitIndex != first.CommitIndex || afterLeaderLoss.Matched != first.Matched || afterLeaderLoss.Modified != first.Modified || !bytes.Equal(afterLeaderLoss.VisibilityToken, first.VisibilityToken) {
			t.Fatalf("leader-loss original=%+v first=%+v err=%v", afterLeaderLoss, first, err)
		}
		nodes[leaderIndex], err = fixedPeerOpenTestRuntimeV1(t, retiredConfig)
		if err != nil {
			t.Fatal(err)
		}
		// Rejoining restarts independent DATA and CATALOG voters. Require
		// actual catalog serving activation as well as the applied DATA prefix
		// before the shared harness captures every voter's current backend.
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, replica := range nodes {
				status, err := replica.Status(ctx)
				if err != nil || len(status.Groups) != 1 || status.Groups[0].Applied.Index < afterLeaderLoss.AppliedIndex || status.VectorPhase != "active" {
					return false
				}
			}
			return true
		})
		for _, replica := range nodes {
			if err := replica.vector.collection.EnsureVectorPartitionLiveBindingV1(ctx, replica.servingVectorConfigV1().Manifest); err != nil {
				t.Fatal(err)
			}
			document, err := replica.vector.collection.Get([]byte("base-minus-y"))
			if err != nil || len(document) == 0 {
				t.Fatalf("source reinsert missing=%q err=%v", document, err)
			}
			logical, err := replica.vector.collection.VectorPartitionColocatedMutationLogicalStateV1(ctx)
			if err != nil || len(logical) == 0 {
				t.Fatalf("atomic witness not replicated err=%v", err)
			}
		}
	})
}

func TestVectorPartitionColocatedPlacementRefusalV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{PartitionCount: 2}
	placement := raftplacement.VectorPartitionPlacementRecordV1{Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "owner"}, {PartitionID: 1, GroupID: "owner"}}}
	if owner, err := vectorPartitionColocatedOwnerV1(manifest, placement); err != nil || owner != "owner" {
		t.Fatalf("colocated owner=%s err=%v", owner, err)
	}
	for _, partitions := range [][]raftplacement.VectorPartitionGroupV1{nil, placement.Partitions[:1], {{PartitionID: 0, GroupID: "owner"}, {PartitionID: 1, GroupID: "other"}}, {{PartitionID: 1, GroupID: "owner"}, {PartitionID: 0, GroupID: "owner"}}, {{PartitionID: 0}, {PartitionID: 1}}} {
		placement.Partitions = partitions
		if _, err := vectorPartitionColocatedOwnerV1(manifest, placement); err == nil {
			t.Fatalf("unsupported placement admitted: %+v", partitions)
		}
	}
}
