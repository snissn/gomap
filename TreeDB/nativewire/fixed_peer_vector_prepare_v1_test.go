package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFixedPeerVectorInitializationRealRaftPrepareServingSnapshotTailV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 3, false)
}

// This recovery witness acknowledges a live overlay before the physical Raft
// snapshot, then retains the ordinary tail/retry/close/reopen assertions below.
func TestFixedPeerVectorInitializationLiveOverlaySnapshotTailRecoveryV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 3, true)
}

func TestFixedPeerVectorInitializationRF4RealRaftPrepareServingSnapshotTailV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 4, false)
}

// Pause after coordinator planning, then delegate to the actual TCP shard and
// its real Raft ReadIndex. No production hook or fabricated search result.
func TestFixedPeerVectorOwnerSearchAdmissionRealRaftV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 3, false, func(t *testing.T, parent context.Context, nodes []*FixedPeerTCPRuntimeV1) {
		ctx, cancel := context.WithTimeout(parent, 30*time.Second)
		defer cancel()
		group := nodes[0].config.Groups[0]
		leader, err := nodes[0].client.leader(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		var owner *FixedPeerTCPRuntimeV1
		for _, node := range nodes {
			if node.config.NodeID == leader {
				owner = node
			}
		}
		if owner == nil || owner.vector == nil {
			t.Fatal("missing actual prepared owner")
		}
		backend, err := owner.vector.ensureBackendV1(ctx)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := backend.opts.Topology.Coordinator()
		delegate := coordinator.dispatcher
		planned := make(chan VectorPartitionShardSearchRequestV1, 2)
		resume := make(chan struct{})
		var release sync.Once
		unpause := func() { release.Do(func() { close(resume) }) }
		defer unpause()
		var calls atomic.Int32
		var workers sync.WaitGroup
		defer func() {
			cancel()
			unpause()
			workers.Wait()
		}()
		// Installed before either request; later requests bypass the two pauses.
		coordinator.dispatcher = VectorPartitionShardSearchDispatcherFuncV1(func(callCtx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
			if calls.Add(1) <= 2 {
				select {
				case planned <- request:
				case <-callCtx.Done():
					return VectorPartitionShardSearchResponseV1{}, callCtx.Err()
				}
				select {
				case <-resume:
				case <-callCtx.Done():
					return VectorPartitionShardSearchResponseV1{}, callCtx.Err()
				}
			}
			return delegate.DispatchVectorPartitionShardSearchV1(callCtx, request)
		})
		generation := public.GenerationIDV1{Index: owner.config.VectorInitialization.IndexDefinition.Name, Generation: owner.config.VectorInitialization.Generation}
		request := public.SearchRequestV1{Version: 1, Generation: generation, Query: []float32{1, 0}, Metric: public.MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 8, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}, Deadline: time.Now().Add(30 * time.Second)}
		type searchReply struct {
			response public.SearchResponseV1
			err      error
		}
		searches := make(chan searchReply, 2)
		for i := 0; i < 2; i++ {
			reader, err := DialContext(ctx, "tcp", owner.servingVectorConfigV1().PublicAddresses[owner.config.NodeID])
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			workers.Add(1)
			go func() {
				defer workers.Done()
				response, err := reader.VectorSearchStrictV1(ctx, request)
				searches <- searchReply{response, err}
			}()
		}
		plans := make([]VectorPartitionShardSearchRequestV1, 2)
		for i := range plans {
			select {
			case plans[i] = <-planned:
			case <-ctx.Done():
				t.Fatal("two concurrent readers did not reach the real shard boundary")
			}
		}
		if plans[0].LiveCoverage == 0 || plans[0].LiveRevision != plans[1].LiveRevision || plans[0].LiveCoverage != plans[1].LiveCoverage {
			t.Fatalf("readers did not plan the same covered live identity: %+v %+v", plans[0], plans[1])
		}
		// Deterministic old-source witness: both genuine plans leave the old
		// mutation mutex available for publication at this exact boundary.
		if owner.vector.mutationMu.TryLock() {
			owner.vector.mutationMu.Unlock()
			t.Fatal("planned strict searches did not exclude actual owner publication")
		}
		writer, err := DialContext(ctx, "tcp", owner.servingVectorConfigV1().PublicAddresses[owner.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		insert := public.InsertRequestV1{Version: 1, Generation: generation, IdempotencyKey: []byte("owner-search-admission"), ID: []byte("admission-minus-x-plus-y"), Vector: []float32{-1, 1}, Document: []byte("{\"embedding\":[-1,1],\"kind\":\"admission\"}"), Deadline: time.Now().Add(30 * time.Second)}
		type insertReply struct {
			response public.InsertResponseV1
			err      error
		}
		inserted := make(chan insertReply, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			response, err := writer.VectorInsertV1(ctx, insert)
			inserted <- insertReply{response, err}
		}()
		// The existing exclusive Lock queues behind both reader leases. Probe
		// its RW reader-side behavior without making the old test-only overlay
		// require a new method on the old sync.Mutex.
		rw, ok := any(&owner.vector.mutationMu).(interface {
			TryRLock() bool
			RUnlock()
		})
		if !ok {
			t.Fatal("owner admission does not preserve concurrent readers")
		}
		tick := time.NewTicker(time.Millisecond)
		for rw.TryRLock() {
			rw.RUnlock()
			select {
			case <-ctx.Done():
				tick.Stop()
				t.Fatal("actual public insert did not queue exclusive admission")
			case <-tick.C:
			}
		}
		tick.Stop()
		select {
		case reply := <-inserted:
			t.Fatalf("insert completed before planned readers retired: %+v err=%v", reply.response, reply.err)
		default:
		}
		// No retry: each response uses its original plan and actual native shard.
		unpause()
		for i := 0; i < 2; i++ {
			select {
			case reply := <-searches:
				c := reply.response.Counters
				if reply.err != nil || reply.response.Generation != generation || len(reply.response.Neighbors) != 1 || reply.response.Neighbors[0].ID != "base-x" || reply.response.Neighbors[0].Score != 1 || c.SelectedPartitions == 0 || c.HNSWServedPartitions != c.SelectedPartitions || c.ExactScanPartitions != 0 || c.ReadProofs == 0 {
					t.Fatalf("planned native proof search failed: %+v err=%v", reply.response, reply.err)
				}
			case <-ctx.Done():
				t.Fatal("native searches did not finish")
			}
		}
		var mutation public.InsertResponseV1
		select {
		case reply := <-inserted:
			mutation = reply.response
			if reply.err != nil {
				t.Fatal(reply.err)
			}
		case <-ctx.Done():
			t.Fatal("actual insert did not finish after reader retirement")
		}
		if err := public.ValidateInsertResponseV1(insert, mutation); err != nil || mutation.LiveRevision <= plans[0].LiveRevision || mutation.OwnerGroup != string(group.ID) {
			t.Fatalf("insert lacks later committed applied visibility: %+v err=%v", mutation, err)
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, node := range nodes {
				state, err := node.Status(ctx)
				if err != nil || len(state.Groups) != 1 || state.Groups[0].Applied.Index < mutation.CommitIndex {
					return false
				}
			}
			return true
		})
		self := request
		self.Query = []float32{-1, 1}
		current, err := writer.VectorSearchStrictV1(ctx, self)
		c := current.Counters
		if err != nil || current.Generation != generation || len(current.Neighbors) != 1 || current.Neighbors[0].ID != string(insert.ID) || current.Neighbors[0].Score < .99999 || current.Neighbors[0].Score > 1.00001 || c.SelectedPartitions == 0 || c.HNSWServedPartitions != c.SelectedPartitions || c.ExactScanPartitions != 0 || c.ReadProofs == 0 {
			t.Fatalf("post-insert native search did not observe acknowledged row: %+v err=%v", current, err)
		}
		// Use the actual owner entrypoint while a writer holds admission.
		owner.vector.mutationMu.Lock()
		blockedCtx, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		refused, err := owner.searchVectorPartitionStrictV1(blockedCtx, request)
		stop()
		owner.vector.mutationMu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) || len(refused.Neighbors) != 0 {
			t.Fatalf("deadline-bound writer-held search was not refused: %+v err=%v", refused, err)
		}
		// Retirement must leave no leaked reader lease.
		if !owner.vector.mutationMu.TryLock() {
			t.Fatal("canceled search leaked owner admission")
		}
		owner.vector.mutationMu.Unlock()
	})
}

func TestFixedPeerVectorInitializationRealRootWithoutResultRefusesReopenV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, true, false, false, 3, false)
}

func TestFixedPeerVectorInitializationRealRaftNonphysicalRefusesBeforeAppendV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, true, false, 3, false)
}

func TestFixedPeerVectorInitializationRealRaftPrepareSourceConvergenceV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, true, 3, false)
}

// Reuse genuinely committed preparation and its covered FSM result. The view
// below has no catalog authority; deriving expected configuration must not
// publish catalog state, initialize a backend, or claim readiness.
func checkPreparedVectorCatalogRecoveryV1(t *testing.T, ctx context.Context, node *FixedPeerTCPRuntimeV1, record raftplacement.CatalogMetaRecordV1) {
	t.Helper()
	view := &FixedPeerTCPRuntimeV1{config: node.config, data: node.data, authority: raftplacement.NewCatalogMetaAuthorityV1(), meta: node.meta}
	expected, err := view.derivePreparedVectorInitializationV1(ctx)
	if err != nil || expected == nil || node.config.Vector != nil {
		t.Fatalf("absent catalog prevented expected-config recovery: %v", err)
	}
	c, db, err := node.data[node.config.VectorInitialization.SourceGroupID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: node.config.VectorInitialization.SourceGroupID}, "docs")
	if err != nil {
		t.Fatal(err)
	}
	view.preparedVector = expected
	// A cached backend must not bypass the actual-catalog check.
	view.vector = &fixedPeerVectorRuntimeV1{parent: view, collection: c, boundDB: db, dataGroup: node.config.VectorInitialization.SourceGroupID, backend: &VectorPartitionPublicBackendV1{}}
	health, err := (&fixedPeerVectorBackendV1{runtime: view.vector}).OperationsHealthV1(ctx)
	if health.Ready || !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) || view.vectorInitializationPhaseV1() != FixedPeerVectorPhaseInitializingV1 {
		t.Fatalf("expected config became serving authority: health=%+v err=%v", health, err)
	}
	if _, err := view.ensureVectorLifecycleLeaderV1(ctx); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
		t.Fatalf("absent catalog allowed lifecycle publication: %v", err)
	}
	if _, ok := view.authority.Status(); ok {
		t.Fatal("startup invented an applied catalog")
	}
	for _, kind := range []string{"epoch", "digest"} {
		t.Run("present-catalog-"+kind+"-mismatch", func(t *testing.T) {
			catalog := record.Catalog
			epoch := record.Epoch + 1
			if kind == "digest" {
				epoch = record.Epoch
				catalog.Groups = slices.Clone(catalog.Groups)
				for _, member := range catalog.Groups[0].Members {
					if member != catalog.Groups[0].LeaderHint {
						catalog.Groups[0].LeaderHint = member
						break
					}
				}
			}
			mismatch, err := raftplacement.NewCatalogMetaRecordV1(epoch, catalog)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "digest" && mismatch.Digest == record.Digest {
				t.Fatal("digest mismatch fixture did not change catalog")
			}
			initial := mismatch
			if kind == "epoch" {
				initial = record
			}
			authority, provider := openVectorSourceHolderTestCatalogV1(t, ctx, initial)
			if kind == "epoch" {
				raw, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: record.Epoch, Record: mismatch})
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := provider.SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
					t.Fatal(err)
				}
			}
			mismatched := &FixedPeerTCPRuntimeV1{config: node.config, data: node.data, authority: authority}
			if cfg, err := mismatched.derivePreparedVectorInitializationV1(ctx); cfg != nil || !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
				t.Fatalf("present mismatched catalog was adopted: cfg=%v err=%v", cfg, err)
			}
			mismatched.preparedVector = expected
			if err := mismatched.requirePreparedVectorCatalogV1(); !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) || mismatched.vectorInitializationPhaseV1() != FixedPeerVectorPhaseInitializingV1 {
				t.Fatalf("present mismatch became ready: %v", err)
			}
		})
	}
}

func runFixedPeerVectorPrepareRealRaftV1(t *testing.T, loseResult, nonphysical, pollRegression bool, replicas int, preSnapshotOverlay bool, servingChecks ...func(*testing.T, context.Context, []*FixedPeerTCPRuntimeV1)) {
	var configs []FixedPeerTCPConfigV1
	if replicas == 4 {
		configs = fourNodeInitializationTestConfigsV1(t)
		// These four-voter fixtures synchronously persist real Raft entries.
		// Leave room for shared CI disk/scheduler pauses without treating them
		// as heartbeat loss; successful writes still require genuine commit/apply.
		for i := range configs {
			configs[i].RequestTimeout = 12 * time.Second
			configs[i].RaftTimeout = time.Second
		}
	} else {
		configs = initializationTestConfigsV1(t)
	}
	nodes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	open := func(round string) {
		t.Helper()
		for i := range configs {
			var err error
			nodes[i], err = fixedPeerOpenTestRuntimeV1(t, configs[i])
			if err != nil {
				t.Fatalf("open %s node %s: %v", round, configs[i].NodeID, err)
			}
			if round == "prepared-unsnapshotted" && i == 0 {
				if nodes[i].preparedVector == nil || nodes[i].vectorInitializationPhaseV1() != FixedPeerVectorPhaseInitializingV1 {
					t.Fatal("first reopened peer did not remain initializing")
				}
				health, err := (&fixedPeerVectorBackendV1{runtime: nodes[i].vector}).OperationsHealthV1(context.Background())
				if health.Ready || !errors.Is(err, raftplacement.ErrCatalogMetaUnavailable) {
					t.Fatalf("first peer served before catalog replay: health=%+v err=%v", health, err)
				}
			}
		}
	}
	closeAll := func() {
		// Retire caller-owned idle sockets before shutting down any recipient.
		for _, node := range nodes {
			if node != nil && node.client != nil {
				node.client.http.CloseIdleConnections()
				node.client.readHTTP.CloseIdleConnections()
			}
		}
		for i, node := range nodes {
			if node != nil {
				if err := node.Close(); err != nil {
					t.Error(err)
				}
				nodes[i] = nil
			}
		}
	}
	defer closeAll()
	open("initial")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	client := nodes[0].client
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			status, err := node.Status(ctx)
			if err == nil && status.CatalogRaft.State == "Leader" {
				leader = node.config.NodeID
				return true
			}
		}
		return false
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, fixedPeerVectorInitializationCatalogV1(nodes[0].config))
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, command); err != nil {
		t.Fatal(err)
	}
	group := configs[0].Groups[0]
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			status, err := node.Status(ctx)
			if err != nil || status.Catalog.Epoch != 1 || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	// A valid newer catalog still cannot alter the immutable initialization
	// epoch. Refusal must leave every replica on the committed epoch 1 record.
	nextRecord, err := raftplacement.NewCatalogMetaRecordV1(2, record.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	nextCommand, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: 1, Record: nextRecord})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, nextCommand); !errors.Is(err, raftplacement.ErrCatalogMetaStaleEpoch) {
		t.Fatalf("initialization epoch publication error: %v", err)
	}
	for _, node := range nodes {
		status, err := node.Status(ctx)
		if err != nil || status.Catalog.Epoch != 1 || status.Catalog.Digest != record.Digest {
			t.Fatalf("rejected publication changed catalog: %+v %v", status.Catalog, err)
		}
	}
	request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "docs", Shape: ClusterRouteShapeCollection}
	submit := func(command iwire.CommandID, sections []iwire.Section) raftcluster.SubmitResultV1 {
		t.Helper()
		sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})}}, sections...)
		validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := iwire.AppendDeterministicEntry(nil, validated)
		if err != nil {
			t.Fatal(err)
		}
		route, err := client.Route(ctx, configs[0].NodeID, request)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		result, err := client.Submit(ctx, configs[0].NodeID, entry, metadata)
		if err != nil || !result.CommittedRecoverable || !result.CommittedApplied {
			t.Fatalf("real Raft command %v result=%+v err=%v", command, result, err)
		}
		return result
	}
	owner, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString},
			{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2},
		}},
	}, VectorIndexes: []collections.VectorIndexDefinition{configs[0].VectorInitialization.IndexDefinition}}
	if nonphysical {
		meta.Options.ColumnStore = nil
	}
	submit(iwire.CommandCreateCollection, raftClusterCreateCollectionSectionsWithMeta(meta, status.Groups[0].CatalogVersion, AckRaftCommitted))
	status, err = client.Status(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	seed := submit(iwire.CommandInsertBatch, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("prepare-seed-through-raft")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, status.Groups[0].CatalogVersion)},
		collectionNameRef("docs"), documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("base-x"), []byte("base-minus-x"), []byte("base-minus-y"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"embedding":[1,0],"kind":"seed"}`), []byte(`{"embedding":[-1,0],"kind":"seed"}`), []byte(`{"embedding":[0,-1],"kind":"seed"}`))},
		ackSection(AckRaftCommitted),
	})
	// Neither source rebuild nor prepare is an offline seeded root.
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			state, err := node.Status(ctx)
			if err != nil || len(state.Groups) != 1 || state.Groups[0].Applied.Index < seed.Evidence.Index {
				return false
			}
			c, _, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID}, "docs")
			if err != nil {
				return false
			}
			cfg := c.Meta().Options.ColumnStore
			if !nonphysical && (cfg == nil || !cfg.Enabled || cfg.AssetManager == nil || cfg.AssetManager.Kind != collections.ColumnAssetManagerValueLogShaped || !cfg.AssetManager.IsolatedNamespace || cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil) {
				return false
			}
			if _, err := c.Get([]byte("base-minus-y")); err != nil {
				return false
			}
		}
		return true
	})
	if nonphysical {
		before, err := client.Status(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "reject-nonphysical-before-append"); err == nil || !strings.Contains(err.Error(), "physical column asset support") {
			t.Fatalf("nonphysical source refusal=%v", err)
		}
		after, err := client.Status(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if before.Groups[0].LastIndex != after.Groups[0].LastIndex || before.Groups[0].Applied != after.Groups[0].Applied || before.Groups[0].CatalogVersion != after.Groups[0].CatalogVersion {
			t.Fatalf("refused prepare appended or applied: before=%+v after=%+v", before.Groups[0], after.Groups[0])
		}
		closeAll()
		open("refused-nonphysical")
		for _, node := range nodes {
			state, err := node.Status(ctx)
			if err != nil || state.VectorPhase != FixedPeerVectorPhaseInitializingV1 {
				t.Fatalf("refused cluster reopened as serving: %+v %v", state, err)
			}
			c, _, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Get([]byte("base-minus-y")); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	// Immutable partition publication is currently proved only on Linux. The
	// real public workflow must refuse before its first rebuild/prepare append,
	// rather than panic later while applying an already committed command.
	if runtime.GOOS != "linux" {
		before := make([]FixedPeerTCPStatusV1, len(nodes))
		beforeRoots := make([]backenddb.StateToken, len(nodes))
		beforeWAL := make([]uint64, len(nodes))
		for i, node := range nodes {
			before[i], err = node.Status(ctx)
			if err != nil || len(before[i].Groups) != 1 {
				t.Fatalf("unsupported publication baseline status: %+v %v", before[i], err)
			}
			_, db, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			var ok bool
			beforeRoots[i], ok = db.StateToken()
			if !ok {
				t.Fatal("unsupported publication baseline has no root")
			}
			beforeWAL[i] = db.CommandWALNextLSN()
			if _, err := os.Lstat(filepath.Join(db.Dir(), "vector_partitions")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected partition namespace before refusal: %v", err)
			}
		}
		if _, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "reject-unsupported-publication-before-append"); err == nil || !strings.Contains(err.Error(), collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1.Error()) {
			t.Fatalf("unsupported partition publication refusal=%v", err)
		}
		for i, node := range nodes {
			after, err := node.Status(ctx)
			if err != nil || len(after.Groups) != 1 {
				t.Fatalf("unsupported publication refusal status: %+v %v", after, err)
			}
			if before[i].Groups[0].LastIndex != after.Groups[0].LastIndex || before[i].Groups[0].Applied != after.Groups[0].Applied || before[i].Groups[0].CatalogVersion != after.Groups[0].CatalogVersion || !reflect.DeepEqual(before[i].Catalog, after.Catalog) || after.VectorPhase != FixedPeerVectorPhaseInitializingV1 {
				t.Fatalf("refused prepare changed Raft/catalog/phase: before=%+v after=%+v", before[i], after)
			}
			c, db, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			roots, ok := db.StateToken()
			if !ok || roots != beforeRoots[i] || db.CommandWALNextLSN() != beforeWAL[i] {
				t.Fatalf("refused prepare changed roots/WAL: before=%+v/%d after=%+v/%d", beforeRoots[i], beforeWAL[i], roots, db.CommandWALNextLSN())
			}
			if _, err := os.Lstat(filepath.Join(db.Dir(), "vector_partitions")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refused prepare created partition namespace: %v", err)
			}
			if _, err := c.Get([]byte("base-minus-y")); err != nil {
				t.Fatalf("refused prepare lost committed source: %v", err)
			}
		}
		return
	}
	marker := make([][]byte, len(nodes))
	digests := make([]string, len(nodes))
	for i, node := range nodes {
		marker[i], err = os.ReadFile(filepath.Join(node.config.RaftRoot, "fixed-peer-v1.json"))
		if err != nil {
			t.Fatal(err)
		}
		digests[i] = node.client.digest
	}
	// Retain genuine durable metadata at the pre-prepare cut. The refusal case
	// restores these exact earlier bytes to model result/progress tail loss after
	// a root-visible prepare; it never manufactures an applied position.
	resolved, err := raftcluster.Validate(raftcluster.Config{Dir: filepath.Join(configs[0].DataRoot, string(group.ID)), ClusterDir: configs[0].RaftRoot, DisableSideStores: true, NodeID: configs[0].NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features})
	if err != nil {
		t.Fatal(err)
	}
	metadataPaths := []string{raftapply.DurableApplyProgressStorePath(resolved.Layout.ApplyDir), raftapply.DurableApplyResultStorePath(resolved.Layout.ApplyDir)}
	metadataBefore := make([][]byte, len(metadataPaths))
	for i, path := range metadataPaths {
		metadataBefore[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Intercept only replies on a private client copy; all submissions and
	// status reads still reach the real authenticated runtimes. The successful
	// submit replies distinguish source polling from completion polling.
	controlledClient := func(edit func(string, *fixedPeerReplyV1)) *FixedPeerTCPClientV1 {
		t.Helper()
		copyClient := *client
		wrap := func(original *http.Client) *http.Client {
			copyHTTP := *original
			base, ok := original.Transport.(*http.Transport)
			if !ok {
				t.Fatal("fixture requires its runtime-owned HTTP transport")
			}
			copyHTTP.Transport = splitInsertRoundTripperV1{Transport: base, roundTrip: func(request *http.Request) (*http.Response, error) {
				response, err := base.RoundTrip(request)
				if err != nil || (request.URL.Path != "/v1/vector-prepare-status" && request.URL.Path != "/v1/submit") {
					return response, err
				}
				raw, readErr := io.ReadAll(response.Body)
				if err := errors.Join(readErr, response.Body.Close()); err != nil {
					return nil, err
				}
				var reply fixedPeerReplyV1
				if err := json.Unmarshal(raw, &reply); err != nil {
					return nil, err
				}
				edit(request.URL.Path, &reply)
				raw, err = json.Marshal(reply)
				if err != nil {
					return nil, err
				}
				response.Body = io.NopCloser(bytes.NewReader(raw))
				response.ContentLength = int64(len(raw))
				response.Header.Set("Content-Length", fmt.Sprint(len(raw)))
				return response, nil
			}}
			return &copyHTTP
		}
		copyClient.readHTTP, copyClient.http = wrap(client.readHTTP), wrap(client.http)
		return &copyClient
	}
	// Cancel only after the real source submit has acknowledged committed apply.
	// Resume the same public request without altering status guards or entries.
	if pollRegression {
		sourceCtx, sourceCancel := context.WithCancel(ctx)
		defer sourceCancel()
		cancelledSubmits := 0
		var cancelledSourceIndex uint64
		cancelClient := controlledClient(func(path string, reply *fixedPeerReplyV1) {
			if path == "/v1/submit" && reply.Error == "" && reply.Submit.CommittedApplied && reply.Submit.CommittedRecoverable {
				cancelledSubmits++
				cancelledSourceIndex = reply.Submit.Evidence.Index
				sourceCancel()
			}
		})
		got, err := cancelClient.PrepareVectorInitializationV1(sourceCtx, configs[0].NodeID, "prepare-first-generation")
		if !errors.Is(err, context.Canceled) || cancelledSubmits != 1 || got != (collections.VectorPartitionPrepareCompletionV1{}) {
			t.Fatalf("cancel after committed source: completion=%+v submits=%d err=%v", got, cancelledSubmits, err)
		}
		if cancelledSourceIndex == 0 {
			t.Fatal("cancelled source has no genuine applied position")
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, node := range nodes {
				status, err := node.Status(ctx)
				if err != nil || len(status.Groups) != 1 || status.Groups[0].Applied.Index < cancelledSourceIndex {
					return false
				}
				state, err := node.vectorPrepareStatusV1(ctx)
				if err != nil || state == nil || state.Source.RowCount != 3 {
					return false
				}
				if state.Completion != nil {
					t.Fatalf("source-only cancellation published completion: state=%+v", state)
				}
			}
			return true
		})
	}
	prepareClient := client
	var sourceSubmits, followerReads int
	var sourceMismatch bool
	if pollRegression {
		prepareClient = controlledClient(func(path string, reply *fixedPeerReplyV1) {
			if path == "/v1/submit" && reply.Error == "" && reply.Submit.CommittedApplied && reply.Submit.CommittedRecoverable {
				sourceSubmits++
			}
			state := reply.VectorPreparation
			if path != "/v1/vector-prepare-status" || sourceSubmits != 1 || reply.NodeID != group.Peers[1].ID || reply.Error != "" || state == nil || state.Completion != nil || state.Source.RowCount == 0 {
				return
			}
			followerReads++
			if !sourceMismatch {
				// Model one stale but nonempty follower source after owner
				// rebuild apply. The next read returns its actual durable tuple.
				state.Source.Checksum ^= 1
				sourceMismatch = true
			}
		})
	}
	completion, err := prepareClient.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "prepare-first-generation")
	if err != nil {
		t.Fatal(err)
	}
	if completion.Command.Term == 0 || completion.Command.IndexPosition == 0 || completion.Command.SourceRowCount != 3 || completion.AssetSetDigest == "" {
		t.Fatalf("no committed prepare evidence: %+v", completion)
	}
	if pollRegression {
		if !sourceMismatch || followerReads < 2 || sourceSubmits != 2 {
			t.Fatalf("source convergence did not cross real rebuild/prepare commits: injected=%v followerReads=%d submits=%d", sourceMismatch, followerReads, sourceSubmits)
		}
		// One missing follower completion must resume through both actual submits,
		// rather than relying on the all-completed fast path.
		partialSubmits, partialProbe := 0, false
		partialClient := controlledClient(func(path string, reply *fixedPeerReplyV1) {
			if path == "/v1/submit" && reply.Error == "" && reply.Submit.CommittedApplied && reply.Submit.CommittedRecoverable {
				partialSubmits++
			}
			if path == "/v1/vector-prepare-status" && partialSubmits == 0 && !partialProbe &&
				reply.NodeID == group.Peers[1].ID && reply.Error == "" && reply.VectorPreparation != nil && reply.VectorPreparation.Completion != nil {
				reply.VectorPreparation.Completion = nil
				partialProbe = true
			}
		})
		partial, err := partialClient.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "prepare-first-generation")
		if err != nil || partial != completion || !partialProbe || partialSubmits != 2 {
			t.Fatalf("partial completion resume: completion=%+v probe=%v submits=%d err=%v", partial, partialProbe, partialSubmits, err)
		}
		// A changed frozen source under the same prepare key must fail before
		// append even though the original durable guard can be recovered.
		current, err := client.Status(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		v := completion.Command
		v.Term, v.IndexPosition, v.CommandDigest = 0, 0, ""
		v.SourceChecksum ^= 1
		payload, err := commitlog.EncodeVectorPreparePayloadV1(v)
		if err != nil {
			t.Fatal(err)
		}
		sections := []iwire.Section{
			{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandVectorPrepareV1, Version: 1})},
			collectionNameRef("docs"),
			{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, current.Groups[0].CatalogVersion)},
			{ID: iwire.SectionIdempotencyKey, Bytes: []byte("prepare-first-generation/prepare")},
			{ID: iwire.SectionVectorPrepareV1, Bytes: payload},
		}
		validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			t.Fatal(err)
		}
		conflictEntry, err := iwire.AppendDeterministicEntry(nil, validated)
		if err != nil {
			t.Fatal(err)
		}
		route, err := client.Route(ctx, owner, request)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		before, err := client.Status(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Submit(ctx, owner, conflictEntry, metadata); err == nil || !strings.Contains(err.Error(), "prepare resume idempotency key conflicts") {
			t.Fatalf("changed source reused durable prepare identity: %v", err)
		}
		after, err := client.Status(ctx, owner)
		if err != nil || before.Groups[0].LastIndex != after.Groups[0].LastIndex || before.Groups[0].Applied != after.Groups[0].Applied || before.Groups[0].CatalogVersion != after.Groups[0].CatalogVersion {
			t.Fatalf("changed-source refusal appended/applied: before=%+v after=%+v err=%v", before.Groups[0], after.Groups, err)
		}
		for _, arm := range []string{"command", "assets", "source", "cancel-source"} {
			t.Run("poll-"+arm, func(t *testing.T) {
				armCtx, armCancel := context.WithCancel(ctx)
				defer armCancel()
				submits := 0
				initialProbe, injected := false, false
				probe := controlledClient(func(path string, reply *fixedPeerReplyV1) {
					if path == "/v1/submit" && reply.Error == "" && reply.Submit.CommittedApplied && reply.Submit.CommittedRecoverable {
						submits++
					}
					state := reply.VectorPreparation
					if path != "/v1/vector-prepare-status" || reply.Error != "" || state == nil || state.Completion == nil {
						return
					}
					if submits == 0 && !initialProbe {
						// Bypass only the completed fast path. The source and
						// prepare submissions are real exact-idempotency retries.
						state.Completion = nil
						initialProbe = true
						return
					}
					if reply.NodeID != group.Peers[1].ID || injected {
						return
					}
					if arm == "cancel-source" && submits == 1 {
						state.Source.Checksum ^= 1
						injected = true
						armCancel()
					} else if arm != "cancel-source" && submits == 2 {
						// Mutation occurs only after the real prepare submit
						// succeeded, so this necessarily exercises poll(true).
						switch arm {
						case "command":
							state.Completion.Command.Term++
						case "assets":
							state.Completion.AssetSetDigest += "-divergent"
						case "source":
							state.Source.Checksum ^= 1
						}
						injected = true
					}
				})
				got, err := probe.PrepareVectorInitializationV1(armCtx, configs[0].NodeID, "prepare-first-generation")
				wantSubmits := 2
				if arm == "cancel-source" {
					wantSubmits = 1
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("incomplete source did not honor cancellation: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "vector prepare voters disagree") {
					t.Fatalf("completed voter disagreement was not refused: %v", err)
				}
				if !initialProbe || !injected || submits != wantSubmits || got != (collections.VectorPartitionPrepareCompletionV1{}) {
					t.Fatalf("poll regression did not refuse with zero completion at intended phase: initial=%v injected=%v submits=%d got=%+v", initialProbe, injected, submits, got)
				}
				for _, node := range nodes {
					actual, err := node.vectorPrepareStatusV1(ctx)
					if err != nil || actual == nil || actual.Completion == nil || *actual.Completion != completion {
						t.Fatalf("controlled reply altered real durable completion: status=%+v err=%v", actual, err)
					}
				}
			})
		}
	}
	retry, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "prepare-first-generation")
	if err != nil || retry.Command != completion.Command || retry.AssetSetDigest != completion.AssetSetDigest {
		t.Fatalf("prepare exact retry=%+v err=%v", retry, err)
	}
	if _, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "different-prepare"); err == nil {
		t.Fatal("conflicting preparation request was adopted")
	}
	localCompletions := make([]collections.VectorPartitionPrepareCompletionV1, len(nodes))
	for i, node := range nodes {
		local, err := node.vectorPrepareStatusV1(ctx)
		if err != nil || local.Completion == nil || local.Completion.Command != completion.Command || local.Completion.AssetSetDigest != completion.AssetSetDigest {
			t.Fatalf("voter completion=%+v err=%v", local, err)
		}
		localCompletions[i] = *local.Completion
		if node.vector != nil || node.config.Vector != nil || node.vectorInitializationPhaseV1() != FixedPeerVectorPhaseInitializingV1 {
			t.Fatal("prepare activated live pointers before restart")
		}
	}
	checkDurableCompletions := func(stage string) {
		t.Helper()
		for i, node := range nodes {
			c, _, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID, MinAppliedIndex: completion.Command.IndexPosition}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			actual, present, err := c.VectorPartitionPrepareCompletionV1(completion.Command.Index)
			if err != nil || !present || actual != localCompletions[i] {
				t.Fatalf("%s voter %s durable completion=%+v want=%+v present=%v err=%v", stage, node.config.NodeID, actual, localCompletions[i], present, err)
			}
		}
	}
	if loseResult {
		closeAll()
		for i, path := range metadataPaths {
			if err := os.WriteFile(path, metadataBefore[i], 0600); err != nil {
				t.Fatal(err)
			}
		}
		// No provider snapshot exists at this cut. The real persisted prepare root
		// outruns the retained covered FSM prefix and must refuse process startup.
		refused, err := OpenFixedPeerTCPRuntimeV1(configs[0])
		if refused != nil {
			_ = refused.Close()
		}
		if err == nil {
			t.Fatal("root-visible preparation silently invented missing result/progress")
		}
		return
	}
	checkPreparedVectorCatalogRecoveryV1(t, ctx, nodes[0], record)
	closeAll()
	open("prepared-unsnapshotted")
	client = nodes[0].client
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			state, err := node.Status(ctx)
			if err != nil || state.Catalog.Epoch != record.Epoch || state.Catalog.Digest != record.Digest || len(state.Groups) != 1 || state.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	searchRequest := func(query []float32) public.SearchRequestV1 {
		return public.SearchRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: configs[0].VectorInitialization.IndexDefinition.Name, Generation: configs[0].VectorInitialization.Generation}, Query: query, Metric: public.MetricCosineV1, TopK: 4, Probes: 1, EfSearch: 8, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}, Deadline: time.Now().Add(30 * time.Second)}
	}
	search := func(id string, query []float32) {
		t.Helper()
		publicClient, err := DialContext(ctx, "tcp", configs[0].VectorInitialization.PublicAddresses[configs[0].NodeID])
		if err != nil {
			t.Fatalf("strict search %s dial/HELLO: %v", id, err)
		}
		defer publicClient.Close()
		result, err := publicClient.VectorSearchStrictV1(ctx, searchRequest(query))
		if err != nil || len(result.Neighbors) == 0 || result.Neighbors[0].ID != id {
			for _, node := range nodes {
				state, statusErr := node.Status(ctx)
				t.Logf("serving failure node=%s prepared=%t vector=%t catalog=%+v statusErr=%v", node.config.NodeID, node.preparedVector != nil, node.vector != nil, state.Catalog, statusErr)
				if node.vector != nil {
					_, backendErr := node.vector.ensureBackendV1(ctx)
					backendError := ""
					if backendErr != nil {
						backendError = backendErr.Error()
					}
					t.Logf("actual lazy backend node=%s closed=%t collection=%t parent=%t sameParent=%t errorType=%T error=%q", node.config.NodeID, node.vector.closed.Load(), node.vector.collection != nil, node.vector.parent != nil, node.vector.parent == node, backendErr, backendError)
					if parent := node.vector.parent; parent != nil {
						v := parent.servingVectorConfigV1()
						t.Logf("captured parent node=%s prepared=%t config=%t meta=%t authority=%t", parent.config.NodeID, parent.preparedVector != nil, v != nil, parent.meta != nil, parent.authority != nil)
						if v != nil {
							t.Logf("actual serving identity=%+v", v.Identity)
						}
					}
				}
			}
			t.Fatalf("strict search want %s: %+v %v", id, result, err)
		}
	}
	search("base-x", []float32{1, 0}) // Real catalog BeginBuild/ready/prepare/activate.
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			report, err := client.ReadinessV1(ctx, node.config.NodeID)
			if err != nil || !report.Ready || report.VectorPhase != "active" {
				return false
			}
		}
		return true
	})
	for i, node := range nodes {
		raw, err := os.ReadFile(filepath.Join(node.config.RaftRoot, "fixed-peer-v1.json"))
		if err != nil || !bytes.Equal(raw, marker[i]) || node.client.digest != digests[i] || node.config.Vector != nil {
			t.Fatalf("restart changed immutable intent/config: %v", err)
		}
	}
	if preSnapshotOverlay {
		request := public.InsertRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: configs[0].VectorInitialization.IndexDefinition.Name, Generation: configs[0].VectorInitialization.Generation}, IdempotencyKey: []byte("pre-snapshot-overlay"), ID: []byte("pre-snapshot-overlay"), Vector: []float32{-1, -1}, Document: []byte(`{"embedding":[-1,-1],"kind":"pre-snapshot"}`), Deadline: time.Now().Add(30 * time.Second)}
		client, err := DialContext(ctx, "tcp", configs[0].VectorInitialization.PublicAddresses[configs[0].NodeID])
		if err != nil {
			t.Fatal(err)
		}
		response, insertErr := client.VectorInsertV1(ctx, request)
		closeErr := client.Close()
		if err := errors.Join(insertErr, closeErr); err != nil {
			t.Fatal(err)
		}
		if err := public.ValidateInsertResponseV1(request, response); err != nil || response.CommitIndex <= completion.Command.IndexPosition {
			t.Fatalf("pre-snapshot insert lacks actual visibility: %+v err=%v", response, err)
		}
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, node := range nodes {
				state, err := node.Status(ctx)
				if err != nil || len(state.Groups) != 1 || state.Groups[0].Applied.Index < response.CommitIndex {
					return false
				}
			}
			return true
		})
	}
	for _, check := range servingChecks {
		check(t, ctx, nodes)
	}
	checkDurableCompletions("before provider snapshot")
	// Force an actual provider snapshot at the prepared/ACTIVE prefix. Fresh
	// insert below is a genuine Raft tail entry after this snapshot.
	for _, node := range nodes {
		snapshot, err := node.data[group.ID].provider.Snapshot(ctx)
		if err != nil || snapshot.LastIncludedIndex < completion.Command.IndexPosition || snapshot.SizeBytes == 0 {
			t.Fatalf("real snapshot=%+v err=%v", snapshot, err)
		}
	}
	insert := public.InsertRequestV1{Version: 1, Generation: public.GenerationIDV1{Index: configs[0].VectorInitialization.IndexDefinition.Name, Generation: configs[0].VectorInitialization.Generation}, IdempotencyKey: []byte("fresh-after-prepare"), ID: []byte("fresh-y"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"fresh"}`), Deadline: time.Now().Add(30 * time.Second)}
	publicClient, err := DialContext(ctx, "tcp", configs[0].VectorInitialization.PublicAddresses[configs[0].NodeID])
	if err != nil {
		t.Fatal(err)
	}
	inserted, err := publicClient.VectorInsertV1(ctx, insert)
	if err != nil {
		_ = publicClient.Close()
		t.Fatal(err)
	}
	if err := public.ValidateInsertResponseV1(insert, inserted); err != nil || inserted.CommitIndex <= completion.Command.IndexPosition {
		t.Fatalf("fresh insert lacks production visibility: %+v err=%v", inserted, err)
	}
	waitInsertPrefix := func(index uint64) {
		t.Helper()
		fixedPeerWaitV1(t, ctx, func() bool {
			for _, node := range nodes {
				state, err := node.Status(ctx)
				if err != nil || len(state.Groups) != 1 || state.Groups[0].Applied.Index < index {
					return false
				}
			}
			return true
		})
	}
	type retryState struct {
		root     backenddb.StateToken
		live     collections.VectorIndexPartitionLiveStatusV1
		document []byte
		original raftapply.ApplyResultRecordV1
	}
	originalID := raftentry.ApplyEntryID{Term: inserted.CommitTerm, Index: inserted.CommitIndex}
	captureRetryState := func() []retryState {
		t.Helper()
		out := make([]retryState, len(nodes))
		for i, node := range nodes {
			data := node.data[group.ID]
			c, db, err := data.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID, MinAppliedIndex: inserted.CommitIndex}, "docs")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := node.vector.ensureBackendV1(ctx); err != nil {
				t.Fatalf("voter %s actual backend: %v", node.config.NodeID, err)
			}
			if err := node.requirePreparedVectorCurrentDBV1(); err != nil {
				t.Fatal(err)
			}
			// Load the existing durable carrier exactly as replicated-live
			// serving does; this does not publish lifecycle authority.
			manifest := node.servingVectorConfigV1().Manifest
			if err := node.vector.collection.EnsureVectorPartitionLiveBindingV1(ctx, manifest); err != nil {
				t.Fatalf("voter %s live binding: %v", node.config.NodeID, err)
			}
			live, err := node.vector.collection.ProveVectorPartitionLiveDocumentV1(ctx, manifest, insert.ID, insert.Document)
			if err != nil || live.Revision != inserted.LiveRevision {
				t.Fatalf("voter %s live proof=%+v inserted=%+v err=%v", node.config.NodeID, live, inserted, err)
			}
			root, ok := db.StateToken()
			if !ok {
				t.Fatal("current FSM DB has no published state")
			}
			document, err := c.Get(insert.ID)
			if err != nil || len(document) == 0 {
				t.Fatalf("voter %s canonical document=%s err=%v", node.config.NodeID, document, err)
			}
			original, found, err := data.fsm.LookupCoveredApplyResultV1(originalID)
			if err != nil || !found || original.Result.AffectedCount != 1 || !bytes.Equal(original.IdempotencyKey, insert.IdempotencyKey) {
				t.Fatalf("voter %s original covered result=%+v found=%v err=%v", node.config.NodeID, original, found, err)
			}
			out[i] = retryState{root: root, live: live, document: bytes.Clone(document), original: original}
		}
		return out
	}
	waitInsertPrefix(inserted.CommitIndex)
	beforeRetry := captureRetryState()
	// Colocated exact retries are new genuine Raft commits. Deterministic
	// apply deduplicates their effects; the reply proves this attempt, while
	// the original covered result remains at its original term/index.
	replay, err := publicClient.VectorInsertV1(ctx, insert)
	if err != nil {
		t.Fatalf("fresh exact retry inserted=%+v replay=%+v err=%v", inserted, replay, err)
	}
	if err := public.ValidateInsertResponseV1(insert, replay); err != nil || replay.CommitIndex <= inserted.CommitIndex || replay.Generation != inserted.Generation || replay.PartitionID != inserted.PartitionID || replay.OwnerGroup != inserted.OwnerGroup || replay.LiveRevision != inserted.LiveRevision || replay.VisibilityGeneration != inserted.VisibilityGeneration || replay.VisibleID != inserted.VisibleID {
		t.Fatalf("fresh exact retry inserted=%+v replay=%+v err=%v", inserted, replay, err)
	}
	waitInsertPrefix(replay.CommitIndex)
	afterRetry := captureRetryState()
	for i := range nodes {
		if beforeRetry[i].root != afterRetry[i].root || beforeRetry[i].live != afterRetry[i].live || !bytes.Equal(beforeRetry[i].document, afterRetry[i].document) || !reflect.DeepEqual(beforeRetry[i].original, afterRetry[i].original) {
			t.Fatalf("voter %s exact retry changed canonical/graph/LSN/original result: before=%+v after=%+v inserted=%+v replay=%+v", nodes[i].config.NodeID, beforeRetry[i], afterRetry[i], inserted, replay)
		}
	}
	if err := publicClient.Close(); err != nil {
		t.Fatal(err)
	}
	search("fresh-y", []float32{0, 1})
	closeAll()
	open("snapshot-plus-tail")
	client = nodes[0].client
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			state, err := node.Status(ctx)
			if err != nil || state.Catalog.Epoch != record.Epoch || state.Catalog.Digest != record.Digest || len(state.Groups) != 1 || state.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	// NewRaft restores the snapshot before starting asynchronous tail apply.
	// A known leader and a successful routed search do not prove that every
	// local follower FSM has caught up. Keep the real retry prefix required.
	waitInsertPrefix(replay.CommitIndex)
	checkDurableCompletions("snapshot-plus-tail reopen")
	// DATA replay and durable preparation do not prove that the independent
	// CATALOG Raft tail has replayed the ACTIVE lifecycle record on every voter.
	// Wait for serving activation under the existing test context before asserting it.
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			if node.preparedVector == nil || node.vector == nil || node.vectorInitializationPhaseV1() != "active" {
				return false
			}
		}
		return true
	})
	for _, node := range nodes {
		if node.preparedVector == nil || node.vector == nil || node.vectorInitializationPhaseV1() != "active" {
			t.Fatalf("voter %s did not recover prepared serving state", node.config.NodeID)
		}
		if err := errors.Join(node.requirePreparedVectorCurrentDBV1(), node.requirePreparedVectorCatalogV1()); err != nil {
			t.Fatal(err)
		}
	}
	search("fresh-y", []float32{0, 1})
	if preSnapshotOverlay {
		search("pre-snapshot-overlay", []float32{-1, -1})
	}
	for i, node := range nodes {
		c, _, err := node.data[group.ID].fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: node.config.NodeID, GroupID: group.ID, MinAppliedIndex: replay.CommitIndex}, "docs")
		if err != nil {
			t.Fatal(err)
		}
		if document, err := c.Get(insert.ID); err != nil || !bytes.Equal(document, beforeRetry[i].document) {
			t.Fatalf("voter %s recovered canonical document=%s want=%s err=%v", node.config.NodeID, document, beforeRetry[i].document, err)
		}
		original, found, err := node.data[group.ID].fsm.LookupCoveredApplyResultV1(originalID)
		if err != nil || !found || !reflect.DeepEqual(original, beforeRetry[i].original) {
			t.Fatalf("voter %s recovered original covered result=%+v want=%+v found=%v err=%v", node.config.NodeID, original, beforeRetry[i].original, found, err)
		}
	}
	// Capture a valid owner proof while the real providers are still running.
	leaderID, err := client.leader(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	var retainedOwner *FixedPeerTCPRuntimeV1
	for _, node := range nodes {
		if node.config.NodeID == leaderID {
			retainedOwner = node
		}
	}
	if retainedOwner == nil || retainedOwner.vector == nil {
		t.Fatal("missing real prepared owner")
	}
	ownerBackend, err := retainedOwner.vector.ensureBackendV1(ctx)
	if err != nil {
		t.Fatal(err)
	}
	proofRequest := insert
	proofRequest.Deadline = time.Now().Add(30 * time.Second)
	coordinator := ownerBackend.opts.Topology.Coordinator()
	lease, err := coordinator.acquireRouterSessionV1(ctx, proofRequest.Generation.Index, proofRequest.Generation.Generation)
	if err != nil {
		t.Fatal(err)
	}
	routerStatus := lease.session.router.Status()
	readySetDigest, proofErr := coordinator.validateReplicatedLifecycle(ctx, routerStatus)
	lease.Close()
	if proofErr != nil {
		t.Fatal(proofErr)
	}
	routed := VectorPartitionRoutedInsertV1{
		Request: proofRequest, Identity: ownerBackend.opts.Identity,
		CatalogProof:   raftplacement.CatalogProofV1{Epoch: ownerBackend.opts.Identity.Index.CatalogEpoch, Digest: ownerBackend.opts.Identity.Index.CatalogDigest},
		ReadySetDigest: readySetDigest, RouterModelDigest: routerStatus.ModelDigest,
		PartitionID: inserted.PartitionID, OwnerGroup: group.ID,
	}
	if err := retainedOwner.validateVectorInsertOwnerV1(ctx, routed); err != nil {
		t.Fatalf("current owner rejected valid routed proof: %v", err)
	}
	// InstallRaftSnapshotV1 requires no concurrent Apply. First prove the known
	// committed retry prefix on every voter, then join every data provider.
	waitInsertPrefix(replay.CommitIndex)
	for _, node := range nodes {
		provider := node.data[group.ID].provider
		if err := provider.Close(); err != nil {
			t.Fatal(err)
		}
		if err := provider.Close(); err != nil {
			t.Fatalf("provider close was not idempotent: %v", err)
		}
		if !node.data[group.ID].fsm.HasCurrentDBV1(node.vector.boundDB) {
			t.Fatal("provider shutdown closed the caller-owned FSM database")
		}
	}
	// Only the quiescent lower FSM is replaced. Never restore behind a running
	// provider or repair/rebind the retained serving handle.
	data := retainedOwner.data[group.ID]
	snapshot, err := data.fsm.ExportRaftSnapshotV1()
	if err != nil {
		t.Fatal(err)
	}
	archive, err := snapshot.OpenArchive()
	if err != nil {
		_ = snapshot.Release()
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(archive)
	if err := errors.Join(readErr, archive.Close(), snapshot.Release()); err != nil {
		t.Fatal(err)
	}
	if err := data.fsm.InstallRaftSnapshotV1(bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if err := retainedOwner.requirePreparedVectorCurrentDBV1(); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("retained runtime silently rebound: %v", err)
	}
	if err := retainedOwner.validateVectorInsertOwnerV1(ctx, routed); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("shared insert validator accepted a snapshot-replaced database: %v", err)
	}
	if _, err := retainedOwner.searchVectorPartitionStrictV1(ctx, searchRequest([]float32{0, 1})); err == nil {
		t.Fatal("stale startup runtime served after root replacement")
	}
}

func TestFixedPeerVectorPreparedAllVotersRejectsMissingLocalCompletionV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	configs := initializationTestConfigsV1(t)
	config := configs[len(configs)-1]
	group := config.Groups[0]
	remote := group.Peers[0].ID
	if remote == config.NodeID {
		t.Fatal("fixture must compare a remote voter before the local voter")
	}
	dir := filepath.Join(config.DataRoot, string(group.ID))
	if err := os.CopyFS(dir, os.DirFS(seed.dir)); err != nil {
		t.Fatal(err)
	}
	if err := backenddb.RebindDurableRootSnapshotV1(dir); err != nil {
		t.Fatal(err)
	}
	bootstrapFixedPeerVectorTrustedGenesisV1(t, config, seed.appliedCommandLSN)
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	fsm, err := raftfsm.Open(raftfsm.Options{
		DB:           database,
		Cluster:      raftcluster.Config{Dir: dir, ClusterDir: config.RaftRoot, DisableSideStores: true, NodeID: config.NodeID, GroupID: group.ID, Peers: group.Peers, Features: group.Features},
		StoreOptions: raftapply.DurableApplyStoreOptions{DisableSync: true, AllowInitialIndexGap: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsm.Close() })
	view := &FixedPeerTCPRuntimeV1{
		config: config, preparedVector: &FixedPeerTCPVectorConfigV1{},
		data: map[raftcluster.GroupID]*fixedPeerDataV1{group.ID: {db: database, fsm: fsm}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	local, err := view.vectorPrepareStatusV1(ctx)
	if err != nil || local == nil || local.Completion != nil || local.Source.RowCount == 0 {
		t.Fatalf("actual unprepared local status: status=%+v err=%v", local, err)
	}
	// Only the remote reply is controlled. The local status comes from the
	// actual current FSM DB; no Apply or snapshot replacement is injected.
	var requests atomic.Int32
	const digest = "missing-local-completion"
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(fixedPeerReplyV1{
			NodeID: remote, ConfigDigest: digest,
			VectorPreparation: &fixedPeerVectorPrepareStatusV1{Source: local.Source, Completion: &collections.VectorPartitionPrepareCompletionV1{}},
		})
	}))
	t.Cleanup(peer.Close)
	view.client = &FixedPeerTCPClientV1{
		digest: digest, readHTTP: peer.Client(), readCalls: make(chan struct{}, 1),
		addresses: map[raftcluster.NodeID]string{remote: strings.TrimPrefix(peer.URL, "http://")},
	}
	err = view.validatePreparedVectorAllVotersV1(ctx)
	if err == nil || !strings.Contains(err.Error(), "local voter has no validated prepared generation") {
		t.Fatalf("missing local completion was not refused: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("missing local completion reached remote comparison")
	}
}
