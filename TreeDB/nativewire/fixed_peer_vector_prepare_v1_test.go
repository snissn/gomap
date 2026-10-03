package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
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
	"sync/atomic"
	"testing"
	"time"
)

func TestFixedPeerVectorInitializationRealRaftPrepareServingSnapshotTailV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false)
}
func TestFixedPeerVectorInitializationRealRootWithoutResultRefusesReopenV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, true, false)
}

func TestFixedPeerVectorInitializationRealRaftNonphysicalRefusesBeforeAppendV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, true)
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

func runFixedPeerVectorPrepareRealRaftV1(t *testing.T, loseResult, nonphysical bool) {
	configs := initializationTestConfigsV1(t)
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
	completion, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "prepare-first-generation")
	if err != nil {
		t.Fatal(err)
	}
	if completion.Command.Term == 0 || completion.Command.IndexPosition == 0 || completion.Command.SourceRowCount != 3 || completion.AssetSetDigest == "" {
		t.Fatalf("no committed prepare evidence: %+v", completion)
	}
	retry, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "prepare-first-generation")
	if err != nil || retry.Command != completion.Command || retry.AssetSetDigest != completion.AssetSetDigest {
		t.Fatalf("prepare exact retry=%+v err=%v", retry, err)
	}
	if _, err := client.PrepareVectorInitializationV1(ctx, configs[0].NodeID, "different-prepare"); err == nil {
		t.Fatal("conflicting preparation request was adopted")
	}
	for _, node := range nodes {
		local, err := node.vectorPrepareStatusV1(ctx)
		if err != nil || local.Completion == nil || local.Completion.Command != completion.Command || local.Completion.AssetSetDigest != completion.AssetSetDigest {
			t.Fatalf("voter completion=%+v err=%v", local, err)
		}
		if node.vector != nil || node.config.Vector != nil || node.vectorInitializationPhaseV1() != FixedPeerVectorPhaseInitializingV1 {
			t.Fatal("prepare activated live pointers before restart")
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
			t.Fatal(err)
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
	search("fresh-y", []float32{0, 1})
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
