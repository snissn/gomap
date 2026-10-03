package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type fixedPeerVectorReadyFixtureV1 struct {
	IngressPublicAddress string
	Generation           public.GenerationIDV1
	RemotePartition      uint32
	RemoteGroup          string
	configs              []FixedPeerTCPConfigV1
	processes            []*fixedPeerTestProcessV1
	client               *FixedPeerTCPClientV1
}

func TestFixedPeerVectorDrainRefusesFreshPublicAndControlWorkV1(t *testing.T) {
	runtime := &FixedPeerTCPRuntimeV1{client: &FixedPeerTCPClientV1{digest: "drain-test"}}
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var once sync.Once
	service, err := public.NewServiceV1(new(vectorPartitionWireInsertBackendV1))
	if err != nil {
		t.Fatal(err)
	}
	config := public.ConservativeOperationsConfigV1()
	config.Enabled = true
	operations, err := public.NewOperationsV1(service, config, func(context.Context) (public.OperationsHealthV1, error) {
		once.Do(func() { close(started) })
		<-release
		return public.OperationsHealthV1{Ready: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(ServerOptions{VectorPartitionOperations: operations})
	server.vectorPartitionDraining = runtime.draining.Load
	defer server.Close()
	client, _, err := NewInProcessClient(t.Context(), server)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	inFlight := make(chan error, 1)
	go func() {
		_, err := client.VectorStatusV1(ctx)
		inFlight <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	runtime.BeginDrainV1()
	close(release)
	if err := <-inFlight; err != nil {
		t.Fatalf("already admitted public operation: %v", err)
	}
	if _, err := client.VectorStatusV1(ctx); err == nil {
		t.Fatal("fresh public status was admitted during drain")
	}
	request := public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 1},
		IdempotencyKey: []byte("fresh-drain-attempt"), ID: []byte("fresh-drain-doc"), Vector: []float32{1},
		Document: []byte(`{"embedding":[1]}`), Deadline: time.Now().Add(time.Second),
	}
	if _, err := client.VectorInsertV1(ctx, request); err == nil {
		t.Fatal("fresh public insert was admitted during drain")
	}
	for _, path := range []string{"/v1/vector-forward", "/v1/vector-lifecycle"} {
		recorder := httptest.NewRecorder()
		runtime.serve(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		var reply fixedPeerReplyV1
		if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
			t.Fatal(err)
		}
		if reply.ErrorCode != raftcluster.ErrAdmissionUnavailable.Error() {
			t.Fatalf("fresh control request %s during drain: %+v", path, reply)
		}
	}
}

func TestFixedPeerVectorSubmitErrorMarksCommittedResultAmbiguousV1(t *testing.T) {
	cause := context.DeadlineExceeded
	committed := raftcluster.SubmitResultV1{CommittedEntry: raftcluster.CommittedCommandEntryV1{Term: 1, Index: 2}}
	if err := fixedPeerVectorSubmitErrorV1(committed, cause); !errors.Is(err, raftcluster.ErrCommitAmbiguous) || !errors.Is(err, cause) {
		t.Fatalf("committed error = %v", err)
	}
	if err := fixedPeerVectorSubmitErrorV1(raftcluster.SubmitResultV1{}, cause); !errors.Is(err, cause) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("uncommitted error = %v", err)
	}
}

func TestFixedPeerVectorSubmitErrorNormalizesOnlyPrecommitConflictV1(t *testing.T) {
	for _, cause := range []error{
		&raftfsm.Error{Code: raftentry.ErrorRejectedConflictV1},
		&raftapply.Error{Code: raftentry.ErrorRejectedConflictV1},
		&raftentry.ValidationError{Code: raftentry.ErrorRejectedConflictV1},
	} {
		wrapped := errors.Join(errors.New("preflight"), cause)
		err := fixedPeerVectorSubmitErrorV1(raftcluster.SubmitResultV1{}, wrapped)
		if !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) || !errors.Is(err, cause) || fixedPeerErrorCodeV1(err) != string(public.ErrorInvalidRequestV1) {
			t.Fatalf("typed conflict mapping = %v", err)
		}
		committed := raftcluster.SubmitResultV1{CommittedEntry: raftcluster.CommittedCommandEntryV1{Term: 1, Index: 2}}
		if err := fixedPeerVectorSubmitErrorV1(committed, wrapped); !errors.Is(err, raftcluster.ErrCommitAmbiguous) || hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
			t.Fatalf("post-commit conflict mapping = %v", err)
		}
		if err := fixedPeerVectorSubmitErrorV1(raftcluster.SubmitResultV1{}, errors.Join(raftcluster.ErrCommitAmbiguous, wrapped)); !errors.Is(err, raftcluster.ErrCommitAmbiguous) || hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
			t.Fatalf("ambiguous conflict mapping = %v", err)
		}
	}
	rejected := &fixedPeerRemoteErrorV1{code: "rejected", message: "idempotency conflict"}
	if err := fixedPeerVectorSubmitErrorV1(raftcluster.SubmitResultV1{}, rejected); err != rejected {
		t.Fatalf("generic rejection was normalized: %v", err)
	}
}

func TestFixedPeerVectorRuntimeCloseReleasesPublicListenerV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	runtime := &fixedPeerVectorRuntimeV1{listener: listener, server: NewServer(ServerOptions{})}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	rebound, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("public listener was not released: %v", err)
	}
	_ = rebound.Close()
}

func TestFixedPeerVectorPublicListenerRetriesTemporaryAcceptV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	temporary := &temporaryWildcardTCPListenerV1{Listener: listener, addr: listener.Addr()}
	runtime := &fixedPeerVectorRuntimeV1{listener: temporary, server: NewServer(ServerOptions{})}
	runtime.start()
	t.Cleanup(func() { _ = runtime.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	client, err := DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("public listener stopped after temporary accept error: %v", err)
	}
	defer client.Close()
	if !temporary.injected.Load() {
		t.Fatal("listener did not inject the temporary accept error")
	}
}

func TestFixedPeerVectorRuntimeRefusesUnsupportedDocumentProofAtStartupV1(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options collections.CollectionOptions
	}{
		{name: "bson", options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}},
		{name: "dropped-column-payload", options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNone,
				Columns: []collections.ColumnStoreColumn{{Name: "value", Path: "value", ValueType: collections.ColumnStoreValueString}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			manager := collections.NewCollectionManager(db)
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "docs", Options: tc.options}); err != nil {
				t.Fatal(err)
			}
			parent := &FixedPeerTCPRuntimeV1{
				config: FixedPeerTCPConfigV1{
					Vector: &FixedPeerTCPVectorConfigV1{Collection: raftplacement.CollectionRefV1{Collection: "docs"}},
					Groups: []FixedPeerTCPGroupV1{{ID: "group-a"}},
				},
				data: map[raftcluster.GroupID]*fixedPeerDataV1{"group-a": {db: db}},
			}
			if runtime, err := openFixedPeerVectorRuntimeV1(parent); !errors.Is(err, ErrFixedPeerVectorDocumentV1) || runtime != nil {
				t.Fatalf("unsupported proof storage opened public vector listener: runtime=%v err=%v", runtime, err)
			}
		})
	}
}

func TestFixedPeerVectorRuntimeDisablesSnapshotCommandsV1(t *testing.T) {
	registry, err := fixedPeerVectorRegistryV1()
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []iwire.CommandID{
		iwire.CommandVectorSearchFast,
		iwire.CommandVectorPinSearchSnapshot,
		iwire.CommandVectorSearchPinned,
		iwire.CommandVectorClosePinnedSnapshot,
	} {
		if _, ok := registry.LookupCommand(command, 1); ok {
			t.Fatalf("fixed-peer registry retained snapshot command %d", command)
		}
		if _, ok := iwire.MustV1Registry().LookupCommand(command, 1); !ok {
			t.Fatalf("generic registry lost snapshot command %d", command)
		}
	}
	for _, command := range []iwire.CommandID{iwire.CommandVectorStatus, iwire.CommandVectorSearchStrict, iwire.CommandVectorInsert} {
		if _, ok := registry.LookupCommand(command, 1); !ok {
			t.Fatalf("fixed-peer registry lost supported command %d", command)
		}
	}

	runtime := &fixedPeerVectorRuntimeV1{}
	backend := &fixedPeerVectorBackendV1{runtime: runtime}
	assertUnavailable := func(err error) {
		t.Helper()
		var publicErr *public.ErrorV1
		if !errors.As(err, &publicErr) || publicErr.Code != public.ErrorUnavailableV1 || !errors.Is(err, errFixedPeerVectorSnapshotV1) {
			t.Fatalf("snapshot command error=%v", err)
		}
		if runtime.backend != nil {
			t.Fatal("snapshot command initialized fixed-peer topology")
		}
	}
	_, _, err = backend.SearchVectorPartitionFastV1(t.Context(), public.SearchRequestV1{}, public.FastSearchOptionsV1{})
	assertUnavailable(err)
	_, _, err = backend.PinVectorPartitionSearchSnapshotV1(t.Context(), public.PinSearchSnapshotOptionsV1{})
	assertUnavailable(err)
}

func fixedPeerVectorReadyV1(t testing.TB, ctx context.Context) fixedPeerVectorReadyFixtureV1 {
	return fixedPeerVectorReadyWithSourceGroupV1(t, ctx, "group-b")
}

func fixedPeerVectorReadyWithSourceGroupV1(t testing.TB, ctx context.Context, sourceGroup raftcluster.GroupID) fixedPeerVectorReadyFixtureV1 {
	return fixedPeerVectorReadyWithSourcePlacementV1(t, ctx, sourceGroup, raftplacement.PlacementModeCollectionV1)
}

func fixedPeerVectorReadyWithSourcePlacementV1(t testing.TB, ctx context.Context, sourceGroup raftcluster.GroupID, mode raftplacement.PlacementModeV1) fixedPeerVectorReadyFixtureV1 {
	return fixedPeerVectorReadyWithSourcePlacementAuthV1(t, ctx, sourceGroup, mode, false)
}

func fixedPeerVectorReadyWithSourcePlacementAuthV1(t testing.TB, ctx context.Context, sourceGroup raftcluster.GroupID, mode raftplacement.PlacementModeV1, authenticated bool) fixedPeerVectorReadyFixtureV1 {
	t.Helper()
	seed := fixedPeerVectorSeedV1(t)
	switch mode {
	case raftplacement.PlacementModeCollectionV1:
		seed.catalog.Placements[0].GroupID = sourceGroup
	case raftplacement.PlacementModeTokenV1, raftplacement.PlacementModeRingV1:
		seed.catalog.Placements[0].Mode = mode
		seed.catalog.Placements[0].GroupID = ""
		seed.catalog.Placements[0].RouteKey = raftplacement.RouteKeyDocumentIDV1
		seed.catalog.Placements[0].TokenPartitions = []raftplacement.TokenPartitionV1{{ID: "token-0", GroupID: sourceGroup, Start: 0, End: ^uint64(0)}}
	default:
		t.Fatalf("unsupported fixture source placement %q", mode)
	}
	configs := fixedPeerVectorTestConfigsV1(t, seed, fixedPeerSubprocessAllocatorV1(t))
	if authenticated {
		ca := newPeerCAFixtureV1(t)
		for i := range configs {
			configs[i].ClusterID = "split-source-insert-checkpoint"
			configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
			// Authenticated roots must receive their persistent identity while
			// empty, before trusted seed copying makes either root nonempty.
			validated, _, err := validateFixedPeerConfigV1(configs[i])
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(validated)
			if err != nil {
				t.Fatal(err)
			}
			if err := preparePeerStorageV1(validated, raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, config := range configs {
		group := "group-b"
		if config.NodeID == "ingress" {
			group = "group-a"
		}
		if err := os.CopyFS(filepath.Join(config.DataRoot, group), os.DirFS(seed.dir)); err != nil {
			t.Fatal(err)
		}
		if err := backenddb.RebindDurableRootSnapshotV1(filepath.Join(config.DataRoot, group)); err != nil {
			t.Fatal(err)
		}
		bootstrapFixedPeerVectorTrustedGenesisV1(t, config, seed.appliedCommandLSN)
	}
	processes := make([]*fixedPeerTestProcessV1, len(configs))
	for i := range configs {
		processes[i] = fixedPeerStartTestProcessV1(t, configs[i])
	}
	// Stop callers before their authority peers. LIFO child cleanup otherwise
	// stops the catalog leader while the target leader still owns control sockets.
	t.Cleanup(func() {
		for _, process := range processes {
			process.stop(t)
		}
	})
	client, err := NewFixedPeerTCPClientV1(configs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := client.Status(ctx, "owner-1")
		if statusErr != nil || status.CatalogRaft.State != "Follower" || status.CatalogRaft.LeaderID != "owner-2" || len(status.Groups) != 1 {
			return false
		}
		return status.Groups[0].GroupID == "group-b" && status.Groups[0].State == "Leader" && status.Groups[0].LeaderID == "owner-1"
	})
	record, err := raftplacement.NewCatalogMetaRecordV1(1, seed.catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, "owner-2", command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs {
			status, statusErr := client.Status(ctx, config.NodeID)
			if statusErr != nil || status.Catalog.Epoch != record.Epoch || status.Catalog.Digest != record.Digest || len(status.Groups) != 1 || status.Groups[0].LeaderID == "" {
				return false
			}
		}
		return true
	})
	ownerPublic := configs[0].Vector.PublicAddresses["owner-1"]
	ownerClient, err := DialContext(ctx, "tcp", ownerPublic)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerClient.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := ownerClient.VectorStatusV1(ctx)
		return statusErr == nil && status.Health.Ready && status.Health.Generation.Index == seed.manifest.IndexName && status.Health.Generation.Generation == seed.manifest.Generation
	})
	ingressPublic := configs[0].Vector.PublicAddresses["ingress"]
	ingressClient, err := DialContext(ctx, "tcp", ingressPublic)
	if err != nil {
		t.Fatal(err)
	}
	defer ingressClient.Close()
	fixedPeerWaitV1(t, ctx, func() bool {
		status, statusErr := ingressClient.VectorStatusV1(ctx)
		return statusErr == nil && status.Health.Ready && status.Health.Generation.Index == seed.manifest.IndexName && status.Health.Generation.Generation == seed.manifest.Generation
	})
	return fixedPeerVectorReadyFixtureV1{
		IngressPublicAddress: ingressPublic,
		Generation:           public.GenerationIDV1{Index: seed.manifest.IndexName, Generation: seed.manifest.Generation},
		RemotePartition:      0, RemoteGroup: "group-b", configs: configs, processes: processes, client: client,
	}
}

func (f fixedPeerVectorReadyFixtureV1) RequireOwnerReplication(t testing.TB, ctx context.Context, index uint64) {
	t.Helper()
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range []raftcluster.NodeID{"owner-1", "owner-2", "owner-3"} {
			status, err := f.client.Status(ctx, node)
			if err != nil || len(status.Groups) != 1 || status.Groups[0].GroupID != "group-b" || status.Groups[0].CommitIndex < index || status.Groups[0].RaftAppliedIndex < index || status.Groups[0].Applied.Index < index {
				return false
			}
		}
		return true
	})
}

func (f fixedPeerVectorReadyFixtureV1) RequireNoWrongGroupMutation(t testing.TB, _ context.Context, ids ...[]byte) {
	t.Helper()
	ingress := -1
	for i := range f.configs {
		if f.configs[i].NodeID == "ingress" {
			ingress = i
			break
		}
	}
	if ingress < 0 {
		t.Fatal("ingress config is missing")
	}
	f.processes[ingress].stop(t)
	database, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(f.configs[ingress].DataRoot, "group-a"), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	collection, err := collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		document, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if document != nil {
			t.Fatalf("wrong-group mutation stored %q in ingress group-a", id)
		}
	}
}

func (f fixedPeerVectorReadyFixtureV1) RequireOwnerDocuments(t testing.TB, present []byte, absent ...[]byte) {
	t.Helper()
	owner := -1
	for i := range f.configs {
		if f.configs[i].NodeID == "owner-1" {
			owner = i
			break
		}
	}
	if owner < 0 {
		t.Fatal("owner-1 fixture process is missing")
	}
	f.processes[owner].stop(t)
	database, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(f.configs[owner].DataRoot, "group-b"), CommandWAL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	}()
	collection, err := collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	document, err := collection.Get(present)
	if err != nil || document == nil {
		t.Fatalf("accepted owner document %q=(%q, %v)", present, document, err)
	}
	for _, id := range absent {
		document, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if document != nil {
			t.Fatalf("rejected owner document %q was stored", id)
		}
	}
}

func (f fixedPeerVectorReadyFixtureV1) SearchRequest(query []float32, topK int) public.SearchRequestV1 {
	return public.SearchRequestV1{Version: 1, Generation: f.Generation, Query: query, Metric: public.MetricCosineV1, TopK: topK, Probes: 1, EfSearch: 8, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 16}, Deadline: time.Now().Add(30 * time.Second)}
}

// A single ANN owner can differ from the committed source collection owner.
// The public write must refuse before committing a canonical document to the
// ANN group; a later source/projection protocol is required to admit this case.
func TestVectorPartitionPublicInsertRefusesSplitSourceAndANNOwnerV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := fixedPeerVectorReadyWithSourceGroupV1(t, ctx, "group-a")
	client, err := DialContext(ctx, "tcp", fixture.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	before, err := fixture.client.Status(ctx, "owner-1")
	if err != nil || len(before.Groups) != 1 {
		t.Fatalf("owner before insert: status=%+v err=%v", before, err)
	}
	request := public.InsertRequestV1{
		Version: 1, Generation: fixture.Generation,
		IdempotencyKey: []byte("split-source-ann-attempt"), ID: []byte("split-source-ann-document"),
		Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"split-source-ann"}`),
		Deadline: time.Now().Add(30 * time.Second),
	}
	result, err := client.VectorInsertV1(ctx, request)
	if err == nil {
		fixture.RequireOwnerReplication(t, ctx, result.CommitIndex)
		fixture.RequireOwnerDocuments(t, request.ID)
		t.Fatalf("public insert committed canonical document to ANN group %q although catalog source is group-a: %+v", result.OwnerGroup, result)
	}
	if !hasPublicVectorErrorCodeV1(err, public.ErrorUnavailableV1) {
		t.Fatalf("split source/ANN insert error=%v, want unavailable before commit", err)
	}
	after, err := fixture.client.Status(ctx, "owner-1")
	if err != nil || len(after.Groups) != 1 {
		t.Fatalf("owner after refusal: status=%+v err=%v", after, err)
	}
	if before.Groups[0].CommitIndex != after.Groups[0].CommitIndex ||
		before.Groups[0].RaftAppliedIndex != after.Groups[0].RaftAppliedIndex ||
		before.Groups[0].Applied.Index != after.Groups[0].Applied.Index {
		t.Fatalf("split source/ANN refusal advanced ANN Raft: before=%+v after=%+v", before.Groups[0], after.Groups[0])
	}
	fixture.RequireNoWrongGroupMutation(t, ctx, request.ID)
	fixture.RequireOwnerDocuments(t, []byte("base-x"), request.ID)
}

func TestVectorPartitionPublicInsertWithColocatedTokenSourceV1(t *testing.T) {
	for _, mode := range []raftplacement.PlacementModeV1{raftplacement.PlacementModeTokenV1, raftplacement.PlacementModeRingV1} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			fixture := fixedPeerVectorReadyWithSourcePlacementV1(t, ctx, "group-b", mode)
			client, err := DialContext(ctx, "tcp", fixture.IngressPublicAddress)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			request := public.InsertRequestV1{
				Version: 1, Generation: fixture.Generation,
				IdempotencyKey: []byte("colocated-" + string(mode)), ID: []byte("colocated-" + string(mode)),
				Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"colocated-token"}`),
				Deadline: time.Now().Add(30 * time.Second),
			}
			result, err := client.VectorInsertV1(ctx, request)
			if err != nil || result.OwnerGroup != "group-b" || !result.ProductionConsensus || result.CommitIndex <= 1 {
				t.Fatalf("colocated %s source insert result=%+v err=%v", mode, result, err)
			}
			fixture.RequireOwnerReplication(t, ctx, result.CommitIndex)
			fixture.RequireNoWrongGroupMutation(t, ctx, request.ID)
			fixture.RequireOwnerDocuments(t, request.ID)
		})
	}
}

// The ingress process owns no partition selected by this vector. The write
// must therefore cross the fixed-peer TCP boundary, commit and apply on the
// three-voter owner group, advance that generation's live delta, and become
// visible through the public production search service backed by those same
// fixed-peer DB/provider/lifecycle instances.
func TestVectorPartitionSystemNativeFourDaemonRemoteWriteRoutesAppliesAndBecomesVisibleV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fixture := fixedPeerVectorReadyV1(t, ctx)
	client, err := DialContext(ctx, "tcp", fixture.IngressPublicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for name, vector := range map[string][]float32{
		"dimension": {1}, "nan": {float32(math.NaN()), 1},
		"infinity": {1, float32(math.Inf(1))}, "zero_cosine": {0, 0},
	} {
		t.Run("invalid_vector_"+name, func(t *testing.T) {
			request := public.InsertRequestV1{
				Version: 1, Generation: fixture.Generation, IdempotencyKey: []byte("invalid-" + name),
				ID: []byte("invalid-" + name), Vector: vector, Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(10 * time.Second),
			}
			if _, err := client.VectorInsertV1(ctx, request); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
				t.Fatalf("malformed vector error=%v", err)
			}
		})
	}
	stale := public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: fixture.Generation.Index, Generation: fixture.Generation.Generation + 1},
		IdempotencyKey: []byte("reject-stale-generation"), ID: []byte("reject-stale-generation"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(30 * time.Second),
	}
	if _, err := client.VectorInsertV1(ctx, stale); !hasPublicVectorErrorCodeV1(err, public.ErrorGenerationMismatchV1) {
		t.Fatalf("stale generation error=%v", err)
	}
	mismatch := public.InsertRequestV1{
		Version: 1, Generation: fixture.Generation,
		IdempotencyKey: []byte("reject-document-mismatch"), ID: []byte("reject-document-mismatch"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[1,0]}`), Deadline: time.Now().Add(30 * time.Second),
	}
	if _, err := client.VectorInsertV1(ctx, mismatch); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
		t.Fatalf("document mismatch error=%v", err)
	}
	invalidID := mismatch
	invalidID.IdempotencyKey = []byte("reject-invalid-id")
	invalidID.ID = []byte{0xff}
	invalidID.Document = []byte(`{"embedding":[0,1]}`)
	if _, err := client.VectorInsertV1(ctx, invalidID); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
		t.Fatalf("invalid UTF-8 ID error=%v", err)
	}
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &VectorPartitionRoutedInsertV1{}}, true); !errors.Is(err, ErrFixedPeerVectorProofMissingV1) {
		t.Fatalf("missing proof error=%v", err)
	}
	private := VectorPartitionRoutedInsertV1{
		Request: public.InsertRequestV1{
			Version: 1, Generation: fixture.Generation, IdempotencyKey: []byte("reject-stale-catalog"), ID: []byte("reject-stale-catalog"), Vector: []float32{0, 1},
			Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(30 * time.Second),
		},
		Identity: fixture.configs[0].Vector.Identity,
		CatalogProof: raftplacement.CatalogProofV1{
			Epoch: fixture.configs[0].Vector.Identity.Index.CatalogEpoch, Digest: "stale-catalog-digest",
		},
		ReadySetDigest: "proof-present", RouterModelDigest: "proof-present", PartitionID: fixture.RemotePartition, OwnerGroup: raftcluster.GroupID(fixture.RemoteGroup),
	}
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &private}, true); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("stale catalog proof error=%v", err)
	}
	private.Request.ID = []byte{0xff}
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &private}, true); !errors.Is(err, ErrFixedPeerVectorDocumentV1) {
		t.Fatalf("owner invalid UTF-8 ID error=%v", err)
	}
	private.Request.ID = []byte("reject-wrong-owner")
	private.CatalogProof.Digest = fixture.configs[0].Vector.Identity.Index.CatalogDigest
	private.OwnerGroup = "group-a"
	if _, err := fixture.client.call(ctx, "owner-1", "vector-forward", fixedPeerRequestV1{VectorInsert: &private}, true); !errors.Is(err, ErrFixedPeerVectorWrongOwnerV1) {
		t.Fatalf("wrong ownership error=%v", err)
	}
	if _, err := client.VectorSearchStrictV1(ctx, fixture.SearchRequest([]float32{0, 1}, 4)); err != nil {
		t.Fatalf("pre-mutation ingress search: %v", err)
	}

	started := time.Now()
	insert := public.InsertRequestV1{
		Version:        1,
		Generation:     fixture.Generation,
		IdempotencyKey: []byte("remote-visible-attempt-1"),
		ID:             []byte("remote-visible"),
		Vector:         []float32{0, 1},
		Document:       []byte(`{"embedding":[0,1],"kind":"remote-visible"}`),
		Deadline:       time.Now().Add(30 * time.Second),
	}
	result, err := client.VectorInsertV1(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("routed mutation acknowledgement latency=%s", time.Since(started))
	if result.Generation != fixture.Generation || result.PartitionID != fixture.RemotePartition || result.OwnerGroup != fixture.RemoteGroup || result.CommitTerm == 0 || result.CommitIndex <= 1 || result.AppliedIndex < result.CommitIndex || !result.ProductionConsensus || result.LiveRevision == 0 || result.VisibilityGeneration != fixture.Generation || result.VisibleID != "remote-visible" {
		t.Fatalf("incomplete mutation evidence: %+v", result)
	}
	if result.Counters.Routes != 1 || result.Counters.Forwards != 1 || result.Counters.Commits != 1 || result.Counters.Replications != 1 || result.Counters.Applies != 1 || result.Counters.VisibilityProofs != 1 {
		t.Fatalf("mutation counters: %+v", result.Counters)
	}
	fixture.RequireOwnerReplication(t, ctx, result.CommitIndex)
	t.Run("unchanged_retry", func(t *testing.T) {
		if replay, err := client.VectorInsertV1(ctx, insert); err != nil || replay.VisibleID != result.VisibleID || !replay.ProductionConsensus || replay.LiveRevision == 0 {
			t.Fatalf("unchanged replay result=%+v err=%v", replay, err)
		}
	})
	for _, node := range []raftcluster.NodeID{"owner-1", "ingress"} {
		t.Run("idempotency_conflict_"+string(node), func(t *testing.T) {
			writer, err := DialContext(ctx, "tcp", fixture.configs[0].Vector.PublicAddresses[node])
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			before, err := fixture.client.Status(ctx, "owner-1")
			if err != nil {
				t.Fatal(err)
			}
			_, err = writer.VectorInsertV1(ctx, public.InsertRequestV1{
				Version: 1, Generation: fixture.Generation, IdempotencyKey: []byte("remote-visible-attempt-1"),
				ID: []byte("conflict-" + string(node)), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"conflict"}`),
				Deadline: time.Now().Add(30 * time.Second),
			})
			if !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
				t.Fatalf("idempotency conflict error=%v", err)
			}
			after, err := fixture.client.Status(ctx, "owner-1")
			if err != nil {
				t.Fatal(err)
			}
			if before.Groups[0].CommitIndex != after.Groups[0].CommitIndex || before.Groups[0].RaftAppliedIndex != after.Groups[0].RaftAppliedIndex || before.Groups[0].Applied.Index != after.Groups[0].Applied.Index {
				t.Fatalf("rejected conflict advanced commit/apply: before=%+v after=%+v", before.Groups[0], after.Groups[0])
			}
		})
	}

	searchStarted := time.Now()
	search, err := client.VectorSearchStrictV1(ctx, fixture.SearchRequest([]float32{0, 1}, 4))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("strict production search latency=%s", time.Since(searchStarted))
	if len(search.Neighbors) == 0 || search.Neighbors[0].ID != "remote-visible" {
		t.Fatalf("subsequent production search did not observe routed mutation: %+v", search)
	}
	// Replace through the ordinary replicated document path, keeping the same
	// vector and stable ID. Presence alone must not acknowledge the old replay.
	submitMutation := func(command iwire.CommandID, attempt string, document []byte) {
		t.Helper()
		owner, err := fixture.client.Status(ctx, "owner-1")
		if err != nil {
			t.Fatal(err)
		}
		sections := []iwire.Section{
			{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})},
			{ID: iwire.SectionIdempotencyKey, Bytes: []byte(attempt)},
			{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, owner.Groups[0].CatalogVersion)},
			collectionNameRef("docs"),
			{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, insert.ID)},
		}
		if command != iwire.CommandDeleteBatch {
			sections = append(sections, documentFormatSection(collections.DocumentFormatJSON), iwire.Section{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, document)})
		}
		if command == iwire.CommandReplaceBatch {
			sections = append(sections, iwire.Section{ID: iwire.SectionReplacementMode, Bytes: []byte{1}})
		}
		validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := iwire.AppendDeterministicEntry(nil, validated)
		if err != nil {
			t.Fatal(err)
		}
		routeRequest := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: "docs", Shape: ClusterRouteShapeCollection}
		route, err := fixture.client.Route(ctx, "owner-1", routeRequest)
		if err != nil {
			t.Fatal(err)
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, routeRequest, route)
		changed, err := fixture.client.Submit(ctx, "owner-1", entry, metadata)
		if err != nil || !changed.CommittedApplied {
			t.Fatalf("%s result=%+v err=%v", attempt, changed, err)
		}
	}
	submitMutation(iwire.CommandReplaceBatch, "replace-visible-metadata", []byte(`{"embedding":[0,1],"kind":"replacement"}`))
	t.Run("old_retry_after_replacement", func(t *testing.T) {
		if _, err := client.VectorInsertV1(ctx, insert); !hasPublicVectorErrorCodeV1(err, public.ErrorCommitAmbiguousV1) {
			t.Fatalf("replaced content replay error=%v", err)
		}
	})
	submitMutation(iwire.CommandDeleteBatch, "delete-visible-document", nil)
	submitMutation(iwire.CommandInsertBatch, "reinsert-visible-document", []byte(`{"embedding":[0,1],"kind":"reinserted"}`))
	t.Run("old_retry_after_delete_reinsert", func(t *testing.T) {
		if _, err := client.VectorInsertV1(ctx, insert); !hasPublicVectorErrorCodeV1(err, public.ErrorCommitAmbiguousV1) {
			t.Fatalf("reinserted content replay error=%v", err)
		}
	})
	fixture.RequireNoWrongGroupMutation(t, ctx,
		[]byte("remote-visible"), []byte("reject-stale-generation"), []byte("reject-document-mismatch"), []byte{0xff}, []byte("reject-stale-catalog"), []byte("reject-wrong-owner"),
	)
	fixture.RequireOwnerDocuments(t, []byte("remote-visible"),
		[]byte("conflict-owner-1"), []byte("conflict-ingress"),
		[]byte("reject-stale-generation"), []byte("reject-document-mismatch"), []byte{0xff}, []byte("reject-stale-catalog"), []byte("reject-wrong-owner"),
	)
}

func hasPublicVectorErrorCodeV1(err error, code public.ErrorCodeV1) bool {
	var publicErr *public.ErrorV1
	return errors.As(err, &publicErr) && publicErr.Code == code
}

type fixedPeerVectorSeedFixtureV1 struct {
	dir               string
	appliedCommandLSN uint64
	manifest          collections.VectorPartitionManifestV1
	catalog           raftplacement.CatalogV1
}

func fixedPeerVectorSeedV1(t testing.TB) fixedPeerVectorSeedFixtureV1 {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	// Direct construction is trusted fixture genesis. The opaque command-WAL
	// coverage is bound to Raft apply progress before any child is started; all
	// behavior under test is a later real committed command.
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	definition := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}}, VectorIndexes: []collections.VectorIndexDefinition{definition}}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&meta); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	for _, document := range []struct{ id, body string }{{"base-x", `{"embedding":[1,0],"kind":"base-x"}`}, {"base-diagonal", `{"embedding":[0.6,0.8],"kind":"base-diagonal"}`}} {
		if _, err := collection.Insert([]byte(document.id), []byte(document.body)); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(definition.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "building", Collection: meta.Name, IndexName: definition.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}, BalancePolicy: "disjoint_v1",
		Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-b"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range rows {
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(definition.Name, manifest, 4701, []collections.VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: definition.Dimensions}})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	routerConfig := internalrouter.DefaultRouterConfigV1()
	routerConfig.BranchFactor, routerConfig.LeafSize, routerConfig.RepresentativeBudget = 2, 1, 1
	routerConfig.MaxDepth, routerConfig.MaxIterations, routerConfig.MaxVectors = 4, 8, 8
	routerConfig.MaxDimensions, routerConfig.MaxRepresentatives, routerConfig.MaxScalarWork = 8, 32, 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, []internalrouter.RouterPartitionV1{partition}, collections.VectorPartitionRouterBuildOptionsV1{Config: routerConfig, AssetFileID: 4702, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	prepared, err := collection.PreparedVectorPartitionManifestWithContextV1(context.Background(), definition.Name, manifest.Generation)
	resources.Release()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	beforeLiveBindingLSN := database.State().AppliedCommandLSN
	if err := collection.EnsureVectorPartitionLiveBindingV1(context.Background(), prepared); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	appliedCommandLSN := database.State().AppliedCommandLSN
	if beforeLiveBindingLSN == 0 || appliedCommandLSN <= beforeLiveBindingLSN {
		_ = database.Close()
		t.Fatalf("trusted genesis live binding command-WAL coverage=%d, want greater than pre-binding %d", appliedCommandLSN, beforeLiveBindingLSN)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: meta.Name}
	catalog := raftplacement.CatalogV1{
		Features: features,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"ingress"}, LeaderHint: "ingress"},
			// Keep the placement hint deliberately different from the Raft
			// bootstrap node so the system proof cannot rely on static ownership.
			{ID: "group-b", Members: []raftcluster.NodeID{"owner-1", "owner-2", "owner-3"}, LeaderHint: "owner-3"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-b", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	return fixedPeerVectorSeedFixtureV1{dir: dir, appliedCommandLSN: appliedCommandLSN, manifest: prepared, catalog: catalog}
}

// bootstrapFixedPeerVectorTrustedGenesisV1 records only the opaque local
// command-WAL coverage created by direct fixture construction. The 1/1 entry
// is a trusted genesis baseline; every mutation asserted by the system test is
// committed and applied through the real fixed-peer Raft group above it.
func bootstrapFixedPeerVectorTrustedGenesisV1(t testing.TB, config FixedPeerTCPConfigV1, coverage uint64) {
	t.Helper()
	if coverage == 0 {
		t.Fatal("trusted genesis requires command-WAL coverage")
	}
	validated, _, err := validateFixedPeerConfigV1(config)
	if err != nil {
		t.Fatalf("validate fixed-peer config: %v", err)
	}
	var local *FixedPeerTCPGroupV1
	for i := range validated.Groups {
		for _, peer := range validated.Groups[i].Peers {
			if peer.ID != validated.NodeID {
				continue
			}
			if local != nil {
				t.Fatalf("node %s unexpectedly hosts multiple data groups", validated.NodeID)
			}
			local = &validated.Groups[i]
		}
	}
	if local == nil {
		t.Fatalf("node %s has no local data group", validated.NodeID)
	}
	cluster, err := raftcluster.Validate(raftcluster.Config{
		Dir:               filepath.Join(validated.DataRoot, string(local.ID)),
		ClusterDir:        validated.RaftRoot,
		DisableSideStores: true,
		NodeID:            validated.NodeID,
		GroupID:           local.ID,
		Peers:             local.Peers,
		Features:          local.Features,
	})
	if err != nil {
		t.Fatalf("validate local data-group layout: %v", err)
	}
	database, err := backenddb.Open(backenddb.Options{Dir: cluster.Dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open trusted genesis DB: %v", err)
	}
	gotCoverage := database.State().AppliedCommandLSN
	if err := database.Close(); err != nil {
		t.Fatalf("close trusted genesis DB: %v", err)
	}
	if gotCoverage != coverage {
		t.Fatalf("trusted genesis coverage=%d, want seed coverage %d", gotCoverage, coverage)
	}
	progress, err := raftapply.OpenDurableApplyProgressStore(cluster.Layout.ApplyDir, raftapply.DurableApplyStoreOptions{DisableSync: true, AllowInitialIndexGap: true})
	if err != nil {
		t.Fatalf("open trusted genesis apply progress: %v", err)
	}
	recordErr := progress.RecordApplied(raftapply.ApplyProgressRecordV1{
		EntryID:           raftentry.ApplyEntryID{Term: 1, Index: 1},
		AppliedCommandLSN: coverage,
	})
	if err := errors.Join(recordErr, progress.Close()); err != nil {
		t.Fatalf("record trusted genesis apply progress: %v", err)
	}
}

func fixedPeerVectorTestConfigsV1(t testing.TB, seed fixedPeerVectorSeedFixtureV1, allocate ...func(raftcluster.NodeID) string) []FixedPeerTCPConfigV1 {
	t.Helper()
	address := fixedPeerTestAllocatorV1(t, allocate)
	nodeIDs := []raftcluster.NodeID{"ingress", "owner-1", "owner-2", "owner-3"}
	features := raftcluster.DefaultFeatureSet()
	features.Required = append(features.Required,
		raftcluster.RequiredFeature{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]},
	)
	nodes := make([]FixedPeerTCPNodeV1, 0, len(nodeIDs))
	meta := FixedPeerTCPGroupV1{ID: "meta", BootstrapNode: "owner-2", Features: features}
	groupA := FixedPeerTCPGroupV1{ID: "group-a", BootstrapNode: "ingress"}
	groupB := FixedPeerTCPGroupV1{ID: "group-b", BootstrapNode: "owner-1"}
	publicAddresses := make(map[raftcluster.NodeID]string, len(nodeIDs))
	shardAddresses := map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-b": {}}
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, FixedPeerTCPNodeV1{ID: nodeID, Address: address(nodeID)})
		meta.Peers = append(meta.Peers, raftcluster.Peer{ID: nodeID, Address: address(nodeID), Capabilities: features})
		peer := raftcluster.Peer{ID: nodeID, Address: address(nodeID)}
		if nodeID == "ingress" {
			groupA.Peers = append(groupA.Peers, peer)
		} else {
			groupB.Peers = append(groupB.Peers, peer)
			shardAddresses["group-b"][nodeID] = address(nodeID)
		}
		publicAddresses[nodeID] = address(nodeID)
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, seed.catalog)
	if err != nil {
		t.Fatal(err)
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: seed.manifest.Collection}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
		SourceGeneration: seed.manifest.SourceGeneration, SourceChecksum: seed.manifest.SourceChecksum, SourceSchemaHash: seed.manifest.SourceSchemaHash, SourceRowCount: seed.manifest.SourceRowCount,
		PartitionGeneration: seed.manifest.Generation, PartitionCount: 1,
		Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-b"}},
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: ref, CollectionIncarnation: 1, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
			IndexEpoch: 1, CatalogEpoch: record.Epoch, CatalogDigest: record.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: seed.manifest.SourceGeneration, Checksum: seed.manifest.SourceChecksum, SchemaHash: seed.manifest.SourceSchemaHash, RowCount: seed.manifest.SourceRowCount,
		},
		Generation: seed.manifest.Generation,
	}
	requestBase := VectorPartitionCoordinatorRequestV1{
		Version: VectorPartitionCoordinatorVersionV1, RequestID: "fixed-peer-vector", CancellationID: "fixed-peer-vector-cancel",
		Database: ref.Database, Catalog: ref.Catalog, Collection: ref.Collection, IndexName: seed.manifest.IndexName, IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest,
		Metric: VectorPartitionShardSearchMetricCosineV1, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: 1, PartitionProbes: 1,
		Consistency: VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
		TopK: 8, EfSearch: 8, RequestBytesLimit: 1 << 20, CandidateBytesLimit: 8 << 20, ResponseBytesLimit: 1 << 20, MergeEntriesLimit: 8,
	}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection: ref, Catalog: seed.catalog, Manifest: seed.manifest, Placement: placement, Identity: identity,
		PublicAddresses: publicAddresses, ShardAddresses: shardAddresses, RequestBase: requestBase,
		IndexedThrough: 1,
	}
	root := t.TempDir()
	configs := make([]FixedPeerTCPConfigV1, len(nodes))
	for i, node := range nodes {
		configs[i] = FixedPeerTCPConfigV1{
			NodeID: node.ID, DataRoot: filepath.Join(root, string(node.ID), "data"), RaftRoot: filepath.Join(root, string(node.ID), "raft"), ListenAddress: node.Address,
			Nodes: nodes, Catalog: meta, Groups: []FixedPeerTCPGroupV1{groupA, groupB}, RequestTimeout: 4 * time.Second, RaftTimeout: 300 * time.Millisecond, Vector: vector,
			RaftListen: map[raftcluster.GroupID]string{},
		}
		for _, group := range []FixedPeerTCPGroupV1{meta, groupA, groupB} {
			for _, peer := range group.Peers {
				if peer.ID == node.ID {
					configs[i].RaftListen[group.ID] = peer.Address
				}
			}
		}
		if _, _, err := validateFixedPeerConfigV1(configs[i]); err != nil {
			t.Fatalf("vector config %s: %v", node.ID, err)
		}
	}
	return configs
}

func TestFixedPeerVectorTestConfigIsFourDaemonsAndThreeOwnerVotersV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	configs := fixedPeerVectorTestConfigsV1(t, seed)
	if len(configs) != 4 || len(configs[0].Groups) != 2 || len(configs[0].Groups[1].Peers) != 3 {
		t.Fatalf("topology is not four daemons with three owner voters: %+v", configs)
	}
	for _, config := range configs {
		if config.Vector == nil || config.Vector.PublicAddresses[config.NodeID] == "" {
			t.Fatalf("missing vector endpoint for %s", config.NodeID)
		}
	}
}

func TestFixedPeerVectorConfigRequiresCatalogLifecycleFeatureV1(t *testing.T) {
	configs := fixedPeerVectorTestConfigsV1(t, fixedPeerVectorSeedV1(t))
	config := configs[0]
	config.Catalog.Features.Required = []raftcluster.RequiredFeature{{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]}}
	if _, _, err := validateFixedPeerConfigV1(config); err == nil {
		t.Fatal("vector config without catalog lifecycle feature was accepted")
	}
}

func TestVectorPartitionReplicatedLiveManifestRejectsSpoofedRepresentativeV1(t *testing.T) {
	manifest := fixedPeerVectorSeedV1(t).manifest
	if !vectorPartitionReplicatedLiveManifestMatchesV1(manifest, manifest) {
		t.Fatal("valid prepared manifest did not match itself")
	}
	spoofed := manifest
	spoofed.Representatives = append([]collections.VectorPartitionRepresentativeV2(nil), manifest.Representatives...)
	spoofed.Representatives[0].NodeID++
	if spoofed.IntegrityDigest != manifest.IntegrityDigest || vectorPartitionReplicatedLiveManifestMatchesV1(spoofed, manifest) {
		t.Fatal("replicated live manifest accepted altered representatives with a retained digest")
	}
}

func TestFixedPeerVectorConfigRequiresLocalCatalogAuthorityV1(t *testing.T) {
	configs := fixedPeerVectorTestConfigsV1(t, fixedPeerVectorSeedV1(t))
	config := configs[0]
	if config.Catalog.Peers[0].ID != config.NodeID {
		t.Fatal("fixture ingress is not the first catalog peer")
	}
	config.Catalog.Peers = config.Catalog.Peers[1:]
	listen := make(map[raftcluster.GroupID]string, len(config.RaftListen)-1)
	for group, address := range config.RaftListen {
		if group != config.Catalog.ID {
			listen[group] = address
		}
	}
	config.RaftListen = listen
	if _, _, err := validateFixedPeerConfigV1(config); err == nil || !strings.Contains(err.Error(), "fixed-peer vector runtime requires local catalog authority") {
		t.Fatalf("catalog-consumer vector node accepted: %v", err)
	}
}

func TestFixedPeerVectorConfigRequiresOneOwnerGroupV1(t *testing.T) {
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
	vector := &FixedPeerTCPVectorConfigV1{
		Collection:      ref,
		Manifest:        collections.VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", Generation: 1, IntegrityDigest: "integrity"},
		Placement:       raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", PartitionGeneration: 1},
		Identity:        raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: ref, IndexName: "embedding", CatalogEpoch: 1, CatalogDigest: "catalog"}, Generation: 1},
		PublicAddresses: map[raftcluster.NodeID]string{"node": "127.0.0.1:10001"},
		ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{
			"group-a": {"node": "127.0.0.1:10002"},
			"group-b": {"node": "127.0.0.1:10003"},
		},
		IndexedThrough: 1,
	}
	config := FixedPeerTCPConfigV1{
		NodeID: "node", Nodes: []FixedPeerTCPNodeV1{{ID: "node"}}, Catalog: FixedPeerTCPGroupV1{ID: "meta"}, Vector: vector,
		Groups: []FixedPeerTCPGroupV1{
			{ID: "group-a", Peers: []raftcluster.Peer{{ID: "node"}}},
			{ID: "group-b", Peers: []raftcluster.Peer{{ID: "node"}}},
		},
	}
	config.Vector.Placement.Partitions = nil
	want := "fixed-peer vector runtime requires exactly one owner group"
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{"meta": true}); err == nil || err.Error() != want {
		t.Fatalf("zero-owner validation error=%v, want %q", err, want)
	}
	config.Vector.Placement.Partitions = []raftplacement.VectorPartitionGroupV1{
		{PartitionID: 0, GroupID: "group-a"},
		{PartitionID: 1, GroupID: "group-b"},
	}
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{"meta": true, "group-a": true, "group-b": true}); err == nil || err.Error() != want {
		t.Fatalf("multi-owner validation error=%v, want %q", err, want)
	}
	config.Vector.Placement.Partitions = []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}}
	config.Vector.ShardAddresses["group-a"]["node"] = "missing-port"
	want = `invalid vector shard address for node "node"`
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{"meta": true, "group-a": true}); err == nil || err.Error() != want {
		t.Fatalf("invalid shard address error=%v, want %q", err, want)
	}
	config.Vector.ShardAddresses["group-a"]["node"] = "127.0.0.1:10002"
	want = "fixed-peer vector runtime requires exactly one local data group"
	for _, localGroups := range []map[raftcluster.GroupID]bool{
		{"meta": true},
		{"meta": true, "group-a": true, "group-b": true},
	} {
		if err := validateFixedPeerVectorConfigV1(config, localGroups); err == nil || err.Error() != want {
			t.Fatalf("local data-group validation error=%v, want %q", err, want)
		}
	}
	config.Vector.Catalog = raftplacement.CatalogV1{
		Features:   raftplacement.DefaultFeatureSet(),
		Groups:     []raftplacement.GroupV1{{ID: "group-a", Members: []raftcluster.NodeID{"node"}, LeaderHint: "node"}},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	config.Vector.Catalog.Features.Required = append(config.Vector.Catalog.Features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	config.Vector.Placement.IndexDefinitionDigest = strings.Repeat("a", 64)
	config.Vector.Placement.SourceGeneration = 1
	config.Vector.Placement.SourceChecksum = 1
	config.Vector.Placement.SourceSchemaHash = 1
	config.Vector.Placement.SourceRowCount = 1
	config.Vector.Placement.PartitionCount = 1
	config.Vector.Manifest.IndexDefinitionDigest = config.Vector.Placement.IndexDefinitionDigest
	config.Vector.Manifest.SourceGeneration = 1
	config.Vector.Manifest.SourceChecksum = 1
	config.Vector.Manifest.SourceSchemaHash = 1
	config.Vector.Manifest.SourceRowCount = 1
	config.Vector.Manifest.PartitionCount = 1
	config.Vector.Manifest.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}}
	config.Vector.Identity.Index.CollectionIncarnation = 1
	config.Vector.Identity.Index.IndexEpoch = 1
	config.Vector.Identity.Index.IndexDefinitionDigest = config.Vector.Placement.IndexDefinitionDigest
	record, err := raftplacement.NewCatalogMetaRecordV1(config.Vector.Identity.Index.CatalogEpoch, config.Vector.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	config.Vector.Identity.Index.CatalogDigest = record.Digest
	config.Vector.Identity.Source = raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 1, Checksum: 1, SchemaHash: 1, RowCount: 1}
	config.Vector.RequestBase = VectorPartitionCoordinatorRequestV1{
		RequestID: "request", CancellationID: "cancel", Database: ref.Database, Catalog: ref.Catalog, Collection: ref.Collection,
		IndexDefinitionDigest: config.Vector.Placement.IndexDefinitionDigest,
		RouterMode:            collections.VectorPartitionRouterModeExactV1, RouterScoreBudget: 1, StatsMode: VectorPartitionShardSearchStatsBasicV1,
	}
	readyConfigs := fixedPeerVectorTestConfigsV1(t, fixedPeerVectorSeedV1(t))
	for i, localGroup := range []raftcluster.GroupID{"group-a", "group-b"} {
		if err := validateFixedPeerVectorConfigV1(readyConfigs[i], map[raftcluster.GroupID]bool{"meta": true, localGroup: true}); err != nil {
			t.Fatalf("one local data group %q rejected: %v", localGroup, err)
		}
	}
	config.Vector = nil
	if err := validateFixedPeerVectorConfigV1(config, map[raftcluster.GroupID]bool{"meta": true, "group-a": true, "group-b": true}); err != nil {
		t.Fatalf("non-vector config rejected: %v", err)
	}
}

func TestFixedPeerVectorConfigCloneRetainsValidatedSnapshotV1(t *testing.T) {
	input := &FixedPeerTCPVectorConfigV1{
		Catalog: raftplacement.CatalogV1{
			Groups: []raftplacement.GroupV1{{ID: "g", Members: []raftcluster.NodeID{"n"}}},
		},
		Manifest: collections.VectorPartitionManifestV1{
			DomainPacks: []collections.VectorPartitionDomainPackV1{{DomainID: 1, PackID: 2}},
		},
		PublicAddresses: map[raftcluster.NodeID]string{"n": "127.0.0.1:10001"},
		ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{
			"g": {"n": "127.0.0.1:10002"},
		},
		RequestBase: VectorPartitionCoordinatorRequestV1{Query: []float32{1}},
	}
	cloned := cloneFixedPeerVectorConfigV1(input)
	input.Catalog.Groups[0].Members[0] = "changed"
	input.Manifest.DomainPacks[0].PackID = 3
	input.PublicAddresses["n"] = "changed"
	input.ShardAddresses["g"]["n"] = "changed"
	input.RequestBase.Query[0] = 2
	if cloned.Catalog.Groups[0].Members[0] != "n" || cloned.Manifest.DomainPacks[0].PackID != 2 ||
		cloned.PublicAddresses["n"] != "127.0.0.1:10001" || cloned.ShardAddresses["g"]["n"] != "127.0.0.1:10002" ||
		cloned.RequestBase.Query[0] != 1 {
		t.Fatal("vector config retained caller-owned mutable storage")
	}
}

func TestFixedPeerVectorConfigPreflightBoundsInventoryAndAuthenticatedListenersV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Vector = &FixedPeerTCPVectorConfigV1{
		RequestBase: VectorPartitionCoordinatorRequestV1{Query: make([]float32, fixedPeerMaxConfigInventoryBytesV1/32+1)},
	}
	if err := preflightFixedPeerConfigV1(config); !errors.Is(err, raftcluster.ErrInvalidConfig) || !strings.Contains(err.Error(), "vector inventory exceeds byte budget") {
		t.Fatalf("oversized vector preflight = %v", err)
	}
	config.Vector.RequestBase.Query = nil
	config.ClusterID = "vector-security-preflight"
	config.Credentials = &PeerCredentialsV1{TrustRootsFile: "ca.pem", CertificateFile: "node.pem", PrivateKeyFile: "key.pem"}
	config.Vector.PublicAddresses = map[raftcluster.NodeID]string{"node": "127.0.0.1:10001"}
	config.Vector.ShardAddresses = map[raftcluster.GroupID]map[raftcluster.NodeID]string{"group-a": {"node": "127.0.0.1:10002"}}
	if err := preflightFixedPeerConfigV1(config); err != nil {
		t.Fatalf("loopback-only credentialed vector preflight = %v", err)
	}
	config.Vector.PublicAddresses["node"] = "192.168.1.10:10001"
	if err := preflightFixedPeerConfigV1(config); !errors.Is(err, raftcluster.ErrInvalidConfig) || !strings.Contains(err.Error(), "public listener must be loopback") {
		t.Fatalf("remote credentialed vector public listener = %v", err)
	}
	config.Vector.PublicAddresses["node"] = "127.0.0.1:10001"
	config.Vector.ShardAddresses["group-a"]["node"] = "192.168.1.10:10002"
	if err := preflightFixedPeerConfigV1(config); !errors.Is(err, raftcluster.ErrInvalidConfig) || !strings.Contains(err.Error(), "shard listener must be loopback") {
		t.Fatalf("remote credentialed vector shard listener = %v", err)
	}
}

func TestFixedPeerVectorConfigPreflightAdmitsBounded100KMembershipsV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	config.Vector = &FixedPeerTCPVectorConfigV1{
		Manifest: collections.VectorPartitionManifestV1{
			Memberships:        make([]collections.VectorPartitionMembershipV1, 100_000),
			OverlapMemberships: make([]collections.VectorPartitionMembershipV1, 20_000),
		},
	}
	for i := range config.Vector.Manifest.Memberships {
		config.Vector.Manifest.Memberships[i] = collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: uint32(i % 64)}
	}
	for i := range config.Vector.Manifest.OverlapMemberships {
		config.Vector.Manifest.OverlapMemberships[i] = collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(i * 5), PartitionID: uint32(i % 64)}
	}
	if err := preflightFixedPeerConfigV1(config); err != nil {
		t.Fatalf("bounded 100K/20K inventory preflight = %v", err)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 1<<20 || len(raw) > fixedPeerMaxConfigBytesV1 {
		t.Fatalf("representative encoded config bytes = %d, want (1 MiB, %d]", len(raw), fixedPeerMaxConfigBytesV1)
	}
	t.Logf("100K/20K membership config encodes to %d bytes", len(raw))
}

func TestFixedPeerVectorConfigPreflightBeforeDiskCreationV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	limits := DefaultVectorPartitionCoordinatorLimitsV1()
	bindCatalog := func(c *FixedPeerTCPConfigV1) {
		record, err := raftplacement.NewCatalogMetaRecordV1(c.Vector.Identity.Index.CatalogEpoch, c.Vector.Catalog)
		if err != nil {
			t.Fatal(err)
		}
		c.Vector.Identity.Index.CatalogDigest = record.Digest
	}
	for name, mutate := range map[string]func(*FixedPeerTCPConfigV1){
		"catalog_lifecycle_feature_missing": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Catalog.Features = raftplacement.DefaultFeatureSet()
			bindCatalog(c)
		},
		"catalog_digest_binding": func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.CatalogDigest = strings.Repeat("b", 64) },
		"catalog_members_binding": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), c.Vector.Catalog.Groups...)
			c.Vector.Catalog.Groups[1].Members = append(append([]raftcluster.NodeID(nil), c.Vector.Catalog.Groups[1].Members...), "ingress")
			bindCatalog(c)
		},
		"catalog_owner_leader_missing": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), c.Vector.Catalog.Groups...)
			c.Vector.Catalog.Groups[1].LeaderHint = ""
			bindCatalog(c)
		},
		"catalog_owner_leader_not_member": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), c.Vector.Catalog.Groups...)
			c.Vector.Catalog.Groups[1].LeaderHint = "ingress"
		},
		"request_id_empty":      func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.RequestID = "" },
		"cancellation_id_empty": func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.CancellationID = "" },
		"request_id_limit": func(c *FixedPeerTCPConfigV1) {
			c.Vector.RequestBase.RequestID = strings.Repeat("r", limits.MaxIdentityBytes-vectorPartitionPublicRequestSuffixBytesV1+1)
		},
		"cancellation_id_limit": func(c *FixedPeerTCPConfigV1) {
			c.Vector.RequestBase.CancellationID = strings.Repeat("c", limits.MaxIdentityBytes-vectorPartitionPublicRequestSuffixBytesV1+1)
		},
		"request_database":   func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.Database = "other" },
		"request_catalog":    func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.Catalog = "other" },
		"request_collection": func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.Collection = "other" },
		"request_digest":     func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.IndexDefinitionDigest = strings.Repeat("b", 64) },
		"effective_index_name_limit": func(c *FixedPeerTCPConfigV1) {
			name := strings.Repeat("i", limits.MaxIdentityBytes+1)
			c.Vector.Manifest.IndexName, c.Vector.Placement.IndexName, c.Vector.Identity.Index.IndexName = name, name, name
		},
		"request_router_mode":        func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.RouterMode = "invalid" },
		"request_router_budget_zero": func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.RouterScoreBudget = 0 },
		"request_router_budget_limit": func(c *FixedPeerTCPConfigV1) {
			c.Vector.RequestBase.RouterScoreBudget = limits.MaxRouterScoreCalls + 1
		},
		"request_local_budget_negative": func(c *FixedPeerTCPConfigV1) {
			c.Vector.RequestBase.LocalScoreBudget = -1
		},
		"request_local_budget_limit": func(c *FixedPeerTCPConfigV1) {
			c.Vector.RequestBase.LocalScoreBudget = limits.MaxLocalScoreCalls + 1
		},
		"request_exact_budget_shortfall": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Manifest.Representatives = append(c.Vector.Manifest.Representatives, collections.VectorPartitionRepresentativeV2{VectorOrdinal: 1, PartitionID: 0, NodeID: 2})
		},
		"manifest_representative_spoofed_digest": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Manifest.Representatives = append([]collections.VectorPartitionRepresentativeV2(nil), c.Vector.Manifest.Representatives...)
			c.Vector.Manifest.Representatives[0].NodeID++
		},
		"request_stats_mode":           func(c *FixedPeerTCPConfigV1) { c.Vector.RequestBase.StatsMode = "invalid" },
		"collection_incarnation":       func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.CollectionIncarnation = 0 },
		"index_epoch":                  func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.IndexEpoch = 0 },
		"malformed_index_digest":       func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.IndexDefinitionDigest = "invalid" },
		"malformed_catalog_digest":     func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.CatalogDigest = "invalid" },
		"zero_source_generation":       func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.Generation = 0 },
		"zero_source_checksum":         func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.Checksum = 0 },
		"zero_source_schema":           func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.SchemaHash = 0 },
		"zero_source_rows":             func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.RowCount = 0 },
		"binding_identity_digest":      func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.IndexDefinitionDigest = strings.Repeat("b", 64) },
		"binding_identity_generation":  func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.Generation++ },
		"binding_identity_checksum":    func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.Checksum++ },
		"binding_identity_schema":      func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.SchemaHash++ },
		"binding_identity_rows":        func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Source.RowCount++ },
		"binding_placement_digest":     func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.IndexDefinitionDigest = strings.Repeat("b", 64) },
		"binding_placement_generation": func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.SourceGeneration++ },
		"binding_placement_checksum":   func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.SourceChecksum++ },
		"binding_placement_schema":     func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.SourceSchemaHash++ },
		"binding_placement_rows":       func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.SourceRowCount++ },
		"binding_placement_count": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Placement.PartitionCount++
			c.Vector.Placement.Partitions = append(c.Vector.Placement.Partitions, raftplacement.VectorPartitionGroupV1{PartitionID: 1, GroupID: "group-b"})
		},
		"binding_partition_id": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Manifest.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 1, GroupID: "group-b"}}
		},
		"binding_partition_group": func(c *FixedPeerTCPConfigV1) {
			c.Vector.Manifest.Placements = []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}}
		},
		"catalog_epoch":  func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.CatalogEpoch = 0 },
		"catalog_digest": func(c *FixedPeerTCPConfigV1) { c.Vector.Identity.Index.CatalogDigest = "" },
		"catalog":        func(c *FixedPeerTCPConfigV1) { c.Vector.Catalog = raftplacement.CatalogV1{} },
		"placement":      func(c *FixedPeerTCPConfigV1) { c.Vector.Placement.PartitionCount++ },
		"unknown_owner":  func(c *FixedPeerTCPConfigV1) { c.Vector.Catalog.Groups = c.Vector.Catalog.Groups[:1] },
		"shard_rpc":      func(c *FixedPeerTCPConfigV1) { c.Vector.ShardAddresses["group-b"]["owner-1"] = c.Nodes[0].Address },
		"shard_catalog_raft": func(c *FixedPeerTCPConfigV1) {
			c.Vector.ShardAddresses["group-b"]["owner-1"] = c.Catalog.Peers[0].Address
		},
		"shard_data_raft": func(c *FixedPeerTCPConfigV1) {
			c.Vector.ShardAddresses["group-b"]["owner-1"] = c.Groups[1].Peers[0].Address
		},
		"shard_public": func(c *FixedPeerTCPConfigV1) {
			c.Vector.ShardAddresses["group-b"]["owner-1"] = c.Vector.PublicAddresses["owner-1"]
		},
		"shard_duplicate": func(c *FixedPeerTCPConfigV1) {
			c.Vector.ShardAddresses["group-b"]["owner-1"] = c.Vector.ShardAddresses["group-b"]["owner-2"]
		},
		"public_duplicate": func(c *FixedPeerTCPConfigV1) {
			c.Vector.PublicAddresses["owner-1"] = c.Vector.PublicAddresses["owner-2"]
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := fixedPeerVectorTestConfigsV1(t, seed)[0]
			if _, _, err := validateFixedPeerConfigV1(config); err != nil {
				t.Fatalf("valid baseline config rejected: %v", err)
			}
			mutate(&config)
			if strings.HasPrefix(name, "binding_") {
				resolved, err := raftplacement.Validate(config.Vector.Catalog)
				if err != nil {
					t.Fatal(err)
				}
				if err := resolved.ValidateVectorPartitionPlacementV1(config.Vector.Placement); err != nil {
					t.Fatalf("binding test has invalid placement shape: %v", err)
				}
				owners, ready, err := fixedPeerVectorLifecycleSpecV1(config.Vector)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := raftplacement.VectorPartitionLifecycleReadySetDigestV1(config.Vector.Identity, owners, []raftplacement.VectorPartitionLifecycleGroupReadyV1{ready}); err != nil {
					t.Fatalf("binding test has invalid lifecycle shape: %v", err)
				}
			}
			if runtime, err := fixedPeerOpenTestRuntimeV1(t, config); !errors.Is(err, raftcluster.ErrInvalidConfig) {
				if runtime != nil {
					_ = runtime.Close()
				}
				t.Fatalf("invalid config error=%v", err)
			}
			for _, path := range []string{config.DataRoot, config.RaftRoot} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("invalid open created %s: %v", path, err)
				}
			}
		})
	}
	t.Run("catalog_owner_hint_need_not_be_bootstrap", func(t *testing.T) {
		config := fixedPeerVectorTestConfigsV1(t, seed)[0]
		config.Vector.Catalog.Groups = append([]raftplacement.GroupV1(nil), config.Vector.Catalog.Groups...)
		config.Vector.Catalog.Groups[1].LeaderHint = "owner-2"
		bindCatalog(&config)
		if _, _, err := validateFixedPeerConfigV1(config); err != nil {
			t.Fatalf("non-bootstrap member leader hint rejected: %v", err)
		}
	})
	t.Run("overwritten_fields_are_not_defaults", func(t *testing.T) {
		config := fixedPeerVectorTestConfigsV1(t, seed)[0]
		base := config.Vector.RequestBase
		config.Vector.RequestBase = VectorPartitionCoordinatorRequestV1{
			RequestID: base.RequestID, CancellationID: base.CancellationID,
			Database: base.Database, Catalog: base.Catalog, Collection: base.Collection, IndexDefinitionDigest: base.IndexDefinitionDigest,
			RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterScoreBudget: 1, StatsMode: base.StatsMode,
			IndexName: strings.Repeat("ignored", limits.MaxIdentityBytes),
		}
		if _, _, err := validateFixedPeerConfigV1(config); err != nil {
			t.Fatalf("valid retained defaults rejected: %v", err)
		}
	})
}

func TestFixedPeerVectorLocalDataGroupValidationPrecedesDiskCreationV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	lifecycleFeature := raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]}
	config.Catalog.Features.Required = append(config.Catalog.Features.Required, lifecycleFeature)
	for i := range config.Catalog.Peers {
		config.Catalog.Peers[i].Capabilities = config.Catalog.Features
	}
	ref := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}
	config.Vector = &FixedPeerTCPVectorConfigV1{
		Collection: ref,
		Manifest: collections.VectorPartitionManifestV1{
			State: "ready", Collection: "docs", IndexName: "embedding", Generation: 1, IntegrityDigest: "integrity",
		},
		Placement:       raftplacement.VectorPartitionPlacementRecordV1{Collection: ref, IndexName: "embedding", PartitionGeneration: 1, Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-b"}}},
		Identity:        raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{Collection: ref, IndexName: "embedding", CatalogEpoch: 1, CatalogDigest: "catalog"}, Generation: 1},
		PublicAddresses: map[raftcluster.NodeID]string{"ingress": "127.0.0.1:21001", "owner-1": "127.0.0.1:21002", "owner-2": "127.0.0.1:21003"},
		ShardAddresses: map[raftcluster.GroupID]map[raftcluster.NodeID]string{
			"group-b": {"owner-1": "127.0.0.1:22001", "owner-2": "127.0.0.1:22002"},
		},
		IndexedThrough: 1,
	}
	reserve := func() net.Listener {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		return listener
	}
	raftListener, shardListener := reserve(), reserve()
	for i := range config.Groups {
		if config.Groups[i].ID == "group-b" {
			config.Groups[i].Peers = append(config.Groups[i].Peers, raftcluster.Peer{ID: config.NodeID, Address: raftListener.Addr().String()})
		}
	}
	config.RaftListen["group-b"] = raftListener.Addr().String()
	config.Vector.ShardAddresses["group-b"][config.NodeID] = shardListener.Addr().String()
	root := t.TempDir()
	config.DataRoot = filepath.Join(root, "data")
	config.RaftRoot = filepath.Join(root, "raft")

	if runtime, err := fixedPeerOpenTestRuntimeV1(t, config); err == nil || !errors.Is(err, raftcluster.ErrInvalidConfig) || !strings.Contains(err.Error(), "requires exactly one local data group") {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatalf("open validation error=%v", err)
	}
	for _, path := range []string{config.DataRoot, config.RaftRoot} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid open created %s: %v", path, err)
		}
	}
}

func TestFixedPeerVectorInsertEntryUsesCallerAttemptIdentityV1(t *testing.T) {
	request := VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7},
		IdempotencyKey: []byte("attempt-1"), ID: []byte("doc"), Vector: []float32{1}, Document: []byte(`{"embedding":[1]}`),
	}, Identity: raftplacement.VectorPartitionLifecycleIdentityV1{Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{IndexName: "embedding"}, Generation: 7}}
	first, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil {
		t.Fatal(err)
	}
	replay, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil || !bytes.Equal(first, replay) {
		t.Fatalf("same attempt changed entry: err=%v", err)
	}
	request.Request.IdempotencyKey = []byte("attempt-2")
	next, _, err := fixedPeerVectorInsertEntryV1("docs", collections.DocumentFormatJSON, 1, request)
	if err != nil || bytes.Equal(first, next) {
		t.Fatalf("new attempt reused entry: err=%v", err)
	}
}

func TestFixedPeerVectorOwnerRevalidatesPublicInsertLimitsV1(t *testing.T) {
	request := VectorPartitionRoutedInsertV1{Request: public.InsertRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: "embedding", Generation: 7},
		IdempotencyKey: make([]byte, raftentry.MaxIdempotencyKeyBytesV1), ID: make([]byte, public.MaxStableIDBytesV1),
		Vector: []float32{1}, Document: []byte(`{"embedding":[1]}`),
	}, CatalogProof: raftplacement.CatalogProofV1{Epoch: 1, Digest: "catalog"}, ReadySetDigest: "ready", RouterModelDigest: "router"}
	runtime := &FixedPeerTCPRuntimeV1{}
	if err := runtime.validateVectorInsertOwnerV1(t.Context(), request); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("boundary request error=%v", err)
	}
	request.Request.ID = make([]byte, public.MaxStableIDBytesV1+1)
	if err := runtime.validateVectorInsertOwnerV1(t.Context(), request); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
		t.Fatalf("oversized stable ID error=%v", err)
	}
	request.Request.ID = []byte("doc")
	request.Request.IdempotencyKey = make([]byte, raftentry.MaxIdempotencyKeyBytesV1+1)
	if err := runtime.validateVectorInsertOwnerV1(t.Context(), request); !hasPublicVectorErrorCodeV1(err, public.ErrorInvalidRequestV1) {
		t.Fatalf("oversized idempotency key error=%v", err)
	}
}
