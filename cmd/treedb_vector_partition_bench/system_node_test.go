package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestVectorPartitionSystemNodeRejectsM8LoopbackAssemblyV1(t *testing.T) {
	root := t.TempDir()
	config := vectorPartitionSystemNodeConfigV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: "m8_loopback", Topology: "single_daemon_four_group", NodeID: "node-0",
		DatasetDirectory: root, DatabaseDirectory: root, StateDirectory: root, ReadyPath: filepath.Join(root, "ready.json"), PublicListen: "127.0.0.1:1",
		LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-a", Listen: "127.0.0.1:2"}}, Endpoints: map[string]string{"group-a": "127.0.0.1:2"},
	}
	path := filepath.Join(root, "config.json")
	writeVectorPartitionSystemJSONTestV1(t, path, config)
	if _, err := loadVectorPartitionSystemNodeConfigV1(path); err == nil || !strings.Contains(err.Error(), "non-production assembly") {
		t.Fatalf("M8 loopback assembly error = %v", err)
	}
}

func TestVectorPartitionSystemConfigLoadDoesNotCreateStateDirectoryV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "missing-state")
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	endpoints := make(map[string]string, len(groups))
	local := make([]vectorPartitionSystemLocalGroupV1, 0, len(groups))
	for index, group := range groups {
		endpoint := fmt.Sprintf("127.0.0.1:%d", 22000+index)
		endpoints[group] = endpoint
		local = append(local, vectorPartitionSystemLocalGroupV1{GroupID: group, Listen: endpoint})
	}
	config := vectorPartitionSystemNodeConfigV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
		Topology: "single_daemon_four_group", NodeID: "single", DatasetDirectory: filepath.Join(root, "dataset"),
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: state, CapabilityKeyPath: writeVectorPartitionSystemCapabilityKeyTestV1(t, root), PublicListen: "127.0.0.1:22004",
		ReadyPath: filepath.Join(state, "ready.json"), ProfileDirectory: filepath.Join(state, "profiles"), LocalGroups: local, Endpoints: endpoints,
		RuntimeOwnership: &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "2,0-1", GoMemoryLimitBytes: 1 << 30},
	}
	path := filepath.Join(root, "config.json")
	writeVectorPartitionSystemJSONTestV1(t, path, config)
	loaded, err := loadVectorPartitionSystemNodeConfigV1(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RuntimeOwnership == nil || loaded.RuntimeOwnership.CPUSet != "0-2" || loaded.RuntimeOwnership.GOMAXPROCS != 3 {
		t.Fatalf("canonical runtime ownership = %+v", loaded.RuntimeOwnership)
	}
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state directory created during config load: %v", err)
	}
	config.ProfileDirectory = filepath.Join(root, "profiles-outside-state")
	outsidePath := filepath.Join(root, "outside-profile-config.json")
	writeVectorPartitionSystemJSONTestV1(t, outsidePath, config)
	if _, err := loadVectorPartitionSystemNodeConfigV1(outsidePath); err == nil || !strings.Contains(err.Error(), "profile directory must be inside") {
		t.Fatalf("outside profile directory error = %v", err)
	}
}

func TestVectorPartitionSystemServerClosesIdleConnectionV1(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &vectorPartitionOperationsTCPServerV1{listener: listener, done: make(chan struct{}), idleTimeout: 20 * time.Millisecond}
	server.start(t.Context())
	defer server.Close()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("idle system connection remained open")
	}
}

func TestVectorPartitionSystemServerClosesBlockedWriterV1(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	server := &vectorPartitionOperationsTCPServerV1{idleTimeout: 20 * time.Millisecond}
	done := make(chan struct{})
	go func() {
		server.serve(t.Context(), serverConn)
		close(done)
	}()
	if err := writeVectorPartitionSystemFrameV1(clientConn, vectorPartitionOperationsWireRequestV1{SchemaVersion: 2}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked system response occupied its connection slot")
	}
}

func TestVectorPartitionSystemClientPreservesWireTimeoutV1(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		var request vectorPartitionOperationsWireRequestV1
		if err := readVectorPartitionSystemFrameV1(server, &request); err != nil {
			done <- err
			return
		}
		done <- writeVectorPartitionSystemFrameV1(server, vectorPartitionOperationsWireResponseV1{
			SchemaVersion: 1, ErrorCode: public.ErrorDeadlineExceededV1, Error: "shard deadline exceeded",
		})
	}()
	wireClient := &vectorPartitionOperationsTCPClientV1{conn: client, r: bufio.NewReader(client)}
	_, timing, err := wireClient.callWithTiming(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search"})
	var typed *public.ErrorV1
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &typed) || typed.Code != public.ErrorDeadlineExceededV1 {
		t.Fatalf("wire timeout = %v", err)
	}
	if timing.EncodeNanos == 0 || timing.WriteNanos == 0 || timing.ReadNanos == 0 || timing.DecodeNanos == 0 || timing.TotalNanos == 0 || timing.RequestBytes == 0 || timing.ResponseBytes == 0 {
		t.Fatalf("wire timing = %+v", timing)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionSystemNodeProcessHelperV1(t *testing.T) {
	config := os.Getenv("GOMAP_SYSTEM_NODE_TEST_CONFIG")
	if config == "" {
		return
	}
	if err := runVectorPartitionSystemNodeV1([]string{"-config", config}, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func TestVectorPartitionSystemNodeSingleDaemonUsesProductionPublicRouteV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	dataset, err := m8CanonicalPathV1(writeFixtureForTest(t, 64, 8, 8))
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	vectors, queries64 := fixtureData(fixture)
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, groups, 16)
	if err != nil {
		t.Fatal(err)
	}
	database := assets.dir
	database, err = m8CanonicalPathV1(database)
	if err != nil {
		t.Fatal(err)
	}
	assets.owned = false
	if err := assets.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(database)
	state, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", state)
	endpoints := make(map[string]string, len(groups))
	local := make([]vectorPartitionSystemLocalGroupV1, 0, len(groups))
	for _, group := range groups {
		address := reserveVectorPartitionSystemTCPAddressTestV1(t)
		endpoints[group] = address
		local = append(local, vectorPartitionSystemLocalGroupV1{GroupID: group, Listen: address})
	}
	config := vectorPartitionSystemNodeConfigV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
		Topology: "single_daemon_four_group", NodeID: "single-0", DatasetDirectory: dataset, DatabaseDirectory: database,
		StateDirectory: state, CapabilityKeyPath: writeVectorPartitionSystemCapabilityKeyTestV1(t, t.TempDir()), PublicListen: "127.0.0.1:0", ReadyPath: filepath.Join(state, "ready.json"), ProfileDirectory: filepath.Join(state, "profiles"), LocalGroups: local, Endpoints: endpoints,
		GroupAppliedIndexes: map[string]uint64{"group-a": 1, "group-b": 1, "group-c": 1, "group-d": 1},
	}
	node, err := openVectorPartitionSystemNodeV1(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(config)
	if err != nil {
		t.Fatal(err)
	}
	if node.ready.NodeConfigSHA256 != configSHA {
		t.Fatalf("ready node config SHA = %q, want %q", node.ready.NodeConfigSHA256, configSHA)
	}
	if node.ready.PublicRoute != "vectorpartition.OperationsV1.Search" || !node.ready.ProductionTopology || node.ready.M8Loopback || node.ready.LogicalCPUs < 1 || node.ready.GOMAXPROCS < 1 || node.ready.GoMemoryLimit < 1 || len(node.ready.Groups) != 4 {
		t.Fatalf("ready evidence = %+v", node.ready)
	}
	for _, group := range node.ready.Groups {
		if !group.ProvesProductionConsensus || group.AppliedIndex == 0 || group.LeaderID == "" {
			t.Fatalf("group evidence = %+v", group)
		}
	}
	client, err := dialVectorPartitionOperationsV1(t.Context(), node.ready.PublicEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	query := make([]float32, len(vectors[0]))
	for i, value := range vectors[0] {
		query[i] = float32(value)
	}
	searchRequest := public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: assets.manifest.IndexName, Generation: assets.manifest.Generation}, Query: query,
		Metric: public.MetricCosineV1, TopK: 10, Probes: 2, EfSearch: 128, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 2560}, Deadline: time.Now().Add(30 * time.Second),
	}
	response, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: searchRequest})
	if err != nil {
		t.Fatal(err)
	}
	if response.Search == nil || len(response.Search.Neighbors) == 0 || len(response.Search.Neighbors) > 10 || response.Search.Counters.SelectedPartitions != 2 || response.Search.Counters.SelectedGroups == 0 || response.Search.Counters.RPCs == 0 || response.Search.Counters.RequestBytes == 0 || response.Search.Counters.ResponseBytes == 0 {
		t.Fatalf("public production response = %+v", response.Search)
	}
	truth, err := m8ExactTruthFixtureV1(vectors, queries64, 4)
	if err != nil {
		t.Fatal(err)
	}
	queries := make([][]float32, len(queries64))
	for i := range queries64 {
		queries[i] = make([]float32, len(queries64[i]))
		for d := range queries64[i] {
			queries[i][d] = float32(queries64[i][d])
		}
	}
	topology := vectorPartitionSystemTopologyEvidenceV1{Endpoints: config.Endpoints, Nodes: []vectorPartitionSystemTopologyNodeV1{{NodeConfigSHA256: configSHA, LocalGroups: config.LocalGroups}}}
	snapshot := func(ctx context.Context) (map[string]vectorPartitionSystemNodeObservationV1, error) {
		return vectorPartitionSystemCatalogReadSnapshotV1(ctx, topology, nativewire.ProbeVectorPartitionShardEndpointV1)
	}
	cell, err := vectorPartitionSystemBenchCell(t.Context(), node.ready.PublicEndpoint, configSHA, queries, truth, 4, 4, 128, 2, len(queries), vectorPartitionSystemSearchStrictV1, 0, 0, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if cell.Status != "valid" || cell.Generation.Index == "" || cell.Generation.Generation == 0 || cell.ElapsedNanos == 0 || cell.Metrics.CompletedQueries != len(queries) || cell.Metrics.RecallAt10 <= 0 || cell.Counters["selected_partitions"] != uint64(4*len(queries)) || cell.Counters["selected_groups"] == 0 || cell.Counters["query_bytes"] == 0 || cell.Counters["response_bytes"] == 0 || cell.Counters["public_request_frame_bytes"] == 0 || cell.Counters["public_response_frame_bytes"] == 0 || cell.Counters["snapshot_pins"] != uint64(len(queries)) || cell.Counters["read_proofs"] != 0 || cell.Counters["generation_pins"] != 0 || cell.Counters["partition_opens"] != 0 || cell.Timings["total"] == 0 || cell.Timings["coordinator_total"] == 0 || cell.Timings["client_total"] == 0 || cell.Timings["rpc"] == 0 || cell.Timings["read_index_apply"] != 0 || cell.Timings["shard_search"] == 0 || cell.CatalogReads.Total.Total.Reads != uint64(len(queries)) || cell.CatalogReads.Total.StrictSearch.Reads != uint64(len(queries)) || cell.CatalogReads.Total.OperationsHealth.Reads != 0 || cell.CatalogReads.Total.CoordinatorLifecycle.Reads != 0 || cell.CatalogReads.Total.ShardLifecycle.Reads != 0 || cell.CatalogReads.Total.Total.LogBarriers != 0 || cell.CatalogReads.Total.Total.NoLogProofs != cell.CatalogReads.Total.Total.Reads || len(cell.TotalNanos) != len(queries) {
		t.Fatalf("system benchmark cell = %+v", cell)
	}
	for _, mode := range []string{vectorPartitionSystemSearchFastV1, vectorPartitionSystemSearchPinnedV1} {
		cell, err := vectorPartitionSystemBenchCell(t.Context(), node.ready.PublicEndpoint, configSHA, queries, truth, 4, 4, 128, 2, 0, mode, time.Hour, time.Minute, snapshot)
		if err != nil {
			t.Fatalf("%s system benchmark cell: %v", mode, err)
		}
		wantPins, wantSessions := uint64(len(queries)), uint64(0)
		if mode == vectorPartitionSystemSearchPinnedV1 {
			wantPins, wantSessions = 0, 2
		}
		if cell.FastEvidence == nil || cell.FastEvidence.IndexedThrough != uint64(len(vectors)) || cell.Counters["snapshot_pins"] != wantPins || cell.Counters["session_pins"] != wantSessions || cell.CatalogReads.Total.StrictSearch.Reads != 0 || cell.CatalogReads.Total.CoordinatorLifecycle.Reads != 0 || cell.CatalogReads.Total.ShardLifecycle.Reads != 0 || cell.Counters["read_proofs"] != 0 || cell.Counters["generation_pins"] != 0 || cell.Counters["partition_opens"] != 0 {
			t.Fatalf("%s system benchmark cell = %+v", mode, cell)
		}
	}
	pinOptions := public.PinSearchSnapshotOptionsV1{FastSearchOptionsV1: public.FastSearchOptionsV1{MaxIndexAge: time.Hour}, MaxSessionAge: time.Minute}
	if _, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "pin_search_snapshot", PinOptions: &pinOptions}); err != nil {
		t.Fatal(err)
	}
	deniedID, overlayDigest := response.Search.Neighbors[0].ID, strings.Repeat("a", 64)
	token, err := node.production.PublishSearchSnapshotV1(t.Context(), uint64(len(vectors)), overlayDigest, []string{deniedID})
	if err != nil || token.Sequence != uint64(len(vectors)) {
		t.Fatalf("publish search snapshot token=%+v err=%v", token, err)
	}
	if _, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search_pinned", Search: searchRequest}); err == nil {
		t.Fatal("older pinned snapshot ignored the current authorization overlay")
	}
	if _, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "close_pinned_snapshot"}); err != nil {
		t.Fatal(err)
	}
	fastOptions := public.FastSearchOptionsV1{MaxIndexAge: time.Hour, MinIndexedThrough: token.Sequence}
	filtered, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search_fast", Search: searchRequest, FastOptions: &fastOptions})
	if err != nil || filtered.Search == nil || filtered.FastEvidence == nil || filtered.FastEvidence.AuthorizationOverlayDigest != overlayDigest {
		t.Fatalf("filtered fast search=%+v err=%v", filtered, err)
	}
	for _, neighbor := range filtered.Search.Neighbors {
		if neighbor.ID == deniedID {
			t.Fatalf("revoked document %q remained visible", deniedID)
		}
	}
	node.publicServer.nodeConfigSHA256 = strings.Repeat("f", 64)
	if _, err := vectorPartitionSystemBenchCell(t.Context(), node.ready.PublicEndpoint, configSHA, queries, truth, 4, 4, 128, 2, 0, vectorPartitionSystemSearchStrictV1, 0, 0, snapshot); err == nil || !strings.Contains(err.Error(), "live node config identity does not match checked topology") {
		t.Fatalf("mismatched live topology error = %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(state, "node.json")
	writeVectorPartitionSystemJSONTestV1(t, configPath, config)
	process := startVectorPartitionSystemNodeProcessTestV1(t, configPath, state)
	defer process.stop()
	waitVectorPartitionSystemReadyTestV1(t, config.ReadyPath, process)
	var subprocessReady vectorPartitionSystemNodeReadyV1
	raw, err := os.ReadFile(config.ReadyPath)
	if err != nil || json.Unmarshal(raw, &subprocessReady) != nil {
		t.Fatalf("read subprocess ready: %v", err)
	}
	repeatedQueries, repeatedTruth := make([][]float32, 1000), make([][]m8CanonicalResultV1, 1000)
	for index := range repeatedQueries {
		repeatedQueries[index], repeatedTruth[index] = queries[index%len(queries)], truth[index%len(truth)]
	}
	cell, err = vectorPartitionSystemBenchCell(t.Context(), subprocessReady.PublicEndpoint, configSHA, repeatedQueries, repeatedTruth, 4, 2, 128, 1, len(repeatedQueries), vectorPartitionSystemSearchStrictV1, 0, 0, snapshot)
	if err != nil || cell.CatalogReads.Total.StrictSearch.Reads != uint64(len(repeatedQueries)) {
		t.Fatalf("subprocess system benchmark cell = %+v, %v\nstdout:\n%s\nstderr:\n%s", cell, err, process.stdout.String(), process.stderr.String())
	}
}

func TestVectorPartitionSystemCatalogAndRuntimeDeltaV1(t *testing.T) {
	stage := func(reads uint64) nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1 {
		return nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1{Reads: reads, Successes: reads, VerifyLeaderCalls: reads, NoLogProofs: reads, TotalNanos: reads * 10, AdmissionNanos: reads, VerifyLeaderNanos: reads, CurrentTermNanos: reads, RaftApplyNanos: reads, AppliedReadNanos: reads}
	}
	before := map[string]vectorPartitionSystemNodeObservationV1{"node": {
		Catalog: nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1{LastTerm: 1, LastCatalogApplied: 2, LastRaftApplied: 5, LastRaftLog: 5},
		Runtime: nativewire.VectorPartitionProcessRuntimeStatsV1{SampleUnixNano: 1, CPUTimeNanos: 10, RunQueueDelayNanos: 10, Timeslices: 10, TotalAllocBytes: 10, Mallocs: 1, Goroutines: 1},
	}}
	afterCatalog := nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1{
		Total: stage(3), StrictSearch: stage(2), ServingRefresh: stage(1),
		LastTerm: 1, LastCatalogApplied: 2, LastRaftApplied: 12, LastRaftLog: 12,
	}
	after := map[string]vectorPartitionSystemNodeObservationV1{"node": {
		Catalog: afterCatalog,
		Runtime: nativewire.VectorPartitionProcessRuntimeStatsV1{SampleUnixNano: 2, CPUTimeNanos: 20, RunQueueDelayNanos: 0, Timeslices: 0, TotalAllocBytes: 20, Mallocs: 2, Goroutines: 1},
	}}
	reads, runtimeStats, err := vectorPartitionSystemCatalogReadDeltaV1(before, after, 2)
	if err != nil || reads.Total.Total.Reads != 3 || reads.Total.StrictSearch.Reads != 2 || reads.Total.ServingRefresh.Reads != 1 || len(runtimeStats) != 1 || runtimeStats[0].After.CPUTimeNanos != 20 {
		t.Fatalf("catalog/runtime delta reads=%+v runtime=%+v err=%v", reads, runtimeStats, err)
	}
	badCatalog := afterCatalog
	badCatalog.ServingRefresh.LogBarriers = 1
	badCatalog.ServingRefresh.NoLogProofs = 0
	bad := map[string]vectorPartitionSystemNodeObservationV1{"node": {Catalog: badCatalog, Runtime: after["node"].Runtime}}
	if _, _, err := vectorPartitionSystemCatalogReadDeltaV1(before, bad, 2); err == nil || !strings.Contains(err.Error(), "stage evidence is malformed") {
		t.Fatalf("invalid serving refresh evidence error = %v", err)
	}
	missingLog := after
	node := missingLog["node"]
	node.Catalog.LastRaftLog = 11
	missingLog["node"] = node
	if _, _, err := vectorPartitionSystemCatalogReadDeltaV1(before, missingLog, 2); err == nil || !strings.Contains(err.Error(), "non-monotonic or lack proof") {
		t.Fatalf("invalid no-log proof evidence error = %v", err)
	}
}

func TestVectorPartitionSystemPeakRSSV1(t *testing.T) {
	var peak vectorPartitionSystemPeakRSSV1
	if peak.observe(10) != 10 || peak.observe(9) != 10 || peak.observe(11) != 11 {
		t.Fatal("peak RSS must be monotonic")
	}
}

func TestVectorPartitionSystemReadyPublicationNoReplaceV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ready.json")
	if err := os.WriteFile(path, []byte("sentinel"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := publishVectorPartitionSystemNodeReadyV1(path, vectorPartitionSystemNodeReadyV1{SchemaVersion: 1}); err == nil {
		t.Fatal("ready publication replaced existing evidence")
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "sentinel" {
		t.Fatalf("sentinel = %q, %v", raw, err)
	}
}

func TestVectorPartitionSystemTopologyRequiresDistinctProductionRootsV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	endpoints := map[string]string{}
	applied := map[string]uint64{}
	capabilityKey := writeVectorPartitionSystemCapabilityKeyTestV1(t, root)
	for index, group := range groups {
		endpoints[group] = fmt.Sprintf("127.0.0.1:%d", 21000+index)
		applied[group] = 1
	}
	configs := make([]vectorPartitionSystemNodeConfigV1, len(groups))
	for index, group := range groups {
		configs[index] = vectorPartitionSystemNodeConfigV1{
			SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
			Topology: "native_four_daemon_four_group", NodeID: "node-" + group, DatasetDirectory: filepath.Join(root, "dataset"),
			DatabaseDirectory: filepath.Join(root, "db-"+group), StateDirectory: filepath.Join(root, "state-"+group), CapabilityKeyPath: capabilityKey,
			ReadyPath: filepath.Join(root, "state-"+group, "ready.json"), LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: group, Listen: endpoints[group]}},
			Endpoints: endpoints, GroupAppliedIndexes: applied,
			RuntimeOwnership: &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: fmt.Sprintf("%d-%d", index*3, index*3+2), GOMAXPROCS: 3, GoMemoryLimitBytes: 6 << 30},
		}
	}
	configs[0].PublicListen = "127.0.0.1:22000"
	configs[0].ProfileDirectory = filepath.Join(configs[0].StateDirectory, "profiles")
	evidence, err := validateVectorPartitionSystemTopologyV1(configs)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.TopologyIdentitySHA256 == "" || evidence.PublicRoute != "vectorpartition.OperationsV1.Search" || evidence.M8Loopback || len(evidence.Nodes) != 4 {
		t.Fatalf("topology evidence = %+v", evidence)
	}
	for index, node := range evidence.Nodes {
		configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(configs[index])
		if err != nil || node.NodeConfigSHA256 != configSHA || node.ProfileDirectory != configs[index].ProfileDirectory {
			t.Fatalf("topology node config SHA = %q, want %q, err=%v", node.NodeConfigSHA256, configSHA, err)
		}
	}
	containerConfigs := append([]vectorPartitionSystemNodeConfigV1(nil), configs...)
	for index := range containerConfigs {
		containerConfigs[index].Topology = "container_four_daemon_four_group"
	}
	if _, err := validateVectorPartitionSystemTopologyV1(containerConfigs); err != nil {
		t.Fatalf("container topology with unique mount destinations: %v", err)
	}
	savedOwnership := cloneVectorPartitionSystemRuntimeOwnershipV1(configs[0].RuntimeOwnership)
	configs[0].RuntimeOwnership = nil
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "requires explicit runtime ownership") {
		t.Fatalf("missing runtime ownership error = %v", err)
	}
	configs[0].RuntimeOwnership = &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "3-5", GOMAXPROCS: 3, GoMemoryLimitBytes: 6 << 30}
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "owned by multiple nodes") {
		t.Fatalf("overlapping runtime ownership error = %v", err)
	}
	configs[0].RuntimeOwnership = &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "1024", GOMAXPROCS: 1, GoMemoryLimitBytes: 6 << 30}
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("out-of-range runtime ownership error = %v", err)
	}
	configs[0].RuntimeOwnership = &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "0-2", GOMAXPROCS: 4, GoMemoryLimitBytes: 6 << 30}
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "GOMAXPROCS") {
		t.Fatalf("overcommitted runtime ownership error = %v", err)
	}
	configs[0].RuntimeOwnership = savedOwnership
	var configPaths []string
	for index, config := range configs {
		path := filepath.Join(root, fmt.Sprintf("node-%d.json", index))
		writeVectorPartitionSystemJSONTestV1(t, path, config)
		configPaths = append(configPaths, path)
	}
	out := filepath.Join(root, "topology.json")
	if err := runVectorPartitionSystemCheckTopologyV1([]string{"-configs", strings.Join(configPaths, ","), "-out", out}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(out); err != nil || !bytes.Contains(raw, []byte(evidence.TopologyIdentitySHA256)) {
		t.Fatalf("published topology evidence = %q, %v", raw, err)
	}
	if err := runVectorPartitionSystemCheckTopologyV1([]string{"-configs", strings.Join(configPaths, ","), "-out", out}, io.Discard); err == nil {
		t.Fatal("topology evidence publication replaced an existing artifact")
	}
	configs[0].PublicListen = "0.0.0.0:21001"
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "listener") {
		t.Fatalf("wildcard listener collision error = %v", err)
	}
	localhost, err := net.LookupIP("localhost")
	if err != nil || len(localhost) == 0 {
		t.Fatalf("resolve localhost = %v, %v", localhost, err)
	}
	configs[0].PublicListen = net.JoinHostPort("localhost", "21001")
	configs[1].LocalGroups[0].Listen = net.JoinHostPort(localhost[0].String(), "21001")
	endpoints["group-b"] = configs[1].LocalGroups[0].Listen
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "listener") {
		t.Fatalf("resolved listener alias collision error = %v", err)
	}
	configs[1].LocalGroups[0].Listen = "127.0.0.1:21001"
	endpoints["group-b"] = configs[1].LocalGroups[0].Listen
	configs[0].PublicListen = "127.0.0.1:22000"
	for _, invalid := range []string{"malformed", "127.0.0.1:0"} {
		configs[0].PublicListen = invalid
		if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "listener") || !strings.Contains(err.Error(), "invalid") {
			t.Fatalf("invalid listener %q error = %v", invalid, err)
		}
	}
	configs[0].PublicListen = "127.0.0.1:22000"
	configs[1].DatabaseDirectory = configs[0].DatabaseDirectory
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "persistent roots must be distinct") {
		t.Fatalf("duplicate database root error = %v", err)
	}
	configs[1].DatabaseDirectory = filepath.Join(root, "db-group-b")
	configs[1].StateDirectory = filepath.Join(configs[1].DatabaseDirectory, "state")
	configs[1].ReadyPath = filepath.Join(configs[1].StateDirectory, "ready.json")
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "persistent roots must be disjoint") {
		t.Fatalf("nested persistent root error = %v", err)
	}
	configs[1].StateDirectory = filepath.Join(root, "state-group-b")
	configs[1].ReadyPath = filepath.Join(configs[1].StateDirectory, "ready.json")
	configs[1].Assembly = "m8_loopback"
	if _, err := validateVectorPartitionSystemTopologyV1(configs); err == nil || !strings.Contains(err.Error(), "production identity") {
		t.Fatalf("M8 topology error = %v", err)
	}
}

func TestVectorPartitionSystemBenchPersistsFailedCellV1(t *testing.T) {
	dataset, cache := writeFixtureForTest(t, 10, 2, 2), t.TempDir()
	var truthOut strings.Builder
	if err := run([]string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-top-k", "10", "-seed", "1", "-max-vectors", "10", "-max-fixture-bytes", fmt.Sprint(maxFixtureBytes), "-max-exact-truth-visits", "20"}, &truthOut); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(truthOut.String())
	if len(fields) < 2 {
		t.Fatalf("truth output = %q", truthOut.String())
	}
	out := filepath.Join(t.TempDir(), "failed.json")
	topology, closeEndpoints := writeVectorPartitionSystemTopologyWithLiveEndpointsTestV1(t, "127.0.0.1:1", dataset)
	defer closeEndpoints()
	args := []string{"-endpoint", "127.0.0.1:1", "-topology", topology, "-dataset", dataset, "-truth-cache", cache, "-truth-cache-sha256", strings.TrimPrefix(fields[1], "artifact_sha256="), "-probes", "2", "-concurrency", "1", "-out", out, "-top-k", "10", "-ef-search", "128", "-warmup", "0"}
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int, string, time.Duration, time.Duration, vectorPartitionSystemCatalogSnapshotV1) (vectorPartitionSystemBenchCellV1, error) {
		return vectorPartitionSystemBenchCellV1{Status: "valid", Budget: map[string]int{"probes": 2}, Concurrency: 1, Metrics: vectorPartitionSystemBenchMetricsV1{Queries: 2}}, context.DeadlineExceeded
	}
	if err := runVectorPartitionSystemBenchWithCellV1(args, io.Discard, runCell, nativewire.ProbeVectorPartitionShardEndpointV1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("system bench failure = %v", err)
	}
	var result vectorPartitionSystemBenchResultV1
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Cells) != 1 || result.Cells[0].Status != "failed" || result.Cells[0].Error == "" || result.Cells[0].Metrics.Errors != 1 || result.Cells[0].Metrics.Timeouts != 1 || !result.CompletedAt.After(result.StartedAt) {
		t.Fatalf("retained failed result = %+v", result)
	}
	if result.Topology != "single_daemon_four_group" || result.TopologyIdentitySHA256 == "" {
		t.Fatalf("retained topology identity = %+v", result)
	}
}

func TestVectorPartitionSystemBenchRejectsShardIdentityDriftAroundCellV1(t *testing.T) {
	dataset, cache := writeFixtureForTest(t, 10, 2, 2), t.TempDir()
	var truthOut strings.Builder
	if err := run([]string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-top-k", "10", "-seed", "1", "-max-vectors", "10", "-max-fixture-bytes", fmt.Sprint(maxFixtureBytes), "-max-exact-truth-visits", "20"}, &truthOut); err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(truthOut.String())
	if len(fields) < 2 {
		t.Fatalf("truth output = %q", truthOut.String())
	}
	topologyPath, closeEndpoints := writeVectorPartitionSystemTopologyWithLiveEndpointsTestV1(t, "127.0.0.1:1", dataset)
	defer closeEndpoints()
	topology, err := loadVectorPartitionSystemTopologyEvidenceV1(topologyPath, "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	drifted, probes := false, 0
	probe := func(_ context.Context, endpoint string) (nativewire.VectorPartitionShardEndpointIdentityV1, error) {
		probes++
		for _, node := range topology.Nodes {
			for _, group := range node.LocalGroups {
				if group.Listen == endpoint {
					identity := node.NodeConfigSHA256
					if drifted && group.GroupID == "group-a" {
						identity = strings.Repeat("f", 64)
					}
					return nativewire.VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: group.GroupID, InstanceIdentity: identity}, nil
				}
			}
		}
		return nativewire.VectorPartitionShardEndpointIdentityV1{}, errors.New("unexpected endpoint")
	}
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int, string, time.Duration, time.Duration, vectorPartitionSystemCatalogSnapshotV1) (vectorPartitionSystemBenchCellV1, error) {
		drifted = true
		return vectorPartitionSystemBenchCellV1{Status: "valid", Budget: map[string]int{"probes": 2}, Concurrency: 1}, nil
	}
	out := filepath.Join(t.TempDir(), "drifted.json")
	args := []string{"-endpoint", "127.0.0.1:1", "-topology", topologyPath, "-dataset", dataset, "-truth-cache", cache, "-truth-cache-sha256", strings.TrimPrefix(fields[1], "artifact_sha256="), "-probes", "2", "-concurrency", "1", "-out", out, "-top-k", "10", "-ef-search", "128", "-warmup", "0"}
	if err := runVectorPartitionSystemBenchWithCellV1(args, io.Discard, runCell, probe); err == nil || !strings.Contains(err.Error(), "live topology after cell") {
		t.Fatalf("shard identity drift error = %v", err)
	}
	if probes != 9 {
		t.Fatalf("live identity probes = %d, want 9", probes)
	}
	var result vectorPartitionSystemBenchResultV1
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Cells) != 1 || result.Cells[0].Status != "failed" {
		t.Fatalf("retained drift result = %+v", result)
	}
}

func TestVectorPartitionSystemBenchRejectsChangedTopologyEvidenceV1(t *testing.T) {
	path := writeVectorPartitionSystemTopologyEvidenceTestV1(t, "127.0.0.1:19000", filepath.Join(t.TempDir(), "dataset"))
	if _, err := loadVectorPartitionSystemTopologyEvidenceV1(path, "localhost:19000"); err != nil {
		t.Fatal(err)
	}
	if _, err := loadVectorPartitionSystemTopologyEvidenceV1(path, "127.0.0.1:19001"); err == nil || !strings.Contains(err.Error(), "endpoint mismatch") {
		t.Fatalf("changed endpoint error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var evidence vectorPartitionSystemTopologyEvidenceV1
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	evidence.DatasetDirectory += "-changed"
	writeVectorPartitionSystemJSONTestV1(t, path, evidence)
	if _, err := loadVectorPartitionSystemTopologyEvidenceV1(path, "127.0.0.1:19000"); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("changed topology digest error = %v", err)
	}
}

func TestVectorPartitionSystemBenchRequiresEveryLiveNodeIdentityV1(t *testing.T) {
	topology := vectorPartitionSystemTopologyEvidenceV1{Nodes: []vectorPartitionSystemTopologyNodeV1{
		{NodeID: "node-a", NodeConfigSHA256: strings.Repeat("a", 64), LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-a", Listen: "127.0.0.1:1"}}},
		{NodeID: "node-b", NodeConfigSHA256: strings.Repeat("b", 64), LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-b", Listen: "127.0.0.1:2"}}},
	}, Endpoints: map[string]string{"group-a": "127.0.0.1:1", "group-b": "127.0.0.1:2"}}
	identities := map[string]nativewire.VectorPartitionShardEndpointIdentityV1{
		"127.0.0.1:1": {Version: 1, GroupID: "group-a", InstanceIdentity: strings.Repeat("a", 64)},
		"127.0.0.1:2": {Version: 1, GroupID: "group-b", InstanceIdentity: strings.Repeat("b", 64)},
	}
	probe := func(_ context.Context, endpoint string) (nativewire.VectorPartitionShardEndpointIdentityV1, error) {
		return identities[endpoint], nil
	}
	if err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(t.Context(), topology, probe); err != nil {
		t.Fatal(err)
	}
	identities["127.0.0.1:2"] = nativewire.VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: "group-b", InstanceIdentity: strings.Repeat("c", 64)}
	if err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(t.Context(), topology, probe); err == nil || !strings.Contains(err.Error(), "node-b") || !strings.Contains(err.Error(), "does not match checked topology") {
		t.Fatalf("mismatched peer identity error = %v", err)
	}
	identities["127.0.0.1:2"] = nativewire.VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: "group-b", InstanceIdentity: strings.Repeat("b", 64)}
	for _, test := range []struct {
		name   string
		groups []vectorPartitionSystemLocalGroupV1
	}{
		{name: "missing", groups: nil},
		{name: "duplicate", groups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-a", Listen: "127.0.0.1:1"}}},
		{name: "wrong_listener", groups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-b", Listen: "127.0.0.1:1"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := topology
			malformed.Nodes = append([]vectorPartitionSystemTopologyNodeV1(nil), topology.Nodes...)
			malformed.Nodes[1].LocalGroups = test.groups
			probes := 0
			err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(t.Context(), malformed, func(ctx context.Context, endpoint string) (nativewire.VectorPartitionShardEndpointIdentityV1, error) {
				probes++
				return probe(ctx, endpoint)
			})
			if err == nil || probes != 0 {
				t.Fatalf("malformed topology error=%v probes=%d", err, probes)
			}
		})
	}
}

func TestVectorPartitionSystemBenchRequiresEffectiveRuntimeOwnershipV1(t *testing.T) {
	ownership := &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "2-3", GOMAXPROCS: 2, GoMemoryLimitBytes: 1 << 30}
	topology := vectorPartitionSystemTopologyEvidenceV1{
		Nodes:     []vectorPartitionSystemTopologyNodeV1{{NodeID: "node-a", NodeConfigSHA256: strings.Repeat("a", 64), LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: "group-a", Listen: "127.0.0.1:1"}}, RuntimeOwnership: ownership}},
		Endpoints: map[string]string{"group-a": "127.0.0.1:1"},
	}
	identity := nativewire.VectorPartitionShardEndpointIdentityV1{
		Version: 1, GroupID: "group-a", InstanceIdentity: strings.Repeat("a", 64),
		ProcessRuntimeStats: nativewire.VectorPartitionProcessRuntimeStatsV1{LogicalCPUs: 4, GOMAXPROCS: 2, GoMemoryLimitBytes: 1 << 30, EffectiveCPUSet: "2-3"},
	}
	probe := func(context.Context, string) (nativewire.VectorPartitionShardEndpointIdentityV1, error) {
		return identity, nil
	}
	if _, err := vectorPartitionSystemCatalogReadSnapshotV1(t.Context(), topology, probe); err != nil {
		t.Fatal(err)
	}
	identity.ProcessRuntimeStats.GOMAXPROCS = 3
	if _, err := vectorPartitionSystemCatalogReadSnapshotV1(t.Context(), topology, probe); err == nil || !strings.Contains(err.Error(), "runtime ownership") {
		t.Fatalf("mismatched runtime ownership error = %v", err)
	}
}

func TestVectorPartitionSystemRuntimeOwnershipUnsupportedPlatformV1(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux supports runtime ownership")
	}
	err := applyVectorPartitionSystemRuntimeOwnershipV1(&vectorPartitionSystemRuntimeOwnershipV1{CPUSet: "0", GOMAXPROCS: 1, GoMemoryLimitBytes: 1 << 30})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported runtime ownership error = %v", err)
	}
}

func TestVectorPartitionSystemBenchRejectsMismatchedTopologyDatasetV1(t *testing.T) {
	dataset := writeFixtureForTest(t, 10, 2, 2)
	topology := writeVectorPartitionSystemTopologyEvidenceTestV1(t, "127.0.0.1:1", t.TempDir())
	called := false
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int, string, time.Duration, time.Duration, vectorPartitionSystemCatalogSnapshotV1) (vectorPartitionSystemBenchCellV1, error) {
		called = true
		return vectorPartitionSystemBenchCellV1{}, nil
	}
	args := []string{"-endpoint", "127.0.0.1:1", "-topology", topology, "-dataset", dataset, "-truth-cache", t.TempDir(), "-truth-cache-sha256", strings.Repeat("a", 64), "-probes", "2", "-concurrency", "1", "-out", filepath.Join(t.TempDir(), "result.json")}
	if err := runVectorPartitionSystemBenchWithCellV1(args, io.Discard, runCell, nativewire.ProbeVectorPartitionShardEndpointV1); err == nil || !strings.Contains(err.Error(), "does not match checked topology") {
		t.Fatalf("mismatched topology dataset error = %v", err)
	}
	if called {
		t.Fatal("benchmark cell ran with a mismatched topology dataset")
	}
}

func TestVectorPartitionSystemNativeFourDaemonProcessLossAndRestartV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	if runtime.GOOS != "linux" {
		t.Skip("native runtime ownership is Linux-only")
	}
	_, runtimeCPUs, err := canonicalVectorPartitionSystemCPUSetV1(vectorPartitionSystemEffectiveCPUSetV1())
	if err != nil || len(runtimeCPUs) < 4 {
		t.Skipf("four disjoint runtime CPUs unavailable: %v", err)
	}
	dataset := writeFixtureForTest(t, 64, 8, 8)
	fixture, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	vectors, _ := fixtureData(fixture)
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	databases := make([]string, len(groups))
	var generation uint64
	for index := range groups {
		assets, err := newM8ProductionMultiGroupAssetsV1(vectors, groups, 16)
		if err != nil {
			t.Fatal(err)
		}
		databases[index] = assets.dir
		if generation == 0 {
			generation = assets.manifest.Generation
		} else if assets.manifest.Generation != generation {
			t.Fatalf("independent M3 generation=%d want %d", assets.manifest.Generation, generation)
		}
		assets.owned = false
		if err := assets.Close(); err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(databases[index])
	}
	root := t.TempDir()
	capabilityKey := writeVectorPartitionSystemCapabilityKeyTestV1(t, root)
	endpoints := make(map[string]string, len(groups))
	for _, group := range groups {
		endpoints[group] = reserveVectorPartitionSystemTCPAddressTestV1(t)
	}
	publicAddress := reserveVectorPartitionSystemTCPAddressTestV1(t)
	applied := map[string]uint64{"group-a": 1, "group-b": 1, "group-c": 1, "group-d": 1}
	configs := make([]string, len(groups))
	ready := make([]string, len(groups))
	states := make([]string, len(groups))
	for index, group := range groups {
		states[index] = filepath.Join(root, "state-"+group)
		ready[index] = filepath.Join(states[index], "ready.json")
		config := vectorPartitionSystemNodeConfigV1{
			SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
			Topology: "native_four_daemon_four_group", NodeID: "native-" + group, DatasetDirectory: dataset,
			DatabaseDirectory: databases[index], StateDirectory: states[index], CapabilityKeyPath: capabilityKey, ReadyPath: ready[index],
			LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: group, Listen: endpoints[group]}}, Endpoints: endpoints,
			GroupAppliedIndexes: applied,
			RuntimeOwnership:    &vectorPartitionSystemRuntimeOwnershipV1{CPUSet: fmt.Sprint(runtimeCPUs[index]), GOMAXPROCS: 1, GoMemoryLimitBytes: 1 << 30},
		}
		if index == 0 {
			config.PublicListen = publicAddress
		}
		configs[index] = filepath.Join(states[index], "config.json")
		if err := os.MkdirAll(states[index], 0o755); err != nil {
			t.Fatal(err)
		}
		writeVectorPartitionSystemJSONTestV1(t, configs[index], config)
	}
	processes := make([]*vectorPartitionSystemNodeProcessTestV1, len(groups))
	defer func() {
		for _, process := range processes {
			if process != nil {
				process.stop()
			}
		}
	}()
	for _, index := range []int{1, 2, 3, 0} {
		processes[index] = startVectorPartitionSystemNodeProcessTestV1(t, configs[index], states[index])
		waitVectorPartitionSystemReadyTestV1(t, ready[index], processes[index])
	}
	for index, path := range ready {
		var nodeReady vectorPartitionSystemNodeReadyV1
		raw, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(raw, &nodeReady) != nil {
			t.Fatalf("read runtime ownership readiness %d: %v", index, err)
		}
		wantCPU := fmt.Sprint(runtimeCPUs[index])
		if nodeReady.RuntimeOwnership == nil || nodeReady.RuntimeOwnership.CPUSet != wantCPU || nodeReady.EffectiveCPUSet != wantCPU || nodeReady.GOMAXPROCS != 1 || nodeReady.GoMemoryLimit != 1<<30 {
			t.Fatalf("runtime ownership readiness %d = %+v", index, nodeReady)
		}
	}
	for index, group := range groups {
		var config vectorPartitionSystemNodeConfigV1
		raw, err := os.ReadFile(configs[index])
		if err != nil || json.Unmarshal(raw, &config) != nil {
			t.Fatalf("read node config %d: %v", index, err)
		}
		configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(config)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := nativewire.ProbeVectorPartitionShardEndpointV1(t.Context(), endpoints[group])
		if err != nil || identity.GroupID != group || identity.InstanceIdentity != configSHA {
			t.Fatalf("live endpoint %q identity = %+v, %v", group, identity, err)
		}
	}
	client, err := dialVectorPartitionOperationsV1(t.Context(), publicAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	query := make([]float32, len(vectors[0]))
	for dimension := range vectors[0] {
		query[dimension] = float32(vectors[0][dimension])
	}
	request := vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: partitionHNSWIndex, Generation: generation}, Query: query,
		Metric: public.MetricCosineV1, TopK: 4, Probes: 16, EfSearch: 128, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 1024}, Deadline: time.Now().Add(30 * time.Second),
	}}
	before, err := nativewire.ProbeVectorPartitionShardEndpointV1(t.Context(), endpoints[groups[0]])
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.call(request); err != nil || response.Search == nil || len(response.Search.Neighbors) != 4 || response.Search.Counters.SelectedGroups != 4 {
		t.Fatalf("native four-daemon search = %+v, %v", response.Search, err)
	}
	after, err := nativewire.ProbeVectorPartitionShardEndpointV1(t.Context(), endpoints[groups[0]])
	if err != nil {
		t.Fatal(err)
	}
	if after.CatalogMetaReadStats.Total.Reads-before.CatalogMetaReadStats.Total.Reads != 1 || after.CatalogMetaReadStats.StrictSearch.Reads-before.CatalogMetaReadStats.StrictSearch.Reads != 1 || after.CatalogMetaReadStats.Total.NoLogProofs-before.CatalogMetaReadStats.Total.NoLogProofs != 1 {
		t.Fatalf("native strict proof delta before=%+v after=%+v", before.CatalogMetaReadStats, after.CatalogMetaReadStats)
	}
	processes[3].stop()
	processes[3] = nil
	if response, err := client.call(request); err == nil || response.Search != nil {
		t.Fatalf("native process loss returned partial result = %+v, %v", response.Search, err)
	}
	restartedReady := filepath.Join(states[3], "ready-restarted.json")
	var restartedConfig vectorPartitionSystemNodeConfigV1
	raw, err := os.ReadFile(configs[3])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &restartedConfig); err != nil {
		t.Fatal(err)
	}
	restartedConfig.ReadyPath = restartedReady
	writeVectorPartitionSystemJSONTestV1(t, configs[3], restartedConfig)
	processes[3] = startVectorPartitionSystemNodeProcessTestV1(t, configs[3], states[3])
	waitVectorPartitionSystemReadyTestV1(t, restartedReady, processes[3])
	var restartedReadyEvidence vectorPartitionSystemNodeReadyV1
	restartedRaw, err := os.ReadFile(restartedReady)
	if err != nil || json.Unmarshal(restartedRaw, &restartedReadyEvidence) != nil {
		t.Fatalf("read restarted runtime ownership: %v", err)
	}
	if restartedReadyEvidence.RuntimeOwnership == nil || restartedReadyEvidence.RuntimeOwnership.CPUSet != fmt.Sprint(runtimeCPUs[3]) || restartedReadyEvidence.EffectiveCPUSet != fmt.Sprint(runtimeCPUs[3]) || restartedReadyEvidence.GOMAXPROCS != 1 || restartedReadyEvidence.GoMemoryLimit != 1<<30 {
		t.Fatalf("restarted runtime ownership = %+v", restartedReadyEvidence)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		request.Search.Deadline = time.Now().Add(30 * time.Second)
		response, callErr := client.call(request)
		if callErr == nil && response.Search != nil && len(response.Search.Neighbors) == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("native four-daemon search after restart = %+v, %v", response.Search, callErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type vectorPartitionSystemNodeProcessTestV1 struct {
	command *exec.Cmd
	done    chan error
	stdout  bytes.Buffer
	stderr  bytes.Buffer
}

func startVectorPartitionSystemNodeProcessTestV1(t *testing.T, config, state string) *vectorPartitionSystemNodeProcessTestV1 {
	t.Helper()
	process := &vectorPartitionSystemNodeProcessTestV1{done: make(chan error, 1)}
	process.command = exec.Command(os.Args[0], "-test.run=^TestVectorPartitionSystemNodeProcessHelperV1$", "-test.v")
	process.command.Env = append(os.Environ(), "GOMAP_SYSTEM_NODE_TEST_CONFIG="+config, "TMPDIR="+state)
	process.command.Stdout, process.command.Stderr = &process.stdout, &process.stderr
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.done <- process.command.Wait() }()
	return process
}

func (p *vectorPartitionSystemNodeProcessTestV1) stop() {
	if p == nil || p.command == nil || p.command.Process == nil {
		return
	}
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
	}
}

func waitVectorPartitionSystemReadyTestV1(t *testing.T, path string, process *vectorPartitionSystemNodeProcessTestV1) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-process.done:
			process.command = nil
			t.Fatalf("system node exited before ready at %s: %v\nstdout:\n%s\nstderr:\n%s", path, err, process.stdout.String(), process.stderr.String())
		default:
		}
		if raw, err := os.ReadFile(path); err == nil {
			var ready vectorPartitionSystemNodeReadyV1
			if json.Unmarshal(raw, &ready) == nil && ready.ProductionTopology && !ready.M8Loopback {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("system node did not become ready at %s", path)
}

func reserveVectorPartitionSystemTCPAddressTestV1(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func writeVectorPartitionSystemJSONTestV1(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeVectorPartitionSystemCapabilityKeyTestV1(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "strict-capability.key")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x5a}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeVectorPartitionSystemTopologyEvidenceTestV1(t *testing.T, endpoint, dataset string) string {
	t.Helper()
	root := t.TempDir()
	capabilityKey := writeVectorPartitionSystemCapabilityKeyTestV1(t, root)
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	endpoints := make(map[string]string, len(groups))
	local := make([]vectorPartitionSystemLocalGroupV1, 0, len(groups))
	for index, group := range groups {
		listen := fmt.Sprintf("127.0.0.1:%d", 21000+index)
		endpoints[group] = listen
		local = append(local, vectorPartitionSystemLocalGroupV1{GroupID: group, Listen: listen})
	}
	config := vectorPartitionSystemNodeConfigV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
		Topology: "single_daemon_four_group", NodeID: "single", DatasetDirectory: dataset,
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: filepath.Join(root, "state"), CapabilityKeyPath: capabilityKey,
		ReadyPath: filepath.Join(root, "state", "ready.json"), PublicListen: endpoint, LocalGroups: local, Endpoints: endpoints,
	}
	evidence, err := validateVectorPartitionSystemTopologyV1([]vectorPartitionSystemNodeConfigV1{config})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "topology.json")
	writeVectorPartitionSystemJSONTestV1(t, path, evidence)
	return path
}

func writeVectorPartitionSystemTopologyWithLiveEndpointsTestV1(t *testing.T, endpoint, dataset string) (string, func()) {
	t.Helper()
	root := t.TempDir()
	capabilityKey := writeVectorPartitionSystemCapabilityKeyTestV1(t, root)
	groups := []string{"group-a", "group-b", "group-c", "group-d"}
	endpoints := make(map[string]string, len(groups))
	local := make([]vectorPartitionSystemLocalGroupV1, 0, len(groups))
	listeners := make([]net.Listener, 0, len(groups))
	for _, group := range groups {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		endpoints[group] = listener.Addr().String()
		local = append(local, vectorPartitionSystemLocalGroupV1{GroupID: group, Listen: listener.Addr().String()})
	}
	config := vectorPartitionSystemNodeConfigV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
		Topology: "single_daemon_four_group", NodeID: "single", DatasetDirectory: dataset,
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: filepath.Join(root, "state"), CapabilityKeyPath: capabilityKey,
		ReadyPath: filepath.Join(root, "state", "ready.json"), PublicListen: endpoint, LocalGroups: local, Endpoints: endpoints,
	}
	evidence, err := validateVectorPartitionSystemTopologyV1([]vectorPartitionSystemNodeConfigV1{config})
	if err != nil {
		t.Fatal(err)
	}
	identity := evidence.Nodes[0].NodeConfigSHA256
	for index, listener := range listeners {
		group := groups[index]
		go func() {
			for {
				conn, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				go (nativewire.VectorPartitionShardSearchTCPServerV1{EndpointIdentity: nativewire.VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: group, InstanceIdentity: identity}}).ServeConn(context.Background(), conn)
			}
		}()
	}
	path := filepath.Join(root, "topology.json")
	writeVectorPartitionSystemJSONTestV1(t, path, evidence)
	return path, func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}
}
