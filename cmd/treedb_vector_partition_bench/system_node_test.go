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
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: state, PublicListen: "127.0.0.1:22004",
		ReadyPath: filepath.Join(state, "ready.json"), LocalGroups: local, Endpoints: endpoints,
	}
	path := filepath.Join(root, "config.json")
	writeVectorPartitionSystemJSONTestV1(t, path, config)
	if _, err := loadVectorPartitionSystemNodeConfigV1(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state directory created during config load: %v", err)
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
	_, err := wireClient.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search"})
	var typed *public.ErrorV1
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &typed) || typed.Code != public.ErrorDeadlineExceededV1 {
		t.Fatalf("wire timeout = %v", err)
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
		StateDirectory: state, PublicListen: "127.0.0.1:0", ReadyPath: filepath.Join(state, "ready.json"), LocalGroups: local, Endpoints: endpoints,
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
	if node.ready.PublicRoute != "vectorpartition.OperationsV1.Search" || !node.ready.ProductionTopology || node.ready.M8Loopback || len(node.ready.Groups) != 4 {
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
	response, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: public.SearchRequestV1{
		Version: 1, Generation: public.GenerationIDV1{Index: assets.manifest.IndexName, Generation: assets.manifest.Generation}, Query: query,
		Metric: public.MetricCosineV1, TopK: 10, Probes: 2, EfSearch: 128, Consistency: public.ConsistencyGenerationSnapshotV1,
		Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 2560}, Deadline: time.Now().Add(30 * time.Second),
	}})
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
	cell, err := vectorPartitionSystemBenchCell(t.Context(), node.ready.PublicEndpoint, configSHA, queries, truth, 4, 4, 128, 2, len(queries))
	if err != nil {
		t.Fatal(err)
	}
	if cell.Status != "valid" || cell.Generation.Index == "" || cell.Generation.Generation == 0 || cell.ElapsedNanos == 0 || cell.Metrics.CompletedQueries != len(queries) || cell.Metrics.RecallAt10 <= 0 || cell.Counters["selected_partitions"] != uint64(4*len(queries)) || cell.Counters["selected_groups"] == 0 || cell.Counters["query_bytes"] == 0 || cell.Counters["response_bytes"] == 0 || cell.Timings["total"] == 0 || cell.Timings["rpc"] == 0 || cell.Timings["read_index_apply"] == 0 || cell.Timings["shard_search"] == 0 || len(cell.TotalNanos) != len(queries) {
		t.Fatalf("system benchmark cell = %+v", cell)
	}
	node.publicServer.nodeConfigSHA256 = strings.Repeat("f", 64)
	if _, err := vectorPartitionSystemBenchCell(t.Context(), node.ready.PublicEndpoint, configSHA, queries, truth, 4, 4, 128, 2, 0); err == nil || !strings.Contains(err.Error(), "live node config identity does not match checked topology") {
		t.Fatalf("mismatched live topology error = %v", err)
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
	for index, group := range groups {
		endpoints[group] = fmt.Sprintf("127.0.0.1:%d", 21000+index)
		applied[group] = 1
	}
	configs := make([]vectorPartitionSystemNodeConfigV1, len(groups))
	for index, group := range groups {
		configs[index] = vectorPartitionSystemNodeConfigV1{
			SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeConfigKindV1, Assembly: vectorPartitionSystemAssemblyV1,
			Topology: "native_four_daemon_four_group", NodeID: "node-" + group, DatasetDirectory: filepath.Join(root, "dataset"),
			DatabaseDirectory: filepath.Join(root, "db-"+group), StateDirectory: filepath.Join(root, "state-"+group),
			ReadyPath: filepath.Join(root, "state-"+group, "ready.json"), LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: group, Listen: endpoints[group]}},
			Endpoints: endpoints, GroupAppliedIndexes: applied,
		}
	}
	configs[0].PublicListen = "127.0.0.1:22000"
	evidence, err := validateVectorPartitionSystemTopologyV1(configs)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.TopologyIdentitySHA256 == "" || evidence.PublicRoute != "vectorpartition.OperationsV1.Search" || evidence.M8Loopback || len(evidence.Nodes) != 4 {
		t.Fatalf("topology evidence = %+v", evidence)
	}
	for index, node := range evidence.Nodes {
		configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(configs[index])
		if err != nil || node.NodeConfigSHA256 != configSHA {
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
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int) (vectorPartitionSystemBenchCellV1, error) {
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
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int) (vectorPartitionSystemBenchCellV1, error) {
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

func TestVectorPartitionSystemBenchRejectsMismatchedTopologyDatasetV1(t *testing.T) {
	dataset := writeFixtureForTest(t, 10, 2, 2)
	topology := writeVectorPartitionSystemTopologyEvidenceTestV1(t, "127.0.0.1:1", t.TempDir())
	called := false
	runCell := func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int) (vectorPartitionSystemBenchCellV1, error) {
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
			DatabaseDirectory: databases[index], StateDirectory: states[index], ReadyPath: ready[index],
			LocalGroups: []vectorPartitionSystemLocalGroupV1{{GroupID: group, Listen: endpoints[group]}}, Endpoints: endpoints,
			GroupAppliedIndexes: applied,
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
	if response, err := client.call(request); err != nil || response.Search == nil || len(response.Search.Neighbors) != 4 || response.Search.Counters.SelectedGroups != 4 {
		t.Fatalf("native four-daemon search = %+v, %v", response.Search, err)
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

func writeVectorPartitionSystemTopologyEvidenceTestV1(t *testing.T, endpoint, dataset string) string {
	t.Helper()
	root := t.TempDir()
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
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: filepath.Join(root, "state"),
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
		DatabaseDirectory: filepath.Join(root, "database"), StateDirectory: filepath.Join(root, "state"),
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
