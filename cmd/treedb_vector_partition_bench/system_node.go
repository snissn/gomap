package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	vectorPartitionSystemNodeConfigKindV1 = "vector_partition_system_node_config_v1"
	vectorPartitionSystemNodeReadyKindV1  = "vector_partition_system_node_ready_v1"
	vectorPartitionSystemTopologyKindV1   = "vector_partition_system_topology_v1"
	vectorPartitionSystemAssemblyV1       = "production_public_v1"
	vectorPartitionSystemWireMaxBytesV1   = 2 << 20
	vectorPartitionSystemMaxConnectionsV1 = 128
	vectorPartitionSystemIdleTimeoutV1    = 5 * time.Minute
)

type vectorPartitionSystemLocalGroupV1 struct {
	GroupID string `json:"group_id"`
	Listen  string `json:"listen"`
}

type vectorPartitionSystemNodeConfigV1 struct {
	SchemaVersion       int                                 `json:"schema_version"`
	ResultKind          string                              `json:"result_kind"`
	Assembly            string                              `json:"assembly"`
	Topology            string                              `json:"topology"`
	NodeID              string                              `json:"node_id"`
	DatasetDirectory    string                              `json:"dataset_directory"`
	DatabaseDirectory   string                              `json:"database_directory"`
	StateDirectory      string                              `json:"state_directory"`
	PublicListen        string                              `json:"public_listen,omitempty"`
	ReadyPath           string                              `json:"ready_path"`
	LocalGroups         []vectorPartitionSystemLocalGroupV1 `json:"local_groups"`
	Endpoints           map[string]string                   `json:"endpoints"`
	GroupAppliedIndexes map[string]uint64                   `json:"group_applied_indexes,omitempty"`
}

type vectorPartitionSystemGroupReadyV1 struct {
	GroupID                   string `json:"group_id"`
	Endpoint                  string `json:"endpoint"`
	LeaderID                  string `json:"leader_id"`
	AppliedIndex              uint64 `json:"applied_index"`
	ProvesProductionConsensus bool   `json:"proves_production_consensus"`
}

type vectorPartitionSystemNodeReadyV1 struct {
	SchemaVersion      int                                 `json:"schema_version"`
	ResultKind         string                              `json:"result_kind"`
	Assembly           string                              `json:"assembly"`
	Topology           string                              `json:"topology"`
	NodeID             string                              `json:"node_id"`
	PID                int                                 `json:"pid"`
	PublicEndpoint     string                              `json:"public_endpoint,omitempty"`
	PublicRoute        string                              `json:"public_route"`
	ProductionTopology bool                                `json:"production_topology"`
	M8Loopback         bool                                `json:"m8_loopback"`
	DatabaseDirectory  string                              `json:"database_directory"`
	StateDirectory     string                              `json:"state_directory"`
	SourceRevision     string                              `json:"source_revision"`
	VCSModified        bool                                `json:"vcs_modified"`
	ExecutableSHA256   string                              `json:"executable_sha256"`
	NodeConfigSHA256   string                              `json:"node_config_sha256"`
	LifecycleState     string                              `json:"lifecycle_state"`
	Groups             []vectorPartitionSystemGroupReadyV1 `json:"groups"`
}

type vectorPartitionSystemTopologyEvidenceV1 struct {
	SchemaVersion          int                                   `json:"schema_version"`
	ResultKind             string                                `json:"result_kind"`
	Assembly               string                                `json:"assembly"`
	Topology               string                                `json:"topology"`
	Nodes                  []vectorPartitionSystemTopologyNodeV1 `json:"nodes"`
	DatasetDirectory       string                                `json:"dataset_directory"`
	Endpoints              map[string]string                     `json:"endpoints"`
	GroupAppliedIndexes    map[string]uint64                     `json:"group_applied_indexes,omitempty"`
	PublicRoute            string                                `json:"public_route"`
	M8Loopback             bool                                  `json:"m8_loopback"`
	TopologyIdentitySHA256 string                                `json:"topology_identity_sha256"`
}

type vectorPartitionSystemTopologyNodeV1 struct {
	NodeID            string                              `json:"node_id"`
	NodeConfigSHA256  string                              `json:"node_config_sha256"`
	DatabaseDirectory string                              `json:"database_directory"`
	StateDirectory    string                              `json:"state_directory"`
	ReadyPath         string                              `json:"ready_path"`
	PublicListen      string                              `json:"public_listen,omitempty"`
	LocalGroups       []vectorPartitionSystemLocalGroupV1 `json:"local_groups"`
}

type vectorPartitionSystemNodeV1 struct {
	config         vectorPartitionSystemNodeConfigV1
	assets         *m8ProductionMultiGroupAssetsV1
	production     *nativewire.VectorPartitionProductionNodeV1
	publicListener net.Listener
	publicServer   *vectorPartitionOperationsTCPServerV1
	ready          vectorPartitionSystemNodeReadyV1
}

func runVectorPartitionSystemNodeV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-node", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var configPath string
	fs.StringVar(&configPath, "config", "", "bounded system node JSON config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || configPath == "" {
		return errors.New("system-node requires -config and no positional arguments")
	}
	config, err := loadVectorPartitionSystemNodeConfigV1(configPath)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	node, err := openVectorPartitionSystemNodeV1(ctx, config)
	if err != nil {
		return err
	}
	defer node.Close()
	if err := publishVectorPartitionSystemNodeReadyV1(config.ReadyPath, node.ready); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stdout, "ready=%s public=%s topology=%s node=%s\n", config.ReadyPath, node.ready.PublicEndpoint, config.Topology, config.NodeID); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func runVectorPartitionSystemCheckTopologyV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-check-topology", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var paths, out string
	fs.StringVar(&paths, "configs", "", "comma-separated bounded system node JSON configs")
	fs.StringVar(&out, "out", "", "exclusive topology evidence JSON path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || paths == "" || out == "" {
		return errors.New("system-check-topology requires -configs, -out, and no positional arguments")
	}
	parts := strings.Split(paths, ",")
	configs := make([]vectorPartitionSystemNodeConfigV1, 0, len(parts))
	for _, path := range parts {
		if path == "" {
			return errors.New("system-check-topology config path is empty")
		}
		config, err := loadVectorPartitionSystemNodeConfigV1(path)
		if err != nil {
			return err
		}
		configs = append(configs, config)
	}
	evidence, err := validateVectorPartitionSystemTopologyV1(configs)
	if err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, evidence); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "topology=%s identity_sha256=%s\n", out, evidence.TopologyIdentitySHA256)
	return err
}

func validateVectorPartitionSystemTopologyV1(configs []vectorPartitionSystemNodeConfigV1) (vectorPartitionSystemTopologyEvidenceV1, error) {
	var evidence vectorPartitionSystemTopologyEvidenceV1
	if len(configs) == 0 {
		return evidence, errors.New("system topology has no node configs")
	}
	first := configs[0]
	wantNodes := 4
	if first.Topology == "single_daemon_four_group" {
		wantNodes = 1
	}
	if len(configs) != wantNodes {
		return evidence, fmt.Errorf("system topology %q requires %d node configs", first.Topology, wantNodes)
	}
	nodes, databases, states := map[string]bool{}, map[string]bool{}, map[string]bool{}
	readyPaths, ownedGroups := map[string]bool{}, map[string]bool{}
	var listeners []string
	addListener := func(listener string) bool {
		for _, existing := range listeners {
			if stringsHostPortEquivalentV1(existing, listener) {
				return false
			}
		}
		listeners = append(listeners, listener)
		return true
	}
	var persistentRoots []string
	publicCount := 0
	for _, config := range configs {
		if config.Assembly != vectorPartitionSystemAssemblyV1 || config.Topology != first.Topology || config.DatasetDirectory != first.DatasetDirectory || !maps.Equal(config.Endpoints, first.Endpoints) || !maps.Equal(config.GroupAppliedIndexes, first.GroupAppliedIndexes) {
			return evidence, errors.New("system topology node configs do not share one production identity")
		}
		if config.NodeID == "" || nodes[config.NodeID] || databases[config.DatabaseDirectory] || states[config.StateDirectory] || readyPaths[config.ReadyPath] {
			return evidence, errors.New("system topology node identities and persistent roots must be distinct")
		}
		if config.ReadyPath == config.StateDirectory || !vectorPartitionSystemPathContainsV1(config.StateDirectory, config.ReadyPath) {
			return evidence, errors.New("system topology ready evidence must be inside its state root")
		}
		for _, root := range persistentRoots {
			if vectorPartitionSystemPathsOverlapV1(root, config.DatabaseDirectory) || vectorPartitionSystemPathsOverlapV1(root, config.StateDirectory) {
				return evidence, errors.New("system topology persistent roots must be disjoint")
			}
		}
		if vectorPartitionSystemPathsOverlapV1(config.DatabaseDirectory, config.StateDirectory) {
			return evidence, errors.New("system topology persistent roots must be disjoint")
		}
		persistentRoots = append(persistentRoots, config.DatabaseDirectory, config.StateDirectory)
		nodes[config.NodeID], databases[config.DatabaseDirectory], states[config.StateDirectory], readyPaths[config.ReadyPath] = true, true, true, true
		if config.PublicListen != "" {
			publicCount++
			if !addListener(config.PublicListen) {
				return evidence, errors.New("system topology listeners must be distinct")
			}
		}
		for _, local := range config.LocalGroups {
			if ownedGroups[local.GroupID] || !addListener(local.Listen) || config.Endpoints[local.GroupID] == "" || !stringsHostPortEquivalentV1(config.Endpoints[local.GroupID], local.Listen) {
				return evidence, errors.New("system topology group ownership or listener binding is invalid")
			}
			ownedGroups[local.GroupID] = true
		}
	}
	if publicCount != 1 || len(ownedGroups) != len(first.Endpoints) {
		return evidence, errors.New("system topology requires exactly one public listener and one owner per group")
	}
	for group := range first.Endpoints {
		if !ownedGroups[group] {
			return evidence, fmt.Errorf("system topology group %q has no owner", group)
		}
	}
	for _, config := range configs {
		configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(config)
		if err != nil {
			return evidence, err
		}
		evidence.Nodes = append(evidence.Nodes, vectorPartitionSystemTopologyNodeV1{
			NodeID: config.NodeID, NodeConfigSHA256: configSHA, DatabaseDirectory: config.DatabaseDirectory, StateDirectory: config.StateDirectory,
			ReadyPath: config.ReadyPath, PublicListen: config.PublicListen, LocalGroups: slices.Clone(config.LocalGroups),
		})
	}
	sort.Slice(evidence.Nodes, func(i, j int) bool { return evidence.Nodes[i].NodeID < evidence.Nodes[j].NodeID })
	evidence.SchemaVersion = 1
	evidence.ResultKind = vectorPartitionSystemTopologyKindV1
	evidence.Assembly = vectorPartitionSystemAssemblyV1
	evidence.Topology = first.Topology
	evidence.DatasetDirectory = first.DatasetDirectory
	evidence.Endpoints = maps.Clone(first.Endpoints)
	evidence.GroupAppliedIndexes = maps.Clone(first.GroupAppliedIndexes)
	evidence.PublicRoute = "vectorpartition.OperationsV1.Search"
	raw, err := json.Marshal(evidence)
	if err != nil {
		return evidence, err
	}
	sum := sha256.Sum256(raw)
	evidence.TopologyIdentitySHA256 = hex.EncodeToString(sum[:])
	return evidence, nil
}

func vectorPartitionSystemNodeConfigSHA256V1(config vectorPartitionSystemNodeConfigV1) (string, error) {
	raw, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func vectorPartitionSystemPathsOverlapV1(left, right string) bool {
	return vectorPartitionSystemPathContainsV1(left, right) || vectorPartitionSystemPathContainsV1(right, left)
}

func vectorPartitionSystemPathContainsV1(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return true
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func loadVectorPartitionSystemNodeConfigV1(path string) (vectorPartitionSystemNodeConfigV1, error) {
	var config vectorPartitionSystemNodeConfigV1
	raw, err := readBoundedRegularFileV1(path, maxManifestBytes)
	if err != nil {
		return config, fmt.Errorf("system node config: %w", err)
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return config, fmt.Errorf("system node config JSON: %w", err)
	}
	canonical := func(value string) (string, error) {
		if value == "" {
			return "", errors.New("path is empty")
		}
		return m8CanonicalPathV1(value)
	}
	if config.SchemaVersion != 1 || config.ResultKind != vectorPartitionSystemNodeConfigKindV1 {
		return config, errors.New("system node config identity is invalid")
	}
	if config.Assembly != vectorPartitionSystemAssemblyV1 {
		return config, fmt.Errorf("system node rejects non-production assembly %q", config.Assembly)
	}
	if config.Topology != "single_daemon_four_group" && config.Topology != "native_four_daemon_four_group" && config.Topology != "container_four_daemon_four_group" {
		return config, fmt.Errorf("system node topology %q is unsupported", config.Topology)
	}
	if config.NodeID == "" || len(config.LocalGroups) == 0 || len(config.Endpoints) != 4 || config.ReadyPath == "" {
		return config, errors.New("system node config is incomplete")
	}
	if config.Topology == "single_daemon_four_group" && (len(config.LocalGroups) != 4 || config.PublicListen == "") {
		return config, errors.New("single-daemon system node requires four local groups and a public listener")
	}
	if config.Topology != "single_daemon_four_group" && len(config.LocalGroups) != 1 {
		return config, errors.New("multi-daemon system node requires exactly one local group")
	}
	if config.Topology != "single_daemon_four_group" && len(config.GroupAppliedIndexes) != len(config.Endpoints) {
		return config, errors.New("multi-daemon system node requires all shared applied indexes")
	}
	seen := map[string]bool{}
	for _, group := range config.LocalGroups {
		if group.GroupID == "" || group.Listen == "" || seen[group.GroupID] || config.Endpoints[group.GroupID] == "" {
			return config, errors.New("system node local group bindings are invalid")
		}
		seen[group.GroupID] = true
	}
	for group, endpoint := range config.Endpoints {
		if group == "" || endpoint == "" || (len(config.GroupAppliedIndexes) != 0 && config.GroupAppliedIndexes[group] == 0) {
			return config, errors.New("system node endpoint bindings are invalid")
		}
	}
	if len(config.GroupAppliedIndexes) != 0 && len(config.GroupAppliedIndexes) != len(config.Endpoints) {
		return config, errors.New("system node shared applied-index bindings are invalid")
	}
	for group, applied := range config.GroupAppliedIndexes {
		if config.Endpoints[group] == "" || applied == 0 {
			return config, errors.New("system node shared applied-index bindings are invalid")
		}
	}
	var pathErr error
	if config.DatasetDirectory, pathErr = canonical(config.DatasetDirectory); pathErr != nil {
		return config, fmt.Errorf("system node dataset directory: %w", pathErr)
	}
	if config.DatabaseDirectory, pathErr = canonical(config.DatabaseDirectory); pathErr != nil {
		return config, fmt.Errorf("system node database directory: %w", pathErr)
	}
	if config.StateDirectory, pathErr = canonical(config.StateDirectory); pathErr != nil {
		return config, fmt.Errorf("system node state directory: %w", pathErr)
	}
	if config.ReadyPath, pathErr = canonical(config.ReadyPath); pathErr != nil {
		return config, fmt.Errorf("system node ready path: %w", pathErr)
	}
	return config, nil
}

func openVectorPartitionSystemNodeV1(ctx context.Context, config vectorPartitionSystemNodeConfigV1) (_ *vectorPartitionSystemNodeV1, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := os.MkdirAll(config.StateDirectory, 0o755); err != nil {
		return nil, err
	}
	tempRoot, err := m8CanonicalPathV1(os.TempDir())
	if err != nil || tempRoot != config.StateDirectory {
		return nil, fmt.Errorf("system node TMPDIR must equal state directory %q", config.StateDirectory)
	}
	groups := make([]string, 0, len(config.Endpoints))
	for group := range config.Endpoints {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	fixture, err := loadFixture(config.DatasetDirectory)
	if err != nil {
		return nil, err
	}
	vectors, _ := fixtureData(fixture)
	assets, err := openM8ProductionMultiGroupExistingAssetsV1(config.DatabaseDirectory, groups, len(groups)*4, fixture, vectors)
	if err != nil {
		return nil, err
	}
	node := &vectorPartitionSystemNodeV1{config: config, assets: assets}
	defer func() {
		if err != nil {
			_ = node.Close()
		}
	}()
	shards := make([]nativewire.VectorPartitionProductionNodeShardV1, 0, len(config.LocalGroups))
	listeners := make([]net.Listener, 0, len(config.LocalGroups))
	defer func() {
		if node.production == nil {
			for _, listener := range listeners {
				_ = listener.Close()
			}
		}
	}()
	for _, local := range config.LocalGroups {
		listener, listenErr := net.Listen("tcp", local.Listen)
		if listenErr != nil {
			return nil, fmt.Errorf("listen system shard %q: %w", local.GroupID, listenErr)
		}
		listeners = append(listeners, listener)
		if config.Endpoints[local.GroupID] != listener.Addr().String() && !stringsHostPortEquivalentV1(config.Endpoints[local.GroupID], listener.Addr().String()) {
			return nil, fmt.Errorf("system shard %q endpoint %q does not match listener %q", local.GroupID, config.Endpoints[local.GroupID], listener.Addr())
		}
		shards = append(shards, nativewire.VectorPartitionProductionNodeShardV1{GroupID: local.GroupID, Listener: listener})
	}
	requestBase := nativewire.VectorPartitionCoordinatorRequestV1{
		Version: 1, RequestID: "system-public", CancellationID: "system-public-cancel", Metric: nativewire.VectorPartitionShardSearchMetricCosineV1, TopK: 10, PartitionProbes: 2,
		EfSearch: 128, RouterCandidateBudget: 256, Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1,
	}
	node.production, err = nativewire.NewVectorPartitionProductionNodeV1(ctx, nativewire.VectorPartitionProductionNodeOptionsV1{
		Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), AssetSetDigests: assets.assetSetDigests,
		GroupAppliedIndexes: config.GroupAppliedIndexes, Database: "default", Catalog: "default", Endpoints: config.Endpoints,
		LocalShards: shards, RequestBase: requestBase, NodeID: config.NodeID,
	})
	if err != nil {
		return nil, err
	}
	publicEndpoint := ""
	if config.PublicListen != "" {
		node.publicListener, err = net.Listen("tcp", config.PublicListen)
		if err != nil {
			return nil, err
		}
		service, serviceErr := public.NewServiceV1(node.production.PublicBackendV1())
		if serviceErr != nil {
			return nil, serviceErr
		}
		opsConfig := public.ConservativeOperationsConfigV1()
		opsConfig.Enabled = true
		operations, operationsErr := public.NewOperationsV1(service, opsConfig, node.production.PublicBackendV1().OperationsHealthV1)
		if operationsErr != nil {
			return nil, operationsErr
		}
		node.publicServer = &vectorPartitionOperationsTCPServerV1{operations: operations, listener: node.publicListener, done: make(chan struct{})}
		node.publicServer.start(ctx)
		publicEndpoint = node.publicListener.Addr().String()
	}
	revision, modified := vectorPartitionSystemBuildVCSIdentityV1()
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	executableSHA, err := m8BenchmarkExecutableSHA256V1(executable)
	if err != nil {
		return nil, err
	}
	configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(config)
	if err != nil {
		return nil, err
	}
	node.ready = vectorPartitionSystemNodeReadyV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeReadyKindV1, Assembly: config.Assembly, Topology: config.Topology, NodeID: config.NodeID,
		PID: os.Getpid(), PublicEndpoint: publicEndpoint, PublicRoute: "vectorpartition.OperationsV1.Search", ProductionTopology: true, M8Loopback: false,
		DatabaseDirectory: config.DatabaseDirectory, StateDirectory: config.StateDirectory, SourceRevision: revision, VCSModified: modified,
		ExecutableSHA256: executableSHA, NodeConfigSHA256: configSHA, LifecycleState: "active",
	}
	for _, evidence := range node.production.GroupEvidenceV1() {
		node.ready.Groups = append(node.ready.Groups, vectorPartitionSystemGroupReadyV1{GroupID: evidence.GroupID, Endpoint: config.Endpoints[evidence.GroupID], LeaderID: evidence.LeaderID, AppliedIndex: evidence.AppliedIndex, ProvesProductionConsensus: true})
	}
	return node, nil
}

func (n *vectorPartitionSystemNodeV1) Close() error {
	if n == nil {
		return nil
	}
	var errs []error
	if n.publicServer != nil {
		errs = append(errs, n.publicServer.Close())
		n.publicServer = nil
	} else if n.publicListener != nil {
		errs = append(errs, n.publicListener.Close())
	}
	if n.production != nil {
		errs = append(errs, n.production.Close())
		n.production = nil
	}
	if n.assets != nil {
		errs = append(errs, n.assets.Close())
		n.assets = nil
	}
	return errors.Join(errs...)
}

func publishVectorPartitionSystemNodeReadyV1(path string, ready vectorPartitionSystemNodeReadyV1) error {
	raw, err := json.MarshalIndent(ready, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(path)
		return errors.Join(writeErr, syncErr, closeErr)
	}
	return nil
}

func vectorPartitionSystemBuildVCSIdentityV1() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", true
	}
	var revision string
	modified := true
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value != "false"
		}
	}
	return revision, modified
}

func stringsHostPortEquivalentV1(configured, actual string) bool {
	configuredHost, configuredPort, err := net.SplitHostPort(configured)
	if err != nil {
		return false
	}
	actualHost, actualPort, err := net.SplitHostPort(actual)
	if err != nil || configuredPort != actualPort {
		return false
	}
	return configuredHost == actualHost || configuredHost == "0.0.0.0" || actualHost == "0.0.0.0" || configuredHost == "::" || actualHost == "::"
}

type vectorPartitionOperationsWireRequestV1 struct {
	SchemaVersion int                    `json:"schema_version"`
	Operation     string                 `json:"operation"`
	Search        public.SearchRequestV1 `json:"search,omitempty"`
}

type vectorPartitionOperationsWireResponseV1 struct {
	SchemaVersion int                        `json:"schema_version"`
	Health        *public.OperationsHealthV1 `json:"health,omitempty"`
	Search        *public.SearchResponseV1   `json:"search,omitempty"`
	ErrorCode     public.ErrorCodeV1         `json:"error_code,omitempty"`
	Error         string                     `json:"error,omitempty"`
}

func vectorPartitionOperationsWireErrorCodeV1(err error) public.ErrorCodeV1 {
	var typed *public.ErrorV1
	if errors.As(err, &typed) {
		return typed.Code
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return public.ErrorDeadlineExceededV1
	}
	if errors.Is(err, context.Canceled) {
		return public.ErrorCanceledV1
	}
	return public.ErrorFailedV1
}

type vectorPartitionOperationsTCPServerV1 struct {
	operations  *public.OperationsV1
	listener    net.Listener
	done        chan struct{}
	slots       chan struct{}
	idleTimeout time.Duration
	connections sync.Map
	serving     sync.WaitGroup
}

func (s *vectorPartitionOperationsTCPServerV1) start(ctx context.Context) {
	if s.slots == nil {
		s.slots = make(chan struct{}, vectorPartitionSystemMaxConnectionsV1)
	}
	if s.idleTimeout <= 0 {
		s.idleTimeout = vectorPartitionSystemIdleTimeoutV1
	}
	go func() {
		defer close(s.done)
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}
			select {
			case s.slots <- struct{}{}:
				s.connections.Store(conn, struct{}{})
				s.serving.Add(1)
				go func() {
					defer s.serving.Done()
					defer s.connections.Delete(conn)
					defer func() { <-s.slots }()
					s.serve(ctx, conn)
				}()
			default:
				_ = conn.Close()
			}
		}
	}()
}

func (s *vectorPartitionOperationsTCPServerV1) serve(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 64<<10)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return
		}
		var request vectorPartitionOperationsWireRequestV1
		if err := readVectorPartitionSystemFrameV1(reader, &request); err != nil {
			return
		}
		response := vectorPartitionOperationsWireResponseV1{SchemaVersion: 1}
		if request.SchemaVersion != 1 {
			response.Error = "unsupported system search protocol"
		} else {
			switch request.Operation {
			case "status":
				health, err := s.operations.Status(ctx)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
				} else {
					response.Health = &health
				}
			case "search":
				result, err := s.operations.Search(ctx, request.Search)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
				} else {
					response.Search = &result
				}
			default:
				response.Error = "unsupported system operation"
			}
		}
		if err := writeVectorPartitionSystemFrameV1(conn, response); err != nil {
			return
		}
	}
}

func (s *vectorPartitionOperationsTCPServerV1) Close() error {
	if s == nil {
		return nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.done != nil {
		<-s.done
	}
	s.connections.Range(func(key, _ any) bool {
		_ = key.(net.Conn).Close()
		return true
	})
	s.serving.Wait()
	return nil
}

type vectorPartitionOperationsTCPClientV1 struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialVectorPartitionOperationsV1(ctx context.Context, endpoint string) (*vectorPartitionOperationsTCPClientV1, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return &vectorPartitionOperationsTCPClientV1{conn: conn, r: bufio.NewReaderSize(conn, 64<<10)}, nil
}

func (c *vectorPartitionOperationsTCPClientV1) call(request vectorPartitionOperationsWireRequestV1) (vectorPartitionOperationsWireResponseV1, error) {
	var response vectorPartitionOperationsWireResponseV1
	deadline := time.Now().Add(30 * time.Second)
	if !request.Search.Deadline.IsZero() && request.Search.Deadline.Before(deadline) {
		deadline = request.Search.Deadline
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return response, err
	}
	if err := writeVectorPartitionSystemFrameV1(c.conn, request); err != nil {
		return response, err
	}
	if err := readVectorPartitionSystemFrameV1(c.r, &response); err != nil {
		return response, err
	}
	if response.SchemaVersion != 1 {
		return response, fmt.Errorf("system operations response schema %d", response.SchemaVersion)
	}
	if response.Error != "" {
		cause := errors.New(response.Error)
		switch response.ErrorCode {
		case public.ErrorDeadlineExceededV1:
			cause = fmt.Errorf("%w: %s", context.DeadlineExceeded, response.Error)
		case public.ErrorCanceledV1:
			cause = fmt.Errorf("%w: %s", context.Canceled, response.Error)
		}
		return response, fmt.Errorf("system operations response: %w", &public.ErrorV1{Code: response.ErrorCode, Err: cause})
	}
	return response, nil
}

func (c *vectorPartitionOperationsTCPClientV1) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func writeVectorPartitionSystemFrameV1(w io.Writer, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > vectorPartitionSystemWireMaxBytesV1 {
		return errors.New("system operations frame exceeds bound")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err = w.Write(raw)
	return err
}

func readVectorPartitionSystemFrameV1(r io.Reader, value any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > vectorPartitionSystemWireMaxBytesV1 {
		return errors.New("system operations frame exceeds bound")
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return err
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return err
	}
	return nil
}

func runVectorPartitionSystemSearchV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-search", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var endpoint, requestPath string
	fs.StringVar(&endpoint, "endpoint", "", "production operations TCP endpoint")
	fs.StringVar(&requestPath, "request", "", "bounded JSON search request")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || endpoint == "" || requestPath == "" {
		return errors.New("system-search requires -endpoint, -request, and no positional arguments")
	}
	raw, err := readBoundedRegularFileV1(requestPath, vectorPartitionSystemWireMaxBytesV1)
	if err != nil {
		return err
	}
	var request public.SearchRequestV1
	if err := json.Unmarshal(raw, &request); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := dialVectorPartitionOperationsV1(ctx, endpoint)
	if err != nil {
		return err
	}
	defer client.Close()
	response, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: request})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(response.Search)
}
