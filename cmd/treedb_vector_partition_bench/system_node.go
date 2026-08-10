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
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strconv"
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

type vectorPartitionSystemRuntimeOwnershipV1 struct {
	CPUSet             string `json:"cpu_set"`
	GOMAXPROCS         int    `json:"gomaxprocs"`
	GoMemoryLimitBytes int64  `json:"go_memory_limit_bytes"`
}

type vectorPartitionSystemNodeConfigV1 struct {
	SchemaVersion       int                                      `json:"schema_version"`
	ResultKind          string                                   `json:"result_kind"`
	Assembly            string                                   `json:"assembly"`
	Topology            string                                   `json:"topology"`
	NodeID              string                                   `json:"node_id"`
	DatasetDirectory    string                                   `json:"dataset_directory"`
	DatabaseDirectory   string                                   `json:"database_directory"`
	StateDirectory      string                                   `json:"state_directory"`
	CapabilityKeyPath   string                                   `json:"capability_key_path"`
	PublicListen        string                                   `json:"public_listen,omitempty"`
	ReadyPath           string                                   `json:"ready_path"`
	ProfileDirectory    string                                   `json:"profile_directory,omitempty"`
	LocalGroups         []vectorPartitionSystemLocalGroupV1      `json:"local_groups"`
	Endpoints           map[string]string                        `json:"endpoints"`
	GroupAppliedIndexes map[string]uint64                        `json:"group_applied_indexes,omitempty"`
	RuntimeOwnership    *vectorPartitionSystemRuntimeOwnershipV1 `json:"runtime_ownership,omitempty"`
}

type vectorPartitionSystemGroupReadyV1 struct {
	GroupID                   string `json:"group_id"`
	Endpoint                  string `json:"endpoint"`
	LeaderID                  string `json:"leader_id"`
	AppliedIndex              uint64 `json:"applied_index"`
	ProvesProductionConsensus bool   `json:"proves_production_consensus"`
}

type vectorPartitionSystemNodeReadyV1 struct {
	SchemaVersion      int                                      `json:"schema_version"`
	ResultKind         string                                   `json:"result_kind"`
	Assembly           string                                   `json:"assembly"`
	Topology           string                                   `json:"topology"`
	NodeID             string                                   `json:"node_id"`
	PID                int                                      `json:"pid"`
	PublicEndpoint     string                                   `json:"public_endpoint,omitempty"`
	PublicRoute        string                                   `json:"public_route"`
	ProductionTopology bool                                     `json:"production_topology"`
	M8Loopback         bool                                     `json:"m8_loopback"`
	DatabaseDirectory  string                                   `json:"database_directory"`
	StateDirectory     string                                   `json:"state_directory"`
	SourceRevision     string                                   `json:"source_revision"`
	VCSModified        bool                                     `json:"vcs_modified"`
	ExecutableSHA256   string                                   `json:"executable_sha256"`
	NodeConfigSHA256   string                                   `json:"node_config_sha256"`
	LogicalCPUs        int                                      `json:"logical_cpus"`
	GOMAXPROCS         int                                      `json:"gomaxprocs"`
	GoMemoryLimit      int64                                    `json:"go_memory_limit"`
	EffectiveCPUSet    string                                   `json:"effective_cpu_set,omitempty"`
	RuntimeOwnership   *vectorPartitionSystemRuntimeOwnershipV1 `json:"runtime_ownership,omitempty"`
	ProfileDirectory   string                                   `json:"profile_directory,omitempty"`
	LifecycleState     string                                   `json:"lifecycle_state"`
	Groups             []vectorPartitionSystemGroupReadyV1      `json:"groups"`
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
	NodeID            string                                   `json:"node_id"`
	NodeConfigSHA256  string                                   `json:"node_config_sha256"`
	DatabaseDirectory string                                   `json:"database_directory"`
	StateDirectory    string                                   `json:"state_directory"`
	CapabilityKeyPath string                                   `json:"capability_key_path"`
	ReadyPath         string                                   `json:"ready_path"`
	ProfileDirectory  string                                   `json:"profile_directory,omitempty"`
	PublicListen      string                                   `json:"public_listen,omitempty"`
	LocalGroups       []vectorPartitionSystemLocalGroupV1      `json:"local_groups"`
	RuntimeOwnership  *vectorPartitionSystemRuntimeOwnershipV1 `json:"runtime_ownership,omitempty"`
}

type vectorPartitionSystemNodeV1 struct {
	config         vectorPartitionSystemNodeConfigV1
	assets         *m8ProductionMultiGroupAssetsV1
	production     *nativewire.VectorPartitionProductionNodeV1
	publicListener net.Listener
	publicServer   *vectorPartitionOperationsTCPServerV1
	ready          vectorPartitionSystemNodeReadyV1
}

func runVectorPartitionSystemNodeV1(args []string, stdout io.Writer) (runErr error) {
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
	if err := applyVectorPartitionSystemRuntimeOwnershipV1(config.RuntimeOwnership); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	node, err := openVectorPartitionSystemNodeV1(ctx, config)
	if err != nil {
		return err
	}
	defer node.Close()
	profileCapture, err := startM8ProfileCaptureV1(config.ProfileDirectory)
	if err != nil {
		return err
	}
	profilePublished := false
	defer func() {
		runErr = errors.Join(runErr, m8FinishDirectProfileCaptureV1(profileCapture, profilePublished))
	}()
	if err := publishVectorPartitionSystemNodeReadyV1(config.ReadyPath, node.ready); err != nil {
		return err
	}
	profilePublished = true
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
	owners := make([]*vectorPartitionSystemRuntimeOwnershipV1, 0, len(configs))
	for _, config := range configs {
		owners = append(owners, config.RuntimeOwnership)
	}
	if err := validateVectorPartitionSystemRuntimeOwnershipSetV1(first.Topology, owners); err != nil {
		return evidence, err
	}
	nodes, databases, states := map[string]bool{}, map[string]bool{}, map[string]bool{}
	readyPaths, ownedGroups := map[string]bool{}, map[string]bool{}
	var listeners []string
	addListener := func(listener string) error {
		_, port, err := net.SplitHostPort(listener)
		value, parseErr := strconv.ParseUint(port, 10, 16)
		if err != nil || parseErr != nil || value == 0 {
			return fmt.Errorf("system topology listener %q is invalid", listener)
		}
		for _, existing := range listeners {
			if stringsHostPortEquivalentV1(existing, listener) {
				return errors.New("system topology listeners must be distinct")
			}
		}
		listeners = append(listeners, listener)
		return nil
	}
	var persistentRoots []string
	publicCount := 0
	for _, config := range configs {
		if config.Assembly != vectorPartitionSystemAssemblyV1 || config.Topology != first.Topology || config.DatasetDirectory != first.DatasetDirectory || config.CapabilityKeyPath == "" || config.CapabilityKeyPath != first.CapabilityKeyPath || !maps.Equal(config.Endpoints, first.Endpoints) || !maps.Equal(config.GroupAppliedIndexes, first.GroupAppliedIndexes) {
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
		if vectorPartitionSystemPathsOverlapV1(config.CapabilityKeyPath, config.DatasetDirectory) || vectorPartitionSystemPathsOverlapV1(config.CapabilityKeyPath, config.DatabaseDirectory) || vectorPartitionSystemPathsOverlapV1(config.CapabilityKeyPath, config.StateDirectory) {
			return evidence, errors.New("system topology capability key must be outside dataset and persistent roots")
		}
		persistentRoots = append(persistentRoots, config.DatabaseDirectory, config.StateDirectory)
		nodes[config.NodeID], databases[config.DatabaseDirectory], states[config.StateDirectory], readyPaths[config.ReadyPath] = true, true, true, true
		if config.PublicListen != "" {
			publicCount++
			if err := addListener(config.PublicListen); err != nil {
				return evidence, err
			}
		}
		for _, local := range config.LocalGroups {
			if ownedGroups[local.GroupID] || config.Endpoints[local.GroupID] == "" || !stringsHostPortEquivalentV1(config.Endpoints[local.GroupID], local.Listen) {
				return evidence, errors.New("system topology group ownership or listener binding is invalid")
			}
			if err := addListener(local.Listen); err != nil {
				return evidence, err
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
			NodeID: config.NodeID, NodeConfigSHA256: configSHA, DatabaseDirectory: config.DatabaseDirectory, StateDirectory: config.StateDirectory, CapabilityKeyPath: config.CapabilityKeyPath,
			ReadyPath: config.ReadyPath, ProfileDirectory: config.ProfileDirectory, PublicListen: config.PublicListen, LocalGroups: slices.Clone(config.LocalGroups), RuntimeOwnership: cloneVectorPartitionSystemRuntimeOwnershipV1(config.RuntimeOwnership),
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

func loadVectorPartitionSystemTopologyEvidenceV1(path, endpoint string) (vectorPartitionSystemTopologyEvidenceV1, error) {
	var evidence vectorPartitionSystemTopologyEvidenceV1
	raw, err := readBoundedRegularFileV1(path, maxManifestBytes)
	if err != nil {
		return evidence, err
	}
	if err := json.Unmarshal(raw, &evidence); err != nil {
		return evidence, fmt.Errorf("topology evidence JSON: %w", err)
	}
	if evidence.SchemaVersion != 1 || evidence.ResultKind != vectorPartitionSystemTopologyKindV1 || evidence.Assembly != vectorPartitionSystemAssemblyV1 || evidence.PublicRoute != "vectorpartition.OperationsV1.Search" || evidence.M8Loopback {
		return evidence, errors.New("topology evidence production identity is invalid")
	}
	wantNodes := 4
	if evidence.Topology == "single_daemon_four_group" {
		wantNodes = 1
	} else if evidence.Topology != "native_four_daemon_four_group" && evidence.Topology != "container_four_daemon_four_group" {
		return evidence, errors.New("topology evidence topology is invalid")
	}
	if len(evidence.Nodes) != wantNodes || len(evidence.Endpoints) != 4 {
		return evidence, errors.New("topology evidence node or group count is invalid")
	}
	owners := make([]*vectorPartitionSystemRuntimeOwnershipV1, 0, len(evidence.Nodes))
	for _, node := range evidence.Nodes {
		owners = append(owners, node.RuntimeOwnership)
	}
	if err := validateVectorPartitionSystemRuntimeOwnershipSetV1(evidence.Topology, owners); err != nil {
		return evidence, err
	}
	wantDigest := evidence.TopologyIdentitySHA256
	evidence.TopologyIdentitySHA256 = ""
	canonical, err := json.Marshal(evidence)
	if err != nil {
		return evidence, err
	}
	sum := sha256.Sum256(canonical)
	evidence.TopologyIdentitySHA256 = hex.EncodeToString(sum[:])
	if wantDigest != evidence.TopologyIdentitySHA256 {
		return evidence, errors.New("topology evidence identity digest mismatch")
	}
	publicCount := 0
	for _, node := range evidence.Nodes {
		if node.PublicListen == "" {
			continue
		}
		publicCount++
		if !stringsHostPortEquivalentV1(node.PublicListen, endpoint) {
			return evidence, errors.New("topology evidence public endpoint mismatch")
		}
	}
	if publicCount != 1 {
		return evidence, errors.New("topology evidence requires exactly one public endpoint")
	}
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

func cloneVectorPartitionSystemRuntimeOwnershipV1(ownership *vectorPartitionSystemRuntimeOwnershipV1) *vectorPartitionSystemRuntimeOwnershipV1 {
	if ownership == nil {
		return nil
	}
	copy := *ownership
	return &copy
}

func canonicalVectorPartitionSystemCPUSetV1(value string) (string, []int, error) {
	const maxCPU = 1023
	seen := make(map[int]bool)
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", nil, errors.New("runtime ownership CPU set is empty")
		}
		bounds := strings.Split(part, "-")
		if len(bounds) > 2 {
			return "", nil, fmt.Errorf("runtime ownership CPU range %q is invalid", part)
		}
		first, err := strconv.Atoi(strings.TrimSpace(bounds[0]))
		if err != nil {
			return "", nil, fmt.Errorf("runtime ownership CPU range %q is invalid", part)
		}
		last := first
		if len(bounds) == 2 {
			last, err = strconv.Atoi(strings.TrimSpace(bounds[1]))
			if err != nil {
				return "", nil, fmt.Errorf("runtime ownership CPU range %q is invalid", part)
			}
		}
		if first < 0 || last < first || last > maxCPU {
			return "", nil, fmt.Errorf("runtime ownership CPU range %q is out of range", part)
		}
		for cpu := first; cpu <= last; cpu++ {
			if seen[cpu] {
				return "", nil, fmt.Errorf("runtime ownership CPU %d is duplicated", cpu)
			}
			seen[cpu] = true
		}
	}
	if len(seen) == 0 {
		return "", nil, errors.New("runtime ownership CPU set is empty")
	}
	cpus := make([]int, 0, len(seen))
	for cpu := range seen {
		cpus = append(cpus, cpu)
	}
	sort.Ints(cpus)
	var parts []string
	for start := 0; start < len(cpus); {
		end := start
		for end+1 < len(cpus) && cpus[end+1] == cpus[end]+1 {
			end++
		}
		if end == start {
			parts = append(parts, strconv.Itoa(cpus[start]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", cpus[start], cpus[end]))
		}
		start = end + 1
	}
	return strings.Join(parts, ","), cpus, nil
}

func canonicalVectorPartitionSystemRuntimeOwnershipV1(ownership vectorPartitionSystemRuntimeOwnershipV1) (vectorPartitionSystemRuntimeOwnershipV1, []int, error) {
	canonical, cpus, err := canonicalVectorPartitionSystemCPUSetV1(ownership.CPUSet)
	if err != nil {
		return ownership, nil, err
	}
	ownership.CPUSet = canonical
	if ownership.GOMAXPROCS == 0 {
		ownership.GOMAXPROCS = len(cpus)
	}
	if ownership.GOMAXPROCS < 1 || ownership.GOMAXPROCS > len(cpus) {
		return ownership, nil, errors.New("runtime ownership GOMAXPROCS exceeds its CPU set")
	}
	if ownership.GoMemoryLimitBytes < 1 {
		return ownership, nil, errors.New("runtime ownership Go memory limit is invalid")
	}
	return ownership, cpus, nil
}

func validateVectorPartitionSystemRuntimeOwnershipSetV1(topology string, owners []*vectorPartitionSystemRuntimeOwnershipV1) error {
	if topology == "single_daemon_four_group" {
		if len(owners) == 1 && owners[0] != nil {
			_, _, err := canonicalVectorPartitionSystemRuntimeOwnershipV1(*owners[0])
			return err
		}
		return nil
	}
	seen := make(map[int]bool)
	for _, owner := range owners {
		if owner == nil {
			return errors.New("multi-daemon system topology requires explicit runtime ownership")
		}
		canonical, cpus, err := canonicalVectorPartitionSystemRuntimeOwnershipV1(*owner)
		if err != nil {
			return err
		}
		if canonical != *owner {
			return errors.New("runtime ownership must use canonical CPU-set and Go limits")
		}
		for _, cpu := range cpus {
			if seen[cpu] {
				return fmt.Errorf("system topology runtime CPU %d is owned by multiple nodes", cpu)
			}
			seen[cpu] = true
		}
	}
	return nil
}

func applyVectorPartitionSystemRuntimeOwnershipV1(ownership *vectorPartitionSystemRuntimeOwnershipV1) error {
	if ownership == nil {
		return nil
	}
	canonical, cpus, err := canonicalVectorPartitionSystemRuntimeOwnershipV1(*ownership)
	if err != nil {
		return err
	}
	if canonical != *ownership {
		return errors.New("runtime ownership must be canonical before application")
	}
	if err := applyVectorPartitionSystemRuntimeOwnershipPlatformV1(cpus); err != nil {
		return err
	}
	runtime.GOMAXPROCS(canonical.GOMAXPROCS)
	debug.SetMemoryLimit(canonical.GoMemoryLimitBytes)
	return verifyVectorPartitionSystemRuntimeOwnershipV1(&canonical)
}

func verifyVectorPartitionSystemRuntimeOwnershipV1(ownership *vectorPartitionSystemRuntimeOwnershipV1) error {
	if ownership == nil {
		return nil
	}
	actualCPUSet := vectorPartitionSystemEffectiveCPUSetV1()
	canonicalCPUSet, _, err := canonicalVectorPartitionSystemCPUSetV1(actualCPUSet)
	if err != nil {
		return fmt.Errorf("runtime ownership is unsupported or unreadable: %w", err)
	}
	if canonicalCPUSet != ownership.CPUSet || runtime.GOMAXPROCS(0) != ownership.GOMAXPROCS || debug.SetMemoryLimit(-1) != ownership.GoMemoryLimitBytes {
		return errors.New("effective runtime ownership does not match its declaration")
	}
	return nil
}

func vectorPartitionSystemRuntimeOwnershipMatchesStatsV1(ownership *vectorPartitionSystemRuntimeOwnershipV1, stats nativewire.VectorPartitionProcessRuntimeStatsV1) bool {
	if ownership == nil {
		return true
	}
	canonicalCPUSet, _, err := canonicalVectorPartitionSystemCPUSetV1(stats.EffectiveCPUSet)
	return err == nil && canonicalCPUSet == ownership.CPUSet && stats.GOMAXPROCS == ownership.GOMAXPROCS && stats.GoMemoryLimitBytes == ownership.GoMemoryLimitBytes && stats.LogicalCPUs > 0
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
	if config.RuntimeOwnership != nil {
		canonical, _, err := canonicalVectorPartitionSystemRuntimeOwnershipV1(*config.RuntimeOwnership)
		if err != nil {
			return config, err
		}
		config.RuntimeOwnership = &canonical
	} else if config.Topology != "single_daemon_four_group" {
		return config, errors.New("multi-daemon system node requires explicit runtime ownership")
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
	if config.CapabilityKeyPath, pathErr = canonical(config.CapabilityKeyPath); pathErr != nil {
		return config, fmt.Errorf("system node capability key path: %w", pathErr)
	}
	if config.ReadyPath, pathErr = canonical(config.ReadyPath); pathErr != nil {
		return config, fmt.Errorf("system node ready path: %w", pathErr)
	}
	if config.ProfileDirectory != "" {
		if config.ProfileDirectory, pathErr = canonical(config.ProfileDirectory); pathErr != nil {
			return config, fmt.Errorf("system node profile directory: %w", pathErr)
		}
		if config.ProfileDirectory == config.StateDirectory || !vectorPartitionSystemPathContainsV1(config.StateDirectory, config.ProfileDirectory) {
			return config, errors.New("system node profile directory must be inside its state directory")
		}
	}
	return config, nil
}

func openVectorPartitionSystemNodeV1(ctx context.Context, config vectorPartitionSystemNodeConfigV1) (_ *vectorPartitionSystemNodeV1, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := verifyVectorPartitionSystemRuntimeOwnershipV1(config.RuntimeOwnership); err != nil {
		return nil, err
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
	capabilityKey, err := readVectorPartitionSystemCapabilityKeyV1(config.CapabilityKeyPath)
	if err != nil {
		return nil, err
	}
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
	configSHA, err := vectorPartitionSystemNodeConfigSHA256V1(config)
	if err != nil {
		return nil, err
	}
	topologyDigest, err := vectorPartitionSystemServingTopologyDigestV1(config)
	if err != nil {
		return nil, err
	}
	authorizationDigest := sha256.Sum256([]byte("vector-partition-no-authorization-overlay-v1"))
	node.production, err = nativewire.NewVectorPartitionProductionNodeV1(ctx, nativewire.VectorPartitionProductionNodeOptionsV1{
		Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(), AssetSetDigests: assets.assetSetDigests,
		GroupAppliedIndexes: config.GroupAppliedIndexes, Database: "default", Catalog: "default", Endpoints: config.Endpoints,
		LocalShards: shards, RequestBase: requestBase, NodeID: config.NodeID, EndpointIdentity: configSHA, RuntimeStats: vectorPartitionSystemProcessRuntimeStatsV1,
		TopologyDigest: topologyDigest, AuthorizationOverlayDigest: hex.EncodeToString(authorizationDigest[:]), IndexedThrough: assets.manifest.SourceRowCount, StrictCapabilityKey: capabilityKey,
		MaxPinnedSessions: 64, MaxPinnedSessionAge: 2 * time.Minute, MaxRetainedSnapshots: 2,
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
		node.publicServer = &vectorPartitionOperationsTCPServerV1{operations: operations, nodeConfigSHA256: configSHA, listener: node.publicListener, done: make(chan struct{})}
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
	node.ready = vectorPartitionSystemNodeReadyV1{
		SchemaVersion: 1, ResultKind: vectorPartitionSystemNodeReadyKindV1, Assembly: config.Assembly, Topology: config.Topology, NodeID: config.NodeID,
		PID: os.Getpid(), PublicEndpoint: publicEndpoint, PublicRoute: "vectorpartition.OperationsV1.Search", ProductionTopology: true, M8Loopback: false,
		DatabaseDirectory: config.DatabaseDirectory, StateDirectory: config.StateDirectory, SourceRevision: revision, VCSModified: modified,
		ExecutableSHA256: executableSHA, NodeConfigSHA256: configSHA,
		LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoMemoryLimit: debug.SetMemoryLimit(-1), EffectiveCPUSet: vectorPartitionSystemEffectiveCPUSetV1(), ProfileDirectory: config.ProfileDirectory,
		RuntimeOwnership: cloneVectorPartitionSystemRuntimeOwnershipV1(config.RuntimeOwnership),
		LifecycleState:   "active",
	}
	for _, evidence := range node.production.GroupEvidenceV1() {
		node.ready.Groups = append(node.ready.Groups, vectorPartitionSystemGroupReadyV1{GroupID: evidence.GroupID, Endpoint: config.Endpoints[evidence.GroupID], LeaderID: evidence.LeaderID, AppliedIndex: evidence.AppliedIndex, ProvesProductionConsensus: true})
	}
	return node, nil
}

func readVectorPartitionSystemCapabilityKeyV1(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() != sha256.Size {
		return nil, errors.New("system node capability key must be a 32-byte mode-0600 regular file")
	}
	key, err := readBoundedRegularFileV1(path, sha256.Size)
	if err != nil || len(key) != sha256.Size {
		return nil, errors.New("system node capability key must be a stable 32-byte regular file")
	}
	return key, nil
}

func vectorPartitionSystemServingTopologyDigestV1(config vectorPartitionSystemNodeConfigV1) (string, error) {
	raw, err := json.Marshal(struct {
		Topology            string
		DatasetDirectory    string
		Endpoints           map[string]string
		GroupAppliedIndexes map[string]uint64
	}{config.Topology, config.DatasetDirectory, config.Endpoints, config.GroupAppliedIndexes})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func vectorPartitionSystemEffectiveCPUSetV1() string {
	entries, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return ""
	}
	var effective string
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join("/proc/self/task", entry.Name(), "status"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return ""
		}
		current := ""
		for _, line := range strings.Split(string(raw), "\n") {
			if value, ok := strings.CutPrefix(line, "Cpus_allowed_list:"); ok {
				current = strings.TrimSpace(value)
				break
			}
		}
		if current == "" || (effective != "" && current != effective) {
			return ""
		}
		effective = current
	}
	return effective
}

type vectorPartitionSystemPeakRSSV1 struct {
	sync.Mutex
	bytes uint64
}

func (p *vectorPartitionSystemPeakRSSV1) observe(bytes uint64) uint64 {
	p.Lock()
	defer p.Unlock()
	p.bytes = max(p.bytes, bytes)
	return p.bytes
}

var vectorPartitionSystemProcessPeakRSSV1 vectorPartitionSystemPeakRSSV1

func vectorPartitionSystemProcessRuntimeStatsV1() nativewire.VectorPartitionProcessRuntimeStatsV1 {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	stats := nativewire.VectorPartitionProcessRuntimeStatsV1{
		SampleUnixNano: uint64(time.Now().UnixNano()), HeapAllocBytes: memory.HeapAlloc, HeapObjects: memory.HeapObjects,
		TotalAllocBytes: memory.TotalAlloc, Mallocs: memory.Mallocs, Frees: memory.Frees, NumGC: uint64(memory.NumGC), PauseTotalNanos: memory.PauseTotalNs, Goroutines: uint64(runtime.NumGoroutine()),
		LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoMemoryLimitBytes: debug.SetMemoryLimit(-1), EffectiveCPUSet: vectorPartitionSystemEffectiveCPUSetV1(),
	}
	stats.CPUTimeNanos, stats.RunQueueDelayNanos, stats.Timeslices, stats.VoluntaryContextSwitches, stats.NonvoluntaryContextSwitches = vectorPartitionSystemKernelRuntimeStatsV1()
	if raw, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				continue
			}
			switch fields[0] {
			case "VmRSS:":
				stats.RSSBytes = value * 1024
			case "VmHWM:":
				stats.PeakRSSBytes = value * 1024
			}
		}
	}
	stats.PeakRSSBytes = vectorPartitionSystemProcessPeakRSSV1.observe(stats.PeakRSSBytes)
	return stats
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
	if configuredHost == actualHost || configuredHost == "" || actualHost == "" || configuredHost == "0.0.0.0" || actualHost == "0.0.0.0" || configuredHost == "::" || actualHost == "::" {
		return true
	}
	configuredIPs, configuredErr := net.LookupIP(configuredHost)
	actualIPs, actualErr := net.LookupIP(actualHost)
	if configuredErr != nil || actualErr != nil {
		return false
	}
	for _, configuredIP := range configuredIPs {
		for _, actualIP := range actualIPs {
			if configuredIP.Equal(actualIP) {
				return true
			}
		}
	}
	return false
}

type vectorPartitionOperationsWireRequestV1 struct {
	SchemaVersion int                                `json:"schema_version"`
	Operation     string                             `json:"operation"`
	Search        public.SearchRequestV1             `json:"search,omitempty"`
	FastOptions   *public.FastSearchOptionsV1        `json:"fast_options,omitempty"`
	PinOptions    *public.PinSearchSnapshotOptionsV1 `json:"pin_options,omitempty"`
}

type vectorPartitionOperationsWireResponseV1 struct {
	SchemaVersion    int                          `json:"schema_version"`
	NodeConfigSHA256 string                       `json:"node_config_sha256,omitempty"`
	Health           *public.OperationsHealthV1   `json:"health,omitempty"`
	Search           *public.SearchResponseV1     `json:"search,omitempty"`
	FastEvidence     *public.FastSearchEvidenceV1 `json:"fast_evidence,omitempty"`
	ErrorCode        public.ErrorCodeV1           `json:"error_code,omitempty"`
	Error            string                       `json:"error,omitempty"`
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
	operations       *public.OperationsV1
	nodeConfigSHA256 string
	listener         net.Listener
	done             chan struct{}
	slots            chan struct{}
	idleTimeout      time.Duration
	connections      sync.Map
	serving          sync.WaitGroup
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
	var pinned *public.PinnedSearchSnapshotV1
	defer func() {
		if pinned != nil {
			_ = pinned.Close()
		}
	}()
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
					response.NodeConfigSHA256 = s.nodeConfigSHA256
				}
			case "search":
				result, err := s.operations.Search(ctx, request.Search)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
				} else {
					response.Search = &result
				}
			case "search_fast":
				if request.FastOptions == nil {
					response.Error, response.ErrorCode = "fast search options are required", public.ErrorInvalidRequestV1
					break
				}
				result, evidence, err := s.operations.SearchFast(ctx, request.Search, *request.FastOptions)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
				} else {
					response.Search, response.FastEvidence = &result, &evidence
				}
			case "pin_search_snapshot":
				if request.PinOptions == nil || pinned != nil {
					response.Error, response.ErrorCode = "one pin options object and no existing pin are required", public.ErrorInvalidRequestV1
					break
				}
				var err error
				pinned, err = s.operations.PinSearchSnapshot(ctx, *request.PinOptions)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
					pinned = nil
				} else {
					evidence := pinned.Evidence()
					response.FastEvidence = &evidence
				}
			case "search_pinned":
				if pinned == nil {
					response.Error, response.ErrorCode = "pinned search snapshot is unavailable", public.ErrorUnavailableV1
					break
				}
				result, err := pinned.Search(ctx, request.Search)
				if err != nil {
					response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
				} else {
					response.Search = &result
				}
			case "close_pinned_snapshot":
				if pinned != nil {
					err := pinned.Close()
					pinned = nil
					if err != nil {
						response.Error, response.ErrorCode = err.Error(), vectorPartitionOperationsWireErrorCodeV1(err)
					}
				}
			default:
				response.Error = "unsupported system operation"
			}
		}
		if err := conn.SetWriteDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			return
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

type vectorPartitionSystemFrameTimingV1 struct {
	EncodeNanos, WriteNanos, ReadNanos, DecodeNanos, TotalNanos uint64
	RequestBytes, ResponseBytes                                 uint64
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
	response, _, err := c.callWithTiming(request)
	return response, err
}

func (c *vectorPartitionOperationsTCPClientV1) callWithTiming(request vectorPartitionOperationsWireRequestV1) (response vectorPartitionOperationsWireResponseV1, timing vectorPartitionSystemFrameTimingV1, err error) {
	started := time.Now()
	defer func() { timing.TotalNanos = uint64(time.Since(started)) }()
	deadline := time.Now().Add(30 * time.Second)
	if !request.Search.Deadline.IsZero() && request.Search.Deadline.Before(deadline) {
		deadline = request.Search.Deadline
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return response, timing, err
	}
	encodeNanos, writeNanos, requestBytes, err := writeVectorPartitionSystemFrameMeasuredV1(c.conn, request)
	timing.EncodeNanos, timing.WriteNanos, timing.RequestBytes = encodeNanos, writeNanos, requestBytes
	if err != nil {
		return response, timing, err
	}
	readNanos, decodeNanos, responseBytes, err := readVectorPartitionSystemFrameMeasuredV1(c.r, &response)
	timing.ReadNanos, timing.DecodeNanos, timing.ResponseBytes = readNanos, decodeNanos, responseBytes
	if err != nil {
		return response, timing, err
	}
	if response.SchemaVersion != 1 {
		return response, timing, fmt.Errorf("system operations response schema %d", response.SchemaVersion)
	}
	if response.Error != "" {
		cause := errors.New(response.Error)
		switch response.ErrorCode {
		case public.ErrorDeadlineExceededV1:
			cause = fmt.Errorf("%w: %s", context.DeadlineExceeded, response.Error)
		case public.ErrorCanceledV1:
			cause = fmt.Errorf("%w: %s", context.Canceled, response.Error)
		}
		return response, timing, fmt.Errorf("system operations response: %w", &public.ErrorV1{Code: response.ErrorCode, Err: cause})
	}
	return response, timing, nil
}

func (c *vectorPartitionOperationsTCPClientV1) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func writeVectorPartitionSystemFrameV1(w io.Writer, value any) error {
	_, _, _, err := writeVectorPartitionSystemFrameMeasuredV1(w, value)
	return err
}

func writeVectorPartitionSystemFrameMeasuredV1(w io.Writer, value any) (encodeNanos, writeNanos, bytes uint64, err error) {
	started := time.Now()
	raw, err := json.Marshal(value)
	encodeNanos = uint64(time.Since(started))
	if err != nil {
		return encodeNanos, 0, 0, err
	}
	if len(raw) == 0 || len(raw) > vectorPartitionSystemWireMaxBytesV1 {
		return encodeNanos, 0, 0, errors.New("system operations frame exceeds bound")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	started = time.Now()
	if err := vectorPartitionSystemWriteAllV1(w, header[:]); err != nil {
		return encodeNanos, uint64(time.Since(started)), 0, err
	}
	if err := vectorPartitionSystemWriteAllV1(w, raw); err != nil {
		return encodeNanos, uint64(time.Since(started)), 0, err
	}
	return encodeNanos, uint64(time.Since(started)), uint64(len(raw) + len(header)), nil
}

func vectorPartitionSystemWriteAllV1(w io.Writer, raw []byte) error {
	for len(raw) > 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}

func readVectorPartitionSystemFrameV1(r io.Reader, value any) error {
	_, _, _, err := readVectorPartitionSystemFrameMeasuredV1(r, value)
	return err
}

func readVectorPartitionSystemFrameMeasuredV1(r io.Reader, value any) (readNanos, decodeNanos, bytes uint64, err error) {
	started := time.Now()
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return uint64(time.Since(started)), 0, 0, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > vectorPartitionSystemWireMaxBytesV1 {
		return uint64(time.Since(started)), 0, 0, errors.New("system operations frame exceeds bound")
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return uint64(time.Since(started)), 0, 0, err
	}
	readNanos = uint64(time.Since(started))
	started = time.Now()
	if err := json.Unmarshal(raw, value); err != nil {
		return readNanos, uint64(time.Since(started)), 0, err
	}
	return readNanos, uint64(time.Since(started)), uint64(size) + uint64(len(header)), nil
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
