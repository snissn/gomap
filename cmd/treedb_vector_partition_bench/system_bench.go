package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	vectorPartitionSystemSearchStrictV1 = "strict"
	vectorPartitionSystemSearchFastV1   = "fast"
	vectorPartitionSystemSearchPinnedV1 = "pinned"
)

type vectorPartitionSystemBenchMetricsV1 struct {
	Queries          int     `json:"queries"`
	CompletedQueries int     `json:"completed_queries"`
	ResultCount      int     `json:"result_count"`
	Errors           int     `json:"errors"`
	Timeouts         int     `json:"timeouts"`
	RecallAt10       float64 `json:"recall_at_10"`
	QPS              float64 `json:"qps"`
	P50Nanos         uint64  `json:"p50_nanos"`
	P95Nanos         uint64  `json:"p95_nanos"`
	P99Nanos         uint64  `json:"p99_nanos"`
}

type vectorPartitionSystemBenchCellV1 struct {
	Status           string                               `json:"status"`
	Error            string                               `json:"error,omitempty"`
	Budget           map[string]int                       `json:"budget"`
	Concurrency      int                                  `json:"concurrency"`
	Generation       public.GenerationIDV1                `json:"generation"`
	Metrics          vectorPartitionSystemBenchMetricsV1  `json:"metrics"`
	Counters         map[string]uint64                    `json:"counters"`
	Timings          map[string]uint64                    `json:"timings"`
	CatalogReads     vectorPartitionSystemCatalogReadsV1  `json:"catalog_reads"`
	Runtime          []vectorPartitionSystemRuntimeNodeV1 `json:"runtime"`
	ElapsedNanos     uint64                               `json:"elapsed_nanos"`
	TotalNanos       []uint64                             `json:"total_nanos"`
	SearchMode       string                               `json:"search_mode"`
	FastEvidence     *public.FastSearchEvidenceV1         `json:"fast_evidence,omitempty"`
	MinIndexAgeNanos uint64                               `json:"min_index_age_nanos,omitempty"`
	MaxIndexAgeNanos uint64                               `json:"max_index_age_nanos,omitempty"`
}

type vectorPartitionSystemCatalogReadNodeV1 struct {
	NodeConfigSHA256 string                                                       `json:"node_config_sha256"`
	Before           nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1 `json:"before"`
	After            nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1 `json:"after"`
	Delta            nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1 `json:"delta"`
}

type vectorPartitionSystemCatalogReadsV1 struct {
	Nodes []vectorPartitionSystemCatalogReadNodeV1                     `json:"nodes"`
	Total nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1 `json:"total"`
}

type vectorPartitionSystemNodeObservationV1 struct {
	Catalog nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1
	Runtime nativewire.VectorPartitionProcessRuntimeStatsV1
}

type vectorPartitionSystemRuntimeNodeV1 struct {
	NodeConfigSHA256 string                                          `json:"node_config_sha256"`
	Before           nativewire.VectorPartitionProcessRuntimeStatsV1 `json:"before"`
	After            nativewire.VectorPartitionProcessRuntimeStatsV1 `json:"after"`
}

type vectorPartitionSystemCatalogSnapshotV1 func(context.Context) (map[string]vectorPartitionSystemNodeObservationV1, error)

type vectorPartitionSystemBenchResultV1 struct {
	SchemaVersion          int                                `json:"schema_version"`
	ResultKind             string                             `json:"result_kind"`
	Endpoint               string                             `json:"endpoint"`
	Topology               string                             `json:"topology"`
	TopologyIdentitySHA256 string                             `json:"topology_identity_sha256"`
	DatasetChecksum        string                             `json:"dataset_checksum"`
	TruthArtifactSHA256    string                             `json:"truth_artifact_sha256"`
	TopK                   int                                `json:"top_k"`
	EfSearch               int                                `json:"ef_search"`
	WarmupQueries          int                                `json:"warmup_queries"`
	SearchMode             string                             `json:"search_mode"`
	MaxIndexAgeNanos       uint64                             `json:"max_index_age_nanos,omitempty"`
	MaxSessionAgeNanos     uint64                             `json:"max_session_age_nanos,omitempty"`
	StartedAt              time.Time                          `json:"started_at"`
	CompletedAt            time.Time                          `json:"completed_at"`
	Cells                  []vectorPartitionSystemBenchCellV1 `json:"cells"`
}

func runVectorPartitionSystemBenchV1(args []string, stdout io.Writer) error {
	return runVectorPartitionSystemBenchWithCellV1(args, stdout, vectorPartitionSystemBenchCell, nativewire.ProbeVectorPartitionShardEndpointV1)
}

func runVectorPartitionSystemBenchWithCellV1(args []string, stdout io.Writer, runCell func(context.Context, string, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int, string, time.Duration, time.Duration, vectorPartitionSystemCatalogSnapshotV1) (vectorPartitionSystemBenchCellV1, error), probe func(context.Context, string) (nativewire.VectorPartitionShardEndpointIdentityV1, error)) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-bench", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var endpoint, topologyPath, dataset, truthCache, truthSHA, probeText, concurrencyText, out, searchMode string
	var topK, efSearch, warmup int
	var maxIndexAge, maxSessionAge time.Duration
	fs.StringVar(&endpoint, "endpoint", "", "production operations TCP endpoint")
	fs.StringVar(&topologyPath, "topology", "", "checked production topology evidence JSON")
	fs.StringVar(&dataset, "dataset", "", "fixture manifest directory")
	fs.StringVar(&truthCache, "truth-cache", "", "trusted truth-cache directory")
	fs.StringVar(&truthSHA, "truth-cache-sha256", "", "trusted truth-cache artifact SHA256")
	fs.StringVar(&probeText, "probes", "", "ordered comma-separated probe budgets")
	fs.StringVar(&concurrencyText, "concurrency", "", "ordered comma-separated concurrency values")
	fs.StringVar(&out, "out", "", "exclusive result JSON path")
	fs.IntVar(&topK, "top-k", 10, "neighbors per query")
	fs.IntVar(&efSearch, "ef-search", 128, "partition-local HNSW ef-search")
	fs.IntVar(&warmup, "warmup", 1000, "warmup queries per cell")
	fs.StringVar(&searchMode, "search-mode", vectorPartitionSystemSearchStrictV1, "strict, fast, or pinned search shape")
	fs.DurationVar(&maxIndexAge, "max-index-age", time.Hour, "maximum published index age for fast and pinned search")
	fs.DurationVar(&maxSessionAge, "max-session-age", 2*time.Minute, "maximum pinned session age")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || endpoint == "" || topologyPath == "" || dataset == "" || truthCache == "" || truthSHA == "" || probeText == "" || concurrencyText == "" || out == "" || topK != 10 || efSearch < topK || warmup < 0 || !validVectorPartitionSystemSearchModeV1(searchMode) || searchMode != vectorPartitionSystemSearchStrictV1 && maxIndexAge <= 0 || searchMode == vectorPartitionSystemSearchPinnedV1 && maxSessionAge <= 0 {
		return errors.New("system-bench requires bounded endpoint, topology, dataset, truth, probes, concurrency, and output")
	}
	canonicalTopology, err := m8CanonicalPathV1(topologyPath)
	if err != nil {
		return fmt.Errorf("system-bench topology: %w", err)
	}
	topology, err := loadVectorPartitionSystemTopologyEvidenceV1(canonicalTopology, endpoint)
	if err != nil {
		return fmt.Errorf("system-bench topology: %w", err)
	}
	canonicalDataset, err := m8CanonicalPathV1(dataset)
	if err != nil {
		return fmt.Errorf("system-bench dataset: %w", err)
	}
	topologyDataset, err := m8CanonicalPathV1(topology.DatasetDirectory)
	if err != nil {
		return fmt.Errorf("system-bench topology dataset: %w", err)
	}
	if canonicalDataset != topologyDataset {
		return errors.New("system-bench dataset does not match checked topology")
	}
	dataset = canonicalDataset
	publicNodeConfigSHA := ""
	for _, node := range topology.Nodes {
		if node.PublicListen != "" && stringsHostPortEquivalentV1(node.PublicListen, endpoint) {
			publicNodeConfigSHA = node.NodeConfigSHA256
			break
		}
	}
	if !m8SHA256V1(publicNodeConfigSHA) {
		return errors.New("system-bench checked topology public node identity is invalid")
	}
	if err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(context.Background(), topology, probe); err != nil {
		return fmt.Errorf("system-bench live topology: %w", err)
	}
	probes, err := vectorPartitionSystemPositiveListV1(probeText)
	if err != nil {
		return fmt.Errorf("system-bench probes: %w", err)
	}
	concurrency, err := vectorPartitionSystemPositiveListV1(concurrencyText)
	if err != nil {
		return fmt.Errorf("system-bench concurrency: %w", err)
	}
	if len(probes)*len(concurrency) > 64 {
		return errors.New("system-bench matrix exceeds 64 cells")
	}
	for _, value := range probes {
		if value > 16 {
			return errors.New("system-bench probes exceed 16")
		}
	}
	for _, value := range concurrency {
		if value > 64 {
			return errors.New("system-bench concurrency exceeds 64")
		}
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	_, queries64 := fixtureData(fixture)
	queries := make([][]float32, len(queries64))
	for i := range queries64 {
		queries[i] = make([]float32, len(queries64[i]))
		for d, value := range queries64[i] {
			queries[i][d] = float32(value)
		}
	}
	truthPath := m8TruthCacheArtifactPathV1(truthCache, m8TruthCacheIdentityV1(fixture, topK))
	truth, artifactSHA, err := m8ReadTruthCacheV1(truthPath, fixture, len(queries), topK, uint64(fixture.Vectors), truthSHA)
	if err != nil {
		return fmt.Errorf("system-bench truth: %w", err)
	}
	result := vectorPartitionSystemBenchResultV1{SchemaVersion: 1, ResultKind: "vector_partition_system_bench_v1", Endpoint: endpoint, Topology: topology.Topology, TopologyIdentitySHA256: topology.TopologyIdentitySHA256, DatasetChecksum: fixture.Checksum, TruthArtifactSHA256: artifactSHA, TopK: topK, EfSearch: efSearch, WarmupQueries: warmup, SearchMode: searchMode, StartedAt: time.Now().UTC()}
	if searchMode != vectorPartitionSystemSearchStrictV1 {
		result.MaxIndexAgeNanos = uint64(maxIndexAge)
	}
	if searchMode == vectorPartitionSystemSearchPinnedV1 {
		result.MaxSessionAgeNanos = uint64(maxSessionAge)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, probes := range probes {
		for _, workers := range concurrency {
			if err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(ctx, topology, probe); err != nil {
				return fmt.Errorf("system-bench live topology before probes=%d concurrency=%d: %w", probes, workers, err)
			}
			snapshot := func(ctx context.Context) (map[string]vectorPartitionSystemNodeObservationV1, error) {
				return vectorPartitionSystemCatalogReadSnapshotV1(ctx, topology, probe)
			}
			cell, runErr := runCell(ctx, endpoint, publicNodeConfigSHA, queries, truth, topK, probes, efSearch, workers, warmup, searchMode, maxIndexAge, maxSessionAge, snapshot)
			if runErr == nil {
				if err := validateVectorPartitionSystemLiveEndpointsWithProbeV1(ctx, topology, probe); err != nil {
					runErr = fmt.Errorf("live topology after cell: %w", err)
				}
			}
			if runErr != nil {
				cell.Status = "failed"
				cell.Error = runErr.Error()
				cell.Metrics.Errors++
				if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runErr, os.ErrDeadlineExceeded) {
					cell.Metrics.Timeouts++
				}
				result.Cells = append(result.Cells, cell)
				result.CompletedAt = time.Now().UTC()
				writeErr := writeVectorPartitionSystemJSONExclusiveV1(out, result)
				if writeErr != nil {
					return errors.Join(fmt.Errorf("system-bench probes=%d concurrency=%d: %w", probes, workers, runErr), fmt.Errorf("retain failed system-bench result: %w", writeErr))
				}
				return fmt.Errorf("system-bench probes=%d concurrency=%d failed result retained at %s: %w", probes, workers, out, runErr)
			}
			result.Cells = append(result.Cells, cell)
		}
	}
	result.CompletedAt = time.Now().UTC()
	if !result.CompletedAt.After(result.StartedAt) {
		return errors.New("system-bench completion timestamp is invalid")
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, result); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "result=%s cells=%d\n", out, len(result.Cells))
	return err
}

func validateVectorPartitionSystemLiveEndpointsWithProbeV1(ctx context.Context, topology vectorPartitionSystemTopologyEvidenceV1, probe func(context.Context, string) (nativewire.VectorPartitionShardEndpointIdentityV1, error)) error {
	_, err := vectorPartitionSystemCatalogReadSnapshotV1(ctx, topology, probe)
	return err
}

func vectorPartitionSystemCatalogReadSnapshotV1(ctx context.Context, topology vectorPartitionSystemTopologyEvidenceV1, probe func(context.Context, string) (nativewire.VectorPartitionShardEndpointIdentityV1, error)) (map[string]vectorPartitionSystemNodeObservationV1, error) {
	ownedGroups := make(map[string]bool, len(topology.Endpoints))
	for _, node := range topology.Nodes {
		for _, group := range node.LocalGroups {
			endpoint, ok := topology.Endpoints[group.GroupID]
			if !ok || ownedGroups[group.GroupID] || !stringsHostPortEquivalentV1(endpoint, group.Listen) {
				return nil, errors.New("system-bench checked topology group ownership or listener binding is invalid")
			}
			ownedGroups[group.GroupID] = true
		}
	}
	if len(ownedGroups) != len(topology.Endpoints) {
		return nil, errors.New("system-bench checked topology does not own every endpoint")
	}
	snapshot := make(map[string]vectorPartitionSystemNodeObservationV1, len(topology.Nodes))
	for _, node := range topology.Nodes {
		for _, group := range node.LocalGroups {
			identity, err := probe(ctx, group.Listen)
			if err != nil {
				return nil, fmt.Errorf("node %q group %q: %w", node.NodeID, group.GroupID, err)
			}
			if identity.GroupID != group.GroupID || identity.InstanceIdentity != node.NodeConfigSHA256 {
				return nil, fmt.Errorf("node %q group %q live endpoint identity does not match checked topology", node.NodeID, group.GroupID)
			}
			if !vectorPartitionSystemRuntimeOwnershipMatchesStatsV1(node.RuntimeOwnership, identity.ProcessRuntimeStats) {
				return nil, fmt.Errorf("node %q effective runtime ownership does not match checked topology", node.NodeID)
			}
			if retained, ok := snapshot[identity.InstanceIdentity]; ok && !reflect.DeepEqual(retained.Catalog, identity.CatalogMetaReadStats) {
				return nil, fmt.Errorf("node %q publishes inconsistent catalog read statistics", node.NodeID)
			}
			if _, ok := snapshot[identity.InstanceIdentity]; !ok {
				snapshot[identity.InstanceIdentity] = vectorPartitionSystemNodeObservationV1{Catalog: identity.CatalogMetaReadStats, Runtime: identity.ProcessRuntimeStats}
			}
		}
	}
	if len(snapshot) != len(topology.Nodes) {
		return nil, errors.New("system-bench catalog read snapshot does not cover every node")
	}
	return snapshot, nil
}

func vectorPartitionSystemBenchCell(ctx context.Context, endpoint, wantNodeConfigSHA string, queries [][]float32, truth [][]m8CanonicalResultV1, topK, probes, efSearch, workers, warmup int, searchMode string, maxIndexAge, maxSessionAge time.Duration, snapshot vectorPartitionSystemCatalogSnapshotV1) (vectorPartitionSystemBenchCellV1, error) {
	cell := vectorPartitionSystemBenchCellV1{Status: "valid", Budget: map[string]int{"probes": probes}, Concurrency: workers, Metrics: vectorPartitionSystemBenchMetricsV1{Queries: len(queries)}, SearchMode: searchMode}
	clients := make([]*vectorPartitionOperationsTCPClientV1, workers)
	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()
	for i := range clients {
		client, err := dialVectorPartitionOperationsV1(ctx, endpoint)
		if err != nil {
			return cell, err
		}
		clients[i] = client
	}
	status, err := clients[0].call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "status"})
	if err != nil {
		return cell, fmt.Errorf("system-bench status: %w", err)
	}
	if status.Health == nil || !status.Health.Ready {
		return cell, errors.New("system-bench status is not ready")
	}
	if status.NodeConfigSHA256 != wantNodeConfigSHA {
		return cell, errors.New("system-bench live node config identity does not match checked topology")
	}
	generation := status.Health.Generation
	cell.Generation = generation
	fastOptions := public.FastSearchOptionsV1{MaxIndexAge: maxIndexAge}
	pinOptions := public.PinSearchSnapshotOptionsV1{FastSearchOptionsV1: fastOptions, MaxSessionAge: maxSessionAge}
	var fastEvidence *public.FastSearchEvidenceV1
	minIndexAge, maxIndexAgeObserved := uint64(math.MaxUint64), uint64(0)
	observeEvidence := func(evidence *public.FastSearchEvidenceV1) error {
		if evidence == nil || evidence.Generation != generation || evidence.IndexAge < 0 || evidence.IndexAge > maxIndexAge || evidence.PublishedAt.IsZero() || evidence.TopologyDigest == "" || evidence.AuthorizationOverlayDigest == "" {
			return errors.New("system-bench fast search evidence is invalid")
		}
		if fastEvidence == nil {
			copy := *evidence
			fastEvidence = &copy
		} else if evidence.Generation != fastEvidence.Generation || evidence.IndexedThrough != fastEvidence.IndexedThrough || !evidence.PublishedAt.Equal(fastEvidence.PublishedAt) || evidence.TopologyDigest != fastEvidence.TopologyDigest || evidence.AuthorizationOverlayDigest != fastEvidence.AuthorizationOverlayDigest {
			return errors.New("system-bench fast search evidence changed immutable snapshot identity")
		}
		age := uint64(evidence.IndexAge)
		minIndexAge, maxIndexAgeObserved = min(minIndexAge, age), max(maxIndexAgeObserved, age)
		return nil
	}
	request := func(query []float32) public.SearchRequestV1 {
		return public.SearchRequestV1{
			Version: 1, Generation: generation, Query: query,
			Metric: public.MetricCosineV1, TopK: topK, Probes: probes, EfSearch: efSearch, Consistency: public.ConsistencyGenerationSnapshotV1,
			Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 256 * topK}, Deadline: time.Now().Add(30 * time.Second),
		}
	}
	callSearch := func(client *vectorPartitionOperationsTCPClientV1, query []float32) (vectorPartitionOperationsWireResponseV1, vectorPartitionSystemFrameTimingV1, error) {
		wire := vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: request(query)}
		switch searchMode {
		case vectorPartitionSystemSearchFastV1:
			wire.Operation, wire.FastOptions = "search_fast", &fastOptions
		case vectorPartitionSystemSearchPinnedV1:
			wire.Operation = "search_pinned"
		}
		return client.callWithTiming(wire)
	}
	if searchMode == vectorPartitionSystemSearchFastV1 {
		wire, _, err := callSearch(clients[0], queries[0])
		if err != nil || wire.Search == nil {
			return cell, errors.Join(errors.New("system-bench fast search preflight failed"), err)
		}
		if err := observeEvidence(wire.FastEvidence); err != nil {
			return cell, err
		}
	}
	if searchMode == vectorPartitionSystemSearchPinnedV1 {
		for _, client := range clients {
			wire, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "pin_search_snapshot", PinOptions: &pinOptions})
			if err != nil {
				return cell, err
			}
			if err := observeEvidence(wire.FastEvidence); err != nil {
				return cell, err
			}
		}
	}
	if err := vectorPartitionSystemRunQueriesV1(ctx, clients, warmup, func(index int) error {
		wire, _, err := callSearch(clients[index%workers], queries[index%len(queries)])
		if err != nil || wire.Search == nil {
			return errors.Join(errors.New("system-bench warmup returned no search response"), err)
		}
		if searchMode == vectorPartitionSystemSearchFastV1 && (wire.FastEvidence == nil || fastEvidence == nil || wire.FastEvidence.Generation != fastEvidence.Generation || wire.FastEvidence.IndexedThrough != fastEvidence.IndexedThrough || !wire.FastEvidence.PublishedAt.Equal(fastEvidence.PublishedAt) || wire.FastEvidence.TopologyDigest != fastEvidence.TopologyDigest || wire.FastEvidence.AuthorizationOverlayDigest != fastEvidence.AuthorizationOverlayDigest) {
			return errors.New("system-bench warmup changed fast snapshot identity")
		}
		return nil
	}); err != nil {
		return cell, fmt.Errorf("warmup: %w", err)
	}
	proofsBefore, err := snapshot(ctx)
	if err != nil {
		return cell, fmt.Errorf("catalog read snapshot before measurement: %w", err)
	}
	if searchMode == vectorPartitionSystemSearchFastV1 {
		minIndexAge, maxIndexAgeObserved = math.MaxUint64, 0
	}
	outcomes := make([]*public.SearchResponseV1, len(queries))
	evidence := make([]*public.FastSearchEvidenceV1, len(queries))
	durations := make([]uint64, len(queries))
	transport := make([]vectorPartitionSystemFrameTimingV1, len(queries))
	started := time.Now()
	err = vectorPartitionSystemRunQueriesV1(ctx, clients, len(queries), func(index int) error {
		wire, callTiming, callErr := callSearch(clients[index%workers], queries[index])
		durations[index], transport[index] = callTiming.TotalNanos, callTiming
		if callErr != nil {
			return callErr
		}
		outcomes[index], evidence[index] = wire.Search, wire.FastEvidence
		return nil
	})
	elapsed := time.Since(started)
	if err != nil {
		return cell, err
	}
	proofsAfter, err := snapshot(ctx)
	if err != nil {
		return cell, fmt.Errorf("catalog read snapshot after measurement: %w", err)
	}
	if searchMode == vectorPartitionSystemSearchPinnedV1 {
		for _, client := range clients {
			if _, err := client.call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "close_pinned_snapshot"}); err != nil {
				return cell, err
			}
		}
	}
	var recall float64
	counters := map[string]uint64{"selected_partitions": 0, "selected_groups": 0, "requests": 0, "rpcs": 0, "retries": 0, "redirects": 0, "candidates": 0, "edges": 0, "snapshot_pins": 0, "session_pins": 0, "read_proofs": 0, "generation_pins": 0, "partition_opens": 0, "query_bytes": 0, "request_bytes": 0, "candidate_bytes": 0, "response_bytes": 0, "public_request_frame_bytes": 0, "public_response_frame_bytes": 0}
	if searchMode == vectorPartitionSystemSearchPinnedV1 {
		counters["session_pins"] = uint64(workers)
	}
	timings := map[string]uint64{"admission": 0, "operations_health": 0, "service_adapter": 0, "public_adapter": 0, "router_open": 0, "router_search": 0, "placement": 0, "coordinator_lifecycle": 0, "dispatch": 0, "queue": 0, "rpc": 0, "network": 0, "read_index_apply": 0, "generation_open": 0, "shard_search": 0, "response": 0, "dedupe": 0, "merge": 0, "coordinator_total": 0, "total": 0, "client_encode": 0, "client_write": 0, "client_response_read": 0, "client_decode": 0, "client_total": 0}
	for index, outcome := range outcomes {
		if outcome == nil {
			return cell, fmt.Errorf("query %d returned no search response", index)
		}
		if len(outcome.Neighbors) != topK {
			return cell, fmt.Errorf("query %d returned %d neighbors, want %d", index, len(outcome.Neighbors), topK)
		}
		if outcome.Generation != generation {
			return cell, fmt.Errorf("query %d returned generation %+v, want %+v", index, outcome.Generation, generation)
		}
		if searchMode == vectorPartitionSystemSearchFastV1 {
			if err := observeEvidence(evidence[index]); err != nil {
				return cell, fmt.Errorf("query %d: %w", index, err)
			}
		}
		got := make([]m8CanonicalResultV1, len(outcome.Neighbors))
		for i, neighbor := range outcome.Neighbors {
			got[i] = m8CanonicalResultV1{ID: neighbor.ID, Score: neighbor.Score}
		}
		recall += m8CanonicalRecallV1(truth[index], got)
		c := outcome.Counters
		for key, value := range map[string]uint64{"selected_partitions": c.SelectedPartitions, "selected_groups": c.SelectedGroups, "requests": c.Requests, "rpcs": c.RPCs, "retries": c.Retries, "redirects": c.Redirects, "candidates": c.Candidates, "edges": c.Edges, "snapshot_pins": c.SnapshotPins, "read_proofs": c.ReadProofs, "generation_pins": c.GenerationPins, "partition_opens": c.PartitionOpens, "query_bytes": c.QueryBytes, "request_bytes": c.RequestBytes, "candidate_bytes": c.CandidateBytes, "response_bytes": c.ResponseBytes} {
			if math.MaxUint64-counters[key] < value {
				return cell, errors.New("system-bench counter overflow")
			}
			counters[key] += value
		}
		for key, value := range map[string]time.Duration{"admission": outcome.Timing.Admission, "operations_health": outcome.Timing.OperationsHealth, "service_adapter": outcome.Timing.ServiceAdapter, "public_adapter": outcome.Timing.PublicAdapter, "router_open": outcome.Timing.RouterOpen, "router_search": outcome.Timing.RouterSearch, "placement": outcome.Timing.Placement, "coordinator_lifecycle": outcome.Timing.CoordinatorLifecycle, "dispatch": outcome.Timing.Dispatch, "queue": outcome.Timing.Queue, "rpc": outcome.Timing.RPC, "network": outcome.Timing.Network, "read_index_apply": outcome.Timing.ReadIndexApply, "generation_open": outcome.Timing.GenerationOpen, "shard_search": outcome.Timing.ShardSearch, "response": outcome.Timing.Response, "dedupe": outcome.Timing.Dedupe, "merge": outcome.Timing.Merge, "coordinator_total": outcome.Timing.CoordinatorTotal, "total": outcome.Timing.Total} {
			if value < 0 || math.MaxUint64-timings[key] < uint64(value) {
				return cell, errors.New("system-bench timing overflow")
			}
			timings[key] += uint64(value)
		}
		for key, value := range map[string]uint64{"public_request_frame_bytes": transport[index].RequestBytes, "public_response_frame_bytes": transport[index].ResponseBytes} {
			if math.MaxUint64-counters[key] < value {
				return cell, errors.New("system-bench public transport byte overflow")
			}
			counters[key] += value
		}
		for key, value := range map[string]uint64{"client_encode": transport[index].EncodeNanos, "client_write": transport[index].WriteNanos, "client_response_read": transport[index].ReadNanos, "client_decode": transport[index].DecodeNanos, "client_total": transport[index].TotalNanos} {
			if math.MaxUint64-timings[key] < value {
				return cell, errors.New("system-bench public transport timing overflow")
			}
			timings[key] += value
		}
	}
	cell.Metrics.CompletedQueries, cell.Metrics.ResultCount = len(queries), len(queries)*topK
	cell.Metrics.RecallAt10, cell.Metrics.QPS = recall/float64(len(queries)), float64(len(queries))/elapsed.Seconds()
	cell.Metrics.P50Nanos, cell.Metrics.P95Nanos, cell.Metrics.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	wantStrictProofs := uint64(0)
	if searchMode == vectorPartitionSystemSearchStrictV1 {
		wantStrictProofs = uint64(len(queries))
	}
	catalogReads, runtimeStats, err := vectorPartitionSystemCatalogReadDeltaV1(proofsBefore, proofsAfter, wantStrictProofs)
	if err != nil {
		return cell, err
	}
	cell.Counters, cell.Timings, cell.CatalogReads, cell.Runtime, cell.ElapsedNanos, cell.TotalNanos = counters, timings, catalogReads, runtimeStats, uint64(elapsed), durations
	if searchMode != vectorPartitionSystemSearchStrictV1 {
		cell.FastEvidence, cell.MinIndexAgeNanos, cell.MaxIndexAgeNanos = fastEvidence, minIndexAge, maxIndexAgeObserved
	}
	return cell, nil
}

func vectorPartitionSystemCatalogReadDeltaV1(before, after map[string]vectorPartitionSystemNodeObservationV1, wantStrictProofs uint64) (vectorPartitionSystemCatalogReadsV1, []vectorPartitionSystemRuntimeNodeV1, error) {
	var result vectorPartitionSystemCatalogReadsV1
	if len(before) == 0 || len(before) != len(after) {
		return result, nil, errors.New("system-bench catalog read snapshots do not cover the same nodes")
	}
	identities := make([]string, 0, len(before))
	for identity := range before {
		if _, ok := after[identity]; !ok {
			return result, nil, errors.New("system-bench catalog read snapshot changed node identity")
		}
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	runtimeStats := make([]vectorPartitionSystemRuntimeNodeV1, 0, len(identities))
	for _, identity := range identities {
		beforeNode, afterNode := before[identity], after[identity]
		delta, ok := vectorPartitionSystemCatalogReadStatsSubtractV1(afterNode.Catalog, beforeNode.Catalog)
		if !ok || delta.Total.Reads > 0 && (afterNode.Catalog.LastTerm == 0 || afterNode.Catalog.LastCatalogApplied == 0 || afterNode.Catalog.LastRaftApplied < afterNode.Catalog.LastCatalogApplied || afterNode.Catalog.LastRaftLog < afterNode.Catalog.LastRaftApplied || afterNode.Catalog.LastRaftLog < beforeNode.Catalog.LastRaftLog) {
			return result, nil, errors.New("system-bench catalog read statistics are non-monotonic or lack proof identity")
		}
		if !vectorPartitionSystemCatalogReadStatsAddV1(&result.Total, delta) {
			return result, nil, errors.New("system-bench catalog read statistics overflow")
		}
		if !vectorPartitionSystemRuntimeMonotonicV1(beforeNode.Runtime, afterNode.Runtime) {
			return result, nil, fmt.Errorf("system-bench process runtime statistics are non-monotonic: identity=%s before=%+v after=%+v", identity, beforeNode.Runtime, afterNode.Runtime)
		}
		result.Nodes = append(result.Nodes, vectorPartitionSystemCatalogReadNodeV1{NodeConfigSHA256: identity, Before: beforeNode.Catalog, After: afterNode.Catalog, Delta: delta})
		runtimeStats = append(runtimeStats, vectorPartitionSystemRuntimeNodeV1{NodeConfigSHA256: identity, Before: beforeNode.Runtime, After: afterNode.Runtime})
	}
	wantTotal, ok := vectorPartitionSystemAddUint64V1(wantStrictProofs, result.Total.ServingRefresh.Reads)
	if !ok || result.Total.Total.Reads != wantTotal || result.Total.StrictSearch.Reads != wantStrictProofs || result.Total.OperationsHealth.Reads != 0 || result.Total.CoordinatorLifecycle.Reads != 0 || result.Total.ShardLifecycle.Reads != 0 || result.Total.Unknown.Reads != 0 {
		return result, nil, fmt.Errorf("system-bench catalog proof counts do not match measured search work: total=%d strict=%d refresh=%d health=%d coordinator=%d shard=%d unknown=%d want_strict=%d", result.Total.Total.Reads, result.Total.StrictSearch.Reads, result.Total.ServingRefresh.Reads, result.Total.OperationsHealth.Reads, result.Total.CoordinatorLifecycle.Reads, result.Total.ShardLifecycle.Reads, result.Total.Unknown.Reads, wantStrictProofs)
	}
	sources := []nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1{result.Total.OperationsHealth, result.Total.StrictSearch, result.Total.ServingRefresh, result.Total.CoordinatorLifecycle, result.Total.ShardLifecycle, result.Total.Unknown}
	var summed nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1
	for _, source := range sources {
		exclusive, ok := vectorPartitionSystemAddUint64V1(source.AdmissionNanos, source.VerifyLeaderNanos)
		if ok {
			exclusive, ok = vectorPartitionSystemAddUint64V1(exclusive, source.CurrentTermNanos)
		}
		if ok {
			exclusive, ok = vectorPartitionSystemAddUint64V1(exclusive, source.RaftApplyNanos)
		}
		if ok {
			exclusive, ok = vectorPartitionSystemAddUint64V1(exclusive, source.AppliedReadNanos)
		}
		if !ok || source.Successes != source.Reads || source.Failures != 0 || source.VerifyLeaderCalls != source.Reads || source.LogBarriers != 0 || source.BarrierNanos != 0 || source.NoLogProofs != source.Reads || source.TotalNanos < exclusive || !vectorPartitionSystemCatalogReadStageAddV1(&summed, source) {
			return result, nil, errors.New("system-bench catalog read stage evidence is malformed")
		}
	}
	if !reflect.DeepEqual(summed, result.Total.Total) {
		return result, nil, errors.New("system-bench catalog read totals do not match attributed sources")
	}
	return result, runtimeStats, nil
}

func vectorPartitionSystemRuntimeMonotonicV1(before, after nativewire.VectorPartitionProcessRuntimeStatsV1) bool {
	if before.SampleUnixNano == 0 || after.SampleUnixNano <= before.SampleUnixNano || after.Goroutines == 0 {
		return false
	}
	beforeValues := [...]uint64{before.CPUTimeNanos, before.VoluntaryContextSwitches, before.NonvoluntaryContextSwitches, before.PeakRSSBytes, before.TotalAllocBytes, before.Mallocs, before.Frees, before.NumGC, before.PauseTotalNanos}
	afterValues := [...]uint64{after.CPUTimeNanos, after.VoluntaryContextSwitches, after.NonvoluntaryContextSwitches, after.PeakRSSBytes, after.TotalAllocBytes, after.Mallocs, after.Frees, after.NumGC, after.PauseTotalNanos}
	for i := range beforeValues {
		if afterValues[i] < beforeValues[i] {
			return false
		}
	}
	return true
}

func vectorPartitionSystemCatalogReadStatsSubtractV1(after, before nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1) (nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1, bool) {
	var out nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1
	var ok bool
	if out.Total, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.Total, before.Total); !ok {
		return out, false
	}
	if out.OperationsHealth, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.OperationsHealth, before.OperationsHealth); !ok {
		return out, false
	}
	if out.StrictSearch, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.StrictSearch, before.StrictSearch); !ok {
		return out, false
	}
	if out.ServingRefresh, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.ServingRefresh, before.ServingRefresh); !ok {
		return out, false
	}
	if out.CoordinatorLifecycle, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.CoordinatorLifecycle, before.CoordinatorLifecycle); !ok {
		return out, false
	}
	if out.ShardLifecycle, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.ShardLifecycle, before.ShardLifecycle); !ok {
		return out, false
	}
	if out.Unknown, ok = vectorPartitionSystemCatalogReadStageSubtractV1(after.Unknown, before.Unknown); !ok {
		return out, false
	}
	out.LastTerm, out.LastCatalogApplied, out.LastRaftApplied, out.LastRaftLog = after.LastTerm, after.LastCatalogApplied, after.LastRaftApplied, after.LastRaftLog
	return out, true
}

func vectorPartitionSystemCatalogReadStageSubtractV1(after, before nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1) (nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1, bool) {
	valuesAfter := [...]uint64{after.Reads, after.Successes, after.Failures, after.VerifyLeaderCalls, after.LogBarriers, after.NoLogProofs, after.TotalNanos, after.AdmissionNanos, after.VerifyLeaderNanos, after.BarrierNanos, after.CurrentTermNanos, after.RaftApplyNanos, after.AppliedReadNanos}
	valuesBefore := [...]uint64{before.Reads, before.Successes, before.Failures, before.VerifyLeaderCalls, before.LogBarriers, before.NoLogProofs, before.TotalNanos, before.AdmissionNanos, before.VerifyLeaderNanos, before.BarrierNanos, before.CurrentTermNanos, before.RaftApplyNanos, before.AppliedReadNanos}
	var delta [len(valuesAfter)]uint64
	for i := range valuesAfter {
		if valuesAfter[i] < valuesBefore[i] {
			return nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1{}, false
		}
		delta[i] = valuesAfter[i] - valuesBefore[i]
	}
	return nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1{
		Reads: delta[0], Successes: delta[1], Failures: delta[2], VerifyLeaderCalls: delta[3], LogBarriers: delta[4],
		NoLogProofs: delta[5], TotalNanos: delta[6], AdmissionNanos: delta[7], VerifyLeaderNanos: delta[8], BarrierNanos: delta[9], CurrentTermNanos: delta[10], RaftApplyNanos: delta[11], AppliedReadNanos: delta[12],
	}, true
}

func vectorPartitionSystemCatalogReadStatsAddV1(total *nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1, value nativewire.VectorPartitionCatalogMetaLinearizableReadStatsV1) bool {
	if total == nil || !vectorPartitionSystemCatalogReadStageAddV1(&total.Total, value.Total) || !vectorPartitionSystemCatalogReadStageAddV1(&total.OperationsHealth, value.OperationsHealth) || !vectorPartitionSystemCatalogReadStageAddV1(&total.StrictSearch, value.StrictSearch) || !vectorPartitionSystemCatalogReadStageAddV1(&total.ServingRefresh, value.ServingRefresh) || !vectorPartitionSystemCatalogReadStageAddV1(&total.CoordinatorLifecycle, value.CoordinatorLifecycle) || !vectorPartitionSystemCatalogReadStageAddV1(&total.ShardLifecycle, value.ShardLifecycle) || !vectorPartitionSystemCatalogReadStageAddV1(&total.Unknown, value.Unknown) {
		return false
	}
	total.LastTerm = max(total.LastTerm, value.LastTerm)
	total.LastCatalogApplied = max(total.LastCatalogApplied, value.LastCatalogApplied)
	total.LastRaftApplied = max(total.LastRaftApplied, value.LastRaftApplied)
	total.LastRaftLog = max(total.LastRaftLog, value.LastRaftLog)
	return true
}

func vectorPartitionSystemCatalogReadStageAddV1(total *nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1, value nativewire.VectorPartitionCatalogMetaLinearizableReadStageStatsV1) bool {
	if total == nil {
		return false
	}
	left := []*uint64{&total.Reads, &total.Successes, &total.Failures, &total.VerifyLeaderCalls, &total.LogBarriers, &total.NoLogProofs, &total.TotalNanos, &total.AdmissionNanos, &total.VerifyLeaderNanos, &total.BarrierNanos, &total.CurrentTermNanos, &total.RaftApplyNanos, &total.AppliedReadNanos}
	right := [...]uint64{value.Reads, value.Successes, value.Failures, value.VerifyLeaderCalls, value.LogBarriers, value.NoLogProofs, value.TotalNanos, value.AdmissionNanos, value.VerifyLeaderNanos, value.BarrierNanos, value.CurrentTermNanos, value.RaftApplyNanos, value.AppliedReadNanos}
	for i := range left {
		if math.MaxUint64-*left[i] < right[i] {
			return false
		}
		*left[i] += right[i]
	}
	return true
}

func vectorPartitionSystemAddUint64V1(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, false
	}
	return a + b, true
}

func vectorPartitionSystemRunQueriesV1(ctx context.Context, clients []*vectorPartitionOperationsTCPClientV1, count int, run func(int) error) error {
	if count == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(clients))
	for worker := range clients {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for index := worker; index < count && ctx.Err() == nil; index += len(clients) {
				if err := run(index); err != nil {
					select {
					case errs <- err:
					default:
					}
					cancel()
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return ctx.Err()
	}
}

func vectorPartitionSystemPositiveListV1(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]bool{}
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value <= 0 || seen[value] {
			return nil, errors.New("values must be distinct positive integers")
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

func validVectorPartitionSystemSearchModeV1(mode string) bool {
	return mode == vectorPartitionSystemSearchStrictV1 || mode == vectorPartitionSystemSearchFastV1 || mode == vectorPartitionSystemSearchPinnedV1
}

func writeVectorPartitionSystemJSONExclusiveV1(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeVectorPartitionSystemBytesExclusiveV1(path, append(raw, '\n'))
}

func writeVectorPartitionSystemBytesExclusiveV1(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
