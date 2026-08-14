package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	m3ReportSchemaVersion   = 4
	m3BenchmarkCollection   = "m3_partition_source"
	m3WarmupPasses          = 1
	m3SourceInsertBatchRows = 8 * 1024
	// m3PartitionAssetFileIDBase reserves a benchmark-owned column-asset
	// segment range, separate from the collection package's production ranges.
	m3PartitionAssetFileIDBase uint64 = 40_000
	// Router assets use a disjoint benchmark-owned segment range so the M4
	// ready-generation publication cannot alias an M3 partition-pack segment.
	m3RouterAssetFileIDBase uint64 = 50_000
)

func m3RouterBuildOptionsV1(config vectorpartition.RouterConfigV1, fileID uint32, partID uint64) collections.VectorPartitionRouterBuildOptionsV1 {
	return collections.VectorPartitionRouterBuildOptionsV1{
		Config: config, AssetFileID: fileID, AssetPartID: partID,
		M: partitionHNSWDegree, EfConstruction: partitionHNSWDefaultEfC, EfSearch: 128,
	}
}

type m3PartitionIndexReport struct {
	SchemaVersion       int                   `json:"schema_version"`
	ResultKind          string                `json:"result_kind"`
	Dataset             fixtureManifest       `json:"dataset"`
	ArtifactSHA256      string                `json:"artifact_sha256"`
	GraphArtifactSHA256 string                `json:"graph_artifact_sha256"`
	BaseSHA             string                `json:"base_sha"`
	HeadSHA             string                `json:"head_sha"`
	GoVersion           string                `json:"go_version"`
	Hardware            string                `json:"hardware_context"`
	Partitions          int                   `json:"partitions"`
	TopK                int                   `json:"top_k"`
	Rows                []m3PartitionIndexRow `json:"rows"`
	ReplicationGate     string                `json:"replication_gate"`
	MillionCorpus       string                `json:"million_corpus"`
	EnablementPolicy    string                `json:"enablement_policy"`
	OwnershipBoundary   string                `json:"ownership_boundary"`
	ExactCommand        []string              `json:"exact_command"`
}

type m3PartitionIndexRow struct {
	Ratio                        float64            `json:"ratio"`
	Budget                       int                `json:"budget"`
	Used                         int                `json:"used"`
	Unspent                      int                `json:"unspent"`
	Capacity                     int                `json:"capacity"`
	OverlapRequested             int                `json:"overlap_requested"`
	OverlapRealized              int                `json:"overlap_realized"`
	OverlapRejected              int                `json:"overlap_rejected"`
	OverlapUseful                int                `json:"overlap_useful"`
	OverlapFiller                int                `json:"overlap_filler"`
	OverlapCumulativeUtility     int                `json:"overlap_cumulative_utility"`
	OverlapDestinationDiversity  []int              `json:"overlap_destination_diversity"`
	OverlapReplicas              []m3OverlapReplica `json:"overlap_replicas"`
	OverlapUnusedCapacity        int                `json:"overlap_unused_capacity"`
	CutReductionPerUsefulReplica float64            `json:"cut_reduction_per_useful_replica"`
	ReplicationFactor            float64            `json:"replication_factor"`
	PartitionLoads               []int              `json:"partition_loads"`
	EdgeCutBefore                int                `json:"edge_cut_before"`
	EdgeCutAfter                 int                `json:"edge_cut_after"`
	BuildWallNanos               int64              `json:"build_wall_nanos"`
	PeakRSSBytes                 int64              `json:"peak_rss_bytes"`
	PeakRSSAvailable             bool               `json:"peak_rss_available"`
	ResidentBytes                int64              `json:"resident_bytes"`
	ResidentBytesAvailable       bool               `json:"resident_bytes_available"`
	SourcePhysicalBytes          int64              `json:"source_physical_bytes"`
	PeakDerivedTemporaryBytes    int64              `json:"peak_derived_temporary_bytes"`
	FinalDerivedPhysicalBytes    int64              `json:"final_derived_physical_bytes"`
	PhysicalBytesPerSourceVector float64            `json:"physical_bytes_per_source_vector"`
	PackBytes                    uint64             `json:"pack_payload_bytes"`
	PartitionHNSWM               int                `json:"partition_hnsw_m"`
	MappedBytes                  uint64             `json:"mapped_bytes"`
	HeapBytes                    uint64             `json:"heap_bytes"`
	SearcherOpenWallNanos        int64              `json:"searcher_open_wall_nanos"`
	PackOpenNanos                uint64             `json:"pack_open_nanos"`
	Queries                      int                `json:"queries"`
	LocalSearches                int                `json:"local_searches"`
	WarmupPasses                 int                `json:"warmup_passes"`
	WarmNSPerOp                  float64            `json:"warm_ns_per_op"`
	WarmQPS                      float64            `json:"warm_qps"`
	WarmBytesPerOp               float64            `json:"warm_bytes_per_op"`
	WarmAllocsPerOp              float64            `json:"warm_allocs_per_op"`
	CandidatesPerOp              float64            `json:"candidates_per_op"`
	EdgesPerOp                   float64            `json:"edges_per_op"`
	ExactLocalRecallAtK          float64            `json:"exact_local_recall_at_k"`
	ManifestDigest               string             `json:"manifest_digest"`
	SourceGeneration             uint64             `json:"source_generation"`
	SourceChecksum               uint64             `json:"source_checksum"`
	SourceSchemaHash             uint64             `json:"source_schema_hash"`
	SourceRows                   uint64             `json:"source_rows"`
	SearchRoute                  string             `json:"search_route"`
	MissingAssets                uint64             `json:"missing_assets"`
	CorruptAssets                uint64             `json:"corrupt_assets"`
	StaleAssets                  uint64             `json:"stale_assets"`
	PersistentDBDir              string             `json:"persistent_db_dir,omitempty"`
}

type m3OverlapReplica struct {
	SourceOrdinal uint64 `json:"source_ordinal"`
	Destination   int    `json:"destination"`
	Policy        string `json:"policy"`
	Gain          int    `json:"gain"`
	Class         string `json:"class"`
}

func runM3PartitionIndexStage(cfg config, fixture fixtureManifest, artifact vectorpartition.Artifact, artifactDigest, graphArtifactDigest, graphBuildDigest, suffix string, vectors, queries [][]float64, stdout io.Writer) error {
	if len(queries) == 0 {
		return errors.New("M3 partition-index stage requires exact-oracle queries")
	}
	ratios := append([]float64(nil), cfg.overlaps...)
	sort.Float64s(ratios)
	report := m3PartitionIndexReport{
		SchemaVersion:       m3ReportSchemaVersion,
		ResultKind:          "m3_native_partition_hnsw_evidence",
		Dataset:             fixture,
		ArtifactSHA256:      artifactDigest,
		GraphArtifactSHA256: graphArtifactDigest,
		BaseSHA:             cfg.baseSHA,
		HeadSHA:             cfg.headSHA,
		GoVersion:           runtime.Version(),
		Hardware:            runtime.GOARCH + "/" + runtime.GOOS,
		Partitions:          cfg.partitions,
		TopK:                cfg.topK,
		MillionCorpus:       "unavailable: no 1M corpus was supplied",
		EnablementPolicy:    "disabled_pending_clustered_1m_quality_or_probe_win",
		OwnershipBoundary:   "derived stable IDs, validated FP32 vectors, and native HNSW packs only; canonical documents and Raft token ownership are unchanged",
		ExactCommand:        append([]string(nil), cfg.command...),
	}
	if fixture.Vectors >= 1_000_000 {
		report.MillionCorpus = "measured from supplied 1M corpus"
	}
	if err := m3ValidateShardPlanGovernsArtifactV1(cfg.shardPlan, artifact, ratios); err != nil {
		return err
	}
	for i, ratio := range ratios {
		capacity, err := m3OverlapCapacityForPlanV1(cfg.shardPlan, artifact, ratio)
		if err != nil {
			return fmt.Errorf("derive exact overlap capacity ratio %.4f: %w", ratio, err)
		}
		overlap, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: ratio, Capacity: capacity, UsefulOnly: true})
		if err != nil {
			return fmt.Errorf("build bounded overlap ratio %.4f: %w", ratio, err)
		}
		if err := m3ValidateShardPackBudgetV1(cfg.shardPlan, overlap); err != nil {
			return fmt.Errorf("account byte-bounded packs ratio %.4f: %w", ratio, err)
		}
		row, err := benchmarkM3PartitionIndexRow(cfg, fixture, artifactDigest, graphArtifactDigest, graphBuildDigest, vectors, queries, artifact, overlap, ratio, uint64(i+1))
		if err != nil {
			return fmt.Errorf("benchmark native partition packs ratio %.4f: %w", ratio, err)
		}
		row.Ratio = ratio
		report.Rows = append(report.Rows, row)
	}
	report.ReplicationGate = m3ReplicationGate(report.Rows)
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	path := filepath.Join(cfg.out, "vector_partition_m3_"+suffix+".json")
	if err := os.WriteFile(path, raw, 0644); err != nil {
		return err
	}
	if err := validateM3PartitionIndexReport(report); err != nil {
		return fmt.Errorf("M3 report %s: %w", path, err)
	}
	if cfg.format == "json" {
		_, err = stdout.Write(raw)
		return err
	}
	_, err = fmt.Fprintf(stdout, "m3 partition packs=%s rows=%d replication_gate=%s\n", path, len(report.Rows), report.ReplicationGate)
	return err
}

// m3ValidateShardPlanGovernsArtifactV1 fails closed when a byte-bounded plan
// did not actually govern the artifact it is about to describe. The plan is
// derived before construction, so any disagreement here means the partition
// geometry was built from something other than the advertised hot-byte budget
// and the persisted plan would be circular rather than authoritative.
func m3ValidateShardPlanGovernsArtifactV1(plan vectorpartition.ShardPlanV1, artifact vectorpartition.Artifact, ratios []float64) error {
	if plan.Partitions == 0 {
		return nil
	}
	if plan.Vectors != len(artifact.IDs) || plan.Dimensions != artifact.Source.Dimensions ||
		plan.Partitions != artifact.Config.Partitions || plan.Imbalance != artifact.Config.Imbalance ||
		plan.HomeCapacity != artifact.Metrics.Cap {
		return fmt.Errorf("byte-bounded plan %+v does not govern artifact rows=%d dimensions=%d partitions=%d cap=%d", plan, len(artifact.IDs), artifact.Source.Dimensions, artifact.Config.Partitions, artifact.Metrics.Cap)
	}
	for _, ratio := range ratios {
		if ratio > plan.OverlapRatio {
			return fmt.Errorf("overlap ratio %.4f exceeds the planned ratio %.4f", ratio, plan.OverlapRatio)
		}
	}
	return nil
}

// m3ValidateShardPackBudgetV1 re-derives the per-pack accounting from the
// realized memberships and rejects any pack outside the planned home,
// membership, or hot-byte budget before the packs are materialized.
func m3ValidateShardPackBudgetV1(plan vectorpartition.ShardPlanV1, overlap vectorpartition.OverlapResult) error {
	if plan.Partitions == 0 {
		return nil
	}
	summaries, err := vectorpartition.AccountShardPacksV1(plan, overlap.Memberships)
	if err != nil {
		return err
	}
	if len(summaries) != len(overlap.Loads) {
		return fmt.Errorf("planned packs=%d realized loads=%d", len(summaries), len(overlap.Loads))
	}
	for partition, summary := range summaries {
		if summary.Rows != overlap.Loads[partition] {
			return fmt.Errorf("pack %d membership rows=%d does not match realized load=%d", partition, summary.Rows, overlap.Loads[partition])
		}
	}
	return nil
}

// m3OverlapCapacityForPlanV1 declares the per-partition total-membership
// capacity for one overlap ratio. Under a byte-bounded plan the capacity is the
// planned one, so packs cannot exceed the advertised hot-byte budget; otherwise
// it falls back to the operator-declared exact global target.
func m3OverlapCapacityForPlanV1(plan vectorpartition.ShardPlanV1, artifact vectorpartition.Artifact, ratio float64) (int, error) {
	if plan.Partitions != 0 {
		return plan.OverlapCapacity, nil
	}
	return m3OverlapCapacityV1(artifact, ratio)
}

// m3OverlapCapacityV1 keeps the immutable M2 home assignment and its
// disjoint epsilon cap intact. For an overlap variant it declares the narrow
// per-partition total-membership capacity that can hold the requested global
// extra-membership target: ceil((source rows + requested extras)/partitions).
// Exact construction still fails closed when affinity/per-vector constraints
// cannot realize that target.
func m3OverlapCapacityV1(artifact vectorpartition.Artifact, ratio float64) (int, error) {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 || artifact.Config.Partitions < 1 {
		return 0, errors.New("invalid M3 overlap capacity inputs")
	}
	requested := int(math.Floor(ratio * float64(len(artifact.IDs))))
	return m3OverlapCapacityForRequestedV1(len(artifact.IDs), requested, artifact.Config.Partitions, artifact.Metrics.Cap)
}

func m3OverlapCapacityForRequestedV1(rows, requested, partitions, baseCapacity int) (int, error) {
	if rows < 0 || requested < 0 || partitions < 1 || baseCapacity < 0 || rows > math.MaxInt-requested {
		return 0, errors.New("M3 overlap capacity overflows int")
	}
	total := rows + requested
	capacity := total / partitions
	if total%partitions != 0 {
		capacity++
	}
	return max(baseCapacity, capacity), nil
}

func m3PartitionAssetFileID(generation uint64) (uint32, error) {
	maxFileID := uint64(^uint32(0))
	if generation == 0 || generation > maxFileID-m3PartitionAssetFileIDBase {
		return 0, fmt.Errorf("M3 generation %d exceeds column-asset file-ID range", generation)
	}
	return uint32(m3PartitionAssetFileIDBase + generation), nil
}

func m3RouterAssetFileID(generation uint64) (uint32, error) {
	maxFileID := uint64(^uint32(0))
	if generation == 0 || generation > maxFileID-m3RouterAssetFileIDBase {
		return 0, fmt.Errorf("M3 router generation %d exceeds column-asset file-ID range", generation)
	}
	return uint32(m3RouterAssetFileIDBase + generation), nil
}

func closeM3PartitionSearchers(searchers []*collections.VectorPartitionLocalSearcherV1) error {
	var resultErr error
	for _, searcher := range searchers {
		if searcher != nil {
			resultErr = errors.Join(resultErr, searcher.Close())
		}
	}
	return resultErr
}

func openM3PartitionSearchers(count int, open func(uint32) (*collections.VectorPartitionLocalSearcherV1, error)) (searchers []*collections.VectorPartitionLocalSearcherV1, resultErr error) {
	if count < 0 || uint64(count) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("M3 partition count %d exceeds uint32 range", count)
	}
	searchers = make([]*collections.VectorPartitionLocalSearcherV1, count)
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, closeM3PartitionSearchers(searchers))
			searchers = nil
		}
	}()
	for partition := range searchers {
		searchers[partition], resultErr = open(uint32(partition))
		if resultErr != nil {
			return searchers, resultErr
		}
	}
	return searchers, nil
}

func benchmarkM3PartitionIndexRow(cfg config, fixture fixtureManifest, artifactDigest, graphArtifactDigest, graphBuildDigest string, vectors, queries [][]float64, artifact vectorpartition.Artifact, overlap vectorpartition.OverlapResult, ratio float64, generation uint64) (_ m3PartitionIndexRow, resultErr error) {
	dir, cleanup, err := m3PartitionIndexDirectory(cfg.m3PersistDir)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if cleanup {
		defer func() {
			resultErr = errors.Join(resultErr, os.RemoveAll(dir))
		}()
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return m3PartitionIndexRow{}, err
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	dbOpen := true
	defer func() {
		if dbOpen {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	manager := collections.NewCollectionManager(db)
	meta := partitionCollectionMeta(m3BenchmarkCollection, len(vectors[0]))
	partitionHNSWM, partitionHNSWEfConstruction, err := m3PartitionLocalHNSWConfigV1(cfg)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	localVariant, err := m3PartitionLocalGraphVariantV1(partitionHNSWM, partitionHNSWEfConstruction)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	variantID, err := m3VariantIDV1(cfg.partitionAssignment, ratio)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	executableSHA256, err := m8BenchmarkExecutableSHA256V1(cfg.command[0])
	if err != nil {
		return m3PartitionIndexRow{}, fmt.Errorf("hash M3 benchmark executable: %w", err)
	}
	identityDescriptor := m3VariantDescriptorV1{
		FixtureChecksum: fixture.Checksum, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, BuildDirty: cfg.m3BuildDirty, ExecutableSHA256: executableSHA256, VariantID: variantID, AssignmentBasis: cfg.partitionAssignment, OverlapRatio: ratio,
		ArtifactSHA256: artifactDigest, GraphArtifactSHA256: graphArtifactDigest, GraphBuildSHA256: graphBuildDigest, ArtifactBackend: artifact.Backend, KaHIPPythonSHA256: cfg.kahipPythonSHA256, KaHIPAdapterSHA256: cfg.kahipAdapterSHA256,
		Source: artifact.Source, IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]), PartitionHNSWM: partitionHNSWM, PartitionHNSWEfC: partitionHNSWEfConstruction, PartitionConfig: cfg.partition,
		PartitionMaxDistanceWork: cfg.partition.MaxDistanceWork, RouterMaxScalarWork: cfg.routerConfig.MaxScalarWork, RouterConfig: cfg.routerConfig, M3MaxBenchmarkVisits: cfg.m3MaxBenchmarkVisits,
		Capacity: overlap.Capacity, OverlapRequested: overlap.Budget,
		OverlapUseful: overlap.Useful, OverlapFiller: overlap.Filler,
		EdgeCutBefore: overlap.EdgeCutBefore, EdgeCutAfter: overlap.EdgeCutAfter,
		ShardPlan: cfg.shardPlan,
	}
	buildIdentityDigest, err := m3VariantBuildIdentityDigestV1(identityDescriptor)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		return m3PartitionIndexRow{}, err
	}
	col, err := manager.OpenCollection(m3BenchmarkCollection)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := insertM3SourceRows(col, vectors); err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := col.Flush(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	status, err := col.RebuildVectorIndex(partitionHNSWIndex)
	if err != nil || !status.Loaded {
		return m3PartitionIndexRow{}, fmt.Errorf("source vector rebuild status=%+v: %w", status, err)
	}
	if err := db.Checkpoint(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := db.Close(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	dbOpen = false
	// A million-row source rebuild can leave several generations of HNSW build
	// scratch reachable until the next GC cycle. The persistent-pack phase
	// deliberately starts from a reopened DB, so release that closed phase
	// before mapping the immutable source and materializing derived assets.
	col = nil
	manager = nil
	db = nil
	debug.FreeOSMemory()
	sourcePhysicalBytes, err := m3DirectoryBytes(dir)
	if err != nil || sourcePhysicalBytes <= 0 {
		return m3PartitionIndexRow{}, fmt.Errorf("source physical footprint=%d: %w", sourcePhysicalBytes, err)
	}
	db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	dbOpen = true
	manager = collections.NewCollectionManager(db)
	col, err = manager.OpenCollection(m3BenchmarkCollection)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	started := time.Now()
	sampler := startM3PhysicalFootprintSampler(dir, sourcePhysicalBytes)
	defer func() {
		if sampler != nil {
			_, sampleErr := sampler.Stop()
			resultErr = errors.Join(resultErr, sampleErr)
		}
	}()
	source, sourceRows, err := col.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	sourceOrdinalDigest, err := m3SourceOrdinalDigestV1(sourceRows)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	sourceOrdinals, err := m3SourceOrdinalsByArtifactID(artifact, sourceRows)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	manifest, membershipOrdinals, err := m3BuildingManifest(*meta, source, artifact, overlap, sourceOrdinals, generation, buildIdentityDigest)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	routerPartitions, err := m3RouterPartitions(artifact, overlap, sourceOrdinals, vectors)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	replicaSourceOrdinals := make([]uint64, len(overlap.Replicas))
	for i, replica := range overlap.Replicas {
		if replica.VectorOrdinal < 0 || replica.VectorOrdinal >= len(sourceOrdinals) {
			return m3PartitionIndexRow{}, errors.New("M3 overlap replica ordinal is invalid")
		}
		replicaSourceOrdinals[i] = uint64(sourceOrdinals[replica.VectorOrdinal])
	}
	sourceRows = nil
	sourceOrdinals = nil
	// Source-ordinal reconciliation owns large transient maps and stable-ID
	// copies at the 1M acceptance shape. They are not part of the persistent
	// pack build and retaining them needlessly raises the materializer's peak.
	debug.FreeOSMemory()
	inputs := make([]collections.VectorPartitionSearchAssetV1, cfg.partitions)
	for partition := range inputs {
		inputs[partition] = collections.VectorPartitionSearchAssetV1{
			Source:      source,
			Generation:  generation,
			PartitionID: uint32(partition),
			Dimensions:  len(vectors[0]),
		}
	}
	assetFileID, err := m3PartitionAssetFileID(generation)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsVariantV1(partitionHNSWIndex, manifest, assetFileID, inputs, localVariant)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if resources != nil {
		resources.Release()
	}
	if err := sampler.Sample(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		return m3PartitionIndexRow{}, err
	}
	routerFileID, err := m3RouterAssetFileID(generation)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	routerStatus, err := col.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, routerPartitions, m3RouterBuildOptionsV1(cfg.routerConfig, routerFileID, uint64(manifest.PartitionCount)+1))
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if routerStatus.Generation != generation || routerStatus.RouterBytes == 0 {
		return m3PartitionIndexRow{}, fmt.Errorf("M3 router publication status=%+v", routerStatus)
	}
	routerPartitions = nil
	debug.FreeOSMemory()
	if err := sampler.Sample(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := db.Checkpoint(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := sampler.Sample(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	if err := db.Close(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	dbOpen = false
	finalTotalPhysicalBytes, err := m3DirectoryBytes(dir)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if finalTotalPhysicalBytes < sourcePhysicalBytes {
		return m3PartitionIndexRow{}, fmt.Errorf("final physical footprint=%d below source baseline=%d", finalTotalPhysicalBytes, sourcePhysicalBytes)
	}
	finalDerivedPhysicalBytes := finalTotalPhysicalBytes - sourcePhysicalBytes
	if err := sampler.Sample(); err != nil {
		return m3PartitionIndexRow{}, err
	}
	peakDerivedTemporaryBytes, err := sampler.Stop()
	sampler = nil
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	buildWall := time.Since(started).Nanoseconds()

	db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	dbOpen = true
	manager = collections.NewCollectionManager(db)
	col, err = manager.OpenCollection(m3BenchmarkCollection)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	lifecycle, err := col.VectorPartitionStatusV1(partitionHNSWIndex, generation)
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	if !lifecycle.Ready || !lifecycle.Active || lifecycle.Manifest.RouterGeneration != generation || lifecycle.Manifest.RouterAsset.Bytes == 0 {
		return m3PartitionIndexRow{}, fmt.Errorf("reopened M3 router lifecycle=%+v", lifecycle)
	}
	manifest = lifecycle.Manifest
	if lifecycle.Capacity != uint64(overlap.Capacity) || lifecycle.OverlapBudget != uint64(overlap.Budget) || lifecycle.UnspentOverlapBudget != uint64(overlap.Unspent) {
		return m3PartitionIndexRow{}, fmt.Errorf("reopened lifecycle accounting=%+v", lifecycle)
	}
	router, _, err := col.OpenVectorPartitionRouterV1(partitionHNSWIndex)
	if err != nil {
		return m3PartitionIndexRow{}, fmt.Errorf("reopen M3 router: %w", err)
	}
	routerRuntime := router.Status()
	if err := router.Close(); err != nil {
		return m3PartitionIndexRow{}, fmt.Errorf("close reopened M3 router: %w", err)
	}
	if routerRuntime.Manifest.ReadySetDigest != manifest.ReadySetDigest || routerRuntime.ModelDigest == "" {
		return m3PartitionIndexRow{}, errors.New("reopened M3 router manifest/model identity does not match ready manifest")
	}
	if routerRuntime.Config != cfg.routerConfig {
		return m3PartitionIndexRow{}, errors.New("reopened M3 router configuration does not match parsed configuration")
	}
	openStarted := time.Now()
	searchers, err := openM3PartitionSearchers(cfg.partitions, func(partition uint32) (*collections.VectorPartitionLocalSearcherV1, error) {
		return col.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, generation, partition)
	})
	if err != nil {
		return m3PartitionIndexRow{}, err
	}
	openWall := time.Since(openStarted).Nanoseconds()
	defer func() {
		resultErr = errors.Join(resultErr, closeM3PartitionSearchers(searchers))
	}()

	var recallTotal float64
	var correctnessSearches int
	for _, query := range queries {
		query32 := m3Float32Vector(query)
		for partition, searcher := range searchers {
			topK := min(cfg.topK, len(membershipOrdinals[partition]))
			got, _, err := searcher.SearchWithMetrics(query32, topK)
			if err != nil {
				return m3PartitionIndexRow{}, err
			}
			want := m3ExactPartitionTopK(vectors, query32, membershipOrdinals[partition], topK)
			if err := validateM3AuthoritativeScores(got, vectors, query32, membershipOrdinals[partition]); err != nil {
				return m3PartitionIndexRow{}, err
			}
			recallTotal += m3ResultRecall(want, got)
			correctnessSearches++
		}
	}

	for pass := 0; pass < m3WarmupPasses; pass++ {
		for _, query := range queries {
			query32 := m3Float32Vector(query)
			for partition, searcher := range searchers {
				if _, _, err := searcher.SearchWithMetrics(query32, min(cfg.topK, len(membershipOrdinals[partition]))); err != nil {
					return m3PartitionIndexRow{}, err
				}
			}
		}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	timedStarted := time.Now()
	var candidates, edges uint64
	timedOps := 0
	for _, query := range queries {
		query32 := m3Float32Vector(query)
		for partition, searcher := range searchers {
			_, stats, err := searcher.SearchWithMetrics(query32, min(cfg.topK, len(membershipOrdinals[partition])))
			if err != nil {
				return m3PartitionIndexRow{}, err
			}
			if stats.Route != collections.VectorPartitionSearchRouteHNSWSearchPackV1 {
				return m3PartitionIndexRow{}, fmt.Errorf("partition %d search route=%q", partition, stats.Route)
			}
			candidates += stats.Candidates
			edges += stats.Edges
			timedOps++
		}
	}
	elapsed := time.Since(timedStarted)
	runtime.ReadMemStats(&after)
	if timedOps == 0 || correctnessSearches == 0 {
		return m3PartitionIndexRow{}, errors.New("M3 native search executed zero operations")
	}
	var packBytes, mappedBytes, heapBytes, packOpenNanos uint64
	for _, searcher := range searchers {
		searchStatus := searcher.Status()
		packBytes += searchStatus.PackBytes
		mappedBytes += searchStatus.MappedBytes
		heapBytes += searchStatus.HeapBytes
		packOpenNanos += searchStatus.OpenNanos
	}
	var packPayloadBytes uint64
	for _, asset := range manifest.Assets {
		packPayloadBytes += asset.Bytes
	}
	if packPayloadBytes != packBytes || lifecycle.AssetBytes != packPayloadBytes+manifest.RouterAsset.Bytes {
		return m3PartitionIndexRow{}, fmt.Errorf("pack byte accounting partitions=%d status=%d router=%d lifecycle=%d", packPayloadBytes, packBytes, manifest.RouterAsset.Bytes, lifecycle.AssetBytes)
	}
	peakRSS, peakAvailable := m3PeakRSS()
	resident, residentAvailable := m3ResidentBytes()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(timedOps)
	persistentDBDir := ""
	if !cleanup {
		persistentDBDir = dir
	}
	totalCapacity, err := memoryMul(int64(overlap.Capacity), int64(len(overlap.Loads)))
	if err != nil || totalCapacity < int64(len(overlap.Memberships)) || totalCapacity > int64(math.MaxInt) {
		return m3PartitionIndexRow{}, errors.New("M3 overlap capacity accounting overflow")
	}
	unusedCapacity := int(totalCapacity) - len(overlap.Memberships)
	cutReductionPerUseful := 0.0
	if overlap.Useful > 0 {
		cutReductionPerUseful = float64(overlap.EdgeCutBefore-overlap.EdgeCutAfter) / float64(overlap.Useful)
	}
	row := m3PartitionIndexRow{
		Budget:                       overlap.Budget,
		Used:                         overlap.Used,
		Unspent:                      overlap.Unspent,
		Capacity:                     overlap.Capacity,
		OverlapRequested:             overlap.Budget,
		OverlapRealized:              overlap.Used,
		OverlapRejected:              overlap.Unspent,
		OverlapUseful:                overlap.Useful,
		OverlapFiller:                overlap.Filler,
		OverlapCumulativeUtility:     overlap.CumulativeUtility,
		OverlapDestinationDiversity:  append([]int(nil), overlap.DestinationDiversity...),
		OverlapUnusedCapacity:        unusedCapacity,
		CutReductionPerUsefulReplica: cutReductionPerUseful,
		ReplicationFactor:            float64(len(overlap.Memberships)) / float64(len(artifact.IDs)),
		PartitionLoads:               append([]int(nil), overlap.Loads...),
		EdgeCutBefore:                overlap.EdgeCutBefore,
		EdgeCutAfter:                 overlap.EdgeCutAfter,
		BuildWallNanos:               buildWall,
		PeakRSSBytes:                 peakRSS,
		PeakRSSAvailable:             peakAvailable,
		ResidentBytes:                resident,
		ResidentBytesAvailable:       residentAvailable,
		SourcePhysicalBytes:          sourcePhysicalBytes,
		PeakDerivedTemporaryBytes:    peakDerivedTemporaryBytes,
		FinalDerivedPhysicalBytes:    finalDerivedPhysicalBytes,
		PhysicalBytesPerSourceVector: float64(finalDerivedPhysicalBytes) / float64(len(vectors)),
		PackBytes:                    packBytes,
		PartitionHNSWM:               partitionHNSWM,
		MappedBytes:                  mappedBytes,
		HeapBytes:                    heapBytes,
		SearcherOpenWallNanos:        openWall,
		PackOpenNanos:                packOpenNanos,
		Queries:                      len(queries),
		LocalSearches:                timedOps,
		WarmupPasses:                 m3WarmupPasses,
		WarmNSPerOp:                  nsPerOp,
		WarmQPS:                      1e9 / nsPerOp,
		WarmBytesPerOp:               float64(after.TotalAlloc-before.TotalAlloc) / float64(timedOps),
		WarmAllocsPerOp:              float64(after.Mallocs-before.Mallocs) / float64(timedOps),
		CandidatesPerOp:              float64(candidates) / float64(timedOps),
		EdgesPerOp:                   float64(edges) / float64(timedOps),
		ExactLocalRecallAtK:          recallTotal / float64(correctnessSearches),
		ManifestDigest:               manifest.IntegrityDigest,
		SourceGeneration:             source.Generation,
		SourceChecksum:               source.Checksum,
		SourceSchemaHash:             source.SchemaHash,
		SourceRows:                   source.RowCount,
		SearchRoute:                  collections.VectorPartitionSearchRouteHNSWSearchPackV1,
		MissingAssets:                lifecycle.MissingAssets,
		CorruptAssets:                lifecycle.CorruptAssets,
		StaleAssets:                  lifecycle.StaleAssets,
		PersistentDBDir:              persistentDBDir,
	}
	row.OverlapReplicas = make([]m3OverlapReplica, len(overlap.Replicas))
	for i, replica := range overlap.Replicas {
		row.OverlapReplicas[i] = m3OverlapReplica{SourceOrdinal: replicaSourceOrdinals[i], Destination: replica.Partition, Policy: replica.Policy, Gain: replica.Gain, Class: string(replica.Class)}
	}
	if !cleanup {
		descriptor := m3VariantDescriptorV1{
			SchemaVersion: 6, ResultKind: "m3_persistent_variant_descriptor_v6", VariantID: variantID,
			AssignmentBasis: cfg.partitionAssignment, OverlapRatio: ratio, OverlapPolicy: manifest.BalancePolicy,
			FixtureChecksum: fixture.Checksum, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, BuildDirty: cfg.m3BuildDirty, ExecutableSHA256: executableSHA256, ArtifactSHA256: artifactDigest, GraphArtifactSHA256: graphArtifactDigest, GraphBuildSHA256: graphBuildDigest, ArtifactBackend: artifact.Backend, KaHIPPythonSHA256: cfg.kahipPythonSHA256, KaHIPAdapterSHA256: cfg.kahipAdapterSHA256, Source: artifact.Source,
			BuildIdentityDigest: buildIdentityDigest,
			DatabaseDirectory:   dir, ManifestIntegrity: manifest.IntegrityDigest, ReadySetDigest: manifest.ReadySetDigest,
			RouterAssetChecksum: manifest.RouterAsset.Checksum, RouterModelDigest: routerRuntime.ModelDigest,
			SourceGeneration: manifest.SourceGeneration, SourceChecksum: manifest.SourceChecksum, SourceSchemaHash: manifest.SourceSchemaHash, SourceRows: manifest.SourceRowCount, SourceOrdinalDigest: sourceOrdinalDigest,
			PartitionGeneration: manifest.Generation, RouterGeneration: manifest.RouterGeneration, Partitions: manifest.PartitionCount, IndexDefinitionDigest: manifest.IndexDefinitionDigest,
			Capacity: overlap.Capacity, OverlapRequested: overlap.Budget, OverlapRealized: overlap.Used, OverlapRejected: overlap.Unspent,
			OverlapUseful: overlap.Useful, OverlapFiller: overlap.Filler, OverlapUnusedCapacity: unusedCapacity,
			EdgeCutBefore: overlap.EdgeCutBefore, EdgeCutAfter: overlap.EdgeCutAfter,
			PartitionLoads: append([]int(nil), overlap.Loads...), OverlapMemberships: len(manifest.OverlapMemberships),
			PartitionHNSWM:           partitionHNSWM,
			PartitionHNSWEfC:         partitionHNSWEfConstruction,
			PartitionConfig:          cfg.partition,
			PartitionMaxDistanceWork: cfg.partition.MaxDistanceWork,
			RouterMaxScalarWork:      cfg.routerConfig.MaxScalarWork,
			RouterConfig:             cfg.routerConfig,
			M3MaxBenchmarkVisits:     cfg.m3MaxBenchmarkVisits,
			RouterRepresentatives:    routerRuntime.Representatives,
			PersistentAssetBytes:     packPayloadBytes + manifest.RouterAsset.Bytes,
			ShardPlan:                cfg.shardPlan,
		}
		if err := m3DescriptorMatchesManifestV1(descriptor, fixture, manifest, routerRuntime.ModelDigest, routerRuntime.Config); err != nil {
			return m3PartitionIndexRow{}, err
		}
		if err := m3WriteVariantDescriptorV1(dir, descriptor); err != nil {
			return m3PartitionIndexRow{}, err
		}
	}
	return row, nil
}

func m3PartitionIndexDirectory(persist string) (dir string, cleanup bool, err error) {
	if persist == "" {
		dir, err = os.MkdirTemp("", "treedb-vector-partition-m3-*")
		return dir, true, err
	}
	dir, err = filepath.Abs(persist)
	if err != nil {
		return "", false, err
	}
	entries, readErr := os.ReadDir(dir)
	switch {
	case readErr == nil && len(entries) != 0:
		return "", false, fmt.Errorf("M3 persistent DB directory %q must be empty", dir)
	case readErr == nil:
	case errors.Is(readErr, os.ErrNotExist):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", false, err
		}
	default:
		return "", false, readErr
	}
	return dir, false, nil
}

func insertM3SourceRows(col *collections.Collection, vectors [][]float64) error {
	// Keep the acceptance load to a bounded number of physical column-graph
	// publications. Tiny batches retain thousands of superseded generations
	// until the benchmark's deliberate close/reopen boundary; 8K rows keeps
	// individual command-WAL frames bounded while reducing the 1M-row load to
	// 123 publications.
	for base := 0; base < len(vectors); base += m3SourceInsertBatchRows {
		end := min(base+m3SourceInsertBatchRows, len(vectors))
		ids := make([][]byte, end-base)
		documents := make([][]byte, end-base)
		for i := base; i < end; i++ {
			vector := m3Float32Vector(vectors[i])
			raw, err := json.Marshal(struct {
				TimeUS    int64     `json:"time_us"`
				Embedding []float32 `json:"embedding"`
			}{TimeUS: int64(i + 1), Embedding: vector})
			if err != nil {
				return err
			}
			ids[i-base] = []byte(fmt.Sprintf("doc-%06d", i))
			documents[i-base] = raw
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			return err
		}
	}
	return nil
}

func m3SourceOrdinalsByArtifactID(artifact vectorpartition.Artifact, rows []collections.VectorPartitionSourceOrdinalV1) ([]int, error) {
	if len(rows) != len(artifact.IDs) {
		return nil, fmt.Errorf("M3 native source rows=%d artifact IDs=%d", len(rows), len(artifact.IDs))
	}
	artifactOrdinals := make(map[string]int, len(artifact.IDs))
	for ordinal, id := range artifact.IDs {
		artifactOrdinals[id] = ordinal
	}
	sourceOrdinals := make([]int, len(artifact.IDs))
	seen := make([]bool, len(artifact.IDs))
	for _, row := range rows {
		artifactOrdinal, ok := artifactOrdinals[row.StableID]
		if !ok || row.Ordinal >= uint64(len(rows)) || seen[artifactOrdinal] {
			return nil, fmt.Errorf("M3 native source stable ID %q does not bind uniquely to artifact", row.StableID)
		}
		sourceOrdinals[artifactOrdinal] = int(row.Ordinal)
		seen[artifactOrdinal] = true
	}
	for ordinal, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("M3 artifact stable ID %q is absent from native source", artifact.IDs[ordinal])
		}
	}
	return sourceOrdinals, nil
}

func m3SourceOrdinalDigestV1(rows []collections.VectorPartitionSourceOrdinalV1) (string, error) {
	raw, err := json.Marshal(rows)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("%x", digest[:]), nil
}

func m3RouterPartitions(artifact vectorpartition.Artifact, overlap vectorpartition.OverlapResult, sourceOrdinals []int, vectors [][]float64) ([]vectorpartition.RouterPartitionV1, error) {
	if len(artifact.Assignment) == 0 || len(artifact.Assignment) != len(sourceOrdinals) || len(vectors) != len(sourceOrdinals) ||
		artifact.Config.Partitions < 1 || len(vectors[0]) < 1 || len(overlap.Memberships) < len(artifact.Assignment) {
		return nil, errors.New("M3 router source shape mismatch")
	}
	dimensions := len(vectors[0])
	if len(vectors) > math.MaxInt/dimensions {
		return nil, errors.New("M3 router vector backing size overflow")
	}
	counts := make([]int, artifact.Config.Partitions)
	seenSourceOrdinals := make([]bool, len(sourceOrdinals))
	for ordinal, partition := range artifact.Assignment {
		if partition < 0 || partition >= len(counts) || sourceOrdinals[ordinal] < 0 || sourceOrdinals[ordinal] >= len(vectors) || seenSourceOrdinals[sourceOrdinals[ordinal]] || len(vectors[ordinal]) != dimensions {
			return nil, errors.New("M3 router assignment or source ordinal is invalid")
		}
		seenSourceOrdinals[sourceOrdinals[ordinal]] = true
	}
	seenMemberships := make(map[[2]int]struct{}, len(overlap.Memberships))
	for _, membership := range overlap.Memberships {
		if membership.VectorOrdinal < 0 || membership.VectorOrdinal >= len(vectors) || membership.Partition < 0 || membership.Partition >= len(counts) || membership.Home != (membership.Partition == artifact.Assignment[membership.VectorOrdinal]) {
			return nil, errors.New("M3 final router membership is invalid")
		}
		key := [2]int{membership.VectorOrdinal, membership.Partition}
		if _, duplicate := seenMemberships[key]; duplicate {
			return nil, errors.New("M3 final router membership is duplicate")
		}
		seenMemberships[key] = struct{}{}
		counts[membership.Partition]++
	}
	partitions := make([]vectorpartition.RouterPartitionV1, len(counts))
	for partition := range partitions {
		partitions[partition].PartitionID = uint32(partition)
		partitions[partition].Vectors = make([]vectorpartition.RouterVectorV1, 0, counts[partition])
	}
	values := make([]float32, len(vectors)*dimensions)
	for _, membership := range overlap.Memberships {
		artifactOrdinal, partition := membership.VectorOrdinal, membership.Partition
		offset := artifactOrdinal * dimensions
		row := values[offset : offset+dimensions : offset+dimensions]
		for dimension, value := range vectors[artifactOrdinal] {
			row[dimension] = float32(value)
		}
		kind := string(collections.VectorPartitionMembershipOverlapV1)
		if membership.Home {
			kind = string(collections.VectorPartitionMembershipHomeV1)
		}
		partitions[partition].Vectors = append(partitions[partition].Vectors, vectorpartition.RouterVectorV1{
			Ordinal:        uint64(sourceOrdinals[artifactOrdinal]),
			Values:         row,
			MembershipKind: kind,
		})
	}
	return partitions, nil
}

func m3BuildingManifest(meta collections.CollectionMeta, source collections.VectorPartitionSourceIdentityV1, artifact vectorpartition.Artifact, overlap vectorpartition.OverlapResult, sourceOrdinals []int, generation uint64, buildIdentityDigest string) (collections.VectorPartitionManifestV1, [][]int, error) {
	if len(sourceOrdinals) != len(artifact.IDs) {
		return collections.VectorPartitionManifestV1{}, nil, errors.New("M3 source ordinal mapping length mismatch")
	}
	policy, err := collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{Capacity: uint64(overlap.Capacity), Budget: uint64(overlap.Budget), Realized: uint64(overlap.Used), Unspent: uint64(overlap.Unspent), BuildIdentityDigest: buildIdentityDigest})
	if err != nil {
		return collections.VectorPartitionManifestV1{}, nil, err
	}
	var def collections.VectorIndexDefinition
	for _, candidate := range meta.VectorIndexes {
		if candidate.Name == partitionHNSWIndex {
			def = candidate
			break
		}
	}
	if def.Name == "" {
		return collections.VectorPartitionManifestV1{}, nil, errors.New("M3 source vector index definition missing")
	}
	manifest := collections.VectorPartitionManifestV1{
		State:                 "building",
		Collection:            meta.Name,
		IndexName:             def.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(def),
		SourceGeneration:      source.Generation,
		SourceChecksum:        source.Checksum,
		SourceSchemaHash:      source.SchemaHash,
		SourceRowCount:        source.RowCount,
		Generation:            generation,
		PartitionCount:        uint32(artifact.Config.Partitions),
		BalancePolicy:         policy,
	}
	membershipOrdinals := make([][]int, artifact.Config.Partitions)
	for partition := 0; partition < artifact.Config.Partitions; partition++ {
		manifest.Placements = append(manifest.Placements, collections.VectorPartitionPlacementV1{PartitionID: uint32(partition), GroupID: fmt.Sprintf("benchmark-group-%06d", partition)})
	}
	for ordinal, partition := range artifact.Assignment {
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(sourceOrdinals[ordinal]), PartitionID: uint32(partition)})
	}
	for _, membership := range overlap.Memberships {
		membershipOrdinals[membership.Partition] = append(membershipOrdinals[membership.Partition], membership.VectorOrdinal)
		if !membership.Home {
			manifest.OverlapMemberships = append(manifest.OverlapMemberships, collections.VectorPartitionMembershipV1{VectorOrdinal: uint64(sourceOrdinals[membership.VectorOrdinal]), PartitionID: uint32(membership.Partition)})
		}
	}
	manifest.Canonicalize()
	return manifest, membershipOrdinals, nil
}

func m3Float32Vector(in []float64) []float32 {
	out := make([]float32, len(in))
	for i, value := range in {
		out[i] = float32(value)
	}
	return out
}

type m3ExactResult struct {
	ID    string
	Score float64
}

func m3ExactPartitionTopK(vectors [][]float64, query []float32, ordinals []int, topK int) []m3ExactResult {
	out := make([]m3ExactResult, 0, len(ordinals))
	for _, ordinal := range ordinals {
		out = append(out, m3ExactResult{ID: fmt.Sprintf("doc-%06d", ordinal), Score: m3ExactCosine(vectors[ordinal], query)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > topK {
		out = out[:topK]
	}
	return out
}

func m3ExactCosine(vector []float64, query []float32) float64 {
	var dot, vectorNorm, queryNorm float64
	for i, value64 := range vector {
		value := float64(float32(value64))
		q := float64(query[i])
		dot += value * q
		vectorNorm += value * value
		queryNorm += q * q
	}
	return dot / math.Sqrt(vectorNorm*queryNorm)
}

func validateM3AuthoritativeScores(got []collections.VectorPartitionSearchResultV1, vectors [][]float64, query []float32, ordinals []int) error {
	allowed := make(map[string]int, len(ordinals))
	for _, ordinal := range ordinals {
		allowed[fmt.Sprintf("doc-%06d", ordinal)] = ordinal
	}
	seen := make(map[string]struct{}, len(got))
	for _, result := range got {
		ordinal, ok := allowed[result.ID]
		if !ok {
			return fmt.Errorf("native partition result %q is not a validated membership", result.ID)
		}
		if _, duplicate := seen[result.ID]; duplicate {
			return fmt.Errorf("native partition result duplicated stable ID %q", result.ID)
		}
		seen[result.ID] = struct{}{}
		want := m3ExactCosine(vectors[ordinal], query)
		if math.Abs(float64(result.Score)-want) > 2e-5 {
			return fmt.Errorf("native partition result %q score=%g want authoritative FP32=%g", result.ID, result.Score, want)
		}
	}
	return nil
}

func m3ResultRecall(want []m3ExactResult, got []collections.VectorPartitionSearchResultV1) float64 {
	if len(want) == 0 {
		return 1
	}
	found := make(map[string]struct{}, len(got))
	for _, result := range got {
		found[result.ID] = struct{}{}
	}
	matches := 0
	for _, result := range want {
		if _, ok := found[result.ID]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(want))
}

func m3ReplicationGate(rows []m3PartitionIndexRow) string {
	var baseline, overlap20 *m3PartitionIndexRow
	for i := range rows {
		switch {
		case rows[i].Ratio == 0:
			baseline = &rows[i]
		case math.Abs(rows[i].Ratio-.20) < 1e-12:
			overlap20 = &rows[i]
		}
	}
	if baseline == nil || overlap20 == nil {
		return "not_evaluated: requires overlap 0 and 0.20 rows"
	}
	if baseline.FinalDerivedPhysicalBytes <= 0 {
		return "failed: zero disjoint baseline bytes"
	}
	if float64(overlap20.FinalDerivedPhysicalBytes) > 1.35*float64(baseline.FinalDerivedPhysicalBytes) {
		return fmt.Sprintf("failed: %.6fx exceeds 1.35x", float64(overlap20.FinalDerivedPhysicalBytes)/float64(baseline.FinalDerivedPhysicalBytes))
	}
	return fmt.Sprintf("passed: %.6fx <= 1.35x", float64(overlap20.FinalDerivedPhysicalBytes)/float64(baseline.FinalDerivedPhysicalBytes))
}

func validateM3PartitionIndexReport(report m3PartitionIndexReport) error {
	if report.SchemaVersion != m3ReportSchemaVersion || report.ResultKind != "m3_native_partition_hnsw_evidence" || len(report.Rows) == 0 || !m8SHA256V1(report.ArtifactSHA256) || !m8SHA256V1(report.GraphArtifactSHA256) || !validSHA(report.BaseSHA) || !validSHA(report.HeadSHA) {
		return errors.New("invalid M3 report identity")
	}
	for _, row := range report.Rows {
		wantBudgetFloat := math.Floor(row.Ratio * float64(row.SourceRows))
		if math.IsNaN(row.Ratio) || math.IsInf(row.Ratio, 0) || row.Ratio < 0 || row.Ratio > 1 || wantBudgetFloat > float64(math.MaxInt) {
			return fmt.Errorf("invalid M3 overlap target: ratio=%g source_rows=%d", row.Ratio, row.SourceRows)
		}
		wantBudget := int(wantBudgetFloat)
		totalCapacity, capacityErr := memoryMul(int64(row.Capacity), int64(len(row.PartitionLoads)))
		if capacityErr != nil || row.SourceRows > math.MaxInt || totalCapacity < int64(row.SourceRows)+int64(row.OverlapRealized) {
			return fmt.Errorf("invalid M3 overlap capacity evidence: %+v", row)
		}
		var loadTotal int64
		for _, load := range row.PartitionLoads {
			if load < 0 || load > row.Capacity {
				return fmt.Errorf("invalid M3 partition load evidence: %+v", row)
			}
			loadTotal += int64(load)
		}
		if loadTotal != int64(row.SourceRows)+int64(row.OverlapRealized) {
			return fmt.Errorf("invalid M3 partition load total: %+v", row)
		}
		wantUnusedCapacity := int(totalCapacity - int64(row.SourceRows) - int64(row.OverlapRealized))
		wantCutReductionPerUseful := 0.0
		if row.OverlapUseful > 0 {
			wantCutReductionPerUseful = float64(row.EdgeCutBefore-row.EdgeCutAfter) / float64(row.OverlapUseful)
		}
		if row.Budget != wantBudget || row.Used > wantBudget || row.OverlapRequested != wantBudget || row.OverlapRealized != row.Used || row.Budget < 0 || row.Used < 0 || row.Unspent != row.Budget-row.Used || row.Capacity < 1 || row.OverlapRejected != row.Unspent || row.OverlapUseful < 0 || row.OverlapFiller != 0 || row.OverlapUseful != row.OverlapRealized || row.OverlapUnusedCapacity != wantUnusedCapacity || row.CutReductionPerUsefulReplica != wantCutReductionPerUseful || row.ReplicationFactor < 1 || row.EdgeCutAfter > row.EdgeCutBefore || row.BuildWallNanos <= 0 || row.SourcePhysicalBytes <= 0 || row.PeakDerivedTemporaryBytes < row.FinalDerivedPhysicalBytes || row.FinalDerivedPhysicalBytes <= 0 || row.PackBytes == 0 || row.PartitionHNSWM < 2 || row.FinalDerivedPhysicalBytes < int64(row.PackBytes) || row.PhysicalBytesPerSourceVector <= 0 || row.SearcherOpenWallNanos <= 0 || row.PackOpenNanos == 0 || row.LocalSearches <= 0 || row.WarmNSPerOp <= 0 || row.WarmQPS <= 0 || row.CandidatesPerOp <= 0 || row.ExactLocalRecallAtK < 0 || row.ExactLocalRecallAtK > 1 || row.ManifestDigest == "" || row.SourceRows != uint64(report.Dataset.Vectors) || row.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || row.MissingAssets != 0 || row.CorruptAssets != 0 || row.StaleAssets != 0 {
			return fmt.Errorf("invalid M3 evidence row: %+v", row)
		}
		if len(row.OverlapReplicas) != row.OverlapRealized || len(row.OverlapDestinationDiversity) != len(row.PartitionLoads) {
			return fmt.Errorf("invalid M3 overlap replica evidence: %+v", row)
		}
		utility, useful, filler := 0, 0, 0
		seenReplica := make(map[[2]uint64]struct{}, len(row.OverlapReplicas))
		for _, replica := range row.OverlapReplicas {
			key := [2]uint64{replica.SourceOrdinal, uint64(replica.Destination)}
			if replica.Destination < 0 || replica.Destination >= len(row.PartitionLoads) || replica.Policy == "" || replica.Gain < 0 || (replica.Class != string(vectorpartition.ReplicaUtilityPositiveGainV1) && replica.Class != string(vectorpartition.ReplicaUtilityZeroUtilityV1)) {
				return fmt.Errorf("invalid M3 overlap replica: %+v", replica)
			}
			if _, duplicate := seenReplica[key]; duplicate {
				return fmt.Errorf("duplicate M3 overlap replica: %+v", replica)
			}
			seenReplica[key] = struct{}{}
			utility += replica.Gain
			if replica.Class == string(vectorpartition.ReplicaUtilityPositiveGainV1) {
				if replica.Gain == 0 {
					return fmt.Errorf("zero-gain positive M3 overlap replica: %+v", replica)
				}
				useful++
			} else {
				if replica.Gain != 0 {
					return fmt.Errorf("positive-gain filler M3 overlap replica: %+v", replica)
				}
				filler++
			}
		}
		if utility != row.OverlapCumulativeUtility || useful != row.OverlapUseful || filler != row.OverlapFiller {
			return fmt.Errorf("invalid M3 overlap utility evidence: %+v", row)
		}
		for _, diversity := range row.OverlapDestinationDiversity {
			if diversity < 0 || diversity > row.OverlapRealized {
				return fmt.Errorf("invalid M3 overlap diversity evidence: %+v", row)
			}
		}
		for _, value := range []float64{row.Ratio, row.ReplicationFactor, row.PhysicalBytesPerSourceVector, row.WarmNSPerOp, row.WarmQPS, row.WarmBytesPerOp, row.WarmAllocsPerOp, row.CandidatesPerOp, row.EdgesPerOp, row.ExactLocalRecallAtK} {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return errors.New("non-finite M3 evidence")
			}
		}
	}
	if len(report.Rows) >= 2 && strings.HasPrefix(report.ReplicationGate, "failed:") {
		return errors.New(report.ReplicationGate)
	}
	return nil
}

func m3DirectoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > math.MaxInt64-total {
			return errors.New("M3 temporary byte accounting overflow")
		}
		total += info.Size()
		return nil
	})
	return total, err
}

type m3PhysicalFootprintSampler struct {
	root     string
	baseline int64
	stop     chan struct{}
	done     chan struct{}

	mu      sync.Mutex
	peak    int64
	err     error
	stopped bool
}

func startM3PhysicalFootprintSampler(root string, baseline int64) *m3PhysicalFootprintSampler {
	sampler := &m3PhysicalFootprintSampler{root: root, baseline: baseline, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = sampler.Sample()
			case <-sampler.stop:
				return
			}
		}
	}()
	return sampler
}

func (sampler *m3PhysicalFootprintSampler) Sample() error {
	if sampler == nil {
		return errors.New("nil M3 physical footprint sampler")
	}
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	if sampler.err != nil {
		return sampler.err
	}
	total, err := m3DirectoryBytes(sampler.root)
	if err != nil {
		sampler.err = err
		return err
	}
	delta := total - sampler.baseline
	if delta < 0 {
		sampler.err = fmt.Errorf("M3 physical footprint=%d below source baseline=%d", total, sampler.baseline)
		return sampler.err
	}
	if delta > sampler.peak {
		sampler.peak = delta
	}
	return nil
}

func (sampler *m3PhysicalFootprintSampler) Stop() (int64, error) {
	if sampler == nil {
		return 0, nil
	}
	sampler.mu.Lock()
	if !sampler.stopped {
		close(sampler.stop)
		sampler.stopped = true
	}
	sampler.mu.Unlock()
	<-sampler.done
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	return sampler.peak, sampler.err
}
