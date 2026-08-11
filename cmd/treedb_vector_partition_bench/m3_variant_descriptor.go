package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	m3VariantDescriptorFileV1     = "vector_partition_variant_v1.json"
	m3VariantDescriptorMaxBytesV1 = 1 << 20
)

// m3VariantDescriptorV1 is an immutable, portable identity for one retained
// M3 database. M8 derives the comparison variant from this descriptor instead
// of accepting graph/hash/overlap labels from its command line.
type m3VariantDescriptorV1 struct {
	SchemaVersion            int                            `json:"schema_version"`
	ResultKind               string                         `json:"result_kind"`
	VariantID                string                         `json:"variant_id"`
	AssignmentBasis          string                         `json:"assignment_basis"`
	OverlapRatio             float64                        `json:"overlap_ratio"`
	OverlapPolicy            string                         `json:"overlap_policy"`
	FixtureChecksum          string                         `json:"fixture_checksum"`
	BaseSHA                  string                         `json:"base_sha"`
	HeadSHA                  string                         `json:"head_sha"`
	BuildDirty               bool                           `json:"build_dirty"`
	ExecutableSHA256         string                         `json:"executable_sha256"`
	ArtifactSHA256           string                         `json:"artifact_sha256"`
	GraphArtifactSHA256      string                         `json:"graph_artifact_sha256"`
	GraphBuildSHA256         string                         `json:"graph_build_sha256"`
	ArtifactBackend          string                         `json:"artifact_backend"`
	KaHIPPythonSHA256        string                         `json:"kahip_python_sha256"`
	KaHIPAdapterSHA256       string                         `json:"kahip_adapter_sha256"`
	Source                   vectorpartition.Source         `json:"source"`
	BuildIdentityDigest      string                         `json:"build_identity_digest"`
	DatabaseDirectory        string                         `json:"database_directory"`
	ManifestIntegrity        string                         `json:"manifest_integrity_digest"`
	ReadySetDigest           string                         `json:"ready_set_digest"`
	RouterAssetChecksum      string                         `json:"router_asset_checksum"`
	RouterModelDigest        string                         `json:"router_model_digest"`
	SourceGeneration         uint64                         `json:"source_generation"`
	SourceChecksum           uint64                         `json:"source_checksum"`
	SourceSchemaHash         uint64                         `json:"source_schema_hash"`
	SourceRows               uint64                         `json:"source_rows"`
	PartitionGeneration      uint64                         `json:"partition_generation"`
	RouterGeneration         uint64                         `json:"router_generation"`
	Partitions               uint32                         `json:"partitions"`
	IndexDefinitionDigest    string                         `json:"index_definition_digest"`
	PartitionHNSWM           int                            `json:"partition_hnsw_m"`
	PartitionConfig          vectorpartition.Config         `json:"partition_config"`
	PartitionMaxDistanceWork int64                          `json:"partition_max_distance_work"`
	RouterMaxScalarWork      int64                          `json:"router_max_scalar_work"`
	RouterConfig             vectorpartition.RouterConfigV1 `json:"router_config"`
	M3MaxBenchmarkVisits     int64                          `json:"m3_max_benchmark_visits"`
	RouterRepresentatives    uint64                         `json:"router_representatives"`
	Capacity                 int                            `json:"capacity"`
	OverlapRequested         int                            `json:"overlap_requested"`
	OverlapRealized          int                            `json:"overlap_realized"`
	OverlapRejected          int                            `json:"overlap_rejected"`
	OverlapUseful            int                            `json:"overlap_useful"`
	OverlapFiller            int                            `json:"overlap_filler"`
	OverlapUnusedCapacity    int                            `json:"overlap_unused_capacity"`
	EdgeCutBefore            int                            `json:"edge_cut_before"`
	EdgeCutAfter             int                            `json:"edge_cut_after"`
	PartitionLoads           []int                          `json:"partition_loads"`
	OverlapMemberships       int                            `json:"overlap_memberships"`
	PersistentAssetBytes     uint64                         `json:"persistent_asset_bytes"`
}

func m3GraphBuildSHA256V1(artifact vectorpartition.Artifact) (string, error) {
	raw, err := json.Marshal(struct {
		Source vectorpartition.Source
		Config vectorpartition.Config
		IDs    []string
		Graph  vectorpartition.Graph
	}{Source: artifact.Source, Config: artifact.Config, IDs: artifact.IDs, Graph: artifact.Graph})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("%x", digest[:]), nil
}

func m3VariantBuildIdentityDigestV1(d m3VariantDescriptorV1) (string, error) {
	identity := struct {
		FixtureChecksum          string
		BaseSHA                  string
		HeadSHA                  string
		BuildDirty               bool
		ExecutableSHA256         string
		VariantID                string
		AssignmentBasis          string
		OverlapRatio             float64
		ArtifactSHA256           string
		GraphArtifactSHA256      string
		GraphBuildSHA256         string
		ArtifactBackend          string
		KaHIPPythonSHA256        string
		KaHIPAdapterSHA256       string
		Source                   vectorpartition.Source
		IndexDefinitionDigest    string
		PartitionHNSWM           int
		PartitionConfig          vectorpartition.Config
		PartitionMaxDistanceWork int64
		RouterMaxScalarWork      int64
		RouterConfig             vectorpartition.RouterConfigV1
		M3MaxBenchmarkVisits     int64
		Capacity                 int
		OverlapRequested         int
		OverlapUseful            int
		OverlapFiller            int
		EdgeCutBefore            int
		EdgeCutAfter             int
	}{
		FixtureChecksum: d.FixtureChecksum, BaseSHA: d.BaseSHA, HeadSHA: d.HeadSHA, BuildDirty: d.BuildDirty, ExecutableSHA256: d.ExecutableSHA256, VariantID: d.VariantID, AssignmentBasis: d.AssignmentBasis, OverlapRatio: d.OverlapRatio,
		ArtifactSHA256: d.ArtifactSHA256, GraphArtifactSHA256: d.GraphArtifactSHA256, GraphBuildSHA256: d.GraphBuildSHA256, ArtifactBackend: d.ArtifactBackend, KaHIPPythonSHA256: d.KaHIPPythonSHA256, KaHIPAdapterSHA256: d.KaHIPAdapterSHA256,
		Source: d.Source, IndexDefinitionDigest: d.IndexDefinitionDigest, PartitionHNSWM: d.PartitionHNSWM, PartitionConfig: d.PartitionConfig,
		PartitionMaxDistanceWork: d.PartitionMaxDistanceWork, RouterMaxScalarWork: d.RouterMaxScalarWork, RouterConfig: d.RouterConfig,
		M3MaxBenchmarkVisits: d.M3MaxBenchmarkVisits,
		Capacity:             d.Capacity, OverlapRequested: d.OverlapRequested,
		OverlapUseful: d.OverlapUseful, OverlapFiller: d.OverlapFiller,
		EdgeCutBefore: d.EdgeCutBefore, EdgeCutAfter: d.EdgeCutAfter,
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("%x", digest[:]), nil
}

func m3VariantIDV1(assignment string, ratio float64) (string, error) {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return "", errors.New("M3 variant overlap ratio must be finite in [0,1]")
	}
	switch assignment {
	case partitionAssignmentStableIDHashV1:
		if ratio != 0 {
			return "", errors.New("stable-ID hash variant requires disjoint membership")
		}
		return "stable-id-hash-disjoint-v1", nil
	case partitionAssignmentGraphV1:
		if ratio == 0 {
			return "graph-disjoint-v1", nil
		}
		if ratio == .2 {
			return "graph-overlap-020-v1", nil
		}
		encoded := strings.NewReplacer(".", "p", "-", "m").Replace(strconv.FormatFloat(ratio, 'g', -1, 64))
		return "graph-overlap-" + encoded + "-v1", nil
	default:
		return "", fmt.Errorf("unknown M3 assignment basis %q", assignment)
	}
}

func m3WriteVariantDescriptorV1(dir string, descriptor m3VariantDescriptorV1) error {
	if err := validateM3VariantDescriptorV1(descriptor); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(descriptor, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > m3VariantDescriptorMaxBytesV1 {
		return errors.New("M3 variant descriptor exceeds byte cap")
	}
	path := filepath.Join(dir, m3VariantDescriptorFileV1)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create immutable M3 variant descriptor: %w", err)
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	return errors.Join(writeErr, file.Close())
}

func m3ReadVariantDescriptorV1(dir string) (m3VariantDescriptorV1, error) {
	path := filepath.Join(dir, m3VariantDescriptorFileV1)
	raw, err := readBoundedRegularFileV1(path, m3VariantDescriptorMaxBytesV1)
	if err != nil {
		return m3VariantDescriptorV1{}, fmt.Errorf("read M3 variant descriptor: %w", err)
	}
	if len(raw) == 0 || len(raw) > m3VariantDescriptorMaxBytesV1 {
		return m3VariantDescriptorV1{}, errors.New("M3 variant descriptor has invalid byte length")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var descriptor m3VariantDescriptorV1
	if err := decoder.Decode(&descriptor); err != nil {
		return m3VariantDescriptorV1{}, fmt.Errorf("decode M3 variant descriptor: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return m3VariantDescriptorV1{}, errors.New("M3 variant descriptor has trailing JSON")
	}
	if err := validateM3VariantDescriptorV1(descriptor); err != nil {
		return m3VariantDescriptorV1{}, err
	}
	return descriptor, nil
}

func validateM3VariantDescriptorV1(d m3VariantDescriptorV1) error {
	if err := vectorpartition.ValidateConfig(d.PartitionConfig); err != nil {
		return fmt.Errorf("malformed M3 partition config: %w", err)
	}
	if err := vectorpartition.ValidateRouterConfigV1(d.RouterConfig); err != nil {
		return fmt.Errorf("malformed M3 router config: %w", err)
	}
	wantVariant, err := m3VariantIDV1(d.AssignmentBasis, d.OverlapRatio)
	if err != nil {
		return err
	}
	wantBudgetFloat := math.Floor(d.OverlapRatio * float64(d.SourceRows))
	if wantBudgetFloat > float64(math.MaxInt) {
		return errors.New("M3 variant overlap target exceeds host integer range")
	}
	wantBudget := int(wantBudgetFloat)
	wantBuildIdentity, err := m3VariantBuildIdentityDigestV1(d)
	if err != nil {
		return err
	}
	if d.Capacity < 1 || d.Partitions < 1 || uint64(d.Capacity) > math.MaxUint64/uint64(d.Partitions) {
		return errors.New("malformed M3 variant descriptor")
	}
	totalCapacity := uint64(d.Capacity) * uint64(d.Partitions)
	usedCapacity := d.SourceRows + uint64(d.OverlapRealized)
	if usedCapacity < d.SourceRows || usedCapacity > totalCapacity || totalCapacity-usedCapacity > uint64(math.MaxInt) {
		return errors.New("malformed M3 variant descriptor")
	}
	if d.SchemaVersion != 5 || d.ResultKind != "m3_persistent_variant_descriptor_v5" || d.VariantID != wantVariant ||
		!m8SHA256V1(d.FixtureChecksum) || !validSHA(d.BaseSHA) || !validSHA(d.HeadSHA) || !m8SHA256V1(d.ExecutableSHA256) || !m8SHA256V1(d.ArtifactSHA256) || !m8SHA256V1(d.GraphArtifactSHA256) || d.ArtifactBackend == "" ||
		!m8SHA256V1(d.GraphBuildSHA256) || !m8SHA256V1(d.BuildIdentityDigest) || d.BuildIdentityDigest != wantBuildIdentity ||
		!m8SHA256V1(d.Source.Checksum) || d.DatabaseDirectory == "" || !m8SHA256V1(d.ManifestIntegrity) || !m8SHA256V1(d.ReadySetDigest) ||
		!m8SHA256V1(d.RouterAssetChecksum) || !m8SHA256V1(d.RouterModelDigest) || d.SourceGeneration == 0 || d.SourceRows == 0 ||
		d.PartitionGeneration == 0 || d.RouterGeneration != d.PartitionGeneration || !m8SHA256V1(d.IndexDefinitionDigest) || d.PartitionHNSWM < 2 || d.PartitionHNSWM > maxPartitionLocalHNSWM || d.PartitionConfig.Partitions != int(d.Partitions) || d.PartitionConfig.MaxDistanceWork != d.PartitionMaxDistanceWork || d.PartitionMaxDistanceWork < 1 || d.RouterMaxScalarWork < 1 || d.M3MaxBenchmarkVisits < 1 || d.RouterRepresentatives == 0 || d.RouterRepresentatives > d.SourceRows || d.RouterConfig.MaxScalarWork != d.RouterMaxScalarWork || d.RouterRepresentatives > uint64(d.RouterConfig.MaxRepresentatives) || d.OverlapRequested != wantBudget || d.OverlapRealized != wantBudget || d.OverlapRequested < 0 || d.OverlapRealized < 0 || d.OverlapRejected < 0 || d.OverlapRequested != d.OverlapRealized+d.OverlapRejected || d.OverlapUseful < 0 || d.OverlapFiller < 0 || d.OverlapUseful+d.OverlapFiller != d.OverlapRealized || d.OverlapMemberships != d.OverlapRealized || d.EdgeCutBefore < d.EdgeCutAfter || d.EdgeCutAfter < 0 || d.OverlapUnusedCapacity != int(totalCapacity-usedCapacity) ||
		len(d.PartitionLoads) != int(d.Partitions) || d.PersistentAssetBytes == 0 {
		return errors.New("malformed M3 variant descriptor")
	}
	policy, ok := collections.ParseVectorPartitionOverlapPolicyV1(d.OverlapPolicy)
	if !ok || policy.Capacity != uint64(d.Capacity) || policy.Budget != uint64(wantBudget) || policy.Realized != uint64(wantBudget) || policy.Unspent != 0 || policy.BuildIdentityDigest != d.BuildIdentityDigest {
		return errors.New("M3 variant descriptor overlap policy is not exact-target canonical")
	}
	if d.AssignmentBasis == partitionAssignmentGraphV1 && d.ArtifactSHA256 != d.GraphArtifactSHA256 {
		return errors.New("graph M3 variant artifact does not match its graph-build identity")
	}
	kahipBackend := fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", d.PartitionConfig.Seed)
	if d.ArtifactBackend == kahipBackend {
		if !m8SHA256V1(d.KaHIPPythonSHA256) || !m8SHA256V1(d.KaHIPAdapterSHA256) {
			return errors.New("KaHIP M3 variant descriptor lacks complete execution identity")
		}
	} else if d.KaHIPPythonSHA256 != "" || d.KaHIPAdapterSHA256 != "" {
		return errors.New("non-KaHIP M3 variant descriptor has KaHIP execution identity")
	}
	var loadTotal uint64
	for _, load := range d.PartitionLoads {
		if load < 0 || load > d.Capacity {
			return errors.New("M3 variant descriptor partition load exceeds capacity")
		}
		loadTotal += uint64(load)
	}
	if loadTotal != usedCapacity {
		return errors.New("M3 variant descriptor partition loads do not match memberships")
	}
	return nil
}

func m3DescriptorMatchesManifestV1(d m3VariantDescriptorV1, fixture fixtureManifest, manifest collections.VectorPartitionManifestV1, routerModelDigest string, routerConfig vectorpartition.RouterConfigV1) error {
	if err := validateM3VariantDescriptorV1(d); err != nil {
		return err
	}
	policy, ok := collections.ParseVectorPartitionOverlapPolicyV1(manifest.BalancePolicy)
	if !ok || policy.BuildIdentityDigest == "" || policy.BuildIdentityDigest != d.BuildIdentityDigest || policy.Capacity != uint64(d.Capacity) {
		return errors.New("M3 variant descriptor build identity or capacity is not manifest-authoritative")
	}
	wantBudget := uint64(math.Floor(d.OverlapRatio * float64(d.SourceRows)))
	used := uint64(len(manifest.OverlapMemberships))
	if policy.Budget != wantBudget || policy.Realized != used || policy.Unspent != policy.Budget-used || d.OverlapRequested != int(policy.Budget) || d.OverlapRealized != int(policy.Realized) || d.OverlapRejected != int(policy.Unspent) {
		return errors.New("M3 variant descriptor overlap accounting does not match the retained manifest")
	}
	loads := make([]int, manifest.PartitionCount)
	for _, membership := range manifest.Memberships {
		if membership.PartitionID >= manifest.PartitionCount {
			return errors.New("M3 retained manifest has an out-of-range home membership")
		}
		loads[membership.PartitionID]++
	}
	for _, membership := range manifest.OverlapMemberships {
		if membership.PartitionID >= manifest.PartitionCount {
			return errors.New("M3 retained manifest has an out-of-range overlap membership")
		}
		loads[membership.PartitionID]++
	}
	if !slices.Equal(loads, d.PartitionLoads) {
		return errors.New("M3 variant descriptor partition loads do not match the retained manifest")
	}
	var persistentAssetBytes uint64
	for _, asset := range manifest.Assets {
		if persistentAssetBytes > math.MaxUint64-asset.Bytes {
			return errors.New("M3 retained manifest asset bytes overflow")
		}
		persistentAssetBytes += asset.Bytes
	}
	if persistentAssetBytes > math.MaxUint64-manifest.RouterAsset.Bytes {
		return errors.New("M3 retained manifest router bytes overflow")
	}
	persistentAssetBytes += manifest.RouterAsset.Bytes
	if d.FixtureChecksum != fixture.Checksum || d.ManifestIntegrity != manifest.IntegrityDigest || d.ReadySetDigest != manifest.ReadySetDigest ||
		d.RouterAssetChecksum != manifest.RouterAsset.Checksum || d.RouterModelDigest != routerModelDigest ||
		d.SourceGeneration != manifest.SourceGeneration || d.SourceChecksum != manifest.SourceChecksum || d.SourceSchemaHash != manifest.SourceSchemaHash ||
		d.SourceRows != manifest.SourceRowCount || d.PartitionGeneration != manifest.Generation || d.RouterGeneration != manifest.RouterGeneration ||
		d.Partitions != manifest.PartitionCount || d.RouterRepresentatives != uint64(len(manifest.Representatives)) || d.IndexDefinitionDigest != manifest.IndexDefinitionDigest || d.OverlapPolicy != manifest.BalancePolicy || d.OverlapMemberships != len(manifest.OverlapMemberships) ||
		d.PersistentAssetBytes != persistentAssetBytes || d.RouterConfig != routerConfig {
		return errors.New("M3 variant descriptor does not bind the retained ready manifest")
	}
	return nil
}
