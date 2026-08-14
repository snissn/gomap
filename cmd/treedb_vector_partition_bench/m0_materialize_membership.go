package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

type m0MaterializeReportV1 struct {
	Schema                    string `json:"schema"`
	SourceDB                  string `json:"source_db"`
	CloneDB                   string `json:"clone_db"`
	AssignmentSHA256          string `json:"assignment_artifact_sha256"`
	Mode                      string `json:"mode"`
	MembershipSHA256          string `json:"membership_sha256"`
	LayoutPlanSHA256          string `json:"layout_plan_sha256,omitempty"`
	SourceOrdinalDigestBefore string `json:"source_ordinal_digest_before"`
	SourceOrdinalDigestAfter  string `json:"source_ordinal_digest_after"`
	Generation                uint64 `json:"generation"`
	ManifestIntegrity         string `json:"manifest_integrity_digest"`
	ReadySetDigest            string `json:"ready_set_digest"`
	PartitionCount            uint32 `json:"partition_count"`
	OverlapCount              int    `json:"overlap_count"`
	PackBytes                 uint64 `json:"pack_bytes"`
	CloneLogicalBytes         int64  `json:"clone_logical_bytes"`
}

func runM0MaterializeMembershipV1(args []string, stdout io.Writer) (err error) {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-materialize-membership", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var sourceDB, artifactPath, graphArtifactPath, membershipPath, layoutPath, root, out, mode string
	fs.StringVar(&sourceDB, "source-db", "", "retained source DB (read only)")
	fs.StringVar(&artifactPath, "artifact", "", "strict canonical assignment artifact")
	fs.StringVar(&graphArtifactPath, "graph-artifact", "", "frozen source graph artifact")
	fs.StringVar(&membershipPath, "membership-report", "", "M0 membership report")
	fs.StringVar(&layoutPath, "layout-plan", "", "optional canonical M2 layout plan")
	fs.StringVar(&root, "root", "", "task-local clone root")
	fs.StringVar(&out, "out", "", "materialization report")
	fs.StringVar(&mode, "mode", "zero", "membership mode: zero or useful_only_20")
	if fs.Parse(args) != nil || fs.NArg() != 0 || sourceDB == "" || artifactPath == "" || graphArtifactPath == "" || membershipPath == "" || root == "" || out == "" || (mode != "zero" && mode != "useful_only_20") {
		return errors.New("m0-materialize-membership requires source-db, artifact, graph-artifact, membership-report, root, out")
	}
	if _, statErr := os.Stat(out); statErr == nil || !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("M0 materialization output already exists")
	}
	artifactRaw, err := os.ReadFile(artifactPath)
	if err != nil {
		return err
	}
	artifact, err := vectorpartition.DecodeArtifact(artifactRaw, len(artifactRaw))
	if err != nil {
		return err
	}
	reportRaw, err := os.ReadFile(membershipPath)
	if err != nil {
		return err
	}
	var account m0MembershipAccountV1
	if err = json.Unmarshal(reportRaw, &account); err != nil {
		return err
	}
	overlap, selected, err := m0SelectedMembershipV1(artifact, artifactRaw, account, mode)
	if err != nil {
		return fmt.Errorf("M0 assignment/report binding: %w", err)
	}
	graphRaw, err := os.ReadFile(graphArtifactPath)
	if err != nil {
		return err
	}
	graph, err := vectorpartition.DecodeArtifact(graphRaw, len(graphRaw))
	if err != nil {
		return err
	}
	if err = m0AssignmentBindsFrozenGraphV1(graph, artifact, graphRaw, account); err != nil {
		return err
	}
	d, err := m3ReadVariantDescriptorV1(sourceDB)
	if err != nil {
		return err
	}
	if d.Source != artifact.Source {
		return errors.New("retained source does not match assignment artifact lineage")
	}
	var layoutPlan m0LayoutPlanV1
	if layoutPath != "" {
		layoutPlan, err = m0ReadLayoutPlanV1(layoutPath)
		if err != nil {
			return err
		}
		if layoutPlan.GraphArtifactSHA256 != m0SHA256V1(graphRaw) {
			return errors.New("layout plan graph artifact identity")
		}
	}
	if err = os.MkdirAll(root, 0755); err != nil {
		return err
	}
	clone, err := os.MkdirTemp(root, "m0-membership-")
	if err != nil {
		return err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.RemoveAll(clone)
		}
	}()
	if output, e := exec.Command("cp", "-a", "--reflink=auto", sourceDB+"/.", clone).CombinedOutput(); e != nil {
		return fmt.Errorf("reflink clone: %w: %s", e, output)
	}
	if err = backenddb.RebindDurableRootSnapshotV1(clone); err != nil {
		return err
	}
	h, err := openM8ProductionExistingAssetSetModeV1(clone, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, h.Close()) }()
	source, rows, err := h.collection.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		return err
	}
	before, err := m3SourceOrdinalDigestV1(rows)
	if err != nil || before != d.SourceOrdinalDigest || source.Generation != d.SourceGeneration || source.Checksum != d.SourceChecksum || source.SchemaHash != d.SourceSchemaHash {
		return errors.New("retained source identity")
	}
	sourceOrdinals, err := m3SourceOrdinalsByArtifactID(artifact, rows)
	if err != nil {
		return err
	}
	generation := max(h.manifest.Generation, h.manifest.RouterGeneration) + 1
	manifest, _, err := m3BuildingManifest(h.collection.Meta(), source, artifact, overlap, sourceOrdinals, generation, account.AssignmentArtifactSHA256)
	if err != nil {
		return err
	}
	var productionLayout collections.VectorPartitionLayoutPlanV1
	if layoutPath != "" {
		productionLayout, err = m0ProductionLayoutPlanV1(layoutPlan, artifact, h.manifest.IntegrityDigest)
		if err != nil {
			return err
		}
		manifest.LayoutPlanDigest = layoutPlan.ArtifactSHA256
		manifest.Canonicalize()
	}
	vectorSource, vectorRows, err := h.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil || vectorSource != source || len(vectorRows) != len(rows) {
		return errors.New("retained router source identity")
	}
	routerParts, err := m0RouterPartitionsV1(artifact, overlap, sourceOrdinals, vectorRows)
	if err != nil {
		return err
	}
	inputs := make([]collections.VectorPartitionSearchAssetV1, artifact.Config.Partitions)
	for p := range inputs {
		inputs[p] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: generation, PartitionID: uint32(p), Dimensions: len(vectorRows[0].Values)}
	}
	fileID, err := m3PartitionAssetFileID(generation)
	if err != nil {
		return err
	}
	localVariant, err := m3PartitionLocalGraphVariantV1(d.PartitionHNSWM, m3DescriptorPartitionHNSWEfCV1(d))
	if err != nil {
		return err
	}
	assets, resources, err := h.collection.MaterializeVectorPartitionLocalSearchAssetsVariantWithLayoutV1(partitionHNSWIndex, manifest, fileID, inputs, localVariant, productionLayout)
	if err != nil {
		return err
	}
	if resources != nil {
		resources.Release()
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err = h.collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		return err
	}
	routerFileID, err := m3RouterAssetFileID(generation)
	if err != nil {
		return err
	}
	_, err = h.collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, routerParts, m3RouterBuildOptionsV1(d.RouterConfig, routerFileID, uint64(manifest.PartitionCount)+1))
	if err != nil {
		return err
	}
	if err = h.db.Checkpoint(); err != nil {
		return err
	}
	if err = h.Close(); err != nil {
		return err
	}
	h, err = openM8ProductionExistingAssetSetModeV1(clone, true)
	if err != nil {
		return err
	}
	h.status = h.router.Status()
	if h.status.Manifest.State != "ready" || h.status.Manifest.Generation != generation || h.status.Manifest.PartitionCount != uint32(artifact.Config.Partitions) || h.status.Manifest.IntegrityDigest == "" || h.status.Manifest.ReadySetDigest == "" || h.status.Manifest.LayoutPlanDigest != layoutPlan.ArtifactSHA256 {
		return errors.New("materialized membership manifest is not ready after reopen")
	}
	assetStatus, err := h.collection.VectorPartitionStatusV1(partitionHNSWIndex, generation)
	if err != nil || !assetStatus.Active || !assetStatus.Ready || assetStatus.MissingAssets != 0 || assetStatus.CorruptAssets != 0 || assetStatus.StaleAssets != 0 || len(h.status.Manifest.Assets) != artifact.Config.Partitions {
		return errors.New("materialized membership asset status")
	}
	afterRowsSource, afterRows, err := h.collection.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		return err
	}
	after, err := m3SourceOrdinalDigestV1(afterRows)
	if err != nil || after != before || afterRowsSource != source {
		return errors.New("source identity changed during materialization")
	}
	var packBytes uint64
	for _, asset := range h.status.Manifest.Assets {
		packBytes += asset.Bytes
	}
	cloneBytes, err := m3DirectoryBytes(clone)
	if err != nil {
		return err
	}
	result := m0MaterializeReportV1{Schema: "treedb_vector_partition_m0_materialize_membership_v1", SourceDB: sourceDB, CloneDB: clone, AssignmentSHA256: account.AssignmentArtifactSHA256, Mode: mode, MembershipSHA256: selected.MembershipSHA256, LayoutPlanSHA256: layoutPlan.ArtifactSHA256, SourceOrdinalDigestBefore: before, SourceOrdinalDigestAfter: after, Generation: generation, ManifestIntegrity: h.status.Manifest.IntegrityDigest, ReadySetDigest: h.status.Manifest.ReadySetDigest, PartitionCount: h.status.Manifest.PartitionCount, OverlapCount: len(h.status.Manifest.OverlapMemberships), PackBytes: packBytes, CloneLogicalBytes: cloneBytes}
	if err = os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err = writeVectorPartitionSystemJSONExclusiveV1(out, result); err != nil {
		return err
	}
	succeeded = true
	_, err = fmt.Fprintf(stdout, "m0_materialized=%s clone=%s generation=%d\n", out, clone, generation)
	return err
}

func m0SelectedMembershipV1(artifact vectorpartition.Artifact, artifactRaw []byte, account m0MembershipAccountV1, requested string) (vectorpartition.OverlapResult, m0MembershipModeV1, error) {
	digest, err := vectorpartition.Digest(artifact)
	if err != nil || account.Schema != "treedb_vector_partition_m0_membership_account_v1" || artifact.Config.Partitions < 1 || account.AssignmentArtifactSHA256 != m0SHA256V1(artifactRaw) || account.RepartitionedArtifactSHA256 != digest || account.Partitions != artifact.Config.Partitions {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, errors.New("M0 membership account identity")
	}
	capacity, err := m3OverlapCapacityV1(artifact, m0OverlapRatioV1)
	if err != nil {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, err
	}
	config := vectorpartition.OverlapConfig{}
	if requested == "useful_only_20" {
		config = vectorpartition.OverlapConfig{Ratio: m0OverlapRatioV1, Capacity: capacity}
	}
	overlap, err := vectorpartition.BuildOverlap(artifact, config)
	if err != nil {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, err
	}
	var selected m0MembershipModeV1
	found := false
	for _, candidate := range account.Modes {
		if candidate.Name == requested {
			selected, found = candidate, true
			break
		}
	}
	actualSHA, err := m0MembershipDigestV1(overlap.Memberships)
	if err != nil || !found || !selected.Materialize || selected.EquivalentTo != "" || selected.Rejected != "" || selected.MembershipSHA256 != actualSHA || selected.Used != overlap.Used || selected.Useful != overlap.Useful || selected.Filler != overlap.Filler {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, errors.New("M0 membership mode disposition")
	}
	if requested == "zero" && (overlap.Used != 0 || overlap.Useful != 0 || overlap.Filler != 0) {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, errors.New("M0 zero membership")
	}
	if requested == "useful_only_20" && (overlap.Used == 0 || overlap.Useful != overlap.Used || overlap.Filler != 0) {
		return vectorpartition.OverlapResult{}, m0MembershipModeV1{}, errors.New("M0 useful-only membership")
	}
	return overlap, selected, nil
}

func m0RouterPartitionsV1(artifact vectorpartition.Artifact, overlap vectorpartition.OverlapResult, sourceOrdinals []int, rows []collections.VectorPartitionRouterSourceRowV1) ([]vectorpartition.RouterPartitionV1, error) {
	if len(sourceOrdinals) != len(artifact.IDs) || len(rows) != len(sourceOrdinals) || len(overlap.Memberships) < len(artifact.Assignment) {
		return nil, errors.New("M0 router source shape")
	}
	byOrdinal := make([]collections.VectorPartitionRouterSourceRowV1, len(rows))
	seen := make([]bool, len(rows))
	for _, row := range rows {
		if row.VectorOrdinal >= uint64(len(rows)) || seen[row.VectorOrdinal] || len(row.Values) == 0 {
			return nil, errors.New("M0 router source ordinal")
		}
		byOrdinal[row.VectorOrdinal] = row
		seen[row.VectorOrdinal] = true
	}
	parts := make([]vectorpartition.RouterPartitionV1, artifact.Config.Partitions)
	for p := range parts {
		parts[p].PartitionID = uint32(p)
	}
	for _, membership := range overlap.Memberships {
		if membership.VectorOrdinal < 0 || membership.VectorOrdinal >= len(sourceOrdinals) || membership.Partition < 0 || membership.Partition >= len(parts) || membership.Home != (artifact.Assignment[membership.VectorOrdinal] == membership.Partition) {
			return nil, errors.New("M0 router membership")
		}
		ordinal := sourceOrdinals[membership.VectorOrdinal]
		if ordinal < 0 || ordinal >= len(byOrdinal) {
			return nil, errors.New("M0 router source mapping")
		}
		row := byOrdinal[ordinal]
		kind := string(collections.VectorPartitionMembershipOverlapV1)
		if membership.Home {
			kind = string(collections.VectorPartitionMembershipHomeV1)
		}
		parts[membership.Partition].Vectors = append(parts[membership.Partition].Vectors, vectorpartition.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: row.Values, MembershipKind: kind})
	}
	return parts, nil
}
