package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const m0LayoutPlanSchemaV1 = "treedb_vector_partition_m2_layout_plan_v1"

// m0LayoutPlanV1 is an offline, explicit rebuild input. Its order is keyed by
// the frozen graph artifact, never by an ambient query or serving input.
type m0LayoutPlanV1 struct {
	Schema, Objective, CalibrationSHA256, GraphArtifactSHA256, TopologyDigest, ArtifactSHA256 string
	PageBytes                                                                                 int
	Partitions                                                                                []m0LayoutPlanPartitionV1
}

type m0LayoutPlanPartitionV1 struct {
	Partition uint32                  `json:"partition"`
	Order     []m0LayoutPlanOrdinalV1 `json:"order"`
}

type m0LayoutPlanOrdinalV1 struct {
	SourceOrdinal uint32 `json:"source_ordinal"`
	DocumentID    string `json:"document_id"`
}

func runM0LayoutPlanV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-layout-plan", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var calibrationPath, artifactPath, out string
	fs.StringVar(&calibrationPath, "calibration", "", "raw frozen calibration capture")
	fs.StringVar(&artifactPath, "graph-artifact", "", "frozen vectorpartition graph artifact")
	fs.StringVar(&out, "out", "", "fresh canonical layout plan output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || calibrationPath == "" || artifactPath == "" || out == "" {
		return errors.New("m0-layout-plan requires calibration, graph-artifact, and output")
	}
	capture, captureSHA, err := m0ReadCaptureV1(calibrationPath)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(artifactPath)
	if err != nil {
		return err
	}
	plan, err := m0BuildLayoutPlanV1(capture, captureSHA, raw)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, plan); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "m0_layout_plan=%s partitions=%d sha256=%s\n", out, len(plan.Partitions), plan.ArtifactSHA256)
	return err
}

func m0BuildLayoutPlanV1(capture m0LocalityCaptureV1, captureSHA string, artifactRaw []byte) (m0LayoutPlanV1, error) {
	if captureSHA == "" || capture.Schema != "treedb_vector_partition_m0_exact_pack_trace_v3" || capture.Manifest == "" {
		return m0LayoutPlanV1{}, errors.New("layout plan capture")
	}
	artifactSHA := m0SHA256V1(artifactRaw)
	artifact, err := vectorpartition.DecodeArtifact(artifactRaw, len(artifactRaw))
	if err != nil {
		return m0LayoutPlanV1{}, err
	}
	if capture.Artifact != artifactSHA || capture.Source != artifact.Source || len(artifact.IDs) != artifact.Source.Vectors {
		return m0LayoutPlanV1{}, errors.New("layout plan graph artifact identity")
	}
	permutations, err := m0BuildPermutationsV1(capture, "co_visitation")
	if err != nil {
		return m0LayoutPlanV1{}, err
	}
	plan := m0LayoutPlanV1{Schema: m0LayoutPlanSchemaV1, Objective: "co_visitation", CalibrationSHA256: captureSHA, GraphArtifactSHA256: artifactSHA, TopologyDigest: capture.Manifest, PageBytes: int(m0PageBytesV1)}
	for partition, snapshot := range capture.Snapshots {
		permutation, ok := permutations[partition]
		if !ok || len(permutation) != snapshot.Rows {
			return m0LayoutPlanV1{}, errors.New("layout plan partition permutation")
		}
		order := make([]m0LayoutPlanOrdinalV1, snapshot.Rows)
		seen := make([]bool, snapshot.Rows)
		for old, next := range permutation {
			if next < 0 || next >= len(order) || seen[next] || int(snapshot.RowOrdinals[old]) >= len(artifact.IDs) {
				return m0LayoutPlanV1{}, errors.New("layout plan permutation coverage")
			}
			seen[next] = true
			ordinal := snapshot.RowOrdinals[old]
			order[next] = m0LayoutPlanOrdinalV1{SourceOrdinal: ordinal, DocumentID: artifact.IDs[ordinal]}
		}
		plan.Partitions = append(plan.Partitions, m0LayoutPlanPartitionV1{Partition: partition, Order: order})
	}
	sort.Slice(plan.Partitions, func(i, j int) bool { return plan.Partitions[i].Partition < plan.Partitions[j].Partition })
	for i := range plan.Partitions {
		if i > 0 && plan.Partitions[i-1].Partition == plan.Partitions[i].Partition {
			return m0LayoutPlanV1{}, errors.New("layout plan duplicate partition")
		}
	}
	digest, err := m0LayoutPlanDigestV1(plan)
	if err != nil {
		return m0LayoutPlanV1{}, err
	}
	plan.ArtifactSHA256 = digest
	return plan, nil
}

func m0LayoutPlanDigestV1(plan m0LayoutPlanV1) (string, error) {
	plan.ArtifactSHA256 = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	return m0SHA256V1(raw), nil
}

func m0ReadLayoutPlanV1(path string) (m0LayoutPlanV1, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return m0LayoutPlanV1{}, err
	}
	var plan m0LayoutPlanV1
	if err := json.Unmarshal(raw, &plan); err != nil {
		return m0LayoutPlanV1{}, err
	}
	if plan.Schema != m0LayoutPlanSchemaV1 || plan.Objective != "co_visitation" || plan.PageBytes != int(m0PageBytesV1) || !m8SHA256V1(plan.CalibrationSHA256) || !m8SHA256V1(plan.GraphArtifactSHA256) || !m8SHA256V1(plan.TopologyDigest) || !m8SHA256V1(plan.ArtifactSHA256) || len(plan.Partitions) == 0 {
		return m0LayoutPlanV1{}, errors.New("layout plan schema")
	}
	want, err := m0LayoutPlanDigestV1(plan)
	if err != nil || want != plan.ArtifactSHA256 {
		return m0LayoutPlanV1{}, errors.New("layout plan digest")
	}
	seenIDs := make(map[string]struct{})
	for i, partition := range plan.Partitions {
		if len(partition.Order) == 0 || i > 0 && plan.Partitions[i-1].Partition >= partition.Partition {
			return m0LayoutPlanV1{}, errors.New("layout plan partition")
		}
		for _, node := range partition.Order {
			if node.DocumentID == "" {
				return m0LayoutPlanV1{}, errors.New("layout plan document ID")
			}
			key := fmt.Sprintf("%d:%d", partition.Partition, node.SourceOrdinal)
			if _, duplicate := seenIDs[key]; duplicate {
				return m0LayoutPlanV1{}, errors.New("layout plan duplicate coverage")
			}
			seenIDs[key] = struct{}{}
		}
	}
	return plan, nil
}
