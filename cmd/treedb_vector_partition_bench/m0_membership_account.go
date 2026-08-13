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
	"path/filepath"
	"reflect"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	m0OverlapRatioV1        = .2
	m0KaHIPBackendLicenseV1 = "MIT; kahip==3.25; wheel_sha256=e6ea76524e9fc01b27e6f5c5f00b7eec71c94cbd1e84678ce2a14d64dfc9eda4; record_sha256=7ff011253147286fcebc9185573662bf31dbcfbab1944f9b4940032f49ea5217; ECO; epsilon=0.05; symmetrized_unweighted_v1"
)

type m0MembershipAccountV1 struct {
	Schema                      string               `json:"schema"`
	GraphArtifactSHA256         string               `json:"graph_artifact_sha256"`
	AssignmentArtifactSHA256    string               `json:"assignment_artifact_sha256"`
	RepartitionedArtifactSHA256 string               `json:"repartitioned_artifact_sha256"`
	Backend                     string               `json:"backend"`
	Partitions                  int                  `json:"partitions"`
	Cap                         int                  `json:"cap"`
	OverlapCapacity             int                  `json:"overlap_capacity"`
	OverlapBudget               int                  `json:"overlap_budget"`
	MaxPartitionSize            int                  `json:"max_partition_size"`
	EdgeCut                     int                  `json:"edge_cut"`
	Modes                       []m0MembershipModeV1 `json:"modes"`
}
type m0MembershipModeV1 struct {
	Name             string `json:"name"`
	Used             int    `json:"used"`
	Useful           int    `json:"useful"`
	Filler           int    `json:"filler"`
	MembershipSHA256 string `json:"membership_sha256"`
	Materialize      bool   `json:"materialize"`
	EquivalentTo     string `json:"equivalent_to,omitempty"`
	Rejected         string `json:"rejected,omitempty"`
}

func runM0MembershipAccountV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-membership-account", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var graphPath, assignmentPath, assignmentOut, out, pythonPath, scriptPath string
	partitions := 0
	fs.StringVar(&graphPath, "artifact", "", "canonical graph artifact")
	fs.StringVar(&assignmentPath, "assignment", "", "KaHIP assignment artifact")
	fs.StringVar(&assignmentOut, "assignment-out", "", "persisted canonical assignment artifact")
	fs.StringVar(&out, "out", "", "fresh JSON output")
	fs.StringVar(&pythonPath, "kahip-python", "", "pinned KaHIP Python")
	fs.StringVar(&scriptPath, "kahip-script", "", "pinned KaHIP adapter")
	fs.IntVar(&partitions, "partitions", 0, "target partition count")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || graphPath == "" || out == "" || partitions < 1 || assignmentPath != "" && (pythonPath != "" || scriptPath != "") || assignmentPath == "" && (pythonPath == "" || scriptPath == "") || assignmentPath != "" && assignmentOut != "" {
		return errors.New("m0-membership-account requires artifact, partitions, output, and either assignment or pinned KaHIP paths")
	}
	if _, err := os.Stat(out); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errors.New("M0 membership account output exists")
	}
	graphRaw, err := os.ReadFile(graphPath)
	if err != nil {
		return err
	}
	graph, err := vectorpartition.DecodeArtifact(graphRaw, len(graphRaw))
	if err != nil {
		return err
	}
	request, err := vectorpartition.RepartitionArtifact(graph, partitions, kahipRequestPartitioner{})
	if err != nil {
		return err
	}
	var candidate vectorpartition.Artifact
	var assignmentRaw []byte
	if assignmentPath != "" {
		assignmentRaw, err = os.ReadFile(assignmentPath)
		if err != nil {
			return err
		}
		candidate, err = vectorpartition.DecodeArtifact(assignmentRaw, len(assignmentRaw))
		if err != nil {
			return err
		}
	} else {
		pythonRaw, e := os.ReadFile(pythonPath)
		if e != nil {
			return e
		}
		scriptRaw, e := os.ReadFile(scriptPath)
		if e != nil {
			return e
		}
		if m0SHA256V1(pythonRaw) != m8QualificationKaHIPPythonSHA256V1 || m0SHA256V1(scriptRaw) != kahipAdapterSHA256 {
			return errors.New("pinned KaHIP identity")
		}
		requestRaw, e := vectorpartition.CanonicalJSON(request)
		if e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(context.Background(), kahipDefaultTimeout)
		defer cancel()
		candidate, e = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{pythonPath, scriptPath}, requestRaw, vectorpartition.ExternalJSONLimits{MaxInput: len(requestRaw), MaxOutput: kahipOutputCap(requestRaw, request)}, request)
		if e != nil {
			return e
		}
		assignmentRaw, e = vectorpartition.CanonicalJSON(candidate)
		if e != nil {
			return e
		}
	}
	if err := m0ValidateAssignmentArtifactV1(graph, request, candidate); err != nil {
		return err
	}
	if assignmentOut != "" {
		if err := os.MkdirAll(filepath.Dir(assignmentOut), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(assignmentOut, assignmentRaw, 0644); err != nil {
			return err
		}
	}
	digest, err := vectorpartition.Digest(candidate)
	if err != nil {
		return err
	}
	capacity, err := m3OverlapCapacityV1(candidate, m0OverlapRatioV1)
	if err != nil {
		return err
	}
	zero, err := vectorpartition.BuildOverlap(candidate, vectorpartition.OverlapConfig{})
	if err != nil {
		return err
	}
	useful, err := vectorpartition.BuildOverlap(candidate, vectorpartition.OverlapConfig{Ratio: m0OverlapRatioV1, Capacity: capacity})
	if err != nil {
		return err
	}
	exact, err := vectorpartition.BuildOverlap(candidate, vectorpartition.OverlapConfig{Ratio: m0OverlapRatioV1, Capacity: capacity, RequireExact: true})
	if err != nil {
		return err
	}
	modes, err := m0MembershipModesV1(candidate, zero, useful, exact)
	if err != nil {
		return err
	}
	report := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: m0SHA256V1(graphRaw), AssignmentArtifactSHA256: m0SHA256V1(assignmentRaw), RepartitionedArtifactSHA256: digest, Backend: candidate.Backend, Partitions: candidate.Config.Partitions, Cap: candidate.Metrics.Cap, OverlapCapacity: capacity, OverlapBudget: int(math.Floor(m0OverlapRatioV1 * float64(len(candidate.IDs)))), MaxPartitionSize: candidate.Metrics.MaxPartitionSize, EdgeCut: candidate.Metrics.EdgeCut, Modes: modes}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err = os.WriteFile(out, append(raw, '\n'), 0644); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "m0_membership_account=%s partitions=%d\n", out, report.Partitions)
	return err
}
func m0ValidateAssignmentArtifactV1(graph, request, candidate vectorpartition.Artifact) error {
	if candidate.Source != graph.Source || !reflect.DeepEqual(candidate.IDs, graph.IDs) || !reflect.DeepEqual(candidate.Graph, graph.Graph) || !reflect.DeepEqual(candidate.Config, request.Config) {
		return errors.New("assignment artifact does not bind frozen source, IDs, graph, and requested config")
	}
	if candidate.Backend != fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", request.Config.Seed) || candidate.BackendLicense != m0KaHIPBackendLicenseV1 {
		return errors.New("assignment artifact does not bind pinned KaHIP execution identity")
	}
	return nil
}

func m0MembershipModesV1(artifact vectorpartition.Artifact, zero, useful, exact vectorpartition.OverlapResult) ([]m0MembershipModeV1, error) {
	zeroDigest, err := m0MembershipDigestV1(zero.Memberships)
	if err != nil {
		return nil, err
	}
	mode := func(name string, overlap vectorpartition.OverlapResult) (m0MembershipModeV1, error) {
		digest, err := m0MembershipDigestV1(overlap.Memberships)
		if err != nil {
			return m0MembershipModeV1{}, err
		}
		return m0MembershipModeV1{Name: name, Used: overlap.Used, Useful: overlap.Useful, Filler: overlap.Filler, MembershipSHA256: digest}, nil
	}
	zeroMode, err := mode("zero", zero)
	if err != nil {
		return nil, err
	}
	zeroMode.Materialize = true
	usefulMode, err := mode("useful_only_20", useful)
	if err != nil {
		return nil, err
	}
	if artifact.Metrics.EdgeCut == 0 && (useful.Used != 0 || useful.Useful != 0 || useful.Filler != 0 || usefulMode.MembershipSHA256 != zeroDigest) {
		return nil, errors.New("zero-cut useful-only overlap differs from zero")
	}
	if usefulMode.MembershipSHA256 == zeroDigest {
		usefulMode.EquivalentTo = "zero"
	} else {
		usefulMode.Materialize = true
	}
	exactMode, err := mode("exact_20", exact)
	if err != nil {
		return nil, err
	}
	budget := int(math.Floor(m0OverlapRatioV1 * float64(len(artifact.IDs))))
	if artifact.Metrics.EdgeCut == 0 && (exact.Used != budget || exact.Useful != 0 || exact.Filler != budget) {
		return nil, errors.New("zero-cut exact-20 overlap is not filler-only at the declared budget")
	}
	if exact.Filler != 0 {
		exactMode.Rejected = "exact-20 contains filler"
	} else if exactMode.MembershipSHA256 == zeroDigest {
		exactMode.EquivalentTo = "zero"
	} else {
		exactMode.Materialize = true
	}
	return []m0MembershipModeV1{zeroMode, usefulMode, exactMode}, nil
}

func m0MembershipDigestV1(memberships []vectorpartition.Membership) (string, error) {
	raw, err := json.Marshal(memberships)
	if err != nil {
		return "", err
	}
	return m0SHA256V1(raw), nil
}
