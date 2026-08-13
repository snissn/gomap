package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

type m0AssignmentPartitionerV1 struct {
	assignment    []int
	name, license string
}

func (p m0AssignmentPartitionerV1) Name() string    { return p.name }
func (p m0AssignmentPartitionerV1) License() string { return p.license }
func (p m0AssignmentPartitionerV1) Partition(_ vectorpartition.Graph, _ int, _ int) ([]int, error) {
	return append([]int(nil), p.assignment...), nil
}

type m0MembershipAccountV1 struct {
	Schema, GraphArtifactSHA256, AssignmentSHA256, RepartitionedArtifactSHA256, Backend string
	Partitions                                                                          int
	Cap                                                                                 int `json:"cap"`
	MaxPartitionSize                                                                    int `json:"max_partition_size"`
	EdgeCut                                                                             int `json:"edge_cut"`
	Modes                                                                               []m0MembershipModeV1
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
	var graphPath, assignmentPath, out, pythonPath, scriptPath string
	fs.StringVar(&graphPath, "artifact", "", "canonical graph artifact")
	fs.StringVar(&assignmentPath, "assignment", "", "KaHIP assignment artifact")
	fs.StringVar(&out, "out", "", "fresh JSON output")
	fs.StringVar(&pythonPath, "kahip-python", "", "pinned KaHIP Python")
	fs.StringVar(&scriptPath, "kahip-script", "", "pinned KaHIP adapter")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || graphPath == "" || out == "" || assignmentPath != "" && (pythonPath != "" || scriptPath != "") || assignmentPath == "" && (pythonPath == "" || scriptPath == "") {
		return errors.New("m0-membership-account requires artifact, assignment, output")
	}
	graphRaw, err := os.ReadFile(graphPath)
	if err != nil {
		return err
	}
	graph, err := vectorpartition.DecodeArtifact(graphRaw, len(graphRaw))
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
		request, e := vectorpartition.RepartitionArtifact(graph, 32, kahipRequestPartitioner{})
		if e != nil {
			return e
		}
		requestRaw, e := vectorpartition.CanonicalJSON(request)
		if e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(context.Background(), kahipDefaultTimeout)
		defer cancel()
		candidate, e = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{pythonPath, "-c", string(scriptRaw)}, requestRaw, vectorpartition.ExternalJSONLimits{MaxInput: len(requestRaw), MaxOutput: kahipOutputCap(requestRaw, request)}, request)
		if e != nil {
			return e
		}
		assignmentRaw, e = vectorpartition.CanonicalJSON(candidate)
		if e != nil {
			return e
		}
	}
	if candidate.Source != graph.Source || len(candidate.Assignment) != len(graph.Assignment) || candidate.Config.Partitions < 1 {
		return errors.New("assignment graph identity")
	}
	artifact, err := vectorpartition.RepartitionArtifact(graph, candidate.Config.Partitions, m0AssignmentPartitionerV1{assignment: candidate.Assignment, name: candidate.Backend, license: candidate.BackendLicense})
	if err != nil {
		return err
	}
	digest, err := vectorpartition.Digest(artifact)
	if err != nil {
		return err
	}
	report := m0MembershipAccountV1{Schema: "treedb_vector_partition_m0_membership_account_v1", GraphArtifactSHA256: m0SHA256V1(graphRaw), AssignmentSHA256: m0SHA256V1(assignmentRaw), RepartitionedArtifactSHA256: digest, Backend: artifact.Backend, Partitions: artifact.Config.Partitions, Cap: artifact.Metrics.Cap, MaxPartitionSize: artifact.Metrics.MaxPartitionSize, EdgeCut: artifact.Metrics.EdgeCut}
	zero, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{})
	if err != nil {
		return err
	}
	zeroDigest, err := m0OverlapDigestV1(zero)
	if err != nil {
		return err
	}
	report.Modes = append(report.Modes, m0MembershipModeV1{Name: "zero", Used: zero.Used, Useful: zero.Useful, Filler: zero.Filler, MembershipSHA256: zeroDigest, Materialize: true})
	for _, exact := range []bool{false, true} {
		name := "useful_only_20"
		if exact {
			name = "exact_20"
		}
		overlap, e := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: .2, Capacity: artifact.Metrics.Cap, RequireExact: exact})
		mode := m0MembershipModeV1{Name: name}
		if e != nil {
			mode.Rejected = e.Error()
		} else {
			mode.Used, mode.Useful, mode.Filler = overlap.Used, overlap.Useful, overlap.Filler
			mode.MembershipSHA256, _ = m0OverlapDigestV1(overlap)
			if exact && overlap.Filler != 0 {
				mode.Rejected = "exact-20 contains filler"
			} else if mode.MembershipSHA256 == zeroDigest {
				mode.EquivalentTo = "zero"
			} else {
				mode.Materialize = true
			}
		}
		report.Modes = append(report.Modes, mode)
	}
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
func m0OverlapDigestV1(overlap vectorpartition.OverlapResult) (string, error) {
	raw, err := json.Marshal(overlap)
	if err != nil {
		return "", err
	}
	return m0SHA256V1(raw), nil
}
