package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
)

// m0-locality-simulate evaluates candidate row permutations against the exact
// read trace captured by m0-locality-capture. It does not mutate a pack: only
// row offsets are remapped, with the fixed pack section geometry retained.
type m0LocalitySimulationV1 struct {
	Schema      string                  `json:"schema"`
	Calibration string                  `json:"calibration_sha256"`
	Holdout     string                  `json:"holdout_sha256"`
	Artifact    string                  `json:"graph_artifact_sha256"`
	PageScope   string                  `json:"page_scope"`
	Objectives  []m0LocalityObjectiveV1 `json:"objectives"`
}

type m0LocalityObjectiveV1 struct {
	Name                    string  `json:"name"`
	Queries                 int     `json:"queries"`
	MedianPages             uint64  `json:"median_unique_graph_vector_pages"`
	P95Pages                uint64  `json:"p95_unique_graph_vector_pages"`
	MeanAdjacencyPageAccess float64 `json:"mean_adjacency_pages_per_expanded_neighbor_list"`
}

func runM0LocalitySimulateV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench m0-locality-simulate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var calibrationPath, holdoutPath, artifactPath, out string
	fs.StringVar(&calibrationPath, "calibration", "", "raw calibration capture")
	fs.StringVar(&holdoutPath, "holdout", "", "raw holdout capture")
	fs.StringVar(&artifactPath, "artifact", "", "frozen vectorpartition graph artifact")
	fs.StringVar(&out, "out", "", "fresh JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || calibrationPath == "" || holdoutPath == "" || artifactPath == "" || out == "" {
		return errors.New("m0-locality-simulate requires raw calibration, holdout, graph artifact, and output")
	}
	calibration, calibrationSHA, err := m0ReadCaptureV1(calibrationPath)
	if err != nil {
		return err
	}
	holdout, holdoutSHA, err := m0ReadCaptureV1(holdoutPath)
	if err != nil {
		return err
	}
	if calibration.PageScope != holdout.PageScope || calibration.Probes != holdout.Probes || calibration.RouterCandidates != holdout.RouterCandidates || calibration.EF != holdout.EF {
		return errors.New("calibration and holdout capture settings differ")
	}
	artifactRaw, err := os.ReadFile(artifactPath)
	if err != nil {
		return err
	}
	artifactSHA := m0SHA256V1(artifactRaw)
	objectives := []string{"source", "bfs", "edge_window", "gorder_like", "co_visitation", "hybrid"}
	report := m0LocalitySimulationV1{Schema: "treedb_vector_partition_m0_layout_simulation_v1", Calibration: calibrationSHA, Holdout: holdoutSHA, Artifact: artifactSHA, PageScope: calibration.PageScope}
	for _, objective := range objectives {
		permutations, err := m0BuildPermutationsV1(calibration, objective)
		if err != nil {
			return fmt.Errorf("%s: %w", objective, err)
		}
		result, err := m0EvaluatePermutationsV1(holdout, permutations)
		if err != nil {
			return fmt.Errorf("%s: %w", objective, err)
		}
		result.Name = objective
		report.Objectives = append(report.Objectives, result)
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "m0_locality_simulation=%s objectives=%d\n", out, len(report.Objectives))
	return err
}

func m0ReadCaptureV1(path string) (m0LocalityCaptureV1, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return m0LocalityCaptureV1{}, "", err
	}
	var capture m0LocalityCaptureV1
	if err := json.Unmarshal(raw, &capture); err != nil {
		return m0LocalityCaptureV1{}, "", err
	}
	if capture.Schema != "treedb_vector_partition_m0_exact_pack_trace_v1" || len(capture.Traces) == 0 || len(capture.Snapshots) == 0 || len(capture.Traces) != len(capture.Rows) {
		return m0LocalityCaptureV1{}, "", errors.New("raw capture schema or trace payload")
	}
	return capture, m0SHA256V1(raw), nil
}

func m0SHA256V1(raw []byte) string {
	// The capture itself records all input identity. Its content digest makes
	// the offline reduction immutable without introducing another format.
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

type m0PermutationV1 []int // old ordinal -> new ordinal

func m0BuildPermutationsV1(capture m0LocalityCaptureV1, objective string) (map[uint32]m0PermutationV1, error) {
	pairs := make(map[uint32]map[[2]int]uint32, len(capture.Snapshots))
	for partition, snapshot := range capture.Snapshots {
		if err := m0ValidateSnapshotV1(snapshot); err != nil {
			return nil, err
		}
		pairs[partition] = make(map[[2]int]uint32)
	}
	for _, trace := range capture.Traces {
		for _, partitionTrace := range trace.Partitions {
			weights, ok := pairs[partitionTrace.Partition]
			if !ok {
				return nil, errors.New("trace partition snapshot")
			}
			sequence := append([]uint32(nil), partitionTrace.ScoreOrdinals...)
			for _, read := range partitionTrace.AdjacencyReads {
				sequence = append(sequence, uint32(read.Ordinal))
			}
			rows := capture.Snapshots[partitionTrace.Partition].Rows
			for i, left := range sequence {
				if int(left) >= rows {
					return nil, errors.New("trace ordinal")
				}
				for j := i + 1; j < len(sequence) && j <= i+8; j++ {
					right := sequence[j]
					if int(right) >= rows || left == right {
						continue
					}
					a, b := int(left), int(right)
					if a > b {
						a, b = b, a
					}
					weights[[2]int{a, b}]++
				}
			}
		}
	}
	out := make(map[uint32]m0PermutationV1, len(capture.Snapshots))
	for partition, snapshot := range capture.Snapshots {
		order, err := m0ObjectiveOrderV1(snapshot, pairs[partition], objective)
		if err != nil {
			return nil, err
		}
		permutation := make(m0PermutationV1, snapshot.Rows)
		for newOrdinal, oldOrdinal := range order {
			permutation[oldOrdinal] = newOrdinal
		}
		out[partition] = permutation
	}
	return out, nil
}

func m0ValidateSnapshotV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1) error {
	if snapshot.Rows < 1 || snapshot.EntryOrdinal < 0 || snapshot.EntryOrdinal >= snapshot.Rows || len(snapshot.RowOrdinals) != snapshot.Rows || snapshot.VectorStride < 1 || snapshot.VectorOffset == 0 || len(snapshot.LayerOffsets) == 0 || len(snapshot.LayerOffsets) != len(snapshot.LayerNeighbors) || len(snapshot.LayerOffsets) != len(snapshot.LayerOffsetsSectionOffsets) || len(snapshot.LayerOffsets) != len(snapshot.LayerNeighborOffsets) {
		return errors.New("snapshot geometry")
	}
	for layer, offsets := range snapshot.LayerOffsets {
		if len(offsets) != snapshot.Rows+1 || len(snapshot.LayerNeighbors[layer]) != int(offsets[len(offsets)-1]) || snapshot.LayerOffsetsSectionOffsets[layer] == 0 || snapshot.LayerNeighborOffsets[layer] == 0 || offsets[0] != 0 {
			return errors.New("layer geometry")
		}
	}
	return nil
}

func m0ObjectiveOrderV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1, pairs map[[2]int]uint32, objective string) ([]int, error) {
	rows := make([]int, snapshot.Rows)
	bySource := func(i, j int) bool { return snapshot.RowOrdinals[rows[i]] < snapshot.RowOrdinals[rows[j]] }
	for i := range rows {
		rows[i] = i
	}
	sort.Slice(rows, bySource)
	if objective == "source" {
		return rows, nil
	}
	neighbors := make([][]int, snapshot.Rows)
	for ordinal := range neighbors {
		start, end := snapshot.LayerOffsets[0][ordinal], snapshot.LayerOffsets[0][ordinal+1]
		for _, neighbor := range snapshot.LayerNeighbors[0][start:end] {
			if int(neighbor) < snapshot.Rows && int(neighbor) != ordinal {
				neighbors[ordinal] = append(neighbors[ordinal], int(neighbor))
			}
		}
		sort.Slice(neighbors[ordinal], func(i, j int) bool {
			return snapshot.RowOrdinals[neighbors[ordinal][i]] < snapshot.RowOrdinals[neighbors[ordinal][j]]
		})
	}
	if objective == "bfs" {
		return m0BFSOrderV1(snapshot.EntryOrdinal, neighbors, rows), nil
	}
	return m0GreedyOrderV1(snapshot, neighbors, pairs, objective), nil
}

func m0GreedyOrderV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1, neighbors [][]int, pairs map[[2]int]uint32, objective string) []int {
	placed := make([]bool, snapshot.Rows)
	out := make([]int, 0, snapshot.Rows)
	out = append(out, snapshot.EntryOrdinal)
	placed[snapshot.EntryOrdinal] = true
	for len(out) < snapshot.Rows {
		best, bestGain := -1, -1
		for node := range placed {
			if placed[node] {
				continue
			}
			gain := 0
			for _, neighbor := range neighbors[node] {
				if placed[neighbor] {
					gain++
				}
				if objective == "gorder_like" {
					for _, twoHop := range neighbors[neighbor] {
						if placed[twoHop] {
							gain++
						}
					}
				}
			}
			if objective == "co_visitation" || objective == "hybrid" {
				for prior := range placed {
					if !placed[prior] {
						continue
					}
					a, b := node, prior
					if a > b {
						a, b = b, a
					}
					weight := int(pairs[[2]int{a, b}])
					if objective == "co_visitation" {
						gain += weight
					} else {
						gain += 2 * weight
					}
				}
			}
			if gain > bestGain || gain == bestGain && (best < 0 || snapshot.RowOrdinals[node] < snapshot.RowOrdinals[best]) {
				best, bestGain = node, gain
			}
		}
		out = append(out, best)
		placed[best] = true
	}
	return out
}

func m0BFSOrderV1(entry int, neighbors [][]int, fallback []int) []int {
	seen := make([]bool, len(neighbors))
	order := make([]int, 0, len(neighbors))
	queue := []int{entry}
	seen[entry] = true
	for len(queue) != 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)
		for _, next := range neighbors[node] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	for _, node := range fallback {
		if !seen[node] {
			order = append(order, node)
		}
	}
	return order
}

func m0EvaluatePermutationsV1(capture m0LocalityCaptureV1, permutations map[uint32]m0PermutationV1) (m0LocalityObjectiveV1, error) {
	pages := make([]uint64, 0, len(capture.Traces))
	var accesses, expanded uint64
	for _, trace := range capture.Traces {
		tokens := map[string]struct{}{}
		for _, partitionTrace := range trace.Partitions {
			snapshot, ok := capture.Snapshots[partitionTrace.Partition]
			if !ok {
				return m0LocalityObjectiveV1{}, errors.New("holdout snapshot")
			}
			permutation, ok := permutations[partitionTrace.Partition]
			if !ok || len(permutation) != snapshot.Rows {
				return m0LocalityObjectiveV1{}, errors.New("permutation")
			}
			for _, ordinal := range partitionTrace.ScoreOrdinals {
				if err := m0AddRangeV1(tokens, snapshot, snapshot.VectorOffset+uint64(permutation[ordinal])*uint64(snapshot.VectorStride)*4, uint64(snapshot.VectorStride)*4); err != nil {
					return m0LocalityObjectiveV1{}, err
				}
			}
			for _, read := range partitionTrace.AdjacencyReads {
				local := map[string]struct{}{}
				if err := m0AddAdjacencyV1(tokens, local, snapshot, permutation, read); err != nil {
					return m0LocalityObjectiveV1{}, err
				}
				accesses += uint64(len(local))
				expanded++
			}
		}
		pages = append(pages, uint64(len(tokens)))
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i] < pages[j] })
	result := m0LocalityObjectiveV1{Queries: len(pages)}
	if len(pages) > 0 {
		result.MedianPages = pages[len(pages)/2]
		result.P95Pages = pages[(len(pages)*95+99)/100-1]
	}
	if expanded > 0 {
		result.MeanAdjacencyPageAccess = float64(accesses) / float64(expanded)
	}
	return result, nil
}

func m0AddAdjacencyV1(all, local map[string]struct{}, snapshot collections.VectorPartitionPackLayoutSnapshotV1, permutation m0PermutationV1, read collections.VectorPartitionSearchPageReadV1) error {
	if read.Ordinal < 0 || read.Ordinal >= snapshot.Rows {
		return errors.New("adjacency ordinal")
	}
	layer, offsets, offsetsSection, neighborSection := read.Layer, snapshot.LayerOffsets, snapshot.LayerOffsetsSectionOffsets, snapshot.LayerNeighborOffsets
	if read.Auxiliary {
		layer = 0
		offsets = [][]uint64{snapshot.AuxiliaryOffsets}
		offsetsSection = []uint64{snapshot.AuxiliaryOffsetsSectionOffset}
		neighborSection = []uint64{snapshot.AuxiliaryNeighborOffset}
	}
	if layer < 0 || layer >= len(offsets) || len(offsets[layer]) != snapshot.Rows+1 {
		return errors.New("adjacency layer")
	}
	old, next := read.Ordinal, permutation[read.Ordinal]
	start, end := offsets[layer][old], offsets[layer][old+1]
	if end < start {
		return errors.New("adjacency offsets")
	}
	var newStart uint64
	for ordinal, destination := range permutation {
		if destination < next {
			newStart += offsets[layer][ordinal+1] - offsets[layer][ordinal]
		}
	}
	if err := m0AddRangeV1(all, snapshot, offsetsSection[layer]+uint64(next)*8, 16); err != nil {
		return err
	}
	if err := m0AddRangeV1(all, snapshot, neighborSection[layer]+newStart*4, (end-start)*4); err != nil {
		return err
	}
	if err := m0AddRangeV1(local, snapshot, offsetsSection[layer]+uint64(next)*8, 16); err != nil {
		return err
	}
	return m0AddRangeV1(local, snapshot, neighborSection[layer]+newStart*4, (end-start)*4)
}

func m0AddRangeV1(tokens map[string]struct{}, snapshot collections.VectorPartitionPackLayoutSnapshotV1, offset, length uint64) error {
	if length == 0 || offset > ^uint64(0)-length || snapshot.BaseOffset > ^uint64(0)-offset {
		return errors.New("page range")
	}
	physical := snapshot.BaseOffset + offset
	for page := physical / 4096; page <= (physical+length-1)/4096; page++ {
		tokens[fmt.Sprintf("%s/%d/%d", snapshot.Namespace, snapshot.FileID, page)] = struct{}{}
		if page == ^uint64(0) {
			break
		}
	}
	return nil
}
