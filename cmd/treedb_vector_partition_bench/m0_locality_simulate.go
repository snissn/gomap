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
	"slices"
	"sort"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const m0PageBytesV1 uint64 = 4096

type m0PageTokenV1 struct {
	Namespace string
	FileID    uint32
	Page      uint64
}

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
	Name                    string   `json:"name"`
	Queries                 int      `json:"queries"`
	MedianPages             uint64   `json:"median_unique_graph_vector_pages"`
	P95Pages                uint64   `json:"p95_unique_graph_vector_pages"`
	MeanAdjacencyPageAccess float64  `json:"mean_adjacency_pages_per_expanded_neighbor_list"`
	PageCounts              []uint64 `json:"-"`
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
	if err := m0ValidateCaptureSplitPairV1(calibration, holdout); err != nil {
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
	if _, err := vectorpartition.DecodeArtifact(artifactRaw, len(artifactRaw)); err != nil {
		return err
	}
	if calibration.Artifact != artifactSHA || holdout.Artifact != artifactSHA {
		return errors.New("capture graph artifact identity")
	}
	report := m0LocalitySimulationV1{Schema: "treedb_vector_partition_m0_layout_simulation_v1", Calibration: calibrationSHA, Holdout: holdoutSHA, Artifact: artifactSHA, PageScope: calibration.PageScope}
	identity, err := m0IdentityPermutationsV1(holdout)
	if err != nil {
		return err
	}
	baseline, err := m0EvaluatePermutationsV1(holdout, identity)
	if err != nil {
		return err
	}
	if baseline.MedianPages != holdout.MedianPages || baseline.P95Pages != holdout.P95Pages || len(baseline.PageCounts) != len(holdout.Rows) {
		return errors.New("identity page simulation does not reproduce capture")
	}
	for i, row := range holdout.Rows {
		if holdout.Traces[i].Query != row.Query || baseline.PageCounts[i] != row.Pages {
			return errors.New("identity page simulation per-query mismatch")
		}
	}
	baseline.Name = "identity_current"
	report.Objectives = append(report.Objectives, baseline)
	objectives := []string{"source", "bfs", "edge_window", "gorder_like", "co_visitation", "hybrid"}
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

func m0ValidateCaptureSplitPairV1(calibration, holdout m0LocalityCaptureV1) error {
	if calibration.Split == "" || calibration.Split == holdout.Split || len(calibration.Rows) == 0 || len(holdout.Rows) == 0 {
		return errors.New("capture split identity")
	}
	seen := make([]bool, len(calibration.Rows)+len(holdout.Rows))
	for kind, capture := range []m0LocalityCaptureV1{calibration, holdout} {
		for i, row := range capture.Rows {
			if row.Query < 0 || row.Query >= len(seen) || seen[row.Query] || i > 0 && capture.Rows[i-1].Query >= row.Query || localHNSWCalibrationOrdinalV1(row.Query) != (kind == 0) || len(capture.Traces) > 0 && capture.Traces[i].Query != row.Query {
				return errors.New("capture split ordinals")
			}
			seen[row.Query] = true
		}
	}
	if slices.Contains(seen, false) {
		return errors.New("capture split coverage")
	}
	return nil
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
			sequence := partitionTrace.ScoreOrdinals
			rows := capture.Snapshots[partitionTrace.Partition].Rows
			window := max(1, int(m0PageBytesV1)/(capture.Snapshots[partitionTrace.Partition].VectorStride*4))
			for i, left := range sequence {
				if int(left) >= rows {
					return nil, errors.New("trace ordinal")
				}
				for j := i + 1; j < len(sequence) && j <= i+window; j++ {
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

func m0IdentityPermutationsV1(capture m0LocalityCaptureV1) (map[uint32]m0PermutationV1, error) {
	out := make(map[uint32]m0PermutationV1, len(capture.Snapshots))
	for partition, snapshot := range capture.Snapshots {
		if err := m0ValidateSnapshotV1(snapshot); err != nil {
			return nil, err
		}
		permutation := make(m0PermutationV1, snapshot.Rows)
		for i := range permutation {
			permutation[i] = i
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
		if len(offsets) != snapshot.Rows+1 || offsets[0] != 0 || offsets[len(offsets)-1] > uint64(len(snapshot.LayerNeighbors[layer])) || len(snapshot.LayerNeighbors[layer]) != int(offsets[len(offsets)-1]) || snapshot.LayerOffsetsSectionOffsets[layer] == 0 || snapshot.LayerNeighborOffsets[layer] == 0 {
			return errors.New("layer geometry")
		}
		for i := 1; i < len(offsets); i++ {
			if offsets[i] < offsets[i-1] {
				return errors.New("non-monotone layer offsets")
			}
		}
		for _, neighbor := range snapshot.LayerNeighbors[layer] {
			if int(neighbor) >= snapshot.Rows {
				return errors.New("layer neighbor ordinal")
			}
		}
	}
	seen := make(map[uint32]struct{}, snapshot.Rows)
	for _, source := range snapshot.RowOrdinals {
		if _, duplicate := seen[source]; duplicate {
			return errors.New("duplicate source ordinal")
		}
		seen[source] = struct{}{}
	}
	if len(snapshot.AuxiliaryOffsets) != 0 && (len(snapshot.AuxiliaryOffsets) != snapshot.Rows+1 || snapshot.AuxiliaryOffsets[0] != 0 || snapshot.AuxiliaryOffsets[len(snapshot.AuxiliaryOffsets)-1] != uint64(len(snapshot.AuxiliaryNeighbors)) || snapshot.AuxiliaryOffsetsSectionOffset == 0 || snapshot.AuxiliaryNeighborOffset == 0) {
		return errors.New("auxiliary geometry")
	}
	for i := 1; i < len(snapshot.AuxiliaryOffsets); i++ {
		if snapshot.AuxiliaryOffsets[i] < snapshot.AuxiliaryOffsets[i-1] {
			return errors.New("non-monotone auxiliary offsets")
		}
	}
	for _, neighbor := range snapshot.AuxiliaryNeighbors {
		if int(neighbor) >= snapshot.Rows {
			return errors.New("auxiliary neighbor ordinal")
		}
	}
	if len(snapshot.AuxiliaryOffsets) == 0 && (len(snapshot.AuxiliaryNeighbors) != 0 || snapshot.AuxiliaryOffsetsSectionOffset != 0 || snapshot.AuxiliaryNeighborOffset != 0) {
		return errors.New("unexpected auxiliary geometry")
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
				neighbors[neighbor] = append(neighbors[neighbor], ordinal)
			}
		}
	}
	for ordinal := range neighbors {
		sort.Slice(neighbors[ordinal], func(i, j int) bool {
			return snapshot.RowOrdinals[neighbors[ordinal][i]] < snapshot.RowOrdinals[neighbors[ordinal][j]]
		})
		neighbors[ordinal] = slices.Compact(neighbors[ordinal])
	}
	if objective == "bfs" {
		return m0BFSOrderV1(snapshot, rows), nil
	}
	if objective != "edge_window" && objective != "gorder_like" && objective != "co_visitation" && objective != "hybrid" {
		return nil, errors.New("unknown objective")
	}
	return m0GreedyOrderV1(snapshot, neighbors, pairs, objective), nil
}

func m0GreedyOrderV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1, neighbors [][]int, pairs map[[2]int]uint32, objective string) []int {
	window := max(1, int(m0PageBytesV1)/(snapshot.VectorStride*4))
	placed := make([]bool, snapshot.Rows)
	pairNeighbors := make([]map[int]uint32, snapshot.Rows)
	for pair, weight := range pairs {
		if pairNeighbors[pair[0]] == nil {
			pairNeighbors[pair[0]] = map[int]uint32{}
		}
		if pairNeighbors[pair[1]] == nil {
			pairNeighbors[pair[1]] = map[int]uint32{}
		}
		pairNeighbors[pair[0]][pair[1]] = weight
		pairNeighbors[pair[1]][pair[0]] = weight
	}
	out := []int{snapshot.EntryOrdinal}
	placed[snapshot.EntryOrdinal] = true
	direct, common, pairGain, candidates := map[int]int{}, map[int]int{}, map[int]int{}, map[int]struct{}{}
	for len(out) < snapshot.Rows {
		clear(direct)
		clear(common)
		clear(pairGain)
		clear(candidates)
		begin := max(0, len(out)-window)
		for _, prior := range out[begin:] {
			for _, node := range neighbors[prior] {
				if !placed[node] {
					direct[node]++
				}
				for _, twoHop := range neighbors[node] {
					if !placed[twoHop] {
						common[twoHop]++
					}
				}
			}
			for node, weight := range pairNeighbors[prior] {
				if !placed[node] {
					pairGain[node] += int(weight)
				}
			}
		}
		if objective == "edge_window" || objective == "gorder_like" || objective == "hybrid" {
			for node := range direct {
				candidates[node] = struct{}{}
			}
		}
		if objective == "gorder_like" || objective == "hybrid" {
			for node := range common {
				candidates[node] = struct{}{}
			}
		}
		if objective == "co_visitation" || objective == "hybrid" {
			for node := range pairGain {
				candidates[node] = struct{}{}
			}
		}
		best, bestGain := -1, -1
		for node := range candidates {
			gain := direct[node]
			if objective == "gorder_like" {
				gain += common[node]
			}
			if objective == "co_visitation" {
				gain = pairGain[node]
			}
			if objective == "hybrid" {
				gain += common[node] + 2*pairGain[node]
			}
			if gain > bestGain || gain == bestGain && (best < 0 || snapshot.RowOrdinals[node] < snapshot.RowOrdinals[best]) {
				best, bestGain = node, gain
			}
		}
		if best < 0 {
			for node := range placed {
				if !placed[node] && (best < 0 || snapshot.RowOrdinals[node] < snapshot.RowOrdinals[best]) {
					best = node
				}
			}
		}
		out = append(out, best)
		placed[best] = true
	}
	return out
}

func m0BFSOrderV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1, fallback []int) []int {
	entry := snapshot.EntryOrdinal
	neighbors := make([][]int, snapshot.Rows)
	for layer := len(snapshot.LayerOffsets) - 1; layer >= 0; layer-- {
		for row := 0; row < snapshot.Rows; row++ {
			for _, next := range snapshot.LayerNeighbors[layer][snapshot.LayerOffsets[layer][row]:snapshot.LayerOffsets[layer][row+1]] {
				if int(next) < snapshot.Rows {
					neighbors[row] = append(neighbors[row], int(next))
				}
			}
		}
	}
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
	startsByPartition := make(map[uint32][][]uint64, len(capture.Snapshots))
	for partition, snapshot := range capture.Snapshots {
		permutation, ok := permutations[partition]
		if !ok || len(permutation) != snapshot.Rows {
			return m0LocalityObjectiveV1{}, errors.New("permutation")
		}
		startsByPartition[partition] = m0CSRStartsV1(snapshot, permutation)
	}
	var accesses, expanded uint64
	for _, trace := range capture.Traces {
		tokens := map[m0PageTokenV1]struct{}{}
		for _, partitionTrace := range trace.Partitions {
			snapshot, ok := capture.Snapshots[partitionTrace.Partition]
			if !ok {
				return m0LocalityObjectiveV1{}, errors.New("holdout snapshot")
			}
			permutation, ok := permutations[partitionTrace.Partition]
			if !ok || len(permutation) != snapshot.Rows {
				return m0LocalityObjectiveV1{}, errors.New("permutation")
			}
			starts := startsByPartition[partitionTrace.Partition]
			for _, ordinal := range partitionTrace.ScoreOrdinals {
				if int(ordinal) >= snapshot.Rows {
					return m0LocalityObjectiveV1{}, errors.New("score ordinal")
				}
				if err := m0AddRangeV1(tokens, snapshot, snapshot.VectorOffset+uint64(permutation[ordinal])*uint64(snapshot.VectorStride)*4, uint64(snapshot.VectorStride)*4); err != nil {
					return m0LocalityObjectiveV1{}, err
				}
			}
			for _, read := range partitionTrace.AdjacencyReads {
				local := map[m0PageTokenV1]struct{}{}
				if err := m0AddAdjacencyV1(tokens, local, snapshot, permutation, starts, read); err != nil {
					return m0LocalityObjectiveV1{}, err
				}
				accesses += uint64(len(local))
				expanded++
			}
		}
		pages = append(pages, uint64(len(tokens)))
	}
	pageCounts := append([]uint64(nil), pages...)
	sort.Slice(pages, func(i, j int) bool { return pages[i] < pages[j] })
	result := m0LocalityObjectiveV1{Queries: len(pages), PageCounts: pageCounts}
	if len(pages) > 0 {
		result.MedianPages = pages[len(pages)/2]
		result.P95Pages = pages[(len(pages)*95+99)/100-1]
	}
	if expanded > 0 {
		result.MeanAdjacencyPageAccess = float64(accesses) / float64(expanded)
	}
	return result, nil
}

func m0CSRStartsV1(snapshot collections.VectorPartitionPackLayoutSnapshotV1, permutation m0PermutationV1) [][]uint64 {
	layers := append([][]uint64(nil), snapshot.LayerOffsets...)
	if len(snapshot.AuxiliaryOffsets) > 0 {
		layers = append(layers, snapshot.AuxiliaryOffsets)
	}
	out := make([][]uint64, len(layers))
	for layer, offsets := range layers {
		starts := make([]uint64, snapshot.Rows)
		order := make([]int, snapshot.Rows)
		for old, next := range permutation {
			order[next] = old
		}
		var at uint64
		for _, old := range order {
			starts[old] = at
			at += offsets[old+1] - offsets[old]
		}
		out[layer] = starts
	}
	return out
}

func m0AddAdjacencyV1(all, local map[m0PageTokenV1]struct{}, snapshot collections.VectorPartitionPackLayoutSnapshotV1, permutation m0PermutationV1, starts [][]uint64, read collections.VectorPartitionSearchPageReadV1) error {
	if read.Ordinal < 0 || read.Ordinal >= snapshot.Rows || len(permutation) != snapshot.Rows {
		return errors.New("adjacency ordinal")
	}
	layer, offsets, offsetsSection, neighborSection := read.Layer, snapshot.LayerOffsets, snapshot.LayerOffsetsSectionOffsets, snapshot.LayerNeighborOffsets
	if read.Auxiliary {
		layer = 0
		offsets = [][]uint64{snapshot.AuxiliaryOffsets}
		offsetsSection = []uint64{snapshot.AuxiliaryOffsetsSectionOffset}
		neighborSection = []uint64{snapshot.AuxiliaryNeighborOffset}
	}
	if layer < 0 || layer >= len(offsets) || len(offsets[layer]) != snapshot.Rows+1 || layer >= len(offsetsSection) || layer >= len(neighborSection) || offsetsSection[layer] == 0 || neighborSection[layer] == 0 {
		return errors.New("adjacency layer")
	}
	old, next := read.Ordinal, permutation[read.Ordinal]
	start, end := offsets[layer][old], offsets[layer][old+1]
	if end < start {
		return errors.New("adjacency offsets")
	}
	startLayer := layer
	if read.Auxiliary {
		startLayer = len(snapshot.LayerOffsets)
	}
	if startLayer < 0 || startLayer >= len(starts) || len(starts[startLayer]) != snapshot.Rows {
		return errors.New("adjacency starts")
	}
	newStart := starts[startLayer][old]
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

func m0AddRangeV1(tokens map[m0PageTokenV1]struct{}, snapshot collections.VectorPartitionPackLayoutSnapshotV1, offset, length uint64) error {
	if length == 0 {
		return nil
	}
	if offset > ^uint64(0)-length || snapshot.BaseOffset > ^uint64(0)-offset {
		return errors.New("page range")
	}
	physical := snapshot.BaseOffset + offset
	for page := physical / m0PageBytesV1; page <= (physical+length-1)/m0PageBytesV1; page++ {
		tokens[m0PageTokenV1{Namespace: snapshot.Namespace, FileID: snapshot.FileID, Page: page}] = struct{}{}
		if page == ^uint64(0) {
			break
		}
	}
	return nil
}
